# backend/api/routes.py
from fastapi import APIRouter, HTTPException, BackgroundTasks, Header
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Optional
import datetime
import polars as pl
import os
import re
import psutil
import psycopg2
import psycopg2.extras
import uuid
from pypinyin import pinyin, Style
from core.data_manager import data_manager
from core.engine import selection_engine
from core.indicator_registry import nl_meta as build_nl_meta
from core.backtest_engine import BacktestEngine, TradingCalendar, BacktestCancelled
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, ExecutionConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator, top_n_equal_weight_allocator
from core.fee_config import load_fee_schedule
from core.strategy import StrategyDefinition, UniverseDefinition, SignalDefinition, PositionSizingDefinition, RebalanceDefinition
from core.backtest_config import BacktestConfig
from core.universe import UniverseFilter
from core.universe_resolver import UniverseResolver
from core.corporate_actions import CorporateActionStore
import logging
import io # New import
import threading

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1")

# 正则用于提取公式中的指标 (从 engine 指标注册表派生，确保与支持指标同步)
# 使用 metric_pattern_mtf 容忍可选的 W./M./D. 前缀
METRIC_REGEX = selection_engine.metric_pattern_mtf


class SelectionRequest(BaseModel):
    formula: str
    timeframe: str = "D"
    date: Optional[datetime.date] = None  # 可选目标交易日（YYYY-MM-DD）；早于数据起点时回退语义见 engine


class BacktestRequest(BaseModel):
    # Legacy flat form remains supported; strategy is the preferred contract.
    formula: Optional[str] = None
    start_date: datetime.date
    end_signal_date: datetime.date
    initial_cash: float = 1_000_000
    strategy: Optional[dict] = None
    top_n: int = 20
    rebalance_freq: str = "daily"
    universe_type: str = "all_a"
    index_id: Optional[str] = None
    min_listing_days: int = 0
    exclude_st: bool = False
    historical_fees: bool = True


class BenchmarkRequest(BaseModel):
    benchmark: str = "B1"  # B1, B2, B3, B4


class CancelBacktestRequest(BaseModel):
    job_id: str
    reason: str = "preempted_by_selection"


def report_metrics_usage(formula: str):
    """
    后台任务：上报指标计数
    策略：全周期统一 Key (如 MA_CLOSE_20)，不带后缀
    """
    if not data_manager.postgres_url: return
    
    matches = METRIC_REGEX.findall(formula)
    if not matches: return

    try:
        conn = psycopg2.connect(data_manager.postgres_url)
        cur = conn.cursor()
        for func, field, param in matches:
            # 统一 Key 格式: MA_CLOSE_20
            metric_key = f"{func.upper()}_{field.upper()}_{param}"
            
            # UPSERT
            cur.execute("""
                INSERT INTO metrics_stats (metric_key, usage_count, last_used)
                VALUES (%s, 1, CURRENT_TIMESTAMP)
                ON CONFLICT (metric_key) 
                DO UPDATE SET usage_count = metrics_stats.usage_count + 1, last_used = CURRENT_TIMESTAMP;
            """, (metric_key,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"DB Report Error: {e}")


@router.post("/select")
async def select_stocks(req: SelectionRequest, background_tasks: BackgroundTasks):
    if data_manager.df_daily is None:
        raise HTTPException(status_code=503, detail="Nodes are loading data...")

    # 在线程池中运行同步选股计算，避免阻塞事件循环
    import asyncio
    result = await asyncio.to_thread(
        selection_engine.execute_selector,
        req.formula,
        req.timeframe,
        background_tasks,
        req.date
    )

    if isinstance(result, dict) and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    # 上报热度 (不再需要传 timeframe)
    background_tasks.add_task(report_metrics_usage, req.formula)

    # 返回完整的 SelectionResult 契约
    return {
        "node": os.getenv("NODE_INDEX"),
        "requested_date": result.requested_date.isoformat() if result.requested_date else None,
        "signal_date": result.signal_date.isoformat(),
        "codes": result.codes,
        "metadata": result.metadata,
    }


def _load_production_corporate_action_store() -> Optional[CorporateActionStore]:
    """加载生产环境已校验的公司行为事件。

    优先使用本地 CORPORATE_ACTIONS_FILE；否则从 HF Dataset 下载标准产物。
    数据必须来自规范化 GBBQ 事件，不从 adjustFactor/价格跳变推断。
    """
    path = os.getenv("CORPORATE_ACTIONS_FILE")
    if path:
        store = CorporateActionStore.from_file(path)
        logger.info("Loaded corporate actions from %s", path)
        return store

    repo_id = os.getenv("CORPORATE_ACTIONS_HF_REPO", "scanli/stocka-data")
    filename = os.getenv(
        "CORPORATE_ACTIONS_HF_FILE",
        "corporate_actions/corporate_actions.parquet",
    )
    try:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            repo_type="dataset",
            token=os.getenv("HF_TOKEN"),
        )
        store = CorporateActionStore.from_file(path)
        logger.info(
            "Loaded corporate actions from HF dataset %s/%s",
            repo_id, filename,
        )
        return store
    except Exception as exc:
        logger.warning("Corporate action dataset unavailable: %s", exc)
        return None


def _build_backtest_request(req: BacktestRequest):
    """Normalize the API request into one immutable BacktestConfig."""
    if req.strategy is not None:
        strategy = StrategyDefinition.from_dict(req.strategy)
    else:
        if not req.formula:
            raise ValueError("formula is required when strategy is not provided")
        if req.universe_type not in ("all_a", "index"):
            raise ValueError("universe_type must be all_a or index")
        if req.universe_type == "index" and not req.index_id:
            raise ValueError("index_id is required for index universe")
        if req.top_n <= 0:
            raise ValueError("top_n must be > 0")
        strategy = StrategyDefinition(
            universe=UniverseDefinition(type=req.universe_type, index_id=req.index_id),
            entry=SignalDefinition(condition=req.formula, timeframe="D"),
            sizing=PositionSizingDefinition(method="top_n_equal_weight", max_positions=req.top_n),
            rebalance=RebalanceDefinition(frequency=req.rebalance_freq),
            mode="target_portfolio",
        )

    config = BacktestConfig(
        start_date=req.start_date,
        end_signal_date=req.end_signal_date,
        initial_cash=req.initial_cash,
        strategy=strategy,
        min_listing_days=req.min_listing_days,
        exclude_st=req.exclude_st,
        historical_fees=req.historical_fees,
    )

    universe_filter = None
    if config.min_listing_days > 0 or config.exclude_st:
        universe_filter = UniverseFilter(
            min_listing_days=config.min_listing_days,
            exclude_st=config.exclude_st,
        )

    resolver = None
    if strategy.universe.type == "index":
        resolver = UniverseResolver.from_huggingface(
            repo_id=data_manager.repo_id,
            token=os.getenv("HF_TOKEN"),
        )
        resolver.resolve_index_id(strategy.universe.index_id)

    fee_schedule = None
    if config.historical_fees:
        fee_schedule_path = os.path.join(
            os.path.dirname(__file__), "..", "config", "fee_schedule.yaml"
        )
        if not os.path.exists(fee_schedule_path):
            fee_schedule_path = os.path.join(
                os.path.dirname(__file__), "..", "..", "config", "fee_schedule.yaml"
            )
        fee_schedule = load_fee_schedule(fee_schedule_path)

    calendar = TradingCalendar()
    trade_dates = (
        data_manager.df_daily
        .select(pl.col("date")).unique().sort("date").to_series().to_list()
    )
    calendar.set_trade_dates(trade_dates)

    raw_data_root = os.getenv("RAW_PRICE_DATA_ROOT")
    raw_price_store = (
        RawPriceStore(data_root=raw_data_root)
        if raw_data_root
        else RawPriceStore(hf_repo_id=data_manager.repo_id)
    )

    corporate_action_store = _load_production_corporate_action_store()

    max_positions = strategy.sizing.max_positions or 20
    engine = BacktestEngine(
        calendar=calendar,
        selection_engine=selection_engine,
        raw_price_store=raw_price_store,
        fee_config=FeeConfig(),
        execution_config=strategy.execution,
        allocator=top_n_equal_weight_allocator(max_positions),
        universe_resolver=resolver,
        universe_filter=universe_filter,
        corporate_action_store=corporate_action_store,
    )
    return engine, config, fee_schedule, universe_filter
}