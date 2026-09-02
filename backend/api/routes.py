from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import Response # New import
from pydantic import BaseModel
from typing import Optional
import datetime
import polars as pl
import os
import re
import psutil
import psycopg2
from pypinyin import pinyin, Style
from core.data_manager import data_manager
from core.engine import selection_engine
from core.indicator_registry import nl_meta as build_nl_meta
from core.backtest_engine import BacktestEngine, TradingCalendar
from core.raw_price_store import RawPriceStore
from core.backtest_types import FeeConfig, ExecutionConfig, MVP_EXECUTION_CONFIG, equal_weight_allocator
import logging
import io # New import

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
    formula: str
    start_date: datetime.date
    end_signal_date: datetime.date
    initial_cash: float = 1_000_000

class BenchmarkRequest(BaseModel):
    benchmark: str = "B1"  # B1, B2, B3, B4

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

    result = selection_engine.execute_selector(req.formula, req.timeframe, background_tasks, target_date=req.date)

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


@router.post("/backtest")
async def run_backtest(req: BacktestRequest, background_tasks: BackgroundTasks):
    if data_manager.df_daily is None:
        raise HTTPException(status_code=503, detail="Nodes are loading data...")

    # 初始化回测引擎组件
    calendar = TradingCalendar()
    # 从 df_daily 获取交易日列表
    if data_manager.df_daily is not None:
        trade_dates = data_manager.df_daily.select(pl.col("date")).unique().sort("date").to_series().to_list()
        calendar.set_trade_dates(trade_dates)

    # raw 数据源：本地目录仅限开发调试（env 覆写）；生产默认走 HF Dataset 按年懒下载
    raw_data_root = os.getenv("RAW_PRICE_DATA_ROOT")
    if raw_data_root:
        raw_price_store = RawPriceStore(data_root=raw_data_root)
    else:
        raw_price_store = RawPriceStore(hf_repo_id=data_manager.repo_id)
    logger.info(f"Backtest raw price source: {raw_price_store.source_type}")

    # 创建回测引擎 - 使用已配置的 calendar 与单一 raw store 实例
    backtest_engine = BacktestEngine(
        calendar=calendar,
        selection_engine=selection_engine,
        raw_price_store=raw_price_store,
        fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG,
        allocator=equal_weight_allocator,
    )

    # 运行回测
    try:
        result = backtest_engine.run(
            formula=req.formula,
            start_date=req.start_date,
            end_signal_date=req.end_signal_date,
            initial_cash=req.initial_cash,
        )

        # 估值截止日 = equity curve 最后一条（可能超出 end_signal_date，属冻结语义）
        valuation_end_date = None
        if not result.equity_curve.is_empty():
            valuation_end_date = result.equity_curve["date"].max().isoformat()

        # 返回结果
        return {
            "formula": req.formula,
            "start_date": req.start_date.isoformat(),
            "signal_end_date": req.end_signal_date.isoformat(),
            "valuation_end_date": valuation_end_date,
            "initial_cash": req.initial_cash,
            "equity_curve": result.equity_curve.to_dicts() if not result.equity_curve.is_empty() else [],
            "trades": result.trades.to_dicts() if not result.trades.is_empty() else [],
            "positions_daily": result.positions_daily.to_dicts() if not result.positions_daily.is_empty() else [],
            "metrics": result.metrics,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/kline")
def get_kline(code: str, timeframe: str = "D"):
    df = data_manager.df_daily
    if timeframe == "W": df = data_manager.df_weekly
    elif timeframe == "M": df = data_manager.df_monthly

    if df is None: raise HTTPException(status_code=503, detail="Data not ready")
    
    # 过滤并排序
    stock_df = df.filter(pl.col("code") == code).sort("date")

    # 动态选择存在的列，防止请求周/月线时崩溃
    target_cols = ["date", "code", "open", "high", "low", "close", "volume", "amount", "turn", "pctChg", "peTTM", "pbMRQ", "isST", "adjustFactor", "net_amount", "main_net", "super_net", "large_net", "medium_net", "small_net", "total_shares", "float_shares", "total_mv", "float_mv", "product_ratios", "forecast_type", "forecast_yoy", "is_forecast_good", "is_forecast_bad"]
    available_cols = [col for col in target_cols if col in stock_df.columns]
    stock_df = stock_df.select(available_cols)



    if len(stock_df) == 0:
        raise HTTPException(status_code=404, detail="Stock not found")

    # 将 Polars DataFrame 写入内存中的 Parquet 文件，并使用 ZSTD 压缩
    buffer = io.BytesIO()
    stock_df.write_parquet(buffer, compression="zstd")
    buffer.seek(0) # 将文件指针移到开头
    

    # 以二进制响应的形式返回 Parquet 数据
    return Response(content=buffer.getvalue(), media_type="application/octet-stream")

@router.get("/sector-kline")
def get_sector_kline(code: str, timeframe: str = "D"):
    if timeframe == "W":
        df = getattr(data_manager, "df_sector_weekly", None)
    elif timeframe == "M":
        df = getattr(data_manager, "df_sector_monthly", None)
    else:
        df = data_manager.df_sector_daily

    if df is None:
        raise HTTPException(status_code=503, detail="Data not ready")

    sector_df = df.filter(pl.col("code") == code).sort("date")
    if len(sector_df) == 0:
        raise HTTPException(status_code=404, detail="Sector not found")

    target_cols = ["date", "code", "name", "type", "open", "high", "low", "close", "volume", "amount"]
    available_cols = [col for col in target_cols if col in sector_df.columns]
    sector_df = sector_df.select(available_cols)

    buffer = io.BytesIO()
    sector_df.write_parquet(buffer, compression="zstd")
    buffer.seek(0)
    return Response(content=buffer.getvalue(), media_type="application/octet-stream")

def _get_pinyin_initials(text: str) -> str:
    """获取中文文本的拼音首字母，并转换为小写"""
    if not text:
        return ""
    
    # 检查是否包含中文字符
    if not any('\u4e00' <= char <= '\u9fff' for char in text):
        return text.lower() # 如果没有中文，直接返回小写

    # full模式返回所有拼音，然后取首字母并拼接
    pinyin_list = pinyin(text, style=Style.FIRST_LETTER)
    initials = ''.join([item[0] for item in pinyin_list])
    # 只保留字母字符，移除所有非字母字符（如空格、括号、数字等）
    return ''.join(c for c in initials.lower() if c.isalpha())

@router.get("/search")
def search_stocks(q: str):
    if not q:
        return []

    q_lower = q.lower()
    q_pinyin_initials = _get_pinyin_initials(q)
    logger.info(f"Search query: {q}, q_lower: {q_lower}, q_pinyin_initials: {q_pinyin_initials}")

    results = []
    
    for code, name in data_manager.code_to_name.items():
        name_lower = name.lower()
        name_pinyin_initials = _get_pinyin_initials(name)
        logger.debug(f"Checking stock: code={code}, name={name}, name_lower={name_lower}, name_pinyin_initials={name_pinyin_initials}")

        if (q_lower in code.lower() or
            q_lower in name_lower or
            q_pinyin_initials and q_pinyin_initials in name_pinyin_initials):
            results.append({"code": code, "name": name})
        if len(results) >= 10: # Limit to 10 results
            break
            
    return results

@router.get("/stock-list")
def get_stock_list():
    """返回所有股票代码与名称的映射，仅用于前端缓存"""
    # 调试：打印前10条数据
    sample = list(data_manager.code_to_name.items())[:10]
    logger.info(f"Stock list sample: {sample}")
    
    # 过滤掉空名称的股票
    filtered = [{"code": code, "name": name}
                for code, name in data_manager.code_to_name.items()
                if name and name.strip()]
    
    logger.info(f"Total stocks: {len(data_manager.code_to_name)}, Filtered: {len(filtered)}")
    return filtered

@router.get("/stock-sectors")
def get_stock_sectors(code: str):
    """返回股票所属的全部板块（行业+概念+地域）"""
    sectors = data_manager.stock_sectors.get(code, [])
    return {
        "code": code,
        "sectors": [
            {"code": sc, "name": name, "type": typ}
            for sc, name, typ in sectors
        ],
    }

@router.get("/nl-meta")
def get_nl_meta():
    """自然语言选股元数据：字段/指标/单位/示例（公开只读）"""
    return build_nl_meta()

@router.get("/status")
def get_node_status():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    
    # 获取系统级统计
    vm = psutil.virtual_memory()
    du = psutil.disk_usage('/')

    return {
        "node": os.getenv("NODE_INDEX"),
        "status": "healthy" if data_manager.df_daily is not None else "loading",
        
        # 进程内存
        "process_memory_gb": round(mem_info.rss / (1024**3), 2),
        
        # 系统内存状态
        "system_memory_total_gb": round(vm.total / (1024**3), 2),
        "system_memory_free_gb": round(vm.available / (1024**3), 2), # available 比 free 更准确反映可用内存
        
        # 磁盘状态
        "disk_total_gb": round(du.total / (1024**3), 2),
        "disk_free_gb": round(du.free / (1024**3), 2),
        
        # 数据量
        "rows_daily": len(data_manager.df_daily) if data_manager.df_daily is not None else 0
    }

@router.get("/health")
def health_check():
    # 只要 Uvicorn 跑起来就回 200，防止 HF 杀掉进程
    # 增加 build_id 返回以进行高可用的版本比对，防止滚动更新假阳性
    b_id = getattr(data_manager, "build_id", "unknown")
    if data_manager.df_daily is not None:
        return {"status": "healthy", "build_id": b_id}
    return {"status": "initializing", "build_id": b_id}

@router.post("/benchmark")
async def run_benchmark(req: BenchmarkRequest):
    """Gate 3B: HF Space benchmark validation (lazy path)."""
    import datetime
    import time
    import polars as pl
    from core.raw_price_store import RawPriceStore
    from core.engine import selection_engine as _sel
    from core.backtest_engine import BacktestEngine, TradingCalendar
    from core.backtest_types import FeeConfig, MVP_EXECUTION_CONFIG, top_n_equal_weight_allocator
    from core.fee_config import load_fee_schedule

    BENCHMARKS = {
        "B1": (datetime.date(2024, 1, 2), datetime.date(2024, 3, 29)),
        "B2": (datetime.date(2024, 1, 2), datetime.date(2024, 12, 30)),
        "B3": (datetime.date(2019, 1, 2), datetime.date(2024, 12, 30)),
        "B4": (datetime.date(2010, 1, 4), datetime.date(2024, 12, 30)),
    }

    if req.benchmark not in BENCHMARKS:
        raise HTTPException(status_code=400, detail=f"Unknown benchmark: {req.benchmark}. Use B1-B4.")

    start_date, end_date = BENCHMARKS[req.benchmark]
    formula = "CLOSE > MA(CLOSE, 20)"

    # Load data
    raw_store = RawPriceStore(hf_repo_id=data_manager.repo_id)
    latest_adj = raw_store.load_latest_adjust_factors()
    trade_dates = raw_store.get_trading_dates(start_date, datetime.date(2025, 1, 10))
    cal = TradingCalendar()
    cal.set_trade_dates(trade_dates)

    # Build engine
    _sel._set_cache.clear()
    allocator = top_n_equal_weight_allocator(20)
    fee_schedule_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'fee_schedule.yaml')
    if not os.path.exists(fee_schedule_path):
        fee_schedule_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'fee_schedule.yaml')
    fee_schedule = load_fee_schedule(fee_schedule_path)

    engine = BacktestEngine(
        calendar=cal, selection_engine=_sel,
        raw_price_store=raw_store, fee_config=FeeConfig(),
        execution_config=MVP_EXECUTION_CONFIG, allocator=allocator,
    )
    engine._latest_adj = latest_adj

    # Run
    t0 = time.time()
    result = engine.run(
        formula=formula, start_date=start_date, end_signal_date=end_date,
        initial_cash=10_000_000, rebalance_freq="weekly",
        top_n=20, fee_schedule=fee_schedule,
    )
    bt_time = time.time() - t0

    diag = result.execution_diagnostics or {}
    return {
        "benchmark": req.benchmark,
        "period": f"{start_date}..{end_date}",
        "trades": result.trades.height,
        "final_equity": result.equity_curve['equity'].tail(1).item(),
        "backtest_time_sec": round(bt_time, 1),
        "has_negative_cash": diag.get('has_negative_cash', False),
        "accounting_violations": diag.get('accounting_invariant_violations', 0),
        "rejections": sum(diag.get('rej_counters', {}).values()),
    }
