from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
import polars as pl
import os
import re
import psutil
import psycopg2
from core.data_manager import data_manager
from core.engine import selection_engine

router = APIRouter(prefix="/api/v1")

# 正则表达式：用于从公式中提取指标，例如 MA(CLOSE, 20)
METRIC_REGEX = re.compile(r'(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)', re.IGNORECASE)

class SelectionRequest(BaseModel):
    formula: str
    timeframe: str = "D"

def report_metrics_usage(formula: str):
    """后台任务：将公式中的指标上报至 Vercel Postgres 计数"""
    if not data_manager.postgres_url:
        return
    
    matches = METRIC_REGEX.findall(formula)
    if not matches:
        return

    try:
        conn = psycopg2.connect(data_manager.postgres_url)
        cur = conn.cursor()
        for func, field, param in matches:
            metric_key = f"{func.upper()}_{field.upper()}_{param}"
            # UPSERT: 存在则自增，不存在则插入
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
        print(f"Postgres Reporting Error: {e}")

@router.post("/select")
async def select_stocks(req: SelectionRequest, background_tasks: BackgroundTasks):
    if data_manager.df_daily is None:
        raise HTTPException(status_code=503, detail="Nodes are loading data...")
    
    results = selection_engine.execute_selector(req.formula, req.timeframe, background_tasks)
    
    if isinstance(results, dict) and "error" in results:
        raise HTTPException(status_code=400, detail=results["error"])
    
    # 选股成功后，异步上报指标使用热度
    background_tasks.add_task(report_metrics_usage, req.formula)
    
    return {"node": os.getenv("NODE_INDEX"), "count": len(results), "results": results}

@router.get("/kline")
def get_kline(code: str, timeframe: str = "D"):
    df = data_manager.df_daily
    if timeframe == "W": df = data_manager.df_weekly
    elif timeframe == "M": df = data_manager.df_monthly
    if df is None: raise HTTPException(status_code=503, detail="Data not ready")
    
    stock_df = df.filter(pl.col("code") == code).sort("date")
    if len(stock_df) == 0:
        raise HTTPException(status_code=404, detail="Stock not found")
    return {"code": code, "data": stock_df.to_dicts()}

@router.get("/status")
def get_node_status():
    process = psutil.Process(os.getpid())
    return {
        "node": os.getenv("NODE_INDEX"),
        "status": "healthy" if data_manager.df_daily is not None else "loading",
        "memory_used_gb": round(process.memory_info().rss / (1024**3), 2),
        "cached_indicators": len(data_manager.column_metadata),
        "rows": len(data_manager.df_daily) if data_manager.df_daily is not None else 0
    }
