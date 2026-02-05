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

METRIC_REGEX = re.compile(r'(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)', re.IGNORECASE)

class SelectionRequest(BaseModel):
    formula: str
    timeframe: str = "D"

# 修改函数签名，接收 timeframe
def report_metrics_usage(formula: str, timeframe: str):
    """后台任务：上报指标计数，包含周期后缀"""
    if not data_manager.postgres_url:
        return
    
    matches = METRIC_REGEX.findall(formula)
    if not matches:
        return

    # 关键修复：根据传入的 timeframe 生成后缀
    suffix = ""
    if timeframe == 'W': suffix = "_W"
    elif timeframe == 'M': suffix = "_M"

    try:
        conn = psycopg2.connect(data_manager.postgres_url)
        cur = conn.cursor()
        for func, field, param in matches:
            # 生成带后缀的 Key，如 MA_CLOSE_20_M
            metric_key = f"{func.upper()}_{field.upper()}_{param}{suffix}"
            
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
    
    # 关键修复：将 req.timeframe 传递给后台任务
    background_tasks.add_task(report_metrics_usage, req.formula, req.timeframe)
    
    return {"node": os.getenv("NODE_INDEX"), "count": len(results), "results": results}

# ... (其他接口保持不变)

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
