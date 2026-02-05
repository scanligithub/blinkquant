from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
import polars as pl
import os
import psutil
from core.data_manager import data_manager
from core.engine import selection_engine

router = APIRouter(prefix="/api/v1")

class SelectionRequest(BaseModel):
    formula: str
    timeframe: str = "D"

@router.post("/select")
async def select_stocks(req: SelectionRequest, background_tasks: BackgroundTasks):
    if data_manager.df_daily is None:
        raise HTTPException(status_code=503, detail="Nodes are loading data...")
    results = selection_engine.execute_selector(req.formula, req.timeframe, background_tasks)
    if isinstance(results, dict) and "error" in results:
        raise HTTPException(status_code=400, detail=results["error"])
    return {"node": os.getenv("NODE_INDEX"), "count": len(results), "results": results}

@router.get("/kline")
def get_kline(code: str, timeframe: str = "D"):
    # 选择数据源
    df = data_manager.df_daily
    if timeframe == "W": df = data_manager.df_weekly
    elif timeframe == "M": df = data_manager.df_monthly

    if df is None: raise HTTPException(status_code=503, detail="Data not ready")

    # 过滤单只股票并按时间排序
    stock_df = df.filter(pl.col("code") == code).sort("date")
    
    if len(stock_df) == 0:
        raise HTTPException(status_code=404, detail="Stock not found on this node")

    # 转换为前端所需的 JSON 数组
    return {
        "code": code,
        "timeframe": timeframe,
        "data": stock_df.to_dicts()
    }

@router.get("/status")
def get_node_status():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info().rss / (1024 ** 3)
    return {
        "node": os.getenv("NODE_INDEX"),
        "status": "healthy" if data_manager.df_daily is not None else "loading",
        "memory_used_gb": round(mem_info, 2),
        "cached_indicators": len(data_manager.column_metadata) if data_manager.column_metadata else 0,
        "rows": len(data_manager.df_daily) if data_manager.df_daily is not None else 0
    }

@router.get("/health")
def health_check():
    return {"status": "healthy" if data_manager.df_daily is not None else "loading"}
