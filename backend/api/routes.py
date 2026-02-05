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

@router.get("/status")
def get_node_status():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info().rss / (1024 ** 3)
    return {
        "node": os.getenv("NODE_INDEX"),
        "status": "healthy" if data_manager.df_daily is not None else "loading",
        "memory_used_gb": round(mem_info, 2),
        "cached_indicators": len(data_manager.column_metadata) if data_manager.column_metadata else 0
    }

@router.get("/health")
def health_check():
    return {"status": "healthy" if data_manager.df_daily is not None else "loading"}

@router.get("/peek")
def peek_data():
    if data_manager.df_daily is None: return {"error": "loading"}
    return {"sample": data_manager.df_daily.head(5).to_dicts(), "cols": data_manager.df_daily.columns}
