from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from core.data_manager import data_manager
from core.engine import selection_engine
import os
import psutil

app = FastAPI(title="BlinkQuant Computing Node")

class SelectionRequest(BaseModel):
    formula: str
    timeframe: str = "D"

@app.on_event("startup")
async def startup_event():
    print(f"Booting Node {os.getenv('NODE_INDEX')}...")
    data_manager.load_data()

@app.get("/")
def read_root():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info().rss / (1024 ** 3) # GB
    return {
        "status": "online", 
        "node": os.getenv("NODE_INDEX"),
        "memory_used_gb": round(mem_info, 2),
        "daily_rows": len(data_manager.df_daily) if data_manager.df_daily is not None else 0
    }

@app.post("/api/v1/select")
async def select_stocks(req: SelectionRequest):
    """
    选股接口：接收公式，返回当前节点命中的股票代码
    """
    if data_manager.df_daily is None:
        raise HTTPException(status_code=503, detail="Data not loaded yet")
    
    try:
        results = selection_engine.execute_selector(req.formula, req.timeframe)
        if isinstance(results, dict) and "error" in results:
            raise HTTPException(status_code=400, detail=results["error"])
        return {
            "node": os.getenv("NODE_INDEX"),
            "count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    if data_manager.df_daily is not None:
        return {"status": "healthy"}
    return {"status": "loading"}
