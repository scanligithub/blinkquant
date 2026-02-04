from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from core.data_manager import data_manager
from core.engine import selection_engine
import os, psutil

app = FastAPI()

class SelectionRequest(BaseModel):
    formula: str
    timeframe: str = "D"

@app.on_event("startup")
async def startup_event():
    data_manager.load_data()

@app.get("/")
def status():
    mem = psutil.Process(os.getpid()).memory_info().rss / (1024**3)
    return {
        "node": os.getenv("NODE_INDEX"),
        "mem_gb": round(mem, 2),
        "stock_rows": len(data_manager.df_daily) if data_manager.df_daily is not None else 0,
        "mapping_ok": data_manager.df_mapping is not None
    }

@app.post("/api/v1/select")
async def select(req: SelectionRequest):
    res = selection_engine.execute_selector(req.formula, req.timeframe)
    if isinstance(res, dict) and "error" in res:
        raise HTTPException(status_code=400, detail=res["error"])
    return {"results": res, "count": len(res)}

@app.get("/health")
def health():
    return {"status": "healthy" if data_manager.df_daily is not None else "loading"}
