from fastapi import FastAPI
from core.data_manager import data_manager
import os

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    # 启动时加载数据
    print(f"Booting Node {os.getenv('NODE_INDEX')}...")
    data_manager.load_data()

@app.get("/")
def read_root():
    return {
        "status": "online", 
        "node": os.getenv("NODE_INDEX"),
        "stock_count": len(data_manager.df_daily) if data_manager.df_daily is not None else 0
    }

@app.get("/health")
def health_check():
    # 供 GitHub Actions 和 Vercel 检查存活
    if data_manager.df_daily is not None:
        return {"status": "healthy", "memory_usage": "TBD"}
    return {"status": "loading"}
