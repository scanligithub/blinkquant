from fastapi import FastAPI
from api.routes import router as api_router
from core.data_manager import data_manager
import os

app = FastAPI(title="BlinkQuant Node")

# 注册路由 (这将开启 /api/v1/select, /api/v1/kline 等)
app.include_router(api_router)

@app.on_event("startup")
async def startup_event():
    print(f"Booting Node {os.getenv('NODE_INDEX')}...")
    data_manager.load_data()

@app.get("/")
def index():
    return {"message": "BlinkQuant Compute Node Online", "node": os.getenv("NODE_INDEX")}
