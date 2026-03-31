from fastapi import FastAPI
from contextlib import asynccontextmanager
from api.routes import router as api_router
from core.data_manager import data_manager
import os
import time
import logging
import asyncio
from contextlib import asynccontextmanager as asynccontextmanager

# 配置标准日志输出到控制台
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    node_idx = os.getenv('NODE_INDEX', 'unknown')
    logger.info(f"Checking environment: NODE_INDEX={node_idx}")

    # --- 核心修改：异步触发加载，不阻塞 lifespan ---
    # 创建后台任务，不使用 await
    asyncio.create_task(data_manager.async_load_data())

    yield
    # --- 停止逻辑 (可选) ---
    logger.info("Shutting down node...")

app = FastAPI(title="BlinkQuant Node", lifespan=lifespan)

app.include_router(api_router)

@app.get("/")
def index():
    return {
        "message": "BlinkQuant Online", 
        "node": os.getenv("NODE_INDEX"),
        "timestamp": time.ctime()
    }
