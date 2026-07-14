import os
import time
import logging
import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from api.routes import router as api_router
from core.data_manager import data_manager

# 1. 配置标准日志输出
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger(__name__)

# 2. 关键修改：降低第三方网络库的日志级别，防止 116 个文件下载时刷屏
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("huggingface_hub").setLevel(logging.WARNING)

@asynccontextmanager
async def lifespan(app: FastAPI):
    node_idx = os.getenv('NODE_INDEX', 'unknown')
    logger.info(f"Checking environment: NODE_INDEX={node_idx}")

    # 异步触发加载
    asyncio.create_task(data_manager.async_load_data())

    yield
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
