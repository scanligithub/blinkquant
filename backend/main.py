import os
import time
import logging
import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from api.routes import router as api_router
from core.data_manager import data_manager

# 全面放开日志，加上时间戳方便对齐
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    node_idx = os.getenv('NODE_INDEX', 'unknown')
    logger.info(f"====== STARTING NODE {node_idx} LIFESPAN ======")
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
