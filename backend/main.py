from fastapi import FastAPI
from contextlib import asynccontextmanager
from api.routes import router as api_router
from core.data_manager import data_manager
import os
import time
import logging

# 配置标准日志输出到控制台
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- 启动逻辑 ---
    node_idx = os.getenv('NODE_INDEX', 'unknown')
    logger.info(f"Checking environment: NODE_INDEX={node_idx}")
    
    start_time = time.time()
    try:
        logger.info(f"🚀 Booting Node {node_idx} - Starting Data Load...")
        # 强制执行加载
        data_manager.load_data()
        logger.info(f"✅ Node {node_idx} Data Load Completed in {time.time() - start_time:.2f}s")
    except Exception as e:
        logger.error(f"❌ Critical Boot Error: {str(e)}")
    
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
