from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware  # 新增导入
from contextlib import asynccontextmanager
from api.routes import router as api_router
from core.data_manager import data_manager
import os
import time
import logging
import asyncio

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

# ---- 核心修改：添加 CORS 跨域配置 ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有域名访问，如果安全性要求高可填入你 Vercel 前端的具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

@app.get("/")
def index():
    return {
        "message": "BlinkQuant Online", 
        "node": os.getenv("NODE_INDEX"),
        "timestamp": time.ctime()
    }
