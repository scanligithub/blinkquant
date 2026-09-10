from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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

# 全局标记：调度器是否在运行
scheduler_running = False


async def ensure_scheduler_db() -> None:
    """确保调度库就绪：integrity_check -> 损坏删 -> 下载 HF -> 404 则新建"""
    from scheduler.config import SCHEDULER_DB_PATH, SCHEDULER_HF_REPO, SCHEDULER_HF_FILE
    from scheduler.db import init_pool, close_pool
    from huggingface_hub import hf_hub_download
    from huggingface_hub.utils import HFValidationError, RepositoryNotFoundError
    import sqlite3
    from pathlib import Path

    db_path = Path(SCHEDULER_DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # 1. 本地文件存在 -> integrity_check
    if db_path.exists():
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.execute("PRAGMA integrity_check")
                result = cur.fetchone()[0]
                if result == "ok":
                    logger.info("Local scheduler.db integrity_check: ok")
                    return
                else:
                    logger.warning("integrity_check failed: %s, will re-download", result)
        except Exception as e:
            logger.warning("integrity_check error: %s, will re-download", e)
        # 损坏 -> 删除
        try:
            db_path.unlink()
            logger.info("Removed corrupted local scheduler.db")
        except Exception as e:
            logger.warning("Failed to remove corrupted db: %s", e)

    # 2. 下载 HF checkpoint
    try:
        logger.info("Downloading scheduler.db from HF: %s/%s", SCHEDULER_HF_REPO, SCHEDULER_HF_FILE)
        hf_hub_download(
            repo_id=SCHEDULER_HF_REPO,
            filename=SCHEDULER_HF_FILE,
            repo_type="dataset",
            local_dir=db_path.parent,
            local_dir_use_symlinks=False,
            token=os.getenv("HF_TOKEN"),
        )
        logger.info("Downloaded scheduler.db from HF")
        return
    except (RepositoryNotFoundError, HFValidationError) as e:
        logger.info("HF repo/file not found (404): %s", e)
    except Exception as e:
        logger.warning("HF download failed: %s, will create new DB", e)

    # 3. 404 或下载失败 -> 新建空库 + schema + seed
    logger.info("Creating new scheduler.db with schema + seed")
    await init_pool()
    await close_pool()


async def recover_stale_on_boot() -> None:
    """启动时恢复：复用 ClusterScheduler._recover_stuck"""
    from scheduler.scheduler import ClusterScheduler
    scheduler = ClusterScheduler()
    await scheduler.recover_stale_on_boot()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler_running
    node_idx = os.getenv('NODE_INDEX', 'unknown')
    logger.info(f"Checking environment: NODE_INDEX={node_idx}")

    # --- 核心修改：异步触发加载，不阻塞 lifespan ---
    asyncio.create_task(data_manager.async_load_data())

    # --- 核心修改：在同一事件循环中启动调度器（仅 node1） ---
    if os.getenv('RUN_SCHEDULER') == 'true':
        logger.info("Starting ClusterScheduler in-process...")

        # 1. ensure_scheduler_db
        await ensure_scheduler_db()

        # 2. recover_stale_on_boot
        await recover_stale_on_boot()

        # 3. 启动调度器
        from scheduler.scheduler import ClusterScheduler
        scheduler = ClusterScheduler()
        await scheduler.start()
        scheduler_running = True
        logger.info("ClusterScheduler started in-process")

        # 4. 后台 checkpoint_loop
        from scheduler.checkpoint import checkpoint_loop
        asyncio.create_task(checkpoint_loop())

    yield
    # --- 停止逻辑 ---
    logger.info("Shutting down node...")
    if scheduler_running:
        from scheduler.checkpoint import shutdown_checkpoint
        await shutdown_checkpoint()
    scheduler_running = False

app = FastAPI(title="BlinkQuant Node", lifespan=lifespan)

# ---- 核心修改：添加 CORS 跨域配置 ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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