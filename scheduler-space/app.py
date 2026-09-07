from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
import os

from scheduler.scheduler import ClusterScheduler
from scheduler.heartbeat import router as heartbeat_router
from scheduler.config import SCHEDULER_HOST, SCHEDULER_PORT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("scheduler")

scheduler_instance = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler_instance
    # 启动调度器
    scheduler_instance = ClusterScheduler()
    import asyncio
    asyncio.create_task(scheduler_instance.run_forever())
    logger.info("Scheduler started in background task")
    yield
    logger.info("Shutting down scheduler")

app = FastAPI(title="BlinkQuant Scheduler", lifespan=lifespan)

# 注册心跳接口
from scheduler.heartbeat import router as heartbeat_router
app.include_router(heartbeat_router, prefix="/internal")

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "scheduler"}

@app.get("/")
async def root():
    return {"service": "blinkquant-scheduler", "version": "1.0.0"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 7860))
    uvicorn.run(app, host="0.0.0.0", port=port)