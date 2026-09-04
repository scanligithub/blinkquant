# backend/scheduler/main.py
import asyncio
import logging
import signal
import sys
import os

from .db import init_pool, close_pool
from .scheduler import ClusterScheduler
from .config import POSTGRES_URL, SCHEDULER_HOST, SCHEDULER_PORT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("scheduler")

async def main() -> None:
    if not os.getenv("POSTGRES_URL"):
        sys.exit("POSTGRES_URL not set")

    await init_pool(os.getenv("POSTGRES_URL"))
    log.info("DB pool initialized")

    scheduler = ClusterScheduler()
    loop = asyncio.get_running_loop()

    def shutdown() -> None:
        log.info("Shutdown signal received")
        for task in asyncio.all_tasks(loop):
            if task is not asyncio.current_task(loop):
                task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown)

    try:
        await ClusterScheduler().run_forever()
    finally:
        await close_pool()
        log.info("Scheduler exited")

if __name__ == "__main__":
    asyncio.run(main())