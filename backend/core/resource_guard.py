"""P2-5: ResourceGuard for HF Space production validation.

Provides memory, CPU timeout, and concurrency guards.
Separate from BacktestEngine — deployment-layer only.
"""
import os
import time
import threading
import logging
import psutil

logger = logging.getLogger(__name__)


class MemoryGuard:
    """Process RSS monitoring with warning/critical/hard-stop thresholds."""

    def __init__(self, warning_mb: int = 4096, critical_mb: int = 8192, hard_stop_mb: int = 12288):
        self.warning_mb = warning_mb
        self.critical_mb = critical_mb
        self.hard_stop_mb = hard_stop_mb
        self._baseline_rss = self._get_rss_mb()
        self._peak_rss = self._baseline_rss
        self._breached = False

    @staticmethod
    def _get_rss_mb() -> float:
        """Get current process RSS in MB (works on Windows/Linux/macOS)."""
        try:
            p = psutil.Process(os.getpid())
            return p.memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0

    def check(self) -> dict:
        """Check current memory state. Returns status dict."""
        current = self._get_rss_mb()
        if current > self._peak_rss:
            self._peak_rss = current

        delta = current - self._baseline_rss

        if current >= self.hard_stop_mb:
            self._breached = True
            return {"status": "HARD_STOP", "current_mb": current, "delta_mb": delta,
                    "peak_mb": self._peak_rss, "message": f"Memory {current:.0f}MB >= {self.hard_stop_mb}MB hard stop"}
        elif current >= self.critical_mb:
            return {"status": "CRITICAL", "current_mb": current, "delta_mb": delta,
                    "peak_mb": self._peak_rss, "message": f"Memory {current:.0f}MB >= {self.critical_mb}MB critical"}
        elif current >= self.warning_mb:
            return {"status": "WARNING", "current_mb": current, "delta_mb": delta,
                    "peak_mb": self._peak_rss, "message": f"Memory {current:.0f}MB >= {self.warning_mb}MB warning"}
        else:
            return {"status": "OK", "current_mb": current, "delta_mb": delta,
                    "peak_mb": self._peak_rss}

    @property
    def is_breached(self) -> bool:
        return self._breached

    @property
    def baseline_mb(self) -> float:
        return self._baseline_rss

    @property
    def peak_mb(self) -> float:
        return self._peak_rss


class TimeoutGuard:
    """Wall-clock timeout for long-running operations."""

    def __init__(self, timeout_seconds: int = 14400):
        self.timeout_seconds = timeout_seconds
        self._start = time.time()
        self._breached = False

    def check(self) -> dict:
        elapsed = time.time() - self._start
        remaining = self.timeout_seconds - elapsed

        if remaining <= 0:
            self._breached = True
            return {"status": "TIMEOUT", "elapsed_s": elapsed, "remaining_s": 0,
                    "message": f"Timeout {self.timeout_seconds}s exceeded ({elapsed:.0f}s elapsed)"}
        elif remaining < self.timeout_seconds * 0.1:
            return {"status": "WARNING", "elapsed_s": elapsed, "remaining_s": remaining,
                    "message": f"Timeout approaching: {remaining:.0f}s remaining"}
        else:
            return {"status": "OK", "elapsed_s": elapsed, "remaining_s": remaining}

    @property
    def is_breached(self) -> bool:
        return self._breached


class ConcurrencyGuard:
    """Semaphore-based concurrency limiter for heavy jobs."""

    def __init__(self, max_concurrent: int = 1):
        self._semaphore = threading.Semaphore(max_concurrent)
        self._active = 0
        self._max = max_concurrent
        self._lock = threading.Lock()

    def acquire(self, blocking: bool = True, timeout: float = None) -> bool:
        result = self._semaphore.acquire(blocking=blocking, timeout=timeout)
        if result:
            with self._lock:
                self._active += 1
        return result

    def release(self):
        with self._lock:
            self._active = max(0, self._active - 1)
        self._semaphore.release()

    @property
    def active_count(self) -> int:
        with self._lock:
            return self._active

    @property
    def available(self) -> int:
        return self._max - self.active_count

    def status(self) -> dict:
        return {"active": self.active_count, "available": self.available, "max": self._max}


class ResourceGuard:
    """Combined resource guard for HF Space production.

    Usage:
        guard = ResourceGuard()
        with guard.run("b4_backtest") as ctx:
            result = engine.run(...)
            ctx.check()  # optional periodic check
    """

    def __init__(self, warning_mb=4096, critical_mb=8192, hard_stop_mb=12288,
                 timeout_seconds=14400, max_concurrent=1):
        self.memory = MemoryGuard(warning_mb, critical_mb, hard_stop_mb)
        self.timeout = TimeoutGuard(timeout_seconds)
        self.concurrency = ConcurrencyGuard(max_concurrent)
        self._results = []

    class RunContext:
        def __init__(self, guard: 'ResourceGuard', name: str):
            self.guard = guard
            self.name = name
            self._start = time.time()

        def __enter__(self):
            if not self.guard.concurrency.acquire(blocking=False):
                raise RuntimeError(f"Concurrency limit reached: {self.guard.concurrency.status()}")
            logger.info(f"[ResourceGuard] {self.name} started, concurrency: {self.guard.concurrency.status()}")
            return self

        def __exit__(self, *args):
            self.guard.concurrency.release()
            elapsed = time.time() - self._start
            mem = self.guard.memory.check()
            logger.info(f"[ResourceGuard] {self.name} done: {elapsed:.1f}s, peak={mem['peak_mb']:.0f}MB")
            self.guard._results.append({
                "name": self.name,
                "elapsed_s": elapsed,
                "peak_mb": mem["peak_mb"],
                "status": "BREACHED" if self.guard.memory.is_breached else "OK",
            })

        def check(self) -> dict:
            mem = self.guard.memory.check()
            tout = self.guard.timeout.check()
            return {"memory": mem, "timeout": tout}

    def run(self, name: str):
        return self.RunContext(self, name)

    def summary(self) -> dict:
        return {
            "baseline_mb": self.memory.baseline_mb,
            "peak_mb": self.memory.peak_mb,
            "runs": self._results,
            "concurrency": self.concurrency.status(),
        }
