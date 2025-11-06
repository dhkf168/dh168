# performance.py - 优化版本
import time
import psutil
import asyncio
import logging
from functools import wraps
from typing import Any, Callable, Dict, Optional
from datetime import datetime
from collections import defaultdict
import weakref

logger = logging.getLogger("GroupCheckInBot")


class PerformanceMonitor:
    """优化的性能监控器"""

    def __init__(self):
        self.metrics = defaultdict(list)
        self.slow_operations = []
        self.start_time = time.time()
        self._operation_count = 0

    def track_operation(self, operation_name: str):
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                self._operation_count += 1

                try:
                    result = await func(*args, **kwargs)
                    return result
                except Exception as e:
                    raise e
                finally:
                    duration = time.time() - start_time
                    self.metrics[f"{operation_name}_time"].append(duration)

                    # 只记录真正慢的操作
                    if duration > 2.0:
                        self.slow_operations.append(
                            {
                                "operation": operation_name,
                                "duration": duration,
                                "timestamp": datetime.now(),
                            }
                        )
                        logger.warning(
                            f"🐌 慢操作: {operation_name} 耗时 {duration:.2f}s"
                        )

                    # 每100次操作报告一次
                    if self._operation_count % 100 == 0:
                        self._report_metrics(operation_name)

            return async_wrapper

        return decorator

    def _report_metrics(self, operation_name: str):
        """简化报告逻辑"""
        times = self.metrics.get(f"{operation_name}_time", [])
        if times:
            avg_time = sum(times) / len(times)
            logger.info(
                f"📊 {operation_name} 平均耗时: {avg_time:.3f}s, 样本数: {len(times)}"
            )

    def get_performance_report(self) -> Dict[str, Any]:
        """简化性能报告"""
        return {
            "uptime": time.time() - self.start_time,
            "memory_usage_mb": self.get_memory_usage(),
            "slow_operations_count": len(self.slow_operations),
            "total_operations": self._operation_count,
        }

    def get_memory_usage(self) -> float:
        """获取内存使用量(MB)"""
        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except:
            return 0


class MemoryAwareTaskManager:
    """简化的任务管理器"""

    def __init__(self, max_memory_mb: int = 400):
        self.max_memory_mb = max_memory_mb
        self._tasks = weakref.WeakSet()

    async def create_task(self, coro, name: Optional[str] = None) -> asyncio.Task:
        """创建任务并检查内存"""
        if not self.memory_usage_ok():
            await self.cleanup_tasks()

        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def memory_usage_ok(self) -> bool:
        """检查内存使用"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            return memory_mb < self.max_memory_mb
        except:
            return True

    async def cleanup_tasks(self):
        """清理已完成的任务"""
        completed = [task for task in self._tasks if task.done()]
        for task in completed:
            try:
                await task
            except Exception:
                pass
            self._tasks.discard(task)


class RetryManager:
    """简化的重试管理器"""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay

    async def execute_with_retry(self, coro, operation_name: str = ""):
        """带重试的执行"""
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                return await coro
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    delay = self.base_delay * (2**attempt)
                    logger.warning(
                        f"⚠️ {operation_name} 第 {attempt + 1} 次失败，{delay:.1f}秒后重试"
                    )
                    await asyncio.sleep(delay)

        logger.error(
            f"❌ {operation_name} 重试{self.max_retries}次后失败: {last_exception}"
        )
        raise last_exception


class AsyncCache:
    """简化的异步缓存"""

    def __init__(self, default_ttl: int = 300):
        self._cache = {}
        self._cache_ttl = {}
        self._default_ttl = default_ttl
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0

    async def get(self, key: str) -> Any:
        """获取缓存值"""
        async with self._lock:
            if key in self._cache_ttl and time.time() < self._cache_ttl[key]:
                self._hits += 1
                return self._cache.get(key)
            else:
                self._misses += 1
                # 自动清理过期缓存
                if key in self._cache:
                    del self._cache[key]
                if key in self._cache_ttl:
                    del self._cache_ttl[key]
                return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """设置缓存值"""
        async with self._lock:
            ttl = ttl or self._default_ttl
            self._cache[key] = value
            self._cache_ttl[key] = time.time() + ttl

    async def delete(self, key: str):
        """删除缓存值"""
        async with self._lock:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)

    async def clear_expired(self):
        """清理过期缓存"""
        async with self._lock:
            now = time.time()
            expired_keys = [
                key for key, expiry in self._cache_ttl.items() if now >= expiry
            ]
            for key in expired_keys:
                self._cache.pop(key, None)
                self._cache_ttl.pop(key, None)

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
            "size": len(self._cache),
        }


# 消息去重装饰器
def message_deduplicate(func):
    recent_messages = set()

    @wraps(func)
    async def wrapper(message, *args, **kwargs):
        msg_key = f"{message.chat.id}-{message.message_id}"

        if msg_key in recent_messages:
            return

        recent_messages.add(msg_key)
        # 10秒后自动清理
        asyncio.create_task(_remove_message(msg_key))

        return await func(message, *args, **kwargs)

    async def _remove_message(key):
        await asyncio.sleep(10)
        recent_messages.discard(key)

    return wrapper


# 便捷装饰器
def track_performance(operation_name: str):
    return performance_monitor.track_operation(operation_name)


def with_retry(operation_name: str = "", max_retries: int = 3):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retry_manager = RetryManager(max_retries=max_retries)
            return await retry_manager.execute_with_retry(
                func(*args, **kwargs), operation_name=operation_name or func.__name__
            )

        return wrapper

    return decorator


# 在 performance.py 中添加
class MemoryMonitor:
    def __init__(self):
        self.warning_threshold = 350  # MB
        self.critical_threshold = 380  # MB

    async def check_and_clean(self):
        memory_mb = self.get_memory_usage()
        if memory_mb > self.warning_threshold:
            await self.force_cleanup()


# 全局实例
performance_monitor = PerformanceMonitor()
task_manager = MemoryAwareTaskManager()
retry_manager = RetryManager()
global_cache = AsyncCache()
