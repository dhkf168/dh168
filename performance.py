import time
import asyncio
import logging
from typing import Dict, Any, Callable, Optional, List
from functools import wraps
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger("GroupCheckInBot")


@dataclass
class PerformanceMetrics:
    """性能指标"""

    count: int = 0
    total_time: float = 0
    avg_time: float = 0
    max_time: float = 0
    min_time: float = float("inf")
    last_updated: float = 0


class PerformanceMonitor:
    """性能监控器"""

    def __init__(self):
        self.metrics: Dict[str, PerformanceMetrics] = {}
        self.slow_operations_count = 0
        self.start_time = time.time()

    def track(self, operation_name: str):
        """性能跟踪装饰器"""

        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    execution_time = time.time() - start_time
                    self._record_metrics(operation_name, execution_time)

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    execution_time = time.time() - start_time
                    self._record_metrics(operation_name, execution_time)

            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

        return decorator

    def _record_metrics(self, operation_name: str, execution_time: float):
        """记录性能指标"""
        if operation_name not in self.metrics:
            self.metrics[operation_name] = PerformanceMetrics()

        metrics = self.metrics[operation_name]
        metrics.count += 1
        metrics.total_time += execution_time
        metrics.avg_time = metrics.total_time / metrics.count
        metrics.max_time = max(metrics.max_time, execution_time)
        metrics.min_time = min(metrics.min_time, execution_time)
        metrics.last_updated = time.time()

        # 记录慢操作
        if execution_time > 1.0:  # 超过1秒视为慢操作
            self.slow_operations_count += 1
            logger.warning(
                f"⏱️ 慢操作检测: {operation_name} 耗时 {execution_time:.3f}秒"
            )

    def get_metrics(self, operation_name: str) -> Optional[PerformanceMetrics]:
        """获取指定操作的性能指标"""
        return self.metrics.get(operation_name)

    def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        uptime = time.time() - self.start_time

        # 计算内存使用（近似值）
        try:
            import psutil

            process = psutil.Process()
            memory_usage_mb = process.memory_info().rss / 1024 / 1024
        except ImportError:
            memory_usage_mb = 0

        # 汇总指标
        metrics_summary = {}
        for op_name, metrics in self.metrics.items():
            if metrics.count > 0:
                metrics_summary[op_name] = {
                    "count": metrics.count,
                    "avg": metrics.avg_time,
                    "max": metrics.max_time,
                    "min": metrics.min_time if metrics.min_time != float("inf") else 0,
                }

        return {
            "uptime": uptime,
            "memory_usage_mb": memory_usage_mb,
            "slow_operations_count": self.slow_operations_count,
            "total_operations": sum(m.count for m in self.metrics.values()),
            "metrics_summary": metrics_summary,
        }

    def reset_metrics(self):
        """重置性能指标"""
        self.metrics.clear()
        self.slow_operations_count = 0


class RetryManager:
    """重试管理器"""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay

    def with_retry(self, operation_name: str = "unknown"):
        """重试装饰器"""

        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(self.max_retries + 1):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        if attempt == self.max_retries:
                            break

                        delay = self.base_delay * (2**attempt)  # 指数退避
                        logger.warning(
                            f"🔄 重试 {operation_name} (尝试 {attempt + 1}/{self.max_retries}): {e}"
                        )
                        await asyncio.sleep(delay)

                logger.error(
                    f"❌ {operation_name} 重试{self.max_retries}次后失败: {last_exception}"
                )
                raise last_exception

            return async_wrapper

        return decorator


class GlobalCache:
    """全局缓存管理器"""

    def __init__(self, default_ttl: int = 300):
        self._cache: Dict[str, Any] = {}
        self._cache_ttl: Dict[str, float] = {}
        self._hits = 0
        self._misses = 0
        self.default_ttl = default_ttl

    def get(self, key: str) -> Any:
        """获取缓存值"""
        if key in self._cache_ttl and time.time() < self._cache_ttl[key]:
            self._hits += 1
            return self._cache.get(key)
        else:
            self._misses += 1
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
            return None

    def set(self, key: str, value: Any, ttl: int = None):
        """设置缓存值"""
        if ttl is None:
            ttl = self.default_ttl

        self._cache[key] = value
        self._cache_ttl[key] = time.time() + ttl

    def delete(self, key: str):
        """删除缓存值"""
        self._cache.pop(key, None)
        self._cache_ttl.pop(key, None)

    def clear_expired(self):
        """清理过期缓存"""
        current_time = time.time()
        expired_keys = [
            key for key, expiry in self._cache_ttl.items() if current_time >= expiry
        ]
        for key in expired_keys:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)

        if expired_keys:
            logger.debug(f"清理了 {len(expired_keys)} 个过期缓存")

    def clear_all(self):
        """清理所有缓存"""
        self._cache.clear()
        self._cache_ttl.clear()
        logger.info("所有缓存已清理")

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0

        return {
            "size": len(self._cache),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
            "total_operations": total,
        }


class TaskManager:
    """任务管理器"""

    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}
        self._task_count = 0

    async def create_task(self, coro, name: str = None) -> asyncio.Task:
        """创建并跟踪任务"""
        if not name:
            self._task_count += 1
            name = f"task_{self._task_count}"

        task = asyncio.create_task(coro, name=name)
        self._tasks[name] = task

        # 任务完成后自动清理
        task.add_done_callback(lambda t: self._tasks.pop(name, None))

        return task

    async def cancel_task(self, name: str):
        """取消指定任务"""
        task = self._tasks.get(name)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            self._tasks.pop(name, None)

    async def cancel_all_tasks(self):
        """取消所有任务"""
        tasks_to_cancel = list(self._tasks.values())
        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()

        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            self._tasks.clear()

    def get_task_count(self) -> int:
        """获取任务数量"""
        return len(self._tasks)

    def get_active_tasks(self) -> List[str]:
        """获取活跃任务列表"""
        return [name for name, task in self._tasks.items() if not task.done()]

    async def cleanup_tasks(self):
        """清理已完成的任务"""
        completed_tasks = [name for name, task in self._tasks.items() if task.done()]
        for name in completed_tasks:
            self._tasks.pop(name, None)

        if completed_tasks:
            logger.debug(f"清理了 {len(completed_tasks)} 个已完成任务")


class MessageDeduplicate:
    """消息去重管理器"""

    def __init__(self, ttl: int = 60):
        self._messages: Dict[str, float] = {}
        self.ttl = ttl

    def is_duplicate(self, message_id: str) -> bool:
        """检查消息是否重复"""
        current_time = time.time()

        # 清理过期消息
        expired_messages = [
            msg_id
            for msg_id, timestamp in self._messages.items()
            if current_time - timestamp > self.ttl
        ]
        for msg_id in expired_messages:
            self._messages.pop(msg_id, None)

        # 检查重复
        if message_id in self._messages:
            return True

        # 记录新消息
        self._messages[message_id] = current_time
        return False

    def clear_expired(self):
        """清理过期消息"""
        current_time = time.time()
        expired_messages = [
            msg_id
            for msg_id, timestamp in self._messages.items()
            if current_time - timestamp > self.ttl
        ]
        for msg_id in expired_messages:
            self._messages.pop(msg_id, None)


# 错误处理装饰器
def handle_database_errors(func):
    """数据库错误处理装饰器"""

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"数据库操作失败 {func.__name__}: {e}")
            # 可以根据异常类型进行不同的处理
            raise

    return async_wrapper


def handle_telegram_errors(func):
    """Telegram API错误处理装饰器"""

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Telegram API操作失败 {func.__name__}: {e}")
            # 可以在这里添加重试逻辑或降级处理
            raise

    return async_wrapper


# 全局实例
performance_monitor = PerformanceMonitor()
retry_manager = RetryManager(max_retries=3, base_delay=1.0)
global_cache = GlobalCache(default_ttl=300)
task_manager = TaskManager()
message_deduplicate = MessageDeduplicate(ttl=60)


# 便捷装饰器
def track_performance(operation_name: str):
    """性能跟踪装饰器"""
    return performance_monitor.track(operation_name)


def with_retry(operation_name: str = "unknown", max_retries: int = 3):
    """重试装饰器"""
    retry_mgr = RetryManager(max_retries=max_retries)
    return retry_mgr.with_retry(operation_name)


def message_deduplicate_decorator(ttl: int = 60):
    """消息去重装饰器"""
    deduplicate = MessageDeduplicate(ttl=ttl)

    def decorator(func):
        @wraps(func)
        async def wrapper(message, *args, **kwargs):
            message_id = f"{message.chat.id}_{message.message_id}"
            if deduplicate.is_duplicate(message_id):
                logger.debug(f"跳过重复消息: {message_id}")
                return
            return await func(message, *args, **kwargs)

        return wrapper

    return decorator


# 简写
message_deduplicate = message_deduplicate_decorator()
