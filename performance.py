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
    """性能监控器 - 线程安全版"""

    def __init__(self):
        self.metrics: Dict[str, PerformanceMetrics] = {}
        self.slow_operations_count = 0
        self.start_time = time.time()
        self._metrics_lock = asyncio.Lock()  # ✅ 添加锁

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
                    # ✅ 异步方法中创建任务来记录指标，不阻塞
                    asyncio.create_task(
                        self._record_metrics_async(operation_name, execution_time)
                    )

            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    execution_time = time.time() - start_time
                    # ✅ 同步方法中直接调用同步记录方法
                    self._record_metrics_sync(operation_name, execution_time)

            return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

        return decorator

    async def _record_metrics_async(self, operation_name: str, execution_time: float):
        """异步记录性能指标"""
        async with self._metrics_lock:
            self._record_metrics_internal(operation_name, execution_time)

    def _record_metrics_sync(self, operation_name: str, execution_time: float):
        """同步记录性能指标（用于同步函数）"""
        # ✅ 同步方法中无法使用 asyncio.Lock，但考虑到：
        # 1. 同步函数在异步环境中很少使用
        # 2. 即使有并发，丢失几次计数影响不大
        # 3. 可以改用 threading.Lock，但会增加复杂度
        self._record_metrics_internal(operation_name, execution_time)

    def _record_metrics_internal(self, operation_name: str, execution_time: float):
        """内部记录方法（调用时需确保线程安全）"""
        if operation_name not in self.metrics:
            self.metrics[operation_name] = PerformanceMetrics()

        metrics = self.metrics[operation_name]
        metrics.count += 1
        metrics.total_time += execution_time
        metrics.avg_time = metrics.total_time / metrics.count
        metrics.max_time = max(metrics.max_time, execution_time)
        metrics.min_time = min(metrics.min_time, execution_time)
        metrics.last_updated = time.time()

        if execution_time > 1.0:
            self.slow_operations_count += 1
            logger.warning(
                f"⏱️ 慢操作检测: {operation_name} 耗时 {execution_time:.3f}秒"
            )

    async def get_metrics(self, operation_name: str) -> Optional[PerformanceMetrics]:
        """获取指定操作的性能指标"""
        async with self._metrics_lock:
            # 返回副本以避免外部修改
            metrics = self.metrics.get(operation_name)
            if metrics:
                return PerformanceMetrics(
                    count=metrics.count,
                    total_time=metrics.total_time,
                    avg_time=metrics.avg_time,
                    max_time=metrics.max_time,
                    min_time=metrics.min_time,
                    last_updated=metrics.last_updated,
                )
            return None

    async def get_performance_report(self) -> Dict[str, Any]:
        """获取性能报告"""
        uptime = time.time() - self.start_time

        try:
            import psutil

            process = psutil.Process()
            memory_usage_mb = process.memory_info().rss / 1024 / 1024
        except ImportError:
            memory_usage_mb = 0

        # ✅ 在锁保护下复制数据
        async with self._metrics_lock:
            metrics_summary = {}
            for op_name, metrics in self.metrics.items():
                if metrics.count > 0:
                    metrics_summary[op_name] = {
                        "count": metrics.count,
                        "avg": metrics.avg_time,
                        "max": metrics.max_time,
                        "min": (
                            metrics.min_time if metrics.min_time != float("inf") else 0
                        ),
                    }

            slow_ops = self.slow_operations_count
            total_ops = sum(m.count for m in self.metrics.values())

        return {
            "uptime": uptime,
            "memory_usage_mb": memory_usage_mb,
            "slow_operations_count": slow_ops,
            "total_operations": total_ops,
            "metrics_summary": metrics_summary,
        }

    async def reset_metrics(self):
        """重置性能指标"""
        async with self._metrics_lock:
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

                        delay = self.base_delay * (2**attempt)
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
    """全局缓存管理器 - 原子计数器版（最佳实践）"""

    def __init__(self, default_ttl: int = 300):
        self._cache: Dict[str, Any] = {}
        self._cache_ttl: Dict[str, float] = {}
        # 使用 asyncio 锁
        self._stats_lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0
        self.default_ttl = default_ttl
        # 用于缓存写入的锁
        self._write_lock = asyncio.Lock()
        # 用于缓存击穿保护
        self._loading: Dict[str, asyncio.Event] = {}

    async def get(self, key: str) -> Optional[Any]:
        """获取缓存值 - 高性能版"""
        # 快速路径：直接读取（Python dict get 是原子的）
        expiry = self._cache_ttl.get(key)

        if expiry is not None:
            if time.time() < expiry:
                # 缓存有效
                value = self._cache.get(key)
                # 原子更新命中数
                async with self._stats_lock:
                    self._hits += 1
                return value
            else:
                # 缓存过期，需要清理
                async with self._write_lock:
                    # 双重检查，防止在获得锁前被清理
                    if key in self._cache_ttl and time.time() >= self._cache_ttl[key]:
                        self._cache.pop(key, None)
                        self._cache_ttl.pop(key, None)

        # 缓存未命中
        async with self._stats_lock:
            self._misses += 1
        return None

    async def get_many(self, keys: list) -> Dict[str, Any]:
        """批量获取缓存 - 减少锁竞争"""
        result = {}
        now = time.time()
        hits = 0

        for key in keys:
            expiry = self._cache_ttl.get(key)
            if expiry and now < expiry:
                result[key] = self._cache.get(key)
                hits += 1

        # 批量更新统计
        async with self._stats_lock:
            self._hits += hits
            self._misses += len(keys) - hits

        return result

    async def set(self, key: str, value: Any, ttl: int = None):
        """设置缓存值"""
        if ttl is None:
            ttl = self.default_ttl

        async with self._write_lock:
            self._cache[key] = value
            self._cache_ttl[key] = time.time() + ttl

    async def set_many(self, items: Dict[str, Any], ttl: int = None):
        """批量设置缓存"""
        if ttl is None:
            ttl = self.default_ttl

        expiry = time.time() + ttl
        async with self._write_lock:
            for key, value in items.items():
                self._cache[key] = value
                self._cache_ttl[key] = expiry

    async def delete(self, key: str):
        """删除缓存值"""
        async with self._write_lock:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)

    async def delete_many(self, keys: list):
        """批量删除缓存"""
        async with self._write_lock:
            for key in keys:
                self._cache.pop(key, None)
                self._cache_ttl.pop(key, None)

    async def clear_expired(self):
        """清理过期缓存 - 批量操作"""
        now = time.time()
        expired = []

        # 收集过期key（无锁）
        for key, expiry in list(self._cache_ttl.items()):
            if now >= expiry:
                expired.append(key)

        # 批量删除（加锁）
        if expired:
            async with self._write_lock:
                for key in expired:
                    self._cache.pop(key, None)
                    self._cache_ttl.pop(key, None)

            logger.info(f"🧹 清理了 {len(expired)} 个过期缓存")

    async def get_or_set(self, key: str, factory, ttl: int = None) -> Any:
        """获取缓存，如果不存在则通过factory创建（带击穿保护）"""
        # 先尝试获取
        value = await self.get(key)
        if value is not None:
            return value

        # 检查是否正在加载
        async with self._write_lock:
            if key in self._loading:
                # 等待其他协程加载完成
                event = self._loading[key]
                async with self._write_lock:
                    pass  # 释放写锁
                await event.wait()
                return await self.get(key)

            # 标记正在加载
            self._loading[key] = asyncio.Event()

        try:
            # 创建新值
            value = await factory()
            await self.set(key, value, ttl)
            return value
        finally:
            # 加载完成，通知其他等待的协程
            async with self._write_lock:
                if key in self._loading:
                    self._loading[key].set()
                    del self._loading[key]

    async def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        async with self._stats_lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0
            hits = self._hits
            misses = self._misses

        return {
            "size": len(self._cache),
            "hits": hits,
            "misses": misses,
            "hit_rate": round(hit_rate * 100, 2),
            "total_operations": hits + misses,
            "memory_estimate": self._estimate_memory(),
            "loading_keys": len(self._loading),
        }

    def _estimate_memory(self) -> str:
        """估算内存使用"""
        approx_size = len(self._cache) * 200
        if approx_size < 1024:
            return f"{approx_size} B"
        elif approx_size < 1024 * 1024:
            return f"{approx_size / 1024:.1f} KB"
        else:
            return f"{approx_size / (1024 * 1024):.1f} MB"


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

        task.add_done_callback(lambda t, n=name: self._tasks.pop(n, None))

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

        expired_messages = [
            msg_id
            for msg_id, timestamp in self._messages.items()
            if current_time - timestamp > self.ttl
        ]
        for msg_id in expired_messages:
            self._messages.pop(msg_id, None)

        if message_id in self._messages:
            return True

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


def handle_database_errors(func):
    """数据库错误处理装饰器"""

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"数据库操作失败 {func.__name__}: {e}")
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
            raise

    return async_wrapper


performance_monitor = PerformanceMonitor()
retry_manager = RetryManager(max_retries=3, base_delay=1.0)
global_cache = GlobalCache(default_ttl=300)
task_manager = TaskManager()
message_deduplicate = MessageDeduplicate(ttl=60)


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


message_deduplicate = message_deduplicate_decorator()
