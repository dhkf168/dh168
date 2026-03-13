# fault_tolerance.py
"""
容错机制模块 - 提供死锁重试、熔断器、看门狗定时器等功能
"""
import asyncio
import time
import logging
import random
from typing import Callable, Any, Optional, Dict
from functools import wraps

logger = logging.getLogger("GroupCheckInBot.FaultTolerance")


# ========== 1. 死锁重试装饰器 ==========


def with_deadlock_retry(max_retries: int = 3, base_delay: float = 0.1):
    """
    死锁自动重试装饰器

    Args:
        max_retries: 最大重试次数
        base_delay: 基础延迟时间（秒）
    """

    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    # 检查是否是死锁错误
                    if is_deadlock_error(e):
                        last_exception = e
                        if attempt == max_retries:
                            break

                        # 指数退避 + 随机抖动
                        delay = base_delay * (2**attempt) * (1 + random.random())
                        logger.warning(
                            f"🔄 检测到死锁，{delay:.2f}秒后第{attempt + 1}次重试 "
                            f"(函数: {func.__name__})"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        # 非死锁错误，直接抛出
                        raise

            logger.error(f"❌ 死锁重试{max_retries}次后失败: {last_exception}")
            raise last_exception

        return async_wrapper

    return decorator


def is_deadlock_error(e: Exception) -> bool:
    """判断是否是死锁错误"""
    error_str = str(e).lower()
    deadlock_keywords = [
        "deadlock detected",
        "deadlock",
        "40p01",  # PostgreSQL死锁错误码
        "deadlock found",
        "deadlock when trying to get lock",
    ]
    return any(keyword in error_str for keyword in deadlock_keywords)


# ========== 2. 熔断器 ==========


class CircuitBreaker:
    """
    熔断器 - 防止雪崩效应

    状态流转:
    CLOSED (关闭) -> OPEN (开启) -> HALF_OPEN (半开) -> CLOSED (关闭)
    """

    # 状态常量
    CLOSED = "CLOSED"  # 关闭状态，正常请求
    OPEN = "OPEN"  # 开启状态，快速失败
    HALF_OPEN = "HALF_OPEN"  # 半开状态，尝试恢复

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 3,
    ):
        """
        Args:
            name: 熔断器名称（用于日志）
            failure_threshold: 失败次数阈值
            recovery_timeout: 恢复超时时间（秒）
            half_open_max_calls: 半开状态最大尝试次数
        """
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls

        # 状态统计
        self.state = self.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0
        self.total_calls = 0
        self.total_failures = 0

        # 半开状态计数器
        self.half_open_calls = 0

        # 锁
        self._lock = asyncio.Lock()

        logger.info(
            f"🛡️ 熔断器 [{name}] 初始化: 阈值={failure_threshold}, 超时={recovery_timeout}s"
        )

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        调用受熔断器保护的函数
        """
        async with self._lock:
            # 检查当前状态
            if self.state == self.OPEN:
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    logger.info(f"🔄 熔断器 [{self.name}] 从 OPEN 切换到 HALF_OPEN")
                    self.state = self.HALF_OPEN
                    self.half_open_calls = 0
                else:
                    # 熔断器开启，快速失败
                    self.total_calls += 1
                    raise Exception(f"熔断器 [{self.name}] 已开启，快速失败")

            elif self.state == self.HALF_OPEN:
                if self.half_open_calls >= self.half_open_max_calls:
                    self.total_calls += 1
                    raise Exception(f"熔断器 [{self.name}] 半开状态限流")

        try:
            # 执行实际函数
            result = await func(*args, **kwargs)

            async with self._lock:
                self.total_calls += 1
                self.success_count += 1

                if self.state == self.HALF_OPEN:
                    self.half_open_calls += 1

                    # 半开状态下连续成功，关闭熔断器
                    if self.half_open_calls >= self.half_open_max_calls:
                        logger.info(f"✅ 熔断器 [{self.name}] 恢复，切换到 CLOSED")
                        self.state = self.CLOSED
                        self.failure_count = 0
                        self.half_open_calls = 0

            return result

        except Exception as e:
            async with self._lock:
                self.total_calls += 1
                self.total_failures += 1
                self.failure_count += 1
                self.last_failure_time = time.time()

                # 判断是否需要开启熔断器
                if (
                    self.state == self.CLOSED
                    and self.failure_count >= self.failure_threshold
                ):
                    logger.warning(f"⚠️ 熔断器 [{self.name}] 触发，切换到 OPEN")
                    self.state = self.OPEN

                elif self.state == self.HALF_OPEN:
                    logger.warning(f"⚠️ 熔断器 [{self.name}] 半开状态失败，回到 OPEN")
                    self.state = self.OPEN

            raise e

    def get_stats(self) -> Dict[str, Any]:
        """获取熔断器统计信息"""
        return {
            "name": self.name,
            "state": self.state,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "total_calls": self.total_calls,
            "total_failures": self.total_failures,
            "failure_rate": f"{(self.total_failures / max(self.total_calls, 1)) * 100:.1f}%",
            "last_failure": self.last_failure_time,
        }


# ========== 3. 看门狗定时器 ==========


class Watchdog:
    """
    看门狗定时器 - 防止任务卡死

    用法:
        watchdog = Watchdog(timeout=30)
        await watchdog.run(my_coroutine())
    """

    def __init__(self, timeout: float = 30.0, name: str = "unnamed"):
        """
        Args:
            timeout: 超时时间（秒）
            name: 任务名称（用于日志）
        """
        self.timeout = timeout
        self.name = name
        self.last_feed = time.time()
        self._task = None
        self._watchdog_task = None
        self._is_running = False
        self._cancelled = False

    async def run(self, coro):
        """
        运行受看门狗保护的协程
        """
        self._is_running = True
        self.last_feed = time.time()
        self._cancelled = False

        # 创建主任务
        self._task = asyncio.create_task(coro)

        # 创建看门狗监控任务
        self._watchdog_task = asyncio.create_task(self._watch())

        try:
            result = await self._task
            return result
        except asyncio.CancelledError:
            self._cancelled = True
            raise
        finally:
            self._is_running = False
            if self._watchdog_task and not self._watchdog_task.done():
                self._watchdog_task.cancel()
                try:
                    await self._watchdog_task
                except asyncio.CancelledError:
                    pass

    async def _watch(self):
        """看门狗监控循环"""
        while self._is_running and not self._cancelled:
            await asyncio.sleep(1)

            elapsed = time.time() - self.last_feed
            if elapsed > self.timeout:
                logger.error(
                    f"⏰ 看门狗 [{self.name}] 触发超时 "
                    f"(已运行 {elapsed:.1f}秒 > {self.timeout}秒)"
                )
                if self._task and not self._task.done():
                    self._task.cancel()
                break

    def feed(self):
        """喂狗 - 重置计时器"""
        self.last_feed = time.time()

    @classmethod
    async def protect(cls, coro, timeout: float = 30.0, name: str = None):
        """
        类方法：直接保护一个协程

        Example:
            result = await Watchdog.protect(my_coro(), timeout=30)
        """
        watchdog = cls(timeout=timeout, name=name or "protected")
        return await watchdog.run(coro)


# ========== 4. 统一的容错装饰器 ==========


def fault_tolerant(
    max_retries: int = 3,
    deadlock_retry: bool = True,
    circuit_breaker: Optional[CircuitBreaker] = None,
    watchdog_timeout: Optional[float] = None,
    name: Optional[str] = None,
):
    """
    统一的容错装饰器 - 整合死锁重试、熔断器、看门狗

    Args:
        max_retries: 最大重试次数
        deadlock_retry: 是否启用死锁重试
        circuit_breaker: 熔断器实例
        watchdog_timeout: 看门狗超时时间（秒）
        name: 操作名称
    """

    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            operation_name = name or func.__name__

            # 创建重试装饰器
            retry_decorator = with_deadlock_retry(max_retries=max_retries)

            async def execute():
                # 应用重试装饰器
                retry_func = retry_decorator(func)
                return await retry_func(*args, **kwargs)

            # 应用熔断器
            if circuit_breaker:

                async def with_breaker():
                    return await circuit_breaker.call(execute)

                exec_func = with_breaker
            else:
                exec_func = execute

            # 应用看门狗
            if watchdog_timeout:
                return await Watchdog.protect(
                    exec_func(), timeout=watchdog_timeout, name=operation_name
                )
            else:
                return await exec_func()

        return async_wrapper

    return decorator


# ========== 5. 创建全局熔断器实例 ==========

# 数据库操作熔断器
db_circuit_breaker = CircuitBreaker(
    name="database", failure_threshold=5, recovery_timeout=60
)

# Telegram API熔断器
telegram_circuit_breaker = CircuitBreaker(
    name="telegram", failure_threshold=10, recovery_timeout=120
)
