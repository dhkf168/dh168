# retry_decorator.py
"""
重试装饰器 - 专门为换班执行设计
"""
import asyncio
import logging
import traceback
from functools import wraps
from typing import Type, Tuple, Optional, Callable, Any
from datetime import datetime

from config import beijing_tz

logger = logging.getLogger("GroupCheckInBot.RetryDecorator")


class RetryableError(Exception):
    """可重试的异常基类"""
    pass


class NonRetryableError(Exception):
    """不可重试的异常基类"""
    pass


def with_handover_retry(
    max_retries: int = 3,
    base_delay: float = 5,
    max_delay: float = 300,
    retryable_exceptions: Tuple[Type[Exception], ...] = (
        ConnectionError,
        TimeoutError,
        asyncio.TimeoutError,
        RetryableError,
    ),
    non_retryable_exceptions: Tuple[Type[Exception], ...] = (
        ValueError,
        TypeError,
        NonRetryableError,
    ),
    on_retry: Optional[Callable] = None,
    on_failure: Optional[Callable] = None
):
    """
    换班执行专用重试装饰器
    
    Args:
        max_retries: 最大重试次数
        base_delay: 基础延迟（秒）
        max_delay: 最大延迟（秒）
        retryable_exceptions: 可重试的异常类型
        non_retryable_exceptions: 不可重试的异常类型（直接抛出）
        on_retry: 重试前的回调函数
        on_failure: 最终失败后的回调函数
    """
    
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            retry_count = 0
            
            # 获取执行上下文信息（用于日志）
            context = {
                'function': func.__name__,
                'time': datetime.now(beijing_tz).isoformat()
            }
            
            # 尝试从参数中获取群组信息
            for arg in args:
                if hasattr(arg, 'get') and callable(getattr(arg, 'get', None)):
                    if 'chat_id' in arg:
                        context['chat_id'] = arg.get('chat_id')
                    if 'target_date' in arg:
                        context['target_date'] = arg.get('target_date')
            
            while retry_count <= max_retries:
                try:
                    if retry_count > 0:
                        logger.info(
                            f"🔄 [{context.get('chat_id', '?')}] "
                            f"第{retry_count}次重试 {func.__name__}"
                        )
                    
                    return await func(*args, **kwargs)
                    
                except non_retryable_exceptions as e:
                    # 不可重试的异常，直接抛出
                    logger.error(
                        f"❌ [{context.get('chat_id', '?')}] "
                        f"不可重试错误: {type(e).__name__}: {e}"
                    )
                    raise
                    
                except retryable_exceptions as e:
                    last_exception = e
                    
                    if retry_count >= max_retries:
                        logger.error(
                            f"❌ [{context.get('chat_id', '?')}] "
                            f"达到最大重试次数 {max_retries}"
                        )
                        break
                    
                    # 计算延迟时间（指数退避）
                    delay = min(base_delay * (2 ** retry_count), max_delay)
                    
                    logger.warning(
                        f"⚠️ [{context.get('chat_id', '?')}] "
                        f"{func.__name__} 失败 ({type(e).__name__}), "
                        f"{delay}秒后第{retry_count + 1}次重试"
                    )
                    
                    # 执行重试前回调
                    if on_retry:
                        try:
                            await on_retry(context, retry_count + 1, delay)
                        except Exception as cb_e:
                            logger.error(f"重试回调失败: {cb_e}")
                    
                    retry_count += 1
                    await asyncio.sleep(delay)
                    
                except Exception as e:
                    # 其他未知异常
                    logger.error(
                        f"❌ [{context.get('chat_id', '?')}] "
                        f"未知错误: {type(e).__name__}: {e}"
                    )
                    logger.error(traceback.format_exc())
                    raise
            
            # 执行最终失败回调
            if on_failure:
                try:
                    await on_failure(context, last_exception, retry_count)
                except Exception as cb_e:
                    logger.error(f"失败回调错误: {cb_e}")
            
            raise last_exception or Exception("重试耗尽但无异常信息")
        
        return wrapper
    return decorator


def with_execution_phase(phase_name: str):
    """
    执行阶段装饰器 - 用于标记换班执行的各个阶段
    
    配合重试管理器使用，记录每个阶段的执行状态
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # 尝试从参数中获取 snapshot_id
            snapshot_id = None
            for arg in args:
                if isinstance(arg, str) and arg.startswith('handover_'):
                    snapshot_id = arg
                    break
            
            if snapshot_id:
                from recovery import recovery_manager
                logger.debug(f"📝 执行阶段 [{phase_name}] 开始: {snapshot_id}")
                
                try:
                    result = await func(*args, **kwargs)
                    logger.debug(f"✅ 执行阶段 [{phase_name}] 完成: {snapshot_id}")
                    return result
                except Exception as e:
                    logger.error(f"❌ 执行阶段 [{phase_name}] 失败: {snapshot_id} - {e}")
                    raise
            else:
                # 没有 snapshot_id，直接执行
                return await func(*args, **kwargs)
        
        return wrapper
    return decorator