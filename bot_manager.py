import asyncio
import logging
import time
from typing import Optional, Dict, Any
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config import Config

logger = logging.getLogger("GroupCheckInBot")


class RobustBotManager:
    """健壮的Bot管理器 - 带自动重连"""

    def __init__(self, token: str):
        self.token = token
        self.bot: Optional[Bot] = None
        self.dispatcher: Optional[Dispatcher] = None
        self._is_running = False
        self._polling_task: Optional[asyncio.Task] = None
        self._max_retries = 10
        self._base_delay = 2.0
        self._current_retry = 0
        self._last_successful_connection = 0
        self._connection_check_interval = 300  # 5分钟检查一次连接

    async def initialize(self):
        """初始化Bot"""
        self.bot = Bot(token=self.token)
        self.dispatcher = Dispatcher(storage=MemoryStorage())
        logger.info("Bot管理器初始化完成")

    async def start_polling_with_retry(self):
        """带重试的轮询启动"""
        self._is_running = True
        self._current_retry = 0

        while self._is_running and self._current_retry < self._max_retries:
            try:
                self._current_retry += 1
                logger.info(
                    f"🤖 启动Bot轮询 (尝试 {self._current_retry}/{self._max_retries})"
                )

                # 删除webhook确保使用轮询模式
                await self.bot.delete_webhook(drop_pending_updates=True)
                logger.info("✅ Webhook已删除，使用轮询模式")

                # 启动轮询
                await self.dispatcher.start_polling(
                    self.bot,
                    skip_updates=True,
                    allowed_updates=["message", "callback_query", "chat_member"],
                )

                # 如果执行到这里，说明轮询正常结束（不是异常）
                self._last_successful_connection = time.time()
                logger.info("Bot轮询正常结束")
                break

            except asyncio.CancelledError:
                logger.info("Bot轮询被取消")
                break

            except Exception as e:
                logger.error(f"❌ Bot轮询失败 (尝试 {self._current_retry}): {e}")

                if self._current_retry >= self._max_retries:
                    logger.critical(
                        f"🚨 Bot启动重试{self._max_retries}次后失败，停止尝试"
                    )
                    break

                # 指数退避延迟
                delay = self._base_delay * (2 ** (self._current_retry - 1))
                delay = min(delay, 300)  # 最大延迟5分钟

                logger.info(f"⏳ {delay:.1f}秒后第{self._current_retry + 1}次重试...")
                await asyncio.sleep(delay)

    async def stop(self):
        """停止Bot"""
        self._is_running = False

        if self._polling_task and not self._polling_task.done():
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                logger.info("Bot轮询任务已取消")

        if self.bot:
            await self.bot.session.close()
            logger.info("Bot会话已关闭")

    async def send_message_with_retry(self, chat_id: int, text: str, **kwargs) -> bool:
        """带重试的消息发送 - 增强版"""
        max_attempts = 3
        base_delay = 2

        for attempt in range(1, max_attempts + 1):
            try:
                await self.bot.send_message(chat_id, text, **kwargs)
                return True

            except Exception as e:
                error_msg = str(e).lower()

                # 网络相关错误 - 重试
                if any(
                    keyword in error_msg
                    for keyword in [
                        "timeout",
                        "connection",
                        "network",
                        "flood",
                        "retry",
                        "cannot connect",
                        "connectorerror",
                        "ssl",
                        "socket",
                    ]
                ):
                    if attempt == max_attempts:
                        logger.error(f"📤 发送消息重试{max_attempts}次后失败: {e}")
                        return False

                    delay = base_delay * (2 ** (attempt - 1))  # 指数退避
                    delay = min(delay, 30)  # 最大延迟30秒

                    logger.warning(
                        f"📤 发送消息失败(网络问题)，{delay}秒后第{attempt + 1}次重试: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue

                # 权限相关错误 - 不重试
                elif any(
                    keyword in error_msg
                    for keyword in [
                        "forbidden",
                        "blocked",
                        "unauthorized",
                        "chat not found",
                        "bot was blocked",
                        "user is deactivated",
                    ]
                ):
                    logger.warning(f"📤 发送消息失败(权限问题): {e}")
                    return False

                # 其他错误 - 重试
                else:
                    if attempt == max_attempts:
                        logger.error(f"📤 发送消息重试{max_attempts}次后失败: {e}")
                        return False

                    delay = base_delay * attempt
                    logger.warning(
                        f"📤 发送消息失败，{delay}秒后第{attempt + 1}次重试: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue

        return False

    async def send_document_with_retry(
        self, chat_id: int, document, caption: str = "", **kwargs
    ) -> bool:
        """带重试的文档发送"""
        max_attempts = 3

        for attempt in range(1, max_attempts + 1):
            try:
                await self.bot.send_document(
                    chat_id, document, caption=caption, **kwargs
                )
                return True

            except Exception as e:
                error_msg = str(e).lower()

                if any(
                    keyword in error_msg
                    for keyword in [
                        "timeout",
                        "connection",
                        "network",
                        "flood",
                        "retry",
                    ]
                ):
                    if attempt == max_attempts:
                        logger.error(f"📎 发送文档重试{max_attempts}次后失败: {e}")
                        return False

                    delay = attempt * 2
                    logger.warning(
                        f"📎 发送文档失败，{delay}秒后第{attempt + 1}次重试: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"📎 发送文档失败（不重试）: {e}")
                    return False

        return False

    async def get_chat_with_retry(self, chat_id: int, **kwargs):
        """带重试的获取聊天信息"""
        max_attempts = 2

        for attempt in range(1, max_attempts + 1):
            try:
                return await self.bot.get_chat(chat_id, **kwargs)
            except Exception as e:
                if attempt == max_attempts:
                    logger.error(f"获取聊天信息重试{max_attempts}次后失败: {e}")
                    raise

                logger.warning(f"获取聊天信息失败，{attempt}秒后重试: {e}")
                await asyncio.sleep(attempt)

    def is_healthy(self) -> bool:
        """检查Bot健康状态"""
        if not self._last_successful_connection:
            return False

        time_since_last_success = time.time() - self._last_successful_connection
        return time_since_last_success < self._connection_check_interval

    async def restart_polling(self):
        """重启轮询"""
        logger.info("🔄 重启Bot轮询...")
        await self.stop()
        await asyncio.sleep(2)
        await self.start_polling_with_retry()

    async def start_health_monitor(self):
        """启动健康监控"""
        asyncio.create_task(self._health_monitor_loop())

    async def _health_monitor_loop(self):
        """健康监控循环"""
        while self._is_running:
            try:
                await asyncio.sleep(60)  # 每分钟检查一次

                # 检查连接健康
                if not self.is_healthy():
                    logger.warning("Bot连接不健康，尝试重启...")
                    await self.restart_polling()

            except Exception as e:
                logger.error(f"健康监控异常: {e}")
                await asyncio.sleep(30)

    async def send_message_with_retry_emergency(
        self, chat_id: int, text: str, **kwargs
    ) -> bool:
        """紧急消息发送 - 超时缩短"""
        max_attempts = 2  # 减少重试次数
        base_delay = 1

        for attempt in range(1, max_attempts + 1):
            try:
                # 设置短超时
                async with asyncio.timeout(10):  # 10秒超时
                    await self.bot.send_message(chat_id, text, **kwargs)
                return True
            except asyncio.TimeoutError:
                logger.warning(f"📤 发送消息超时 (尝试 {attempt}/{max_attempts})")
                if attempt == max_attempts:
                    return False
            except Exception as e:
                error_msg = str(e).lower()

                # 只重试网络错误
                if any(
                    keyword in error_msg
                    for keyword in ["timeout", "connection", "network"]
                ):
                    if attempt == max_attempts:
                        logger.error(f"📤 发送消息重试{max_attempts}次后失败: {e}")
                        return False

                    delay = base_delay * attempt
                    await asyncio.sleep(delay)
                    continue
                else:
                    # 其他错误不重试
                    logger.warning(f"📤 发送消息失败(不重试): {e}")
                    return False

        return False


# 全局Bot管理器实例
bot_manager = RobustBotManager(Config.TOKEN)
