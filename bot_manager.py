import asyncio
import logging
import time
import os
import sys
import fcntl
from typing import Optional, Dict, Any
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config import Config

from fault_tolerance import telegram_circuit_breaker

logger = logging.getLogger("GroupCheckInBot")


class RobustBotManager:
    """健壮的Bot管理器 - 带自动重连"""

    def __init__(self, token: str):

        # 1. Render 环境实例检查：防止云平台蓝绿部署时产生两个活跃实例
        if os.environ.get("RENDER"):
            render_instance_index = os.environ.get("RENDER_INSTANCE_INDEX", "0")

            # 只允许主实例（index=0）运行，非主实例直接优雅退出
            if render_instance_index != "0":
                print(f"⏭️ Render 非主实例 (index={render_instance_index})，退出")
                time.sleep(3)
                sys.exit(0)

            print(f"✅ Render 主实例 (index=0) 继续运行")

        # 2. 进程锁检查（基于 Unix 的文件锁，防止同一台机器启动多个进程）
        lock_file = "/tmp/bot_instance.lock"
        try:
            self.lock_fd = open(lock_file, "w")
            # 使用非阻塞互斥锁
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.lock_fd.write(str(os.getpid()))
            self.lock_fd.flush()
            print(f"✅ 获取进程锁成功，PID: {os.getpid()}")
        except (IOError, OSError):
            # 如果锁已被占用，尝试读取持有锁的 PID 并退出
            try:
                with open(lock_file, "r") as f:
                    existing_pid = f.read().strip()
                print(f"❌ 另一个机器人实例正在运行 (PID: {existing_pid})！退出...")
            except:
                print("❌ 另一个机器人实例正在运行！退出...")

            # Render 环境下退出前稍作等待，防止平台重启循环过快
            if os.environ.get("RENDER"):
                time.sleep(5)
            sys.exit(1)
        # ===== 添加结束 =====

        self.token = token
        self.bot: Optional[Bot] = None
        self.dispatcher: Optional[Dispatcher] = None
        self._is_running = False
        self._polling_task: Optional[asyncio.Task] = None
        self._max_retries = 10
        self._base_delay = 2.0
        self._current_retry = 0
        self._last_successful_connection = 0
        self._connection_check_interval = 300

    def __del__(self):
        """析构时释放锁（安全网）"""
        if hasattr(self, "lock_fd") and self.lock_fd:
            try:
                fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
                self.lock_fd.close()
            except:
                pass

    async def initialize(self):
        """初始化Bot"""
        self.bot = Bot(token=self.token)
        self.dispatcher = Dispatcher(storage=MemoryStorage())
        logger.info("Bot管理器初始化完成")

    async def start_polling_with_retry(self):
        """带重试的轮询启动（增强锁监控与多实例检测版）"""

        # ===== 1. 启动前置检查：验证进程锁是否依然有效 =====
        if hasattr(self, "lock_fd") and self.lock_fd:
            try:
                # 通过同步文件描述符状态检查文件是否仍处于有效打开状态
                os.fsync(self.lock_fd.fileno())
                logger.debug("✅ 进程锁状态正常")
            except Exception as e:
                logger.error(f"❌ 进程锁已失效: {e}，退出以防止多开")
                # 尝试安全释放旧资源
                try:
                    import fcntl

                    fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
                    self.lock_fd.close()
                except:
                    pass
                sys.exit(1)
        else:
            logger.error("❌ 进程锁不存在，拒绝启动")
            sys.exit(1)
        # ===== 检查结束 =====

        self._is_running = True
        self._current_retry = 0

        while self._is_running and self._current_retry < self._max_retries:
            try:
                self._current_retry += 1
                logger.info(
                    f"🤖 启动 Bot 轮询 (尝试 {self._current_retry}/{self._max_retries})"
                )

                # 2. 轮询循环前的“最后一公里”锁状态确认
                if hasattr(self, "lock_fd") and self.lock_fd:
                    try:
                        os.fsync(self.lock_fd.fileno())
                    except Exception as e:
                        logger.error(f"❌ 轮询前检测到锁失效: {e}，退出")
                        sys.exit(1)

                # 3. 清理环境：删除可能存在的 Webhook，防止轮询冲突
                await self.bot.delete_webhook(drop_pending_updates=True)
                logger.info("✅ Webhook 已删除，切换至长轮询模式")

                # 4. 进入 aiogram/框架轮询逻辑
                await self.dispatcher.start_polling(
                    self.bot,
                    skip_updates=True,
                    allowed_updates=["message", "callback_query", "chat_member"],
                )

                self._last_successful_connection = time.time()
                logger.info("Bot 轮询正常结束")
                break

            except asyncio.CancelledError:
                # 外部调用 stop() 时抛出，属于预期内的停止
                logger.info("Bot 轮询由于任务取消而停止")
                break

            except Exception as e:
                logger.error(f"❌ Bot 轮询异常 (尝试 {self._current_retry}): {e}")

                # 5. 核心冲突检测：如果发现 409 错误，说明有另一个实例持有了 Telegram Token
                error_str = str(e).lower()
                if (
                    "conflict" in error_str
                    or "terminated by other getupdates" in error_str
                ):
                    logger.critical(
                        "🚨 [CRITICAL] 检测到外部多实例冲突！为保护账号，本进程立即退出"
                    )
                    sys.exit(1)

                # 6. 重试策略：指数退避
                if self._current_retry >= self._max_retries:
                    logger.critical(
                        f"🚨 Bot 启动重试 {self._max_retries} 次后全部失败，放弃连接"
                    )
                    break

                # 计算退避时间：2, 4, 8, 16... 最大 300秒
                delay = self._base_delay * (2 ** (self._current_retry - 1))
                delay = min(delay, 300)

                logger.info(
                    f"⏳ {delay:.1f} 秒后开始第 {self._current_retry + 1} 次重试..."
                )
                await asyncio.sleep(delay)

    async def stop(self):
        """停止 Bot 并释放所有资源"""
        self._is_running = False

        # 1. 取消正在运行的轮询任务
        if self._polling_task and not self._polling_task.done():
            self._polling_task.cancel()
            try:
                # 等待任务完成取消动作，确保清理逻辑执行完毕
                await self._polling_task
            except asyncio.CancelledError:
                logger.info("Bot 轮询任务已取消")

        # 2. 关闭 aiohttp 会话，释放底层网络连接
        if self.bot:
            await self.bot.session.close()
            logger.info("Bot 会话已关闭")

        # ===== 3. 释放进程锁：允许其他实例立即启动 =====
        if hasattr(self, "lock_fd") and self.lock_fd:
            try:
                import fcntl

                # 执行 LOCK_UN 操作解除锁定
                fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
                self.lock_fd.close()
                logger.info("✅ 进程锁已释放")
            except Exception as e:
                logger.error(f"❌ 释放锁失败: {e}")
        # ===== 释放结束 =====

    async def send_message_with_retry(self, chat_id: int, text: str, **kwargs) -> bool:
        """带重试的消息发送"""
        max_attempts = 3
        base_delay = 2

        for attempt in range(1, max_attempts + 1):
            try:
                await self.bot.send_message(chat_id, text, **kwargs)
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
                        "cannot connect",
                        "connectorerror",
                        "ssl",
                        "socket",
                    ]
                ):
                    if attempt == max_attempts:
                        logger.error(f"📤 发送消息重试{max_attempts}次后失败: {e}")
                        return False

                    delay = base_delay * (2 ** (attempt - 1))
                    delay = min(delay, 30)

                    logger.warning(
                        f"📤 发送消息失败(网络问题)，{delay}秒后第{attempt + 1}次重试: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue

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

    async def send_message_with_protection(
        self, chat_id: int, text: str, **kwargs
    ) -> bool:
        """
        带熔断器保护的消息发送

        使用熔断器防止Telegram API故障导致雪崩效应

        Args:
            chat_id: 目标聊天ID
            text: 消息文本
            **kwargs: 其他参数 (parse_mode, reply_markup等)

        Returns:
            bool: 是否发送成功
        """

        async def _send():
            """实际发送函数"""
            try:
                await self.bot.send_message(chat_id, text, **kwargs)
                return True
            except Exception as e:
                logger.error(f"❌ 发送消息失败: {e}")
                raise  # 重新抛出异常，让熔断器捕获

        try:
            return await telegram_circuit_breaker.call(_send)
        except Exception as e:
            logger.error(f"❌ 熔断器保护的消息发送失败: {e}")
            return False

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
                await asyncio.sleep(60)

                if not self.is_healthy():
                    logger.warning("Bot连接不健康，尝试重启...")
                    await self.restart_polling()

            except Exception as e:
                logger.error(f"健康监控异常: {e}")
                await asyncio.sleep(30)


bot_manager = RobustBotManager(Config.TOKEN)
