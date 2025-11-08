# notification_recovery.py - 遗漏通知恢复管理器（安全注入 pool 版本）
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
from config import Config, beijing_tz
from database import db

logger = logging.getLogger("GroupCheckInBot")


class NotificationRecoveryManager:
    """遗漏通知恢复管理器"""

    def __init__(self):
        self.enabled = True
        self.recovery_window_minutes = 30  # 恢复最近30分钟内的遗漏通知
        self._recovery_in_progress = False
        self.pool = None  # 数据库连接池，启动时注入

    def set_pool(self, pool):
        """注入数据库连接池"""
        if pool is None:
            raise ValueError("数据库连接池不可为 None")
        self.pool = pool

    async def initialize(self):
        """初始化恢复管理器"""
        if not self.enabled:
            return
        if self.pool is None:
            logger.error("❌ 初始化失败：数据库连接池未注入")
            return

        try:
            await self._create_notification_tables()
            logger.info("✅ 通知恢复管理器初始化完成")
        except Exception as e:
            logger.error(f"❌ 通知恢复管理器初始化失败: {e}")

    async def _create_notification_tables(self):
        """创建通知状态表"""
        if self.pool is None:
            logger.error("❌ 数据库连接池未初始化，无法创建通知表")
            return

        async with self.pool.acquire() as conn:
            # 通知状态表
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS notification_states (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    activity_name TEXT,
                    notification_type TEXT,
                    scheduled_time TIMESTAMP,
                    sent_time TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 通知历史表
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS notification_history (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    activity_name TEXT,
                    notification_type TEXT,
                    scheduled_time TIMESTAMP,
                    actual_sent_time TIMESTAMP,
                    recovery_sent BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 索引
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_notification_states_pending 
                ON notification_states (status, scheduled_time)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_notification_states_user 
                ON notification_states (chat_id, user_id, activity_name)
            """)

    async def schedule_notification(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        notification_type: str,
        scheduled_time: datetime,
    ):
        """调度通知"""
        if not self.enabled or self.pool is None:
            return

        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO notification_states 
                    (chat_id, user_id, activity_name, notification_type, scheduled_time, status)
                    VALUES ($1, $2, $3, $4, $5, 'pending')
                    ON CONFLICT (chat_id, user_id, activity_name, notification_type) 
                    DO UPDATE SET 
                        scheduled_time = EXCLUDED.scheduled_time,
                        status = 'pending',
                        updated_at = CURRENT_TIMESTAMP
                """, chat_id, user_id, activity, notification_type, scheduled_time)
        except Exception as e:
            logger.error(f"❌ 调度通知失败: {e}")

    async def mark_notification_sent(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        notification_type: str,
        actual_sent_time: datetime = None,
    ):
        """标记通知已发送"""
        if not self.enabled or self.pool is None:
            return

        try:
            sent_time = actual_sent_time or datetime.now(beijing_tz)
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    result = await conn.execute("""
                        UPDATE notification_states 
                        SET status = 'sent', sent_time = $1, updated_at = CURRENT_TIMESTAMP
                        WHERE chat_id = $2 AND user_id = $3 AND activity_name = $4 AND notification_type = $5
                    """, sent_time, chat_id, user_id, activity, notification_type)

                    if "UPDATE 1" in result:
                        await conn.execute("""
                            INSERT INTO notification_history 
                            (chat_id, user_id, activity_name, notification_type, scheduled_time, actual_sent_time)
                            SELECT chat_id, user_id, activity_name, notification_type, scheduled_time, $1
                            FROM notification_states 
                            WHERE chat_id = $2 AND user_id = $3 AND activity_name = $4 AND notification_type = $5
                        """, sent_time, chat_id, user_id, activity, notification_type)

        except Exception as e:
            logger.error(f"❌ 标记通知已发送失败: {e}")

    async def get_pending_notifications(self, recovery_window_minutes: int = None) -> List[Dict[str, Any]]:
        """获取待处理通知"""
        if not self.enabled or self.pool is None:
            return []

        window = recovery_window_minutes or self.recovery_window_minutes
        cutoff_time = datetime.now(beijing_tz) - timedelta(minutes=window)

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT * FROM notification_states 
                    WHERE status = 'pending' AND scheduled_time >= $1
                    ORDER BY scheduled_time ASC
                """, cutoff_time)
                return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"❌ 获取待处理通知失败: {e}")
            return []

    async def recover_missed_notifications(self):
        """恢复遗漏通知"""
        if not self.enabled or self._recovery_in_progress or self.pool is None:
            return

        self._recovery_in_progress = True
        try:
            logger.info("🔍 开始检查遗漏通知...")
            pending_notifications = await self.get_pending_notifications()
            if not pending_notifications:
                logger.info("✅ 没有发现遗漏通知")
                return

            recovery_count = 0
            current_time = datetime.now(beijing_tz)

            for notification in pending_notifications:
                try:
                    scheduled_time = notification['scheduled_time']
                    time_diff = (current_time - scheduled_time).total_seconds() / 60
                    if 0 <= time_diff <= self.recovery_window_minutes:
                        success = await self._send_recovery_notification(notification)
                        if success:
                            recovery_count += 1
                            await self.mark_notification_sent(
                                notification['chat_id'],
                                notification['user_id'],
                                notification['activity_name'],
                                notification['notification_type'],
                                current_time
                            )
                            await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"❌ 恢复通知失败 {notification}: {e}")

            logger.info(f"✅ 成功恢复 {recovery_count}/{len(pending_notifications)} 个遗漏通知")

        except Exception as e:
            logger.error(f"❌ 恢复遗漏通知过程失败: {e}")
        finally:
            self._recovery_in_progress = False

    async def _send_recovery_notification(self, notification: Dict[str, Any]) -> bool:
        """发送恢复通知"""
        try:
            from main import bot, MessageFormatter
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

            chat_id = notification['chat_id']
            user_id = notification['user_id']
            activity = notification['activity_name']
            notification_type = notification['notification_type']

            user_data = await db.get_user_cached(chat_id, user_id)
            if not user_data:
                return False
            nickname = user_data.get('nickname', str(user_id))

            if notification_type == "1min_warning":
                message = (
                    f"🔄 <b>系统恢复提醒</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(user_id, nickname)}\n"
                    f"⏰ 您本次 {MessageFormatter.format_copyable_text(activity)} 还有 <code>1</code> 分钟即将超时！\n"
                    f"💡 请及时回座，避免超时罚款\n"
                    f"📝 <i>（系统恢复后自动补发）</i>"
                )
            elif notification_type == "timeout_immediate":
                message = (
                    f"🔄 <b>系统恢复提醒</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(user_id, nickname)}\n"
                    f"❌ 您的 {MessageFormatter.format_copyable_text(activity)} 已经<code>超时</code>！\n"
                    f"💢 请立即回座，避免产生更多罚款！\n"
                    f"📝 <i>（系统恢复后自动补发）</i>"
                )
            elif notification_type == "timeout_5min":
                message = (
                    f"🔄 <b>系统恢复提醒</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(user_id, nickname)}\n"
                    f"❌ 您的 {MessageFormatter.format_copyable_text(activity)} 已经超时 <code>5</code> 分钟！\n"
                    f"💢 请立即回座，避免罚款增加！\n"
                    f"📝 <i>（系统恢复后自动补发）</i>"
                )
            else:
                message = (
                    f"🔄 <b>系统恢复提醒</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(user_id, nickname)}\n"
                    f"⚠️ 您的 {MessageFormatter.format_copyable_text(activity)} 已超时！\n"
                    f"💡 请及时回座\n"
                    f"📝 <i>（系统恢复后自动补发）</i>"
                )

            back_keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="👉 点击✅立即回座 👈",
                        callback_data=f"quick_back:{chat_id}:{user_id}"
                    )
                ]]
            )

            await bot.send_message(chat_id, message, parse_mode="HTML", reply_markup=back_keyboard)
            logger.info(f"✅ 已补发遗漏通知: 用户{user_id} 活动{activity} 类型{notification_type}")
            return True
        except Exception as e:
            logger.error(f"❌ 发送恢复通知失败: {e}")
            return False

    async def cleanup_old_notifications(self, days: int = 7):
        """清理旧通知记录"""
        if not self.enabled or self.pool is None:
            return

        try:
            cutoff_date = datetime.now(beijing_tz) - timedelta(days=days)
            async with self.pool.acquire() as conn:
                await conn.execute("DELETE FROM notification_history WHERE created_at < $1", cutoff_date)
                await conn.execute("DELETE FROM notification_states WHERE status = 'sent' AND updated_at < $1", cutoff_date)
            logger.info(f"✅ 已清理 {days} 天前的通知记录")
        except Exception as e:
            logger.error(f"❌ 清理通知记录失败: {e}")


# 全局通知恢复管理器实例
notification_recovery_manager = NotificationRecoveryManager()
