import os
import time
import asyncio
import logging
import gc
import psutil

from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Tuple
from config import Config, beijing_tz
from functools import wraps
from aiogram import types
from database import db
from performance import global_cache, task_manager
from datetime import time as dt_time


logger = logging.getLogger("GroupCheckInBot")


class MessageFormatter:
    """消息格式化工具类"""

    @staticmethod
    def format_time(seconds: int) -> str:
        """格式化时间显示"""
        if seconds is None:
            return "0秒"

        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)

        if h > 0:
            return f"{h}小时{m}分{s}秒"
        elif m > 0:
            return f"{m}分{s}秒"
        else:
            return f"{s}秒"

    @staticmethod
    def format_time_for_csv(seconds: int) -> str:
        """为CSV导出格式化时间显示"""
        if seconds is None:
            return "0分0秒"

        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours}时{minutes}分{secs}秒"
        else:
            return f"{minutes}分{secs}秒"

    @staticmethod
    def format_user_link(user_id: int, user_name: str) -> str:
        """格式化用户链接"""
        if not user_name:
            user_name = f"用户{user_id}"
        clean_name = (
            str(user_name)
            .replace("<", "")
            .replace(">", "")
            .replace("&", "")
            .replace('"', "")
        )
        return f'<a href="tg://user?id={user_id}">{clean_name}</a>'

    @staticmethod
    def create_dashed_line() -> str:
        """创建短虚线分割线"""
        return MessageFormatter.format_copyable_text("--------------------------")

    @staticmethod
    def format_copyable_text(text: str) -> str:
        """格式化可复制文本"""
        return f"<code>{text}</code>"

    @staticmethod
    def format_activity_message(
        user_id: int,
        user_name: str,
        activity: str,
        time_str: str,
        count: int,
        max_times: int,
        time_limit: int,
        shift: str = None,
    ) -> str:
        """格式化打卡消息"""

        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"
        dashed_line = MessageFormatter.create_dashed_line()

        message = (
            f"{first_line}\n"
            f"✅ 打卡成功：{MessageFormatter.format_copyable_text(activity)} - {MessageFormatter.format_copyable_text(time_str)}\n"
        )

        if shift:
            shift_text = "白班" if shift == "day" else "夜班"
            message += f"📊 班次：{MessageFormatter.format_copyable_text(shift_text)}\n"

        message += (
            f"▫️ 本次活动类型：{MessageFormatter.format_copyable_text(activity)}\n"
            f"⏰ 单次时长限制：{MessageFormatter.format_copyable_text(str(time_limit))}分钟 \n"
            f"📈 今日{MessageFormatter.format_copyable_text(activity)}次数：第 {MessageFormatter.format_copyable_text(str(count))} 次（上限 {MessageFormatter.format_copyable_text(str(max_times))} 次）\n"
        )

        if count >= max_times:
            message += f"🚨 警告：本次结束后，您今日的{MessageFormatter.format_copyable_text(activity)}次数将达到上限，请留意！\n"

        message += (
            f"{dashed_line}\n"
            f"💡 操作提示\n"
            f"活动结束后请及时点击 👉【✅ 回座】👈按钮。"
        )

        return message

    @staticmethod
    def format_back_message(
        user_id: int,
        user_name: str,
        activity: str,
        time_str: str,
        elapsed_time: str,
        total_activity_time: str,
        total_time: str,
        activity_counts: dict,
        total_count: int,
        is_overtime: bool = False,
        overtime_seconds: int = 0,
        fine_amount: int = 0,
    ) -> str:
        """格式化回座消息"""
        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"
        dashed_line = MessageFormatter.create_dashed_line()

        today_count = activity_counts.get(activity, 0)

        message = (
            f"{first_line}\n"
            f"✅ 回座打卡：{MessageFormatter.format_copyable_text(time_str)}\n"
            f"{dashed_line}\n"
            f"📍 活动记录\n"
            f"▫️ 活动类型：{MessageFormatter.format_copyable_text(activity)}\n"
            f"▫️ 本次耗时：{MessageFormatter.format_copyable_text(elapsed_time)} ⏰\n"
            f"▫️ 累计时长：{MessageFormatter.format_copyable_text(total_activity_time)}\n"
            f"▫️ 今日次数：{MessageFormatter.format_copyable_text(str(today_count))}次\n"
        )

        if is_overtime:
            overtime_time = MessageFormatter.format_time(int(overtime_seconds))
            message += f"\n⚠️ 超时提醒\n"
            message += f"▫️ 超时时长：{MessageFormatter.format_copyable_text(overtime_time)} 🚨\n"
            if fine_amount > 0:
                message += f"▫️ 罚款金额：{MessageFormatter.format_copyable_text(str(fine_amount))} 泰铢 💸\n"

        message += f"{dashed_line}\n"
        message += f"📊 今日总计\n"
        message += f"▫️ 活动详情\n"

        for act, count in activity_counts.items():
            if count > 0:
                message += f"   ➤ {MessageFormatter.format_copyable_text(act)}：{MessageFormatter.format_copyable_text(str(count))} 次 📝\n"

        message += f"▫️ 总活动次数：{MessageFormatter.format_copyable_text(str(total_count))}次\n"
        message += f"▫️ 总活动时长：{MessageFormatter.format_copyable_text(total_time)}"

        return message

    @staticmethod
    def format_duration(seconds: int) -> str:
        seconds = int(seconds)

        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60

        parts = []

        if h > 0:
            parts.append(f"{h}小时")

        if m > 0:
            parts.append(f"{m}分钟")

        if s > 0:
            parts.append(f"{s}秒")

        if not parts:
            return "0分钟"

        return "".join(parts)


class NotificationService:
    """统一推送服务"""

    def __init__(self, bot_manager=None):
        self.bot_manager = bot_manager
        self.bot = None
        self._last_notification_time = {}
        self._rate_limit_window = 60

    async def send_notification(
        self, chat_id: int, text: str, notification_type: str = "all"
    ):
        """发送通知到绑定的频道和群组"""
        if not self.bot_manager and not self.bot:
            logger.warning("NotificationService: bot_manager 和 bot 都未初始化")
            return False

        notification_key = f"{chat_id}:{hash(text)}"
        current_time = time.time()
        if (
            notification_key in self._last_notification_time
            and current_time - self._last_notification_time[notification_key]
            < self._rate_limit_window
        ):
            logger.debug(f"跳过重复通知: {notification_key}")
            return True

        sent = False
        push_settings = await db.get_push_settings()

        group_data = await db.get_group_cached(chat_id)

        if self.bot_manager and hasattr(self.bot_manager, "send_message_with_retry"):
            sent = await self._send_with_bot_manager(
                chat_id, text, group_data, push_settings
            )
        elif self.bot:
            sent = await self._send_with_bot(chat_id, text, group_data, push_settings)

        if sent:
            self._last_notification_time[notification_key] = current_time

        return sent

    async def _send_with_bot_manager(
        self, chat_id: int, text: str, group_data: dict, push_settings: dict
    ) -> bool:
        """使用 bot_manager 发送通知"""
        sent = False

        if (
            push_settings.get("enable_channel_push")
            and group_data
            and group_data.get("channel_id")
        ):
            try:
                success = await self.bot_manager.send_message_with_retry(
                    group_data["channel_id"], text, parse_mode="HTML"
                )
                if success:
                    sent = True
                    logger.info(f"✅ 已发送到频道: {group_data['channel_id']}")
            except Exception as e:
                logger.error(f"❌ 发送到频道失败: {e}")

        if (
            push_settings.get("enable_group_push")
            and group_data
            and group_data.get("notification_group_id")
        ):
            try:
                success = await self.bot_manager.send_message_with_retry(
                    group_data["notification_group_id"], text, parse_mode="HTML"
                )
                if success:
                    sent = True
                    logger.info(
                        f"✅ 已发送到通知群组: {group_data['notification_group_id']}"
                    )
            except Exception as e:
                logger.error(f"❌ 发送到通知群组失败: {e}")

        if not sent and push_settings.get("enable_admin_push"):
            for admin_id in Config.ADMINS:
                try:
                    success = await self.bot_manager.send_message_with_retry(
                        admin_id, text, parse_mode="HTML"
                    )
                    if success:
                        logger.info(f"✅ 已发送给管理员: {admin_id}")
                        sent = True
                        break
                except Exception as e:
                    logger.error(f"❌ 发送给管理员失败: {e}")

        return sent

    async def _send_with_bot(
        self, chat_id: int, text: str, group_data: dict, push_settings: dict
    ) -> bool:
        """直接使用 bot 实例发送通知"""
        sent = False

        if (
            push_settings.get("enable_channel_push")
            and group_data
            and group_data.get("channel_id")
        ):
            try:
                await self.bot.send_message(
                    group_data["channel_id"], text, parse_mode="HTML"
                )
                sent = True
                logger.info(f"✅ 已发送到频道: {group_data['channel_id']}")
            except Exception as e:
                logger.error(f"❌ 发送到频道失败: {e}")

        if (
            push_settings.get("enable_group_push")
            and group_data
            and group_data.get("notification_group_id")
        ):
            try:
                await self.bot.send_message(
                    group_data["notification_group_id"], text, parse_mode="HTML"
                )
                sent = True
                logger.info(
                    f"✅ 已发送到通知群组: {group_data['notification_group_id']}"
                )
            except Exception as e:
                logger.error(f"❌ 发送到通知群组失败: {e}")

        if not sent and push_settings.get("enable_admin_push"):
            for admin_id in Config.ADMINS:
                try:
                    await self.bot.send_message(admin_id, text, parse_mode="HTML")
                    logger.info(f"✅ 已发送给管理员: {admin_id}")
                    sent = True
                    break
                except Exception as e:
                    logger.error(f"❌ 发送给管理员失败: {e}")

        return sent

    async def send_document(self, chat_id: int, document, caption: str = ""):
        """发送文档到绑定的频道和群组"""
        if not self.bot_manager and not self.bot:
            logger.warning("NotificationService: bot_manager 和 bot 都未初始化")
            return False

        sent = False
        push_settings = await db.get_push_settings()
        group_data = await db.get_group_cached(chat_id)

        if self.bot_manager and hasattr(self.bot_manager, "send_document_with_retry"):
            if (
                push_settings.get("enable_channel_push")
                and group_data
                and group_data.get("channel_id")
            ):
                try:
                    success = await self.bot_manager.send_document_with_retry(
                        group_data["channel_id"],
                        document,
                        caption=caption,
                        parse_mode="HTML",
                    )
                    if success:
                        sent = True
                        logger.info(f"✅ 已发送文档到频道: {group_data['channel_id']}")
                except Exception as e:
                    logger.error(f"❌ 发送文档到频道失败: {e}")

            if (
                push_settings.get("enable_group_push")
                and group_data
                and group_data.get("notification_group_id")
            ):
                try:
                    success = await self.bot_manager.send_document_with_retry(
                        group_data["notification_group_id"],
                        document,
                        caption=caption,
                        parse_mode="HTML",
                    )
                    if success:
                        sent = True
                        logger.info(
                            f"✅ 已发送文档到通知群组: {group_data['notification_group_id']}"
                        )
                except Exception as e:
                    logger.error(f"❌ 发送文档到通知群组失败: {e}")

            if not sent and push_settings.get("enable_admin_push"):
                for admin_id in Config.ADMINS:
                    try:
                        success = await self.bot_manager.send_document_with_retry(
                            admin_id, document, caption=caption, parse_mode="HTML"
                        )
                        if success:
                            logger.info(f"✅ 已发送文档给管理员: {admin_id}")
                            sent = True
                            break
                    except Exception as e:
                        logger.error(f"❌ 发送文档给管理员失败: {e}")

        elif self.bot:
            if (
                push_settings.get("enable_channel_push")
                and group_data
                and group_data.get("channel_id")
            ):
                try:
                    await self.bot.send_document(
                        group_data["channel_id"],
                        document,
                        caption=caption,
                        parse_mode="HTML",
                    )
                    sent = True
                    logger.info(f"✅ 已发送文档到频道: {group_data['channel_id']}")
                except Exception as e:
                    logger.error(f"❌ 发送文档到频道失败: {e}")

            if (
                push_settings.get("enable_group_push")
                and group_data
                and group_data.get("notification_group_id")
            ):
                try:
                    await self.bot.send_document(
                        group_data["notification_group_id"],
                        document,
                        caption=caption,
                        parse_mode="HTML",
                    )
                    sent = True
                    logger.info(
                        f"✅ 已发送文档到通知群组: {group_data['notification_group_id']}"
                    )
                except Exception as e:
                    logger.error(f"❌ 发送文档到通知群组失败: {e}")

            if not sent and push_settings.get("enable_admin_push"):
                for admin_id in Config.ADMINS:
                    try:
                        await self.bot.send_document(
                            admin_id, document, caption=caption, parse_mode="HTML"
                        )
                        logger.info(f"✅ 已发送文档给管理员: {admin_id}")
                        sent = True
                        break
                    except Exception as e:
                        logger.error(f"❌ 发送文档给管理员失败: {e}")

        return sent


class UserLockManager:
    """用户锁管理器 - 实用版（适合10个群组）"""

    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._cleanup_interval = 3600
        self._last_cleanup = time.time()
        self._max_locks = 2000
        self._cleanup_task: Optional[asyncio.Task] = None
        self._stats = {"hits": 0, "misses": 0, "cleanups": 0}
        # ❌ 不要在 __init__ 中启动任务
        # self._start_cleanup_task()

    async def start(self):
        """启动清理任务 - 需要在事件循环运行时调用"""
        self._start_cleanup_task()
        logger.info("用户锁管理器清理任务已启动")

    async def get_lock(self, chat_id: int, uid: int) -> asyncio.Lock:
        """获取用户级锁"""
        key = f"{chat_id}-{uid}"

        # 快速路径
        lock = self._locks.get(key)
        if lock is not None:
            self._stats["hits"] += 1
            if hash(key) % 10 == 0:
                asyncio.create_task(self._touch_lock(key))
            return lock

        self._stats["misses"] += 1

        # 慢速路径
        async with self._lock:
            if key not in self._locks:
                if len(self._locks) >= self._max_locks:
                    await self._simple_cleanup()
                self._locks[key] = asyncio.Lock()

            self._access_times[key] = time.time()
            return self._locks[key]

    async def _touch_lock(self, key: str):
        """更新访问时间"""
        try:
            async with self._lock:
                if key in self._locks:
                    self._access_times[key] = time.time()
        except Exception:
            pass

    async def _simple_cleanup(self):
        """简单的清理：移除最旧的100个非活动锁"""
        now = time.time()
        async with self._lock:
            in_use = {k for k, v in self._locks.items() if v.locked()}

            inactive = [
                (k, self._access_times.get(k, 0))
                for k in self._locks.keys()
                if k not in in_use
            ]
            inactive.sort(key=lambda x: x[1])

            removed = 0
            for key, _ in inactive[:100]:
                self._locks.pop(key, None)
                self._access_times.pop(key, None)
                removed += 1

            if removed:
                self._stats["cleanups"] += removed
                logger.info(f"🧹 清理了 {removed} 个旧锁")

    def _start_cleanup_task(self):
        """启动后台清理（内部方法）"""

        async def _cleanup_loop():
            while True:
                try:
                    await asyncio.sleep(3600)
                    async with self._lock:
                        now = time.time()
                        in_use = {k for k, v in self._locks.items() if v.locked()}

                        to_remove = [
                            k
                            for k, last in self._access_times.items()
                            if k not in in_use and now - last > 86400
                        ]

                        for key in to_remove:
                            self._locks.pop(key, None)
                            self._access_times.pop(key, None)

                        if to_remove:
                            self._stats["cleanups"] += len(to_remove)
                            logger.info(f"🧹 后台清理了 {len(to_remove)} 个过期锁")
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"清理任务出错: {e}")
                    await asyncio.sleep(60)

        self._cleanup_task = asyncio.create_task(_cleanup_loop())

    async def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        async with self._lock:
            active = sum(1 for v in self._locks.values() if v.locked())
            total_ops = self._stats["hits"] + self._stats["misses"]
            hit_rate = self._stats["hits"] / total_ops if total_ops > 0 else 0

            return {
                "total_locks": len(self._locks),
                "active_locks": active,
                "idle_locks": len(self._locks) - active,
                "hit_rate": f"{hit_rate*100:.1f}%",
                "total_cleanups": self._stats["cleanups"],
            }

    async def close(self):
        """关闭管理器"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        async with self._lock:
            self._locks.clear()
            self._access_times.clear()
            logger.info("用户锁管理器已关闭")


class ActivityTimerManager:
    """活动定时器管理器"""

    def __init__(self):
        self._timers = {}
        self.active_timers = {}
        self._lock = asyncio.Lock()
        self._cleanup_interval = 300
        self._last_cleanup = time.time()
        self.activity_timer_callback = None

    def set_activity_timer_callback(self, callback):
        """设置活动定时器回调"""
        self.activity_timer_callback = callback

    async def start_timer(
        self,
        chat_id: int,
        uid: int,
        act: str,
        limit: int,
        shift: str = "day",
    ) -> bool:
        """启动活动定时器"""
        timer_key = f"{chat_id}-{uid}-{shift}"

        if timer_key in self.active_timers:
            await self.cancel_timer(timer_key, preserve_message=False)

        if not self.activity_timer_callback:
            logger.error("ActivityTimerManager: 未设置回调函数")
            return False

        timer_task = asyncio.create_task(
            self._activity_timer_wrapper(chat_id, uid, act, limit, shift),
            name=f"timer_{timer_key}",
        )

        self.active_timers[timer_key] = {
            "task": timer_task,
            "activity": act,
            "limit": limit,
            "shift": shift,
            "chat_id": chat_id,
            "uid": uid,
        }

        logger.info(f"⏰ 启动定时器: {timer_key} - {act}（班次: {shift}）")
        return True

    async def cancel_timer(self, timer_key: str, preserve_message: bool = False):
        """取消并清理指定的定时器 - 紧凑优化版"""
        # 获取需要取消的 key 列表（支持前缀匹配，如取消某个群组的所有定时器）
        keys_to_cancel = [
            k for k in self.active_timers.keys() if k.startswith(timer_key)
        ]

        if not keys_to_cancel:
            return 0

        cancelled_count = 0
        tasks_to_cancel = []
        cleanup_tasks = []

        # 第 1 步：在锁保护下收集信息，快速释放锁以提高并发性能
        async with self._lock:
            for key in keys_to_cancel:
                timer_info = self.active_timers.pop(key, None)
                if timer_info:
                    task = timer_info.get("task")
                    if task:
                        tasks_to_cancel.append((key, task))
                        # 如果不需要保留消息，记录需要清理数据库的 chat_id 和 uid
                        if not preserve_message:
                            chat_id = timer_info.get("chat_id")
                            uid = timer_info.get("uid")
                            if chat_id and uid:
                                cleanup_tasks.append((key, chat_id, uid))
                        cancelled_count += 1

        # 第 2 步：批量取消异步任务
        for key, task in tasks_to_cancel:
            # 动态注入属性，告知任务在被取消时是否保留消息状态
            if hasattr(task, "preserve_message"):
                task.preserve_message = preserve_message

            if not task.done():
                task.cancel()
                try:
                    # 等待任务响应取消信号并完成退出
                    await task
                    logger.info(f"⏹️ 定时器任务已取消: {key}")
                except asyncio.CancelledError:
                    logger.info(f"⏹️ 定时器任务已取消: {key}")
                except Exception as e:
                    logger.error(f"❌ 定时器任务取消异常 ({key}): {e}")
            else:
                logger.debug(f"定时器任务已完成: {key}")

        # 第 3 步：批量清理数据库中的消息 ID 记录
        if not preserve_message and cleanup_tasks:
            for key, chat_id, uid in cleanup_tasks:
                try:
                    # 调用数据库接口清理 checkin_message_id，防止消息挂起
                    await db.clear_user_checkin_message(chat_id, uid)
                    logger.debug(f"🧹 定时器消息ID已清理: {key}")
                except Exception as e:
                    logger.error(f"❌ 定时器消息清理异常 ({key}): {e}")

        # 第 4 步：记录最终状态日志
        for key in keys_to_cancel:
            msg = f"🗑️ 定时器已取消: {key}"
            if preserve_message:
                msg += " (保留消息ID)"
            logger.info(msg)

        return cancelled_count

    async def cancel_all_timers(self):
        """取消所有定时器"""
        keys = list(self.active_timers.keys())
        cancelled_count = 0

        for key in keys:
            try:
                await self.cancel_timer(key, preserve_message=False)
                cancelled_count += 1
            except Exception as e:
                logger.error(f"取消定时器 {key} 失败: {e}")

        logger.info(f"已取消所有定时器: {cancelled_count} 个")
        return cancelled_count

    async def cancel_all_timers_for_group(
        self, chat_id: int, preserve_message: bool = False
    ) -> int:
        """取消指定群组的所有定时器"""
        cancelled_count = 0
        prefix = f"{chat_id}-"

        keys_to_cancel = [k for k in self.active_timers.keys() if k.startswith(prefix)]

        for key in keys_to_cancel:
            await self.cancel_timer(key, preserve_message=preserve_message)
            cancelled_count += 1

        if cancelled_count > 0:
            msg = f"🗑️ 已取消群组 {chat_id} 的 {cancelled_count} 个定时器"
            if preserve_message:
                msg += " (保留消息ID)"
            logger.info(msg)

        return cancelled_count

    async def _activity_timer_wrapper(
        self, chat_id: int, uid: int, act: str, limit: int, shift: str
    ):
        """定时器包装器"""
        timer_key = f"{chat_id}-{uid}-{shift}"
        preserve_message = getattr(asyncio.current_task(), "preserve_message", False)

        try:
            from main import activity_timer

            await activity_timer(chat_id, uid, act, limit, shift, preserve_message)
        except asyncio.CancelledError:
            logger.info(f"定时器 {timer_key} 被取消")
            if preserve_message:
                logger.debug(f"⏭️ 被取消的定时器保留消息ID")
        except Exception as e:
            logger.error(f"定时器异常 {timer_key}: {e}")
            import traceback

            logger.error(traceback.format_exc())
        finally:
            self.active_timers.pop(timer_key, None)
            logger.debug(f"已清理定时器: {timer_key}")

    async def cleanup_finished_timers(self):
        """清理已完成定时器"""
        if time.time() - self._last_cleanup < self._cleanup_interval:
            return

        finished_keys = [
            key
            for key, task in self.active_timers.items()
            if task.get("task", None) and task["task"].done()
        ]
        for key in finished_keys:
            self.active_timers.pop(key, None)

        if finished_keys:
            logger.info(f"定时器清理: 移除了 {len(finished_keys)} 个已完成定时器")

        self._last_cleanup = time.time()

    def get_stats(self) -> Dict[str, Any]:
        """获取定时器统计"""
        return {"active_timers": len(self.active_timers)}


class EnhancedPerformanceOptimizer:
    """增强版性能优化器"""

    def __init__(self):
        self.cleanup_interval = 300
        self.last_cleanup = time.time()

        self.is_render = self._detect_render_environment()

        self.render_memory_limit = 400

        logger.info(
            f"🧠 EnhancedPerformanceOptimizer 初始化 - Render 环境: {self.is_render}"
        )

    def _detect_render_environment(self) -> bool:
        """检测是否运行在 Render 环境"""
        if os.environ.get("RENDER"):
            return True

        if "RENDER_EXTERNAL_URL" in os.environ:
            return True

        if os.environ.get("PORT"):
            return True

        return False

    async def memory_cleanup(self):
        """智能内存清理"""
        if self.is_render:
            return await self._render_cleanup()
        else:
            await self._regular_cleanup()
            return None

    async def _render_cleanup(self) -> float:
        """Render 环境专用清理"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024

            logger.debug(f"🔵 Render 内存监测: {memory_mb:.1f} MB")

            if memory_mb > self.render_memory_limit:
                logger.warning(f"🚨 Render 内存过高 {memory_mb:.1f}MB，执行紧急清理")

                stats = await global_cache.get_stats()
                old_cache_size = stats.get("size", 0)
                await global_cache.clear_all()

                await task_manager.cleanup_tasks()

                await db.cleanup_cache()

                collected = gc.collect()

                logger.info(
                    f"🆘 紧急清理完成: 清缓存 {old_cache_size} 项, GC 回收 {collected} 对象"
                )

            return memory_mb

        except Exception as e:
            logger.error(f"Render 内存清理失败: {e}")
            return 0.0

    async def _regular_cleanup(self):
        """普通环境的智能周期清理"""
        try:
            now = time.time()
            if now - self.last_cleanup < self.cleanup_interval:
                return

            logger.debug("🟢 执行周期性内存清理...")

            tasks = [
                task_manager.cleanup_tasks(),
                global_cache.clear_expired(),
                db.cleanup_cache(),
            ]

            await asyncio.gather(*tasks, return_exceptions=True)

            collected = gc.collect()
            if collected > 0:
                logger.info(f"周期清理完成 - GC 回收对象: {collected}")
            else:
                logger.debug("周期清理完成 - 无需要回收的对象")

            self.last_cleanup = now

        except Exception as e:
            logger.error(f"周期清理失败: {e}")

    def memory_usage_ok(self) -> bool:
        """检查内存使用是否正常"""
        try:
            process = psutil.Process()
            memory_percent = process.memory_percent()
            memory_mb = process.memory_info().rss / 1024 / 1024

            if self.is_render:
                return memory_mb < self.render_memory_limit
            else:
                return memory_percent < 80
        except ImportError:
            return True

    def get_memory_info(self) -> dict:
        """获取当前内存信息"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            memory_percent = process.memory_percent()

            return {
                "memory_usage_mb": round(memory_mb, 1),
                "memory_percent": round(memory_percent, 1),
                "is_render": self.is_render,
                "render_memory_limit": self.render_memory_limit,
                "needs_cleanup": (
                    memory_mb > self.render_memory_limit if self.is_render else False
                ),
                "status": "healthy" if self.memory_usage_ok() else "warning",
            }
        except Exception as e:
            logger.error(f"获取内存信息失败: {e}")
            return {"error": str(e)}


class HeartbeatManager:
    """心跳管理器"""

    def __init__(self):
        self._last_heartbeat = time.time()
        self._is_running = False
        self._task = None

    async def initialize(self):
        """初始化心跳管理器"""
        self._is_running = True
        self._task = asyncio.create_task(self._heartbeat_loop())
        logger.info("心跳管理器已初始化")

    async def stop(self):
        """停止心跳管理器"""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("心跳管理器已停止")

    async def _heartbeat_loop(self):
        """心跳循环"""
        while self._is_running:
            try:
                self._last_heartbeat = time.time()
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"心跳循环异常: {e}")
                await asyncio.sleep(10)

    def get_status(self) -> Dict[str, Any]:
        """获取心跳状态"""
        current_time = time.time()
        last_heartbeat_ago = current_time - self._last_heartbeat

        return {
            "is_running": self._is_running,
            "last_heartbeat": self._last_heartbeat,
            "last_heartbeat_ago": last_heartbeat_ago,
            "status": "healthy" if last_heartbeat_ago < 120 else "unhealthy",
        }


class ShiftStateManager:
    """班次状态管理器"""

    def __init__(self):
        self._check_interval = 300
        self._is_running = False
        self._task = None
        self.logger = logging.getLogger("GroupCheckInBot.ShiftStateManager")

    async def start(self):
        """启动清理任务"""
        self._is_running = True
        self._task = asyncio.create_task(self._cleanup_loop())
        self.logger.info("✅ 班次状态管理器已启动")

    async def stop(self):
        """停止清理任务"""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.logger.info("🛑 班次状态管理器已停止")

    async def _cleanup_loop(self):
        """清理循环"""
        while self._is_running:
            try:
                await asyncio.sleep(self._check_interval)

                from database import db

                cleaned_count = await db.cleanup_expired_shift_states()

                if cleaned_count > 0:
                    self.logger.info(f"🧹 自动清理了 {cleaned_count} 个过期班次状态")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"清理循环异常: {e}")
                await asyncio.sleep(60)


def get_beijing_time() -> datetime:
    """获取北京时间"""
    return datetime.now(beijing_tz)


def calculate_cross_day_time_diff(
    current_dt: datetime,
    expected_time: str,
    checkin_type: str,
    record_date: Optional[date] = None,
) -> Tuple[float, int, datetime]:
    """智能化的时间差计算"""
    try:
        expected_hour, expected_minute = map(int, expected_time.split(":"))

        if record_date is None:
            logger.error(f"❌ calculate_cross_day_time_diff 缺少 record_date 参数")
            record_date = current_dt.date()
            logger.warning(f"⚠️ 降级使用今天日期: {record_date}")

        expected_dt = datetime.combine(
            record_date, dt_time(expected_hour, expected_minute)
        ).replace(tzinfo=current_dt.tzinfo)

        logger.debug(
            f"📅 时间差计算 - 使用指定日期: {record_date}, "
            f"期望时间: {expected_dt.strftime('%Y-%m-%d %H:%M')}"
        )

        time_diff_seconds = int((current_dt - expected_dt).total_seconds())
        time_diff_minutes = time_diff_seconds / 60

        return time_diff_minutes, time_diff_seconds, expected_dt

    except Exception as e:
        logger.error(f"时间差计算出错: {e}")
        return 0.0, 0, current_dt


def rate_limit(rate: int = 1, per: int = 1):
    """速率限制装饰器"""

    def decorator(func):
        calls = []

        @wraps(func)
        async def wrapper(*args, **kwargs):
            now = time.time()
            calls[:] = [call for call in calls if now - call < per]

            if len(calls) >= rate:
                if args and isinstance(args[0], types.Message):
                    await args[0].answer("⏳ 操作过于频繁，请稍后再试")
                return

            calls.append(now)
            return await func(*args, **kwargs)

        return wrapper

    return decorator


user_lock_manager = UserLockManager()
timer_manager = ActivityTimerManager()
performance_optimizer = EnhancedPerformanceOptimizer()
heartbeat_manager = HeartbeatManager()
notification_service = NotificationService()
shift_state_manager = ShiftStateManager()


async def send_reset_notification(
    chat_id: int, completion_result: Dict[str, Any], reset_time: datetime
):
    """发送重置通知"""
    try:
        completed_count = completion_result.get("completed_count", 0)
        total_fines = completion_result.get("total_fines", 0)
        details = completion_result.get("details", [])

        if completed_count == 0:
            notification_text = (
                f"🔄 <b>系统重置完成</b>\n"
                f"🏢 群组: <code>{chat_id}</code>\n"
                f"⏰ 重置时间: <code>{reset_time.strftime('%m/%d %H:%M')}</code>\n"
                f"✅ 没有进行中的活动需要结束"
            )
        else:
            notification_text = (
                f"🔄 <b>系统重置完成通知</b>\n"
                f"🏢 群组: <code>{chat_id}</code>\n"
                f"⏰ 重置时间: <code>{reset_time.strftime('%m/%d %H:%M')}</code>\n"
                f"📊 自动结束活动: <code>{completed_count}</code> 个\n"
                f"💰 总罚款金额: <code>{total_fines}</code> 元\n"
            )

            if details:
                notification_text += f"\n📋 <b>活动结束详情:</b>\n"
                for i, detail in enumerate(details[:5], 1):
                    user_link = MessageFormatter.format_user_link(
                        detail["user_id"], detail.get("nickname", "用户")
                    )
                    time_str = MessageFormatter.format_time(detail["elapsed_time"])
                    fine_info = (
                        f" (罚款: {detail['fine_amount']}元)"
                        if detail["fine_amount"] > 0
                        else ""
                    )
                    overtime_info = " ⏰超时" if detail["is_overtime"] else ""

                    notification_text += (
                        f"{i}. {user_link} - {detail['activity']} "
                        f"({time_str}){fine_info}{overtime_info}\n"
                    )

                if len(details) > 5:
                    notification_text += f"... 还有 {len(details) - 5} 个活动\n"

            notification_text += f"\n💡 所有进行中的活动已自动结束并计入月度统计"

        await notification_service.send_notification(chat_id, notification_text)
        logger.info(f"重置通知发送成功: {chat_id}")

    except Exception as e:
        logger.error(f"发送重置通知失败 {chat_id}: {e}")


def init_notification_service(bot_manager_instance=None, bot_instance=None):
    """初始化通知服务"""
    global notification_service

    if "notification_service" not in globals():
        logger.error("❌ notification_service 全局实例不存在")
        return

    if bot_manager_instance:
        notification_service.bot_manager = bot_manager_instance
        logger.info(
            f"✅ notification_service.bot_manager 已设置: {bot_manager_instance}"
        )

    if bot_instance:
        notification_service.bot = bot_instance
        logger.info(f"✅ notification_service.bot 已设置: {bot_instance}")

    logger.info(
        f"📊 通知服务初始化状态: bot_manager={notification_service.bot_manager is not None}, bot={notification_service.bot is not None}"
    )
