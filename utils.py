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
    def format_minutes_to_hms(minutes: float) -> str:
        """将分钟数格式化为小时:分钟:秒的字符串"""
        if minutes is None:
            return "0小时0分0秒"

        total_seconds = int(minutes * 60)
        hours = total_seconds // 3600
        minutes_remaining = (total_seconds % 3600) // 60
        seconds_remaining = total_seconds % 60

        if hours > 0:
            return f"{hours}小时{minutes_remaining}分{seconds_remaining}秒"
        elif minutes_remaining > 0:
            return f"{minutes_remaining}分{seconds_remaining}秒"
        else:
            return f"{seconds_remaining}秒"

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
    ) -> str:
        """格式化打卡消息 - 改为新模板"""
        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"
        dashed_line = MessageFormatter.create_dashed_line()

        message = (
            f"{first_line}\n"
            f"✅ 打卡成功：{MessageFormatter.format_copyable_text(activity)} - {MessageFormatter.format_copyable_text(time_str)}\n"
            f"▫️ 本次活动类型：{MessageFormatter.format_copyable_text(activity)}\n"
            f"⏰ 单次时长限制：{MessageFormatter.format_copyable_text(str(time_limit))} 分钟 \n"
            f"📈 今日{MessageFormatter.format_copyable_text(activity)}次数：第 {MessageFormatter.format_copyable_text(str(count))} 次（上限 {MessageFormatter.format_copyable_text(str(max_times))} 次）\n"
        )

        if count >= max_times:
            message += f"🚨 警告：本次结束后，您今日的{MessageFormatter.format_copyable_text(activity)}次数将达到上限，请留意！\n"

        message += (
            f"{dashed_line}\n"
            f"💡 操作提示\n"
            f"完成后请及时点击 👉【✅ 回座】👈按钮。"
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
        """格式化回座消息 - 改为新模板"""
        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"
        dashed_line = MessageFormatter.create_dashed_line()

        # 今日次数从activity_counts中获取
        today_count = activity_counts.get(activity, 0)

        # 构建消息
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

        # 超时罚款部分 - 改为新模板格式
        if is_overtime:
            overtime_time = MessageFormatter.format_time(int(overtime_seconds))
            message += f"\n⚠️ 超时提醒\n"
            message += f"▫️ 超时时长：{MessageFormatter.format_copyable_text(overtime_time)} 🚨\n"
            if fine_amount > 0:
                message += f"▫️ 罚款金额：{MessageFormatter.format_copyable_text(str(fine_amount))}元 💸\n"

        # 今日总计
        message += f"{dashed_line}\n"
        message += f"📊 今日总计\n"
        message += f"▫️ 活动详情\n"

        # 添加活动详情 - 改为新模板格式
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
    """统一推送服务 - 完整修复版"""

    def __init__(self, bot_manager=None):
        self.bot_manager = bot_manager
        self.bot = None  # 🆕 添加直接 bot 实例作为备用
        self._last_notification_time = {}
        self._rate_limit_window = 60  # 60秒内不重复发送相同通知

    async def send_notification(
        self, chat_id: int, text: str, notification_type: str = "all"
    ):
        """发送通知到绑定的频道和群组 - 完整修复版"""
        # 🆕 双重检查：优先使用 bot_manager，备用使用 bot
        if not self.bot_manager and not self.bot:
            logger.warning("NotificationService: bot_manager 和 bot 都未初始化")
            return False

        # 检查速率限制
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

        # 获取群组数据
        group_data = await db.get_group_cached(chat_id)

        # 🆕 优先使用 bot_manager 的带重试方法
        if self.bot_manager and hasattr(self.bot_manager, "send_message_with_retry"):
            sent = await self._send_with_bot_manager(
                chat_id, text, group_data, push_settings
            )
        # 🆕 备用：直接使用 bot 实例
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

        # 发送到频道
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

        # 发送到通知群组
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

        # 管理员兜底推送
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
        """直接使用 bot 实例发送通知（备用方案）"""
        sent = False

        # 发送到频道
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

        # 发送到通知群组
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

        # 管理员兜底推送
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
        """发送文档到绑定的频道和群组 - 完整修复版"""
        # 🆕 双重检查
        if not self.bot_manager and not self.bot:
            logger.warning("NotificationService: bot_manager 和 bot 都未初始化")
            return False

        sent = False
        push_settings = await db.get_push_settings()
        group_data = await db.get_group_cached(chat_id)

        # 🆕 优先使用 bot_manager 的带重试方法
        if self.bot_manager and hasattr(self.bot_manager, "send_document_with_retry"):
            # 发送到频道
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

            # 发送到通知群组
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

            # 管理员兜底推送
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

        # 🆕 备用：直接使用 bot 实例
        elif self.bot:
            # 发送到频道
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

            # 发送到通知群组
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

            # 管理员兜底推送
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
    """用户锁管理器"""

    def __init__(self):
        self._locks = {}
        self._access_times = {}
        self._cleanup_interval = 3600
        self._last_cleanup = time.time()
        self._max_locks = 5000

    def get_lock(self, chat_id: int, uid: int):
        """获取用户级锁"""
        key = f"{chat_id}-{uid}"

        if len(self._locks) >= self._max_locks:
            self._emergency_cleanup()

        # 记录访问时间
        self._access_times[key] = time.time()

        # 检查是否需要清理
        self._maybe_cleanup()

        # 返回或创建锁
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()

        return self._locks[key]

    def _maybe_cleanup(self):
        """按需清理过期锁"""
        current_time = time.time()
        if current_time - self._last_cleanup < self._cleanup_interval:
            return

        self._last_cleanup = current_time
        self._cleanup_old_locks()

    def _cleanup_old_locks(self):
        """清理长时间未使用的锁"""
        now = time.time()
        max_age = 86400  # 24小时

        old_keys = [
            key
            for key, last_used in self._access_times.items()
            if now - last_used > max_age
        ]

        for key in old_keys:
            self._locks.pop(key, None)
            self._access_times.pop(key, None)

        if old_keys:
            logger.info(f"用户锁清理: 移除了 {len(old_keys)} 个过期锁")

    async def force_cleanup(self):
        """强制立即清理"""
        old_count = len(self._locks)
        self._cleanup_old_locks()
        new_count = len(self._locks)
        logger.info(f"强制用户锁清理: {old_count} -> {new_count}")

    def get_stats(self) -> Dict[str, Any]:
        """获取锁管理器统计"""
        return {
            "active_locks": len(self._locks),
            "tracked_users": len(self._access_times),
            "last_cleanup": self._last_cleanup,
        }

    def _emergency_cleanup(self):
        """🆕 紧急清理 - 当锁数量达到上限时"""
        now = time.time()
        max_age = 3600  # 1小时未使用的锁

        # 清理长时间未使用的锁
        old_keys = [
            key
            for key, last_used in self._access_times.items()
            if now - last_used > max_age
        ]

        # 如果还不够，按LRU清理最旧的20%
        if len(self._locks) >= self._max_locks:
            sorted_keys = sorted(
                self._access_times.items(), key=lambda x: x[1]  # 按访问时间排序
            )
            additional_cleanup = max(100, len(sorted_keys) // 5)  # 至少100个或20%
            old_keys.extend([key for key, _ in sorted_keys[:additional_cleanup]])

        for key in set(old_keys):  # 去重
            self._locks.pop(key, None)
            self._access_times.pop(key, None)

        logger.warning(f"紧急锁清理: 移除了 {len(old_keys)} 个锁")


class ActivityTimerManager:
    """活动定时器管理器"""

    def __init__(self):
        self._timers = {}
        self._cleanup_interval = 300
        self._last_cleanup = time.time()
        self.activity_timer_callback = None  # 回调函数

    def set_activity_timer_callback(self, callback):
        """设置活动定时器回调"""
        self.activity_timer_callback = callback

    async def start_timer(self, chat_id: int, uid: int, act: str, limit: int):
        """启动活动定时器"""
        key = f"{chat_id}-{uid}"
        await self.cancel_timer(key)

        if not self.activity_timer_callback:
            logger.error("ActivityTimerManager: 未设置回调函数")
            return

        timer_task = asyncio.create_task(
            self._activity_timer_wrapper(chat_id, uid, act, limit), name=f"timer_{key}"
        )
        self._timers[key] = timer_task
        logger.debug(f"启动定时器: {key} - {act}")

    async def _activity_timer_wrapper(
        self, chat_id: int, uid: int, act: str, limit: int
    ):
        """定时器包装器"""
        try:
            if self.activity_timer_callback:
                await self.activity_timer_callback(chat_id, uid, act, limit)
        except Exception as e:
            logger.error(f"定时器异常 {chat_id}-{uid}: {e}")

    async def cancel_timer(self, key: str):
        """取消定时器"""
        if key in self._timers:
            task = self._timers[key]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            del self._timers[key]

    async def cancel_all_timers(self):
        """取消所有定时器"""
        keys = list(self._timers.keys())
        cancelled_count = 0

        for key in keys:
            try:
                await self.cancel_timer(key)
                cancelled_count += 1
            except Exception as e:
                logger.error(f"取消定时器 {key} 失败: {e}")

        logger.info(f"已取消所有定时器: {cancelled_count}/{len(keys)} 个")
        return cancelled_count

    async def cancel_all_timers_for_group(self, chat_id: int) -> int:
        """取消指定群组的所有定时器"""
        cancelled_count = 0
        keys_to_remove = []

        # 查找属于该群组的所有定时器
        for key in list(self._timers.keys()):
            if key.startswith(f"{chat_id}-"):
                task = self._timers[key]
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    cancelled_count += 1
                keys_to_remove.append(key)

        # 移除已取消的定时器
        for key in keys_to_remove:
            del self._timers[key]

        logger.info(f"已取消群组 {chat_id} 的 {cancelled_count} 个定时器")
        return cancelled_count

    async def cleanup_finished_timers(self):
        """清理已完成定时器"""
        if time.time() - self._last_cleanup < self._cleanup_interval:
            return

        finished_keys = [key for key, task in self._timers.items() if task.done()]
        for key in finished_keys:
            del self._timers[key]

        if finished_keys:
            logger.info(f"定时器清理: 移除了 {len(finished_keys)} 个已完成定时器")

        self._last_cleanup = time.time()

    def get_stats(self) -> Dict[str, Any]:
        """获取定时器统计"""
        return {"active_timers": len(self._timers)}


class EnhancedPerformanceOptimizer:
    """增强版性能优化器 - 现在包含智能内存管理"""

    def __init__(self):
        # 定期清理间隔（秒）
        self.cleanup_interval = 300
        self.last_cleanup = time.time()

        # 自动判断是否为 Render 环境
        self.is_render = self._detect_render_environment()

        # Render 内存阈值（单位 MB）
        self.render_memory_limit = 400  # 留 100MB 缓冲区（Render 免费版=512MB）

        logger.info(
            f"🧠 EnhancedPerformanceOptimizer 初始化 - Render 环境: {self.is_render}"
        )

    def _detect_render_environment(self) -> bool:
        """检测是否运行在 Render 环境"""
        # 方法1: 检查 RENDER 环境变量
        if os.environ.get("RENDER"):
            return True

        # 方法2: 检查 Render 特定的环境变量
        if "RENDER_EXTERNAL_URL" in os.environ:
            return True

        # 方法3: 检查 PORT 环境变量（Render 会自动设置）
        if os.environ.get("PORT"):
            return True

        return False

    async def memory_cleanup(self):
        """
        智能内存清理 - 替换原有的实现
        """
        if self.is_render:
            return await self._render_cleanup()
        else:
            await self._regular_cleanup()
            return None

    # ---------------------------------------------------------
    # 1️⃣ Render 紧急保护模式
    # ---------------------------------------------------------
    async def _render_cleanup(self) -> float:
        """Render 环境专用清理（带紧急 OOM 防护）"""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024

            # 输出 Render 专用监控日志
            logger.debug(f"🔵 Render 内存监测: {memory_mb:.1f} MB")

            # 如果内存太高，执行紧急清理
            if memory_mb > self.render_memory_limit:
                logger.warning(f"🚨 Render 内存过高 {memory_mb:.1f}MB，执行紧急清理")

                # 清理缓存
                old_cache_size = global_cache.get_stats().get("size", 0)
                global_cache.clear_all()

                # 清理已完成任务
                await task_manager.cleanup_tasks()

                # 清理数据库缓存
                await db.cleanup_cache()

                # 强制 GC
                collected = gc.collect()

                logger.info(
                    f"🆘 紧急清理完成: 清缓存 {old_cache_size} 项, GC 回收 {collected} 对象"
                )

            return memory_mb

        except Exception as e:
            logger.error(f"Render 内存清理失败: {e}")
            return 0.0

    # ---------------------------------------------------------
    # 2️⃣ 常规服务器智能清理模式
    # ---------------------------------------------------------
    async def _regular_cleanup(self):
        """普通环境的智能周期清理"""
        try:
            now = time.time()
            if now - self.last_cleanup < self.cleanup_interval:
                return  # 未到周期，无需清理

            logger.debug("🟢 执行周期性内存清理...")

            # 并行执行多个清理任务
            tasks = [
                task_manager.cleanup_tasks(),
                global_cache.clear_expired(),
                db.cleanup_cache(),
            ]

            await asyncio.gather(*tasks, return_exceptions=True)

            # 强制 GC
            collected = gc.collect()
            if collected > 0:
                logger.info(f"周期清理完成 - GC 回收对象: {collected}")
            else:
                logger.debug("周期清理完成 - 无需要回收的对象")

            self.last_cleanup = now

        except Exception as e:
            logger.error(f"周期清理失败: {e}")

    def memory_usage_ok(self) -> bool:
        """检查内存使用是否正常 - 保持原有接口"""
        try:
            process = psutil.Process()
            memory_percent = process.memory_percent()
            memory_mb = process.memory_info().rss / 1024 / 1024

            # Render 环境使用绝对值检查，其他环境使用百分比
            if self.is_render:
                return memory_mb < self.render_memory_limit
            else:
                return memory_percent < 80  # 原有逻辑
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
                await asyncio.sleep(60)  # 每分钟一次心跳
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


# 工具函数
def get_beijing_time() -> datetime:
    """获取北京时间"""
    return datetime.now(beijing_tz)


def calculate_cross_day_time_diff(
    current_dt: datetime, expected_time: str, checkin_type: str
) -> Tuple[float, int, datetime]:
    """
    智能化的时间差计算（支持跨天和最近匹配）
    """
    try:
        expected_hour, expected_minute = map(int, expected_time.split(":"))

        # 生成前一天、当天、后一天三个候选时间点
        candidates = []
        for d in (-1, 0, 1):
            candidate = current_dt.replace(
                hour=expected_hour, minute=expected_minute, second=0, microsecond=0
            ) + timedelta(days=d)
            candidates.append(candidate)

        # 找到与当前时间最接近的 expected_dt
        expected_dt = min(
            candidates, key=lambda t: abs((t - current_dt).total_seconds())
        )

        # 计算时间差（单位：分钟）
        time_diff_minutes = (current_dt - expected_dt).total_seconds() / 60

        time_diff_seconds = int((current_dt - expected_dt).total_seconds())
        return time_diff_minutes, time_diff_seconds, expected_dt

    except Exception as e:
        logger.error(f"时间差计算出错: {e}")
        return 0, current_dt


async def is_valid_checkin_time(
    chat_id: int, checkin_type: str, current_time: datetime
) -> Tuple[bool, datetime]:
    """
    检查是否在允许的打卡时间窗口内（前后 7 小时）
    """
    try:
        work_hours = await db.get_group_work_time(chat_id)
        if checkin_type == "work_start":
            expected_time_str = work_hours["work_start"]
        else:
            expected_time_str = work_hours["work_end"]

        exp_h, exp_m = map(int, expected_time_str.split(":"))

        # 在 -1/0/+1 天范围内生成候选 expected_dt
        candidates = []
        for d in (-1, 0, 1):
            candidate = current_time.replace(
                hour=exp_h, minute=exp_m, second=0, microsecond=0
            ) + timedelta(days=d)
            candidates.append(candidate)

        # 选择与 current_time 时间差绝对值最小的 candidate
        expected_dt = min(
            candidates, key=lambda t: abs((t - current_time).total_seconds())
        )

        # 允许前后窗口：7小时
        earliest = expected_dt - timedelta(hours=7)
        latest = expected_dt + timedelta(hours=7)

        is_valid = earliest <= current_time <= latest

        if not is_valid:
            logger.warning(
                f"打卡时间超出允许窗口: {checkin_type}, 当前: {current_time.strftime('%Y-%m-%d %H:%M')}, "
                f"允许: {earliest.strftime('%Y-%m-%d %H:%M')} ~ {latest.strftime('%Y-%m-%d %H:%M')}"
            )

        return is_valid, expected_dt

    except Exception as e:
        logger.error(f"检查打卡时间范围失败: {e}")
        fallback = current_time.replace(hour=9, minute=0, second=0, microsecond=0)
        return True, fallback


# ========== 装饰器和工具函数 ==========
def rate_limit(rate: int = 1, per: int = 1):
    """速率限制装饰器"""

    def decorator(func):
        calls = []

        @wraps(func)
        async def wrapper(*args, **kwargs):
            now = time.time()
            # 清理过期记录
            calls[:] = [call for call in calls if now - call < per]

            if len(calls) >= rate:
                if args and isinstance(args[0], types.Message):
                    await args[0].answer("⏳ 操作过于频繁，请稍后再试")
                return

            calls.append(now)
            return await func(*args, **kwargs)

        return wrapper

    return decorator


# ========== 重置通知函数 ==========
async def send_reset_notification(
    chat_id: int, completion_result: Dict[str, Any], reset_time: datetime
):
    """发送重置通知"""
    try:
        completed_count = completion_result.get("completed_count", 0)
        total_fines = completion_result.get("total_fines", 0)
        details = completion_result.get("details", [])

        if completed_count == 0:
            # 没有活动被结束，发送简单通知
            notification_text = (
                f"🔄 <b>系统重置完成</b>\n"
                f"🏢 群组: <code>{chat_id}</code>\n"
                f"⏰ 重置时间: <code>{reset_time.strftime('%m/%d %H:%M')}</code>\n"
                f"✅ 没有进行中的活动需要结束"
            )
        else:
            # 有活动被结束，发送详细通知
            notification_text = (
                f"🔄 <b>系统重置完成通知</b>\n"
                f"🏢 群组: <code>{chat_id}</code>\n"
                f"⏰ 重置时间: <code>{reset_time.strftime('%m/%d %H:%M')}</code>\n"
                f"📊 自动结束活动: <code>{completed_count}</code> 个\n"
                f"💰 总罚款金额: <code>{total_fines}</code> 元\n"
            )

            if details:
                notification_text += f"\n📋 <b>活动结束详情:</b>\n"
                for i, detail in enumerate(details[:5], 1):  # 最多显示5条详情
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

        # 发送通知
        await notification_service.send_notification(chat_id, notification_text)
        logger.info(f"重置通知发送成功: {chat_id}")

    except Exception as e:
        logger.error(f"发送重置通知失败 {chat_id}: {e}")


# 全局实例
user_lock_manager = UserLockManager()
timer_manager = ActivityTimerManager()
performance_optimizer = EnhancedPerformanceOptimizer()
heartbeat_manager = HeartbeatManager()
notification_service = NotificationService()
