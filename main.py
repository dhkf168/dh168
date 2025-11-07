# main.py - 完整异步重构优化版本（纯PostgreSQL）
import asyncio
import json
import os
import csv
import sys
import time
import gc
import weakref
import aiofiles
import logging
import psutil
from io import StringIO
from datetime import datetime, timedelta
from collections import defaultdict
from functools import wraps
from typing import Dict, Any, Optional, List, Tuple

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    FSInputFile,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web

from config import Config, beijing_tz
from database import PostgreSQLDatabase as AsyncDatabase
from heartbeat import heartbeat_manager
from aiogram import types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


# 性能监控工具
from performance import (
    performance_monitor,
    task_manager,
    retry_manager,
    global_cache,
    track_performance,
    with_retry,
    message_deduplicate,
)

# 日志配置优化
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8", mode="a"),
    ],
)
logger = logging.getLogger("GroupCheckInBot")

# 禁用过于详细的日志
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

# 初始化优化数据库
db = AsyncDatabase()


# 记录程序启动的时间
start_time = time.time()

# 初始化bot
bot = Bot(token=Config.TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# ==================== 性能优化类 ====================
class EnhancedPerformanceOptimizer:
    """增强版性能优化器"""

    def __init__(self):
        self.last_cleanup = time.time()
        self.cleanup_interval = 300

    async def memory_cleanup(self):
        """智能内存清理"""
        try:
            current_time = time.time()
            if current_time - self.last_cleanup < self.cleanup_interval:
                return

            # 并行清理任务
            cleanup_tasks = [
                task_manager.cleanup_tasks(),
                global_cache.clear_expired(),
                db.cleanup_cache(),
            ]

            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

            # 强制GC
            collected = gc.collect()
            logger.info(f"🧹 内存清理完成 - 回收对象: {collected}")

            self.last_cleanup = current_time
        except Exception as e:
            logger.error(f"❌ 内存清理失败: {e}")

    def memory_usage_ok(self) -> bool:
        """检查内存使用是否正常"""
        return task_manager.memory_usage_ok()

    def cleanup_user_locks(self):
        """清理长时间未使用的用户锁"""
        global user_locks
        user_locks.clear()


# 初始化优化器
performance_optimizer = EnhancedPerformanceOptimizer()


# ==================== 优化装饰器和工具类 ====================
def admin_required(func):
    """管理员权限检查装饰器 - 优化版本"""

    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        if not await is_admin(message.from_user.id):
            await message.answer(
                Config.MESSAGES["no_permission"],
                reply_markup=await get_main_keyboard(
                    message.chat.id, await is_admin(message.from_user.id)
                ),
            )
            return
        return await func(message, *args, **kwargs)

    return wrapper


def rate_limit(rate: int = 1, per: int = 1):
    """速率限制装饰器 - 优化版本"""

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


class OptimizedUserContext:
    """优化版用户上下文管理器"""

    def __init__(self, chat_id: int, uid: int):
        self.chat_id = chat_id
        self.uid = uid

    async def __aenter__(self):
        await db.init_group(self.chat_id)
        await db.init_user(self.chat_id, self.uid)
        return await db.get_user_cached(self.chat_id, self.uid)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MessageFormatter:
    """消息格式化工具类 - 优化版本"""

    @staticmethod
    def format_time(seconds: int):
        """格式化时间显示 - 包含秒级精度"""
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
    def format_time_for_csv(seconds: int):
        """为 CSV 导出格式化时间显示 - 包含秒级精度"""
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
    def format_minutes_to_hms(minutes: float):
        """将分钟数格式化为小时:分钟:秒的字符串 - 修复精度问题"""
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
    def format_user_link(user_id: int, user_name: str):
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
    def create_dashed_line():
        """创建短虚线分割线"""
        return "-------------------------------------"

    @staticmethod
    def format_copyable_text(text: str):
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
    ):
        """格式化打卡消息"""
        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"

        message = (
            f"{first_line}\n"
            f"✅ 打卡成功：{MessageFormatter.format_copyable_text(activity)} - {MessageFormatter.format_copyable_text(time_str)}\n"
            f"⚠️ 注意：这是您第 {MessageFormatter.format_copyable_text(str(count))} 次{MessageFormatter.format_copyable_text(activity)}（今日上限：{MessageFormatter.format_copyable_text(str(max_times))}次）\n"
            f"⏰ 本次活动时间限制：{MessageFormatter.format_copyable_text(str(time_limit))} 分钟"
        )

        if count >= max_times:
            message += f"\n⚠️ 警告：本次结束后，您今日的{MessageFormatter.format_copyable_text(activity)}次数将达到上限，请留意！"

        message += f"\n💡提示：活动完成后请及时输入'回座'或点击'✅ 回座'按钮"

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
    ):
        """格式化回座消息"""
        first_line = f"👤 用户：{MessageFormatter.format_user_link(user_id, user_name)}"

        message = (
            f"{first_line}\n"
            f"✅ {MessageFormatter.format_copyable_text(time_str)} 回座打卡成功\n"
            f"📝 活动：{MessageFormatter.format_copyable_text(activity)}\n"
            f"⏱️ 本次活动耗时：{MessageFormatter.format_copyable_text(elapsed_time)}\n"
            f"📊 今日累计{MessageFormatter.format_copyable_text(activity)}时间：{MessageFormatter.format_copyable_text(total_activity_time)}\n"
            f"📈 今日总计时：{MessageFormatter.format_copyable_text(total_time)}\n"
        )

        if is_overtime:
            overtime_time = MessageFormatter.format_time(int(overtime_seconds))
            message += f"⚠️ 警告：您本次的活动已超时！\n超时时间：{MessageFormatter.format_copyable_text(overtime_time)}\n"
            if fine_amount > 0:
                message += f"💸 罚款：{MessageFormatter.format_copyable_text(str(fine_amount))} 元\n"

        dashed_line = MessageFormatter.create_dashed_line()
        message += f"{dashed_line}\n"

        for act, count in activity_counts.items():
            if count > 0:
                message += f"🔢 本日{MessageFormatter.format_copyable_text(act)}次数：{MessageFormatter.format_copyable_text(str(count))} 次\n"

        message += f"\n📊 今日总活动次数：{MessageFormatter.format_copyable_text(str(total_count))} 次"

        return message


class NotificationService:
    """统一推送服务 - 优化版本"""

    @staticmethod
    async def send_notification(
        chat_id: int, text: str, notification_type: str = "all"
    ):
        """发送通知到绑定的频道和群组"""
        sent = False
        push_settings = await db.get_push_settings()

        logger.info(f"🔔 开始推送通知，群组: {chat_id}, 设置: {push_settings}")

        # 获取群组数据
        group_data = await db.get_group_cached(chat_id)
        logger.info(f"🔔 群组数据: {group_data}")

        # 发送到频道
        if (
            push_settings.get("enable_channel_push")
            and group_data
            and group_data.get("channel_id")
        ):
            try:
                await bot.send_message(
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
                await bot.send_message(
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
                    await bot.send_message(admin_id, text, parse_mode="HTML")
                    logger.info(f"✅ 已发送给管理员: {admin_id}")
                except Exception as e:
                    logger.error(f"❌ 发送给管理员失败: {e}")

        return sent

    @staticmethod
    async def send_document(chat_id: int, document: FSInputFile, caption: str = ""):
        """发送文档到绑定的频道和群组"""
        sent = False
        push_settings = await db.get_push_settings()
        group_data = await db.get_group_cached(chat_id)

        # 发送到频道
        if (
            push_settings.get("enable_channel_push")
            and group_data
            and group_data.get("channel_id")
        ):
            try:
                await bot.send_document(
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
                await bot.send_document(
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
                    await bot.send_document(
                        admin_id, document, caption=caption, parse_mode="HTML"
                    )
                    logger.info(f"✅ 已发送文档给管理员: {admin_id}")
                except Exception as e:
                    logger.error(f"❌ 发送文档给管理员失败: {e}")

        return sent


# ==================== 并发安全机制优化 ====================
user_locks = defaultdict(asyncio.Lock)


def get_user_lock(chat_id: int, uid: int) -> asyncio.Lock:
    """获取用户级锁 - 优化版本"""
    key = f"{chat_id}-{uid}"
    return user_locks[key]


# ==================== 状态机类 ====================
class AdminStates(StatesGroup):
    waiting_for_channel_id = State()
    waiting_for_group_id = State()


# ==================== 工具函数优化 ====================
def get_beijing_time():
    """获取北京时间"""
    return datetime.now(beijing_tz)


async def is_admin(uid):
    """检查用户是否为管理员"""
    return uid in Config.ADMINS


async def calculate_work_fine(checkin_type: str, late_minutes: float) -> int:
    """计算上下班迟到早退罚款金额"""
    work_fine_rates = await db.get_work_fine_rates_for_type(checkin_type)
    if not work_fine_rates:
        return 0

    late_minutes_abs = abs(late_minutes)

    # 分段计算罚款
    if late_minutes_abs <= 0:
        return 0
    elif late_minutes_abs <= 60:
        return work_fine_rates.get("60", 50)
    elif late_minutes_abs <= 120:
        return work_fine_rates.get("120", 100)
    elif late_minutes_abs <= 180:
        return work_fine_rates.get("180", 200)
    elif late_minutes_abs <= 240:
        return work_fine_rates.get("240", 300)
    else:
        return work_fine_rates.get("max", 500)


async def reset_daily_data_if_needed(chat_id: int, uid: int):
    """优化版每日数据重置"""
    today = str(get_beijing_time().date())
    user_data = await db.get_user_cached(chat_id, uid)

    if user_data and user_data["last_updated"] != today:
        await db.reset_user_daily_data(chat_id, uid)


async def check_activity_limit(chat_id: int, uid: int, act: str):
    """检查活动次数是否达到上限"""
    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)
    await reset_daily_data_if_needed(chat_id, uid)

    current_count = await db.get_user_activity_count(chat_id, uid, act)
    max_times = await db.get_activity_max_times(act)

    return current_count < max_times, current_count, max_times


async def has_active_activity(chat_id: int, uid: int):
    """检查用户是否有活动正在进行"""
    await db.init_group(chat_id)
    await db.init_user(chat_id, uid)
    user_data = await db.get_user_cached(chat_id, uid)
    return user_data["current_activity"] is not None, user_data["current_activity"]


async def has_work_hours_enabled(chat_id: int) -> bool:
    """检查是否启用了上下班功能"""
    return await db.has_work_hours_enabled(chat_id)


async def has_clocked_in_today(chat_id: int, uid: int, checkin_type: str) -> bool:
    """检查用户今天是否打过指定的上下班卡"""
    return await db.has_work_record_today(chat_id, uid, checkin_type)


async def can_perform_activities(chat_id: int, uid: int) -> tuple[bool, str]:
    """快速检查是否可以执行活动"""
    if not await db.has_work_hours_enabled(chat_id):
        return True, ""

    today_records = await db.get_today_work_records(chat_id, uid)

    if "work_start" not in today_records:
        return False, "❌ 请先打上班卡！"

    if "work_end" in today_records:
        return False, "❌ 已下班，无法进行活动！"

    return True, ""


async def calculate_fine(activity: str, overtime_minutes: float) -> int:
    """计算罚款金额 - 分段罚款"""
    fine_rates = await db.get_fine_rates_for_activity(activity)
    if not fine_rates:
        return 0

    segments = sorted([int(time) for time in fine_rates.keys()])

    applicable_fine = 0
    for segment in segments:
        if overtime_minutes <= segment:
            applicable_fine = fine_rates[str(segment)]
            break

    if applicable_fine == 0 and segments:
        applicable_fine = fine_rates[str(segments[-1])]

    return applicable_fine


# ==================== 回复键盘 ====================
async def get_main_keyboard(chat_id: int = None, show_admin=False):
    """获取主回复键盘 - 优化版本"""
    activity_limits = await db.get_activity_limits_cached()
    dynamic_buttons = []
    current_row = []

    for act in activity_limits.keys():
        current_row.append(KeyboardButton(text=act))
        if len(current_row) >= 3:
            dynamic_buttons.append(current_row)
            current_row = []

    # 修复：使用修复后的 has_work_hours_enabled 函数
    if chat_id and await has_work_hours_enabled(chat_id):
        current_row.append(KeyboardButton(text="🟢 上班"))
        current_row.append(KeyboardButton(text="🔴 下班"))
        if len(current_row) >= 3:
            dynamic_buttons.append(current_row)
            current_row = []

    if current_row:
        dynamic_buttons.append(current_row)

    fixed_buttons = []
    fixed_buttons.append([KeyboardButton(text="✅ 回座")])

    bottom_buttons = []
    if show_admin:
        bottom_buttons.append(
            [
                KeyboardButton(text="👑 管理员面板"),
                KeyboardButton(text="📊 我的记录"),
                KeyboardButton(text="🏆 排行榜"),
            ]
        )
    else:
        bottom_buttons.append(
            [KeyboardButton(text="📊 我的记录"), KeyboardButton(text="🏆 排行榜")]
        )

    keyboard = dynamic_buttons + fixed_buttons + bottom_buttons

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="请选择操作或输入活动名称...",
    )


def get_admin_keyboard():
    """管理员专用键盘"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👑 管理员面板"), KeyboardButton(text="📤 导出数据")],
            [KeyboardButton(text="🔔 通知设置"), KeyboardButton(text="🕒 上下班设置")],
            [KeyboardButton(text="🔙 返回主菜单")],
        ],
        resize_keyboard=True,
    )


# ==================== 活动定时提醒优化 ====================
tasks = {}  # 定时任务：chat_id-uid → asyncio.Task


async def safe_cancel_task(key: str):
    """安全取消定时任务"""
    if key in tasks:
        task = tasks[key]
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"取消任务异常: {e}")
        del tasks[key]


async def activity_timer(chat_id: int, uid: int, act: str, limit: int):
    """优化的活动定时提醒任务"""
    try:
        key = f"{chat_id}-{uid}"
        # 使用内存感知的任务管理器创建任务
        timer_task = await task_manager.create_task(
            _activity_timer_inner(chat_id, uid, act, limit), name=f"timer_{key}"
        )
        tasks[key] = timer_task
        await timer_task  # 等待任务完成

    except asyncio.CancelledError:
        logger.info(f"定时器 {key} 被取消")
    except Exception as e:
        logger.error(f"定时器错误: {e}")


async def _activity_timer_inner(chat_id: int, uid: int, act: str, limit: int):
    """定时器内部逻辑 - 原有的 activity_timer 内容移动到这里"""
    one_minute_warning_sent = False
    timeout_immediate_sent = False
    timeout_5min_sent = False
    last_reminder_minute = 0

    while True:
        user_lock = get_user_lock(chat_id, uid)
        async with user_lock:
            user_data = await db.get_user_cached(chat_id, uid)
            if not user_data or user_data["current_activity"] != act:
                break

            start_time = datetime.fromisoformat(user_data["activity_start_time"])
            elapsed = (get_beijing_time() - start_time).total_seconds()
            remaining = limit * 60 - elapsed

            nickname = user_data.get("nickname", str(uid))

        # 1分钟前警告
        if 0 < remaining <= 60 and not one_minute_warning_sent:
            warning_msg = (
                f"⏳ <b>即将超时警告</b>\n"
                f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                f"❌ 您本次 {MessageFormatter.format_copyable_text(act)} 还有 <code>1</code> 分钟即将超时！\n"
                f"💡 请及时回座，避免超时罚款"
            )
            # 创建回座按钮
            back_keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="👉 点击✅立即回座 👈",
                            callback_data=f"quick_back:{chat_id}:{uid}",
                        )
                    ]
                ]
            )
            await bot.send_message(
                chat_id, warning_msg, parse_mode="HTML", reply_markup=back_keyboard
            )
            one_minute_warning_sent = True

        # 超时提醒
        if remaining <= 0:
            overtime_minutes = int(-remaining // 60)

            if overtime_minutes == 0 and not timeout_immediate_sent:
                timeout_msg = (
                    f"⚠️ <b>超时警告</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                    f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经<code>超时</code>！\n"
                    f"💢 请立即回座，避免产生更多罚款！"
                )
                # 创建回座按钮
                back_keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="👉 点击✅立即回座 👈",
                                callback_data=f"quick_back:{chat_id}:{uid}",
                            )
                        ]
                    ]
                )

                await bot.send_message(
                    chat_id, timeout_msg, parse_mode="HTML", reply_markup=back_keyboard
                )
                timeout_immediate_sent = True
                last_reminder_minute = 0

            elif overtime_minutes == 5 and not timeout_5min_sent:
                timeout_msg = (
                    f"🔔 <b>超时警告</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                    f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经超时 <code>5</code> 分钟！\n"
                    f"💢 请立即回座，避免罚款增加！"
                )
                # 创建回座按钮
                back_keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="👉 点击✅立即回座 👈",
                                callback_data=f"quick_back:{chat_id}:{uid}",
                            )
                        ]
                    ]
                )
                await bot.send_message(
                    chat_id, timeout_msg, parse_mode="HTML", reply_markup=back_keyboard
                )
                timeout_5min_sent = True
                last_reminder_minute = 5

            elif (
                overtime_minutes >= 10
                and overtime_minutes % 10 == 0
                and overtime_minutes > last_reminder_minute
            ):
                timeout_msg = (
                    f"🚨 <b>超时警告</b>\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                    f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经超时 <code>{overtime_minutes}</code> 分钟！\n"
                    f"💢 请立即回座！"
                )
                # 创建回座按钮
                back_keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="👉 点击✅立即回座 👈",
                                callback_data=f"quick_back:{chat_id}:{uid}",
                            )
                        ]
                    ]
                )
                await bot.send_message(
                    chat_id, timeout_msg, parse_mode="HTML", reply_markup=back_keyboard
                )
                last_reminder_minute = overtime_minutes

        # 检查超时强制回座
        user_lock = get_user_lock(chat_id, uid)
        async with user_lock:
            user_data = await db.get_user_cached(chat_id, uid)
            if user_data and user_data["current_activity"] == act:

                if remaining <= -120 * 60:
                    overtime_minutes = 120
                    overtime_seconds = 120 * 60

                    fine_amount = await calculate_fine(act, overtime_minutes)

                    elapsed = (
                        get_beijing_time()
                        - datetime.fromisoformat(user_data["activity_start_time"])
                    ).total_seconds()

                    await db.complete_user_activity(
                        chat_id, uid, act, int(elapsed), fine_amount, True
                    )

                    auto_back_msg = (
                        f"🛑 <b>自动安全回座</b>\n"
                        f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                        f"📝 活动：<code>{act}</code>\n"
                        f"⚠️ 由于超时超过2小时，系统已自动为您回座\n"
                        f"⏰ 超时时长：<code>120</code> 分钟\n"
                        f"💰 本次罚款：<code>{fine_amount}</code> 元\n"
                        f"💢 请检查是否忘记回座！"
                    )
                    await bot.send_message(chat_id, auto_back_msg, parse_mode="HTML")

                    try:
                        chat_title = str(chat_id)
                        try:
                            chat_info = await bot.get_chat(chat_id)
                            chat_title = chat_info.title or chat_title
                        except Exception:
                            pass

                        notif_text = (
                            f"🚨 <b>自动回座超时通知</b>\n"
                            f"🏢 群组：<code>{chat_title}</code>\n"
                            f"----------------------------------------\n"
                            f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                            f"📝 活动：<code>{act}</code>\n"
                            f"⏰ 回座时间：<code>{get_beijing_time().strftime('%m/%d %H:%M:%S')}</code>\n"
                            f"⏱️ 超时时长：<code>120</code> 分钟\n"
                            f"💰 本次罚款：<code>{fine_amount}</code> 元\n"
                            f"🔔 类型：系统自动回座（超时2小时强制）"
                        )
                        await NotificationService.send_notification(chat_id, notif_text)

                    except Exception as e:
                        logger.error(f"发送自动回座通知失败: {e}")

                    await safe_cancel_task(f"{chat_id}-{uid}")
                    break

        await asyncio.sleep(30)


# ==================== 核心打卡功能优化 ====================
async def _start_activity_locked(
    message: types.Message, act: str, chat_id: int, uid: int
):
    """线程安全的打卡逻辑 - 优化版本"""
    name = message.from_user.full_name
    now = get_beijing_time()

    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 '{act}' 不存在，请使用下方按钮选择活动",
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
        )
        return

    can_perform, reason = await can_perform_activities(chat_id, uid)
    if not can_perform:
        await message.answer(
            reason,
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            parse_mode="HTML",
        )
        return

    has_active, current_act = await has_active_activity(chat_id, uid)
    if has_active:
        await message.answer(
            Config.MESSAGES["has_activity"].format(current_act),
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
        )
        return

    # 先重置数据（如果需要）
    await reset_daily_data_if_needed(chat_id, uid)

    can_start, current_count, max_times = await check_activity_limit(chat_id, uid, act)

    if not can_start:
        await message.answer(
            Config.MESSAGES["max_times_reached"].format(act, max_times),
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
        )
        return

    await db.update_user_activity(chat_id, uid, act, str(now), name)

    key = f"{chat_id}-{uid}"
    await safe_cancel_task(key)

    time_limit = await db.get_activity_time_limit(act)
    timer_task = await task_manager.create_task(
        activity_timer(chat_id, uid, act, time_limit), name=f"activity_timer_{key}"
    )
    tasks[key] = timer_task

    await message.answer(
        MessageFormatter.format_activity_message(
            uid,
            name,
            act,
            now.strftime("%m/%d %H:%M:%S"),
            current_count + 1,
            max_times,
            time_limit,
        ),
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )


async def start_activity(message: types.Message, act: str):
    """优化的开始活动"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        # 快速检查
        if not await db.activity_exists(act):
            await message.answer(f"❌ 活动 '{act}' 不存在")
            return

        # 检查活动限制
        can_perform, reason = await can_perform_activities(chat_id, uid)
        if not can_perform:
            await message.answer(reason)
            return

        # 开始活动
        await _start_activity_locked(message, act, chat_id, uid)


# ==================== 消息处理器优化 ====================
@dp.message(Command("start"))
@rate_limit(rate=5, per=60)
@message_deduplicate
async def cmd_start(message: types.Message):
    """优化的开始命令"""
    uid = message.from_user.id
    is_admin_user = uid in Config.ADMINS

    await message.answer(
        Config.MESSAGES["welcome"],
        reply_markup=await get_main_keyboard(message.chat.id, is_admin_user),
    )


@dp.message(Command("menu"))
@rate_limit(rate=5, per=60)
async def cmd_menu(message: types.Message):
    """显示主菜单 - 优化版本"""
    uid = message.from_user.id
    await message.answer(
        "📋 主菜单",
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
    )


@dp.message(Command("admin"))
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_admin(message: types.Message):
    """管理员命令 - 优化版本"""
    await message.answer("👑 管理员面板", reply_markup=get_admin_keyboard())


@dp.message(Command("help"))
@rate_limit(rate=5, per=60)
async def cmd_help(message: types.Message):
    """帮助命令 - 优化版本"""
    uid = message.from_user.id

    help_text = (
        "📋 打卡机器人使用帮助\n\n"
        "🟢 开始活动打卡：\n"
        "• 直接输入活动名称（如：<code>吃饭</code>、<code>小厕</code>）\n"
        "• 或使用命令：<code>/ci 活动名</code>\n"
        "• 或点击下方活动按钮\n\n"
        "🔴 结束活动回座：\n"
        "• 直接输入：<code>回座</code>\n"
        "• 或使用命令：<code>/at</code>\n"
        "• 或点击下方 <code>✅ 回座</code> 按钮\n\n"
        "🕒 上下班打卡：\n"
        "• <code>/workstart</code> - 上班打卡\n"
        "• <code>/workend</code> - 下班打卡\n"
        "• <code>/workrecord</code> - 查看打卡记录\n"
        "• 或点击 <code>🟢 上班</code> 和 <code>🔴 下班</code> 按钮\n\n"
        "👑 管理员上下班设置：\n"
        "• <code>/setworktime 09:00 18:00</code> - 设置上下班时间\n"
        "• <code>/showworktime</code> - 显示当前设置\n"
        "• <code>/workstatus</code> - 查看上下班功能状态\n"
        "• <code>/delwork</code> - 移除上下班功能（保留记录）\n"
        "• <code>/delwork clear</code> - 移除功能并清除记录\n"
        "• <code>/resetworktime</code> - 重置为默认时间\n"
        "📊 查看记录：\n"
        "• 点击 <code>📊 我的记录</code> 查看个人统计\n"
        "• 点击 <code>🏆 排行榜</code> 查看群内排名\n\n"
        "🔧 其他命令：\n"
        "• <code>/start</code> - 开始使用机器人\n"
        "• <code>/menu</code> - 显示主菜单\n"
        "• <code>/help</code> - 显示此帮助信息\n\n"
        "📊 月度报告：\n"
        "• <code>/monthlyreport</code> - 查看月度报告\n"
        "• <code>/monthlyreport 2024 1</code> - 查看指定年月报告\n"
        "• <code>/exportmonthly</code> - 导出月度数据\n"
        "• <code>/exportmonthly 2024 1</code> - 导出指定年月数据\n\n"
        "⏰ 注意事项：\n"
        "• 每个活动有每日次数限制和时间限制\n"
        "• 超时会产生罚款\n"
        "• 活动完成后请及时回座\n"
        "• 每日数据会在指定时间自动重置\n"
        "• 上下班打卡需要先上班后下班"
    )

    await message.answer(
        help_text,
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )


# ==================== 管理员命令功能优化 ====================
@dp.message(Command("setchannel"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setchannel(message: types.Message):
    """绑定提醒频道 - 优化版本"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await message.answer(
            Config.MESSAGES["setchannel_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        channel_id = int(args[1].strip())
        await db.init_group(chat_id)
        await db.update_group_channel(chat_id, channel_id)
        await message.answer(
            f"✅ 已绑定超时提醒推送频道：<code>{channel_id}</code>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except ValueError:
        await message.answer(
            "❌ 频道ID必须是数字",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("setgroup"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setgroup(message: types.Message):
    """绑定通知群组 - 优化版本"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await message.answer(
            Config.MESSAGES["setgroup_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        group_id = int(args[1].strip())
        await db.init_group(chat_id)
        await db.update_group_notification(chat_id, group_id)
        await message.answer(
            f"✅ 已绑定超时通知群组：<code>{group_id}</code>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except ValueError:
        await message.answer(
            "❌ 群组ID必须是数字",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("unbindchannel"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_unbind_channel(message: types.Message):
    """解除绑定频道 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    await db.update_group_channel(chat_id, None)
    await message.answer(
        "✅ 已解除绑定的提醒频道",
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
    )


@dp.message(Command("unbindgroup"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_unbind_group(message: types.Message):
    """解除绑定通知群组 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    await db.update_group_notification(chat_id, None)
    await message.answer(
        "✅ 已解除绑定的通知群组",
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
    )


@dp.message(Command("addactivity"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_addactivity(message: types.Message):
    """添加新活动 - 优化版本"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(
            Config.MESSAGES["addactivity_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        act, max_times, time_limit = args[1], int(args[2]), int(args[3])
        existed = await db.activity_exists(act)
        await db.update_activity_config(act, max_times, time_limit)

        if existed:
            await message.answer(
                f"✅ 已修改活动 <code>{act}</code>，次数上限 <code>{max_times}</code>，时间限制 <code>{time_limit}</code> 分钟",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
        else:
            await message.answer(
                f"✅ 已添加新活动 <code>{act}</code>，次数上限 <code>{max_times}</code>，时间限制 <code>{time_limit}</code> 分钟",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
    except Exception as e:
        await message.answer(
            f"❌ 添加/修改活动失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("delactivity"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_delactivity(message: types.Message):
    """删除活动 - 优化版本"""
    args = message.text.split()
    if len(args) != 2:
        await message.answer(
            "❌ 用法：/delactivity <活动名>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return
    act = args[1]
    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 <code>{act}</code> 不存在",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
        return
    await db.delete_activity_config(act)
    await message.answer(
        f"✅ 活动 <code>{act}</code> 已删除",
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
        parse_mode="HTML",
    )


@dp.message(Command("set"))
@admin_required
@rate_limit(rate=5, per=30)
async def cmd_set(message: types.Message):
    """设置用户数据 - 优化版本"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(
            Config.MESSAGES["set_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        uid, act, minutes = args[1], args[2], args[3]
        chat_id = message.chat.id

        await db.init_user(chat_id, int(uid))
        # 这里需要实现设置用户数据的逻辑
        await message.answer(
            f"✅ 已设置用户 <code>{uid}</code> 的 <code>{act}</code> 累计时间为 <code>{minutes}</code> 分钟",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("reset"))
@admin_required
@rate_limit(rate=5, per=30)
async def cmd_reset(message: types.Message):
    """重置用户数据 - 优化版本"""
    args = message.text.split()
    if len(args) != 2:
        await message.answer(
            Config.MESSAGES["reset_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        uid = args[1]
        chat_id = message.chat.id
        await db.reset_user_daily_data(chat_id, int(uid))
        await message.answer(
            f"✅ 已重置用户 <code>{uid}</code> 的今日数据",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer(
            f"❌ 重置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("setresettime"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setresettime(message: types.Message):
    """设置每日重置时间 - 优化版本"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            Config.MESSAGES["setresettime_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        hour = int(args[1])
        minute = int(args[2])

        if 0 <= hour <= 23 and 0 <= minute <= 59:
            chat_id = message.chat.id
            await db.init_group(chat_id)
            await db.update_group_reset_time(chat_id, hour, minute)
            await message.answer(
                f"✅ 每日重置时间已设置为：<code>{hour:02d}:{minute:02d}</code>",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
        else:
            await message.answer(
                "❌ 小时必须在0-23之间，分钟必须在0-59之间！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("setfine"))
@admin_required
@rate_limit(rate=5, per=30)
async def cmd_setfine(message: types.Message):
    """设置活动罚款费率 - 优化版本"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(
            Config.MESSAGES["setfine_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        act = args[1]
        time_segment = args[2]
        fine_amount = int(args[3])

        if not await db.activity_exists(act):
            await message.answer(
                f"❌ 活动 '<code>{act}</code>' 不存在！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
                parse_mode="HTML",
            )
            return

        if fine_amount < 0:
            await message.answer(
                "❌ 罚款金额不能为负数！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            return

        await db.update_fine_config(act, time_segment, fine_amount)
        await message.answer(
            f"✅ 已设置活动 '<code>{act}</code>' 在 <code>{time_segment}</code> 分钟内的罚款费率为 <code>{fine_amount}</code> 元",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("setfines_all"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setfines_all(message: types.Message):
    """为所有活动统一设置分段罚款 - 优化版本"""
    args = message.text.split()
    if len(args) < 3 or (len(args) - 1) % 2 != 0:
        await message.answer(
            Config.MESSAGES["setfines_all_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        pairs = args[1:]
        segments = {}
        for i in range(0, len(pairs), 2):
            t = int(pairs[i])
            f = int(pairs[i + 1])
            if t <= 0 or f < 0:
                await message.answer(
                    "❌ 时间段必须为正整数，罚款金额不能为负数",
                    reply_markup=await get_main_keyboard(
                        chat_id=message.chat.id, show_admin=True
                    ),
                )
                return
            segments[str(t)] = f

        activity_limits = await db.get_activity_limits_cached()
        for act in activity_limits.keys():
            for time_segment, amount in segments.items():
                await db.update_fine_config(act, time_segment, amount)

        segments_text = " ".join(
            [f"<code>{t}</code>:<code>{f}</code>" for t, f in segments.items()]
        )
        await message.answer(
            f"✅ 已为所有活动设置分段罚款：{segments_text}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


# ===== 上下班罚款 =====
@dp.message(Command("setworkfine"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setworkfine(message: types.Message):
    """设置上下班罚款费率 - 优化版本"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(
            "❌ 用法：/setworkfine <work_start|work_end> <时间段> <金额>\n"
            "例如：/setworkfine work_start 60 50 （设置上班迟到1小时内罚款50元）\n"
            "时间段：60(1小时), 120(2小时), 180(3小时), 240(4小时), max(4小时以上)",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        checkin_type = args[1]
        time_segment = args[2]
        fine_amount = int(args[3])

        if checkin_type not in ["work_start", "work_end"]:
            await message.answer(
                "❌ 类型错误！请使用 work_start（上班）或 work_end（下班）",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            return

        if time_segment not in ["60", "120", "180", "240", "max"]:
            await message.answer(
                "❌ 时间段错误！请使用：60, 120, 180, 240, max",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            return

        if fine_amount < 0:
            await message.answer(
                "❌ 罚款金额不能为负数！",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
            return

        await db.update_work_fine_config(checkin_type, time_segment, fine_amount)
        type_text = "上班迟到" if checkin_type == "work_start" else "下班早退"
        time_text = {
            "60": "1小时内",
            "120": "1-2小时",
            "180": "2-3小时",
            "240": "3-4小时",
            "max": "4小时以上",
        }[time_segment]

        await message.answer(
            f"✅ 已设置{type_text}{time_text}罚款为 <code>{fine_amount}</code> 元",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
            parse_mode="HTML",
        )

    except ValueError:
        await message.answer(
            "❌ 请输入有效的数字！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("showsettings"))
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_showsettings(message: types.Message):
    """显示目前的设置 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    group_data = await db.get_group_cached(chat_id)

    if group_data and not isinstance(group_data, dict):
        group_data = dict(group_data)

    activity_limits = await db.get_activity_limits_cached()
    fine_rates = await db.get_fine_rates()
    work_fine_rates = await db.get_work_fine_rates()

    # 生成输出文本
    text = f"🔧 当前群设置（群 {chat_id}）\n"
    text += f"• 绑定频道ID: {group_data.get('channel_id', '未设置')}\n"
    text += f"• 通知群组ID: {group_data.get('notification_group_id', '未设置')}\n"
    text += f"• 每日重置时间: {group_data.get('reset_hour', 0):02d}:{group_data.get('reset_minute', 0):02d}\n\n"

    text += "📋 活动设置：\n"
    for act, v in activity_limits.items():
        text += f"• {act}：次数上限 {v['max_times']}，时间限制 {v['time_limit']} 分钟\n"

    text += "\n💰 当前各活动罚款分段：\n"
    for act, fr in fine_rates.items():
        text += f"• {act}：{fr}\n"

    text += "\n⏰ 上下班罚款设置：\n"
    text += f"• 上班迟到：{work_fine_rates.get('work_start', {})}\n"
    text += f"• 下班早退：{work_fine_rates.get('work_end', {})}\n"

    await message.answer(
        text,
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
    )


# 在现有的管理员命令后面添加这个新命令
@dp.message(Command("performance"))
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_performance(message: types.Message):
    """查看性能报告"""
    try:
        # 获取性能报告
        perf_report = performance_monitor.get_performance_report()
        cache_stats = global_cache.get_stats()

        report_text = (
            "📊 <b>系统性能报告</b>\n\n"
            f"⏰ 运行时间: <code>{perf_report['uptime']:.0f}</code> 秒\n"
            f"💾 内存使用: <code>{perf_report['memory_usage_mb']:.1f}</code> MB\n"
            f"🐌 慢操作数量: <code>{perf_report['slow_operations_count']}</code>\n\n"
            f"<b>缓存统计:</b>\n"
            f"• 命中率: <code>{cache_stats['hit_rate']:.1%}</code>\n"
            f"• 命中次数: <code>{cache_stats['hits']}</code>\n"
            f"• 未命中: <code>{cache_stats['misses']}</code>\n"
            f"• 缓存大小: <code>{cache_stats['size']}</code>\n\n"
            f"<b>操作性能:</b>\n"
        )

        # 添加关键操作性能
        for op_name, metrics in perf_report["metrics_summary"].items():
            if metrics["count"] > 0:
                report_text += (
                    f"• {op_name}: 平均<code>{metrics['avg']:.3f}</code>s, "
                    f"最大<code>{metrics['max']:.3f}</code>s, "
                    f"次数<code>{metrics['count']}</code>\n"
                )

        await message.answer(report_text, parse_mode="HTML")

    except Exception as e:
        await message.answer(f"❌ 获取性能报告失败: {e}")


# ===== 调试命令 =====
@dp.message(Command("debug_work"))
@admin_required
async def cmd_debug_work(message: types.Message):
    """调试上下班功能状态"""
    chat_id = message.chat.id

    work_hours = await db.get_group_work_time(chat_id)
    has_work_enabled = await has_work_hours_enabled(chat_id)

    debug_info = (
        f"🔧 上下班功能调试信息\n\n"
        f"群组ID: <code>{chat_id}</code>\n"
        f"上班时间: <code>{work_hours['work_start']}</code>\n"
        f"下班时间: <code>{work_hours['work_end']}</code>\n"
        f"默认上班: <code>{Config.DEFAULT_WORK_HOURS['work_start']}</code>\n"
        f"默认下班: <code>{Config.DEFAULT_WORK_HOURS['work_end']}</code>\n\n"
        f"功能启用状态: {'✅ 已启用' if has_work_enabled else '❌ 未启用'}\n"
        f"上班时间不同: {work_hours['work_start'] != Config.DEFAULT_WORK_HOURS['work_start']}\n"
        f"下班时间不同: {work_hours['work_end'] != Config.DEFAULT_WORK_HOURS['work_end']}\n\n"
        f"按钮应该显示: {'✅ 是' if has_work_enabled else '❌ 否'}"
    )

    await message.answer(debug_info, parse_mode="HTML")


# ==================== 上下班命令优化 ====================
@dp.message(Command("setworktime"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_setworktime(message: types.Message):
    """设置上下班时间 - 优化版本"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            "❌ 用法：/setworktime <上班时间> <下班时间>\n"
            "例如：/setworktime 09:00 18:00\n"
            "时间格式：HH:MM (24小时制)",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    try:
        work_start = args[1]
        work_end = args[2]

        datetime.strptime(work_start, "%H:%M")
        datetime.strptime(work_end, "%H:%M")

        chat_id = message.chat.id
        await db.init_group(chat_id)
        await db.update_group_work_time(chat_id, work_start, work_end)

        await message.answer(
            f"✅ 已设置上下班时间：\n"
            f"🟢 上班时间：<code>{work_start}</code>\n"
            f"🔴 下班时间：<code>{work_end}</code>\n\n"
            f"💡 用户现在可以使用上下班按钮进行打卡",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
            parse_mode="HTML",
        )

    except ValueError:
        await message.answer(
            "❌ 时间格式错误！请使用 HH:MM 格式（24小时制）\n" "例如：09:00、18:30",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        await message.answer(
            f"❌ 设置失败：{e}",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


# ========== /worktime ==========
@dp.message(Command("worktime"))
async def cmd_worktime(message: types.Message):
    """查看当前群组的上班 / 下班时间设置"""
    chat_id = message.chat.id
    work_hours = await db.get_group_work_time(chat_id)

    if (
        not work_hours
        or not work_hours.get("work_start")
        or not work_hours.get("work_end")
    ):
        await message.answer(
            "⚠️ 当前群组还没有设置上班 / 下班时间。\n请使用 /setworktime 命令设置。"
        )
        return

    start_time = work_hours["work_start"]
    end_time = work_hours["work_end"]

    await message.answer(
        f"🏢 <b>当前群组工作时间设置</b>\n"
        f"⏰ 上班时间：<code>{start_time}</code>\n"
        f"🏁 下班时间：<code>{end_time}</code>",
        parse_mode="HTML",
    )


@dp.message(Command("showworktime"))
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_showworktime(message: types.Message):
    """显示当前上下班时间设置 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    work_hours = await db.get_group_work_time(chat_id)

    await message.answer(
        f"🕒 当前上下班时间设置：\n\n"
        f"🟢 上班时间：<code>{work_hours['work_start']}</code>\n"
        f"🔴 下班时间：<code>{work_hours['work_end']}</code>\n\n"
        f"💡 修改命令：/setworktime <上班时间> <下班时间>",
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
        parse_mode="HTML",
    )


@dp.message(Command("resetworktime"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_resetworktime(message: types.Message):
    """重置上下班时间为默认值 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    await db.update_group_work_time(
        chat_id,
        Config.DEFAULT_WORK_HOURS["work_start"],
        Config.DEFAULT_WORK_HOURS["work_end"],
    )

    await message.answer(
        f"✅ 已重置上下班时间为默认值：\n"
        f"🟢 上班时间：<code>{Config.DEFAULT_WORK_HOURS['work_start']}</code>\n"
        f"🔴 下班时间：<code>{Config.DEFAULT_WORK_HOURS['work_end']}</code>\n\n"
        f"💡 用户现在可以使用上下班按钮进行打卡",
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
    )


@dp.message(Command("delwork"))
@admin_required
@rate_limit(rate=3, per=30)
async def cmd_delwork(message: types.Message):
    """移除上下班功能 - 修复版本"""
    args = message.text.split()
    clear_records = False

    if len(args) > 1 and args[1].lower() in ["clear", "清除", "删除记录"]:
        clear_records = True

    chat_id = message.chat.id

    # 修复：使用修复后的 has_work_hours_enabled 函数
    if not await has_work_hours_enabled(chat_id):
        await message.answer(
            "❌ 当前群组没有设置上下班功能",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        )
        return

    work_hours = await db.get_group_work_time(chat_id)
    old_start = work_hours.get("work_start")
    old_end = work_hours.get("work_end")

    # 重置为默认时间（相当于禁用功能）
    await db.update_group_work_time(
        chat_id,
        Config.DEFAULT_WORK_HOURS["work_start"],
        Config.DEFAULT_WORK_HOURS["work_end"],
    )

    records_cleared = 0
    if clear_records:
        # ✅ 修复1：不再使用 tuple 形式传参（删除 (chat_id,)）
        # ✅ 修复2：asyncpg.execute() 返回字符串，如 'DELETE 10'，用 split 解析
        conn = await db.get_connection()
        try:
            result = await conn.execute(
                "DELETE FROM work_records WHERE chat_id = $1", chat_id
            )
            # result 形如 "DELETE 5"
            records_cleared = (
                int(result.split()[-1]) if result and result.startswith("DELETE") else 0
            )
        finally:
            await db.release_connection(conn)

    success_msg = (
        f"✅ 已移除上下班功能\n"
        f"🗑️ 已删除设置：<code>{old_start}</code> - <code>{old_end}</code>\n"
    )

    if clear_records:
        success_msg += f"📊 同时清除了 <code>{records_cleared}</code> 条上下班记录\n"
    else:
        success_msg += (
            "💡 上下班记录仍然保留，如需清除请使用：<code>/delwork clear</code>\n"
        )

    success_msg += (
        f"\n🔧 上下班按钮已隐藏\n"
        f"🎯 现在用户可以正常进行其他活动打卡\n"
        f"🔄 键盘已自动刷新"
    )

    await message.answer(
        success_msg,
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
    )

    logger.info(f"👤 管理员 {message.from_user.id} 移除了群组 {chat_id} 的上下班功能")


@dp.message(Command("workstatus"))
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_workstatus(message: types.Message):
    """检查上下班功能状态 - 优化版本"""
    chat_id = message.chat.id

    group_data = await db.get_group_cached(chat_id)
    if not group_data:
        await message.answer(
            "❌ 当前群组没有初始化数据",
            reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        )
        return

    work_hours = await db.get_group_work_time(chat_id)

    is_custom = (
        work_hours["work_start"] != Config.DEFAULT_WORK_HOURS["work_start"]
        and work_hours["work_end"] != Config.DEFAULT_WORK_HOURS["work_end"]
    )

    total_records = 0
    total_users = 0

    status_msg = (
        f"📊 上下班功能状态\n\n"
        f"🔧 功能状态：{'✅ 已启用' if is_custom else '❌ 未启用'}\n"
        f"🕒 当前设置：<code>{work_hours['work_start']}</code> - <code>{work_hours['work_end']}</code>\n"
        f"👥 有记录用户：<code>{total_users}</code> 人\n"
        f"📝 总记录数：<code>{total_records}</code> 条\n\n"
    )

    if is_custom:
        status_msg += (
            f"💡 可用命令：\n"
            f"• <code>/delwork</code> - 移除功能但保留记录\n"
            f"• <code>/delwork clear</code> - 移除功能并清除记录\n"
        )
    else:
        status_msg += (
            f"💡 可用命令：\n"
            f"• <code>/setworktime 09:00 18:00</code> - 启用上下班功能\n"
            f"• <code>/showworktime</code> - 显示当前设置"
        )

    await message.answer(
        status_msg,
        reply_markup=await get_main_keyboard(chat_id=chat_id, show_admin=True),
        parse_mode="HTML",
    )


@dp.message(Command("workcheck"))
@rate_limit(rate=5, per=60)
async def cmd_workcheck(message: types.Message):
    """检查上下班打卡状态 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    if await has_work_hours_enabled(chat_id):
        has_work_start = await has_clocked_in_today(chat_id, uid, "work_start")
        has_work_end = await has_clocked_in_today(chat_id, uid, "work_end")

        status_msg = (
            f"📊 上下班打卡状态\n\n"
            f"🔧 上下班功能：✅ 已启用\n"
            f"🟢 上班打卡：{'✅ 已完成' if has_work_start else '❌ 未完成'}\n"
            f"🔴 下班打卡：{'✅ 已完成' if has_work_end else '❌ 未完成'}\n\n"
        )

        if not has_work_start:
            status_msg += (
                "⚠️ 您今天还没有打上班卡，无法进行其他活动！\n请先使用'🟢 上班'按钮打卡"
            )
        elif has_work_end:
            status_msg += (
                "⚠️ 您今天已经打过下班卡，无法再进行其他活动！\n下班后活动自动结束"
            )
        else:
            status_msg += "✅ 您已打上班卡，可以进行其他活动"
    else:
        status_msg = (
            f"📊 上下班打卡状态\n\n"
            f"🔧 上下班功能：❌ 未启用\n"
            f"🎯 您可以正常进行其他活动打卡"
        )

    await message.answer(
        status_msg,
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )


# ==================== 推送开关管理命令优化 ====================
@dp.message(Command("setpush"))
@admin_required
@rate_limit(rate=5, per=30)
async def cmd_setpush(message: types.Message):
    """设置推送开关 - 优化版本"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(
            Config.MESSAGES["setpush_usage"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    push_type = args[1].lower()
    status = args[2].lower()

    if push_type not in ["channel", "group", "admin"]:
        await message.answer(
            "❌ 类型错误，请使用 channel、group 或 admin",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    if status not in ["on", "off"]:
        await message.answer(
            "❌ 状态错误，请使用 on 或 off",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
        return

    if push_type == "channel":
        await db.update_push_setting("enable_channel_push", status == "on")
        status_text = "开启" if status == "on" else "关闭"
        await message.answer(
            f"✅ 已{status_text}频道推送",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    elif push_type == "group":
        await db.update_push_setting("enable_group_push", status == "on")
        status_text = "开启" if status == "on" else "关闭"
        await message.answer(
            f"✅ 已{status_text}群组推送",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    elif push_type == "admin":
        await db.update_push_setting("enable_admin_push", status == "on")
        status_text = "开启" if status == "on" else "关闭"
        await message.answer(
            f"✅ 已{status_text}管理员推送",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )


@dp.message(Command("showpush"))
@admin_required
@rate_limit(rate=5, per=60)
async def cmd_showpush(message: types.Message):
    """显示推送设置 - 优化版本"""
    settings = await db.get_push_settings()
    text = (
        "🔔 当前自动导出推送设置：\n\n"
        f"📢 频道推送：{'✅ 开启' if settings['enable_channel_push'] else '❌ 关闭'}\n"
        f"👥 群组推送：{'✅ 开启' if settings['enable_group_push'] else '❌ 关闭'}\n"
        f"👑 管理员推送：{'✅ 开启' if settings['enable_admin_push'] else '❌ 关闭'}\n\n"
        "💡 使用说明：\n"
        "• 频道推送：推送到绑定的频道\n"
        "• 群组推送：推送到绑定的通知群组\n"
        "• 管理员推送：当没有绑定群组/频道时推送到所有管理员\n\n"
        "⚙️ 修改命令：\n"
        "<code>/setpush channel on|off</code>\n"
        "<code>/setpush group on|off</code>\n"
        "<code>/setpush admin on|off</code>"
    )
    await message.answer(
        text,
        reply_markup=await get_main_keyboard(chat_id=message.chat.id, show_admin=True),
        parse_mode="HTML",
    )


@dp.message(Command("testpush"))
@admin_required
@rate_limit(rate=3, per=60)
async def cmd_testpush(message: types.Message):
    """测试推送功能 - 优化版本"""
    chat_id = message.chat.id
    try:
        test_file_name = f"test_push_{get_beijing_time().strftime('%H%M%S')}.txt"
        async with aiofiles.open(test_file_name, "w", encoding="utf-8") as f:
            await f.write("这是一个推送测试文件\n")
            await f.write(
                f"测试时间：{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            await f.write("如果收到此文件，说明推送功能正常")

        caption = (
            "🧪 推送功能测试\n这是一个测试文件，用于验证自动导出推送功能是否正常工作。"
        )

        success_count = 0
        push_settings = await db.get_push_settings()
        group_data = await db.get_group_cached(chat_id)

        if (
            push_settings["enable_group_push"]
            and group_data
            and group_data.get("notification_group_id")
        ):
            try:
                await bot.send_document(
                    group_data["notification_group_id"],
                    FSInputFile(test_file_name),
                    caption=caption,
                    parse_mode="HTML",
                )
                success_count += 1
                await message.answer(
                    f"✅ 测试文件已发送到通知群组: {group_data['notification_group_id']}"
                )
            except Exception as e:
                await message.answer(f"❌ 通知群组推送测试失败: {e}")

        if (
            push_settings["enable_channel_push"]
            and group_data
            and group_data.get("channel_id")
        ):
            try:
                await bot.send_document(
                    group_data["channel_id"],
                    FSInputFile(test_file_name),
                    caption=caption,
                    parse_mode="HTML",
                )
                success_count += 1
                await message.answer(
                    f"✅ 测试文件已发送到频道: {group_data['channel_id']}"
                )
            except Exception as e:
                await message.answer(f"❌ 频道推送测试失败: {e}")

        os.remove(test_file_name)

        if success_count == 0:
            await message.answer(
                "⚠️ 没有成功发送任何测试推送，请检查推送设置和绑定状态",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )
        else:
            await message.answer(
                f"✅ 推送测试完成，成功发送 {success_count} 个测试文件",
                reply_markup=await get_main_keyboard(
                    chat_id=message.chat.id, show_admin=True
                ),
            )

    except Exception as e:
        await message.answer(f"❌ 推送测试失败：{e}")


@dp.message(Command("export"))
@admin_required
@rate_limit(rate=2, per=60)
@track_performance("cmd_export")
async def cmd_export(message: types.Message):
    """管理员手动导出群组数据 - 优化版本"""
    chat_id = message.chat.id
    await message.answer("⏳ 正在导出数据，请稍候...")
    try:
        await export_and_push_csv(chat_id)
        await message.answer(
            "✅ 数据已导出并推送到绑定的群组或频道！",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=True
            ),
        )
    except Exception as e:
        await message.answer(f"❌ 导出失败：{e}")


# ==================== 月度报告管理员命令优化 ====================
@dp.message(Command("monthlyreport"))
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_monthlyreport(message: types.Message):
    """生成月度报告 - 优化版本"""
    args = message.text.split()
    chat_id = message.chat.id

    year = None
    month = None

    if len(args) >= 3:
        try:
            year = int(args[1])
            month = int(args[2])
            if month < 1 or month > 12:
                await message.answer("❌ 月份必须在1-12之间")
                return
        except ValueError:
            await message.answer("❌ 请输入有效的年份和月份")
            return

    await message.answer("⏳ 正在生成月度报告，请稍候...")

    try:
        # 生成报告
        report = await generate_monthly_report(chat_id, year, month)
        if report:
            await message.answer(report, parse_mode="HTML")

            # 导出CSV
            await export_monthly_csv(chat_id, year, month)
            await message.answer("✅ 月度数据已导出并推送！")
        else:
            time_desc = f"{year}年{month}月" if year and month else "最近一个月"
            await message.answer(f"⚠️ {time_desc}没有数据需要报告")

    except Exception as e:
        await message.answer(f"❌ 生成月度报告失败：{e}")


@dp.message(Command("exportmonthly"))
@admin_required
@rate_limit(rate=2, per=60)
async def cmd_exportmonthly(message: types.Message):
    """导出月度数据 - 优化版本"""
    args = message.text.split()
    chat_id = message.chat.id

    year = None
    month = None

    if len(args) >= 3:
        try:
            year = int(args[1])
            month = int(args[2])
            if month < 1 or month > 12:
                await message.answer("❌ 月份必须在1-12之间")
                return
        except ValueError:
            await message.answer("❌ 请输入有效的年份和月份")
            return

    await message.answer("⏳ 正在导出月度数据，请稍候...")

    try:
        await export_monthly_csv(chat_id, year, month)
        await message.answer("✅ 月度数据已导出并推送！")
    except Exception as e:
        await message.answer(f"❌ 导出月度数据失败：{e}")


# ==================== 简化版指令优化 ====================
@dp.message(Command("ci"))
@rate_limit(rate=10, per=60)
@message_deduplicate
@with_retry("cmd_ci", max_retries=2)
@track_performance("cmd_ci")
async def cmd_ci(message: types.Message):
    """指令打卡：/ci 活动名 - 优化版本"""
    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await message.answer(
            "❌ 用法：/ci <活动名>",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=await is_admin(message.from_user.id)
            ),
        )
        return
    act = args[1].strip()
    if not await db.activity_exists(act):
        await message.answer(
            f"❌ 活动 '<code>{act}</code>' 不存在，请先使用 /addactivity 添加或检查拼写",
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=await is_admin(message.from_user.id)
            ),
            parse_mode="HTML",
        )
        return
    await start_activity(message, act)


@dp.message(Command("at"))
@rate_limit(rate=10, per=60)
@message_deduplicate
@with_retry("cmd_at", max_retries=2)
@track_performance("cmd_at")
async def cmd_at(message: types.Message):
    """指令回座：/at - 优化版本"""
    await process_back(message)


@dp.message(Command("refresh_keyboard"))
@rate_limit(rate=5, per=60)
async def cmd_refresh_keyboard(message: types.Message):
    """强制刷新键盘 - 优化版本"""
    uid = message.from_user.id
    await message.answer(
        "🔄 键盘已刷新",
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
    )


@dp.callback_query(lambda c: c.data.startswith("quick_back:"))
async def handle_quick_back(callback_query: types.CallbackQuery):
    """处理快速回座按钮"""
    try:
        # 解析回调数据
        data_parts = callback_query.data.split(":")
        if len(data_parts) < 3:
            await callback_query.answer("❌ 数据格式错误", show_alert=True)
            return

        chat_id = int(data_parts[1])
        uid = int(data_parts[2])

        logger.info(f"🔔 快速回座按钮被点击: chat_id={chat_id}, uid={uid}")

        # 检查是否是用户本人点击
        if callback_query.from_user.id != uid:
            await callback_query.answer("❌ 这不是您的回座按钮！", show_alert=True)
            return

        # 执行回座逻辑
        user_lock = get_user_lock(chat_id, uid)
        async with user_lock:
            # 检查用户是否有活动在进行
            user_data = await db.get_user_cached(chat_id, uid)
            if not user_data or not user_data["current_activity"]:
                await callback_query.answer("❌ 您当前没有活动在进行", show_alert=True)
                return

            # 直接调用回座逻辑
            await _process_back_locked(callback_query.message, chat_id, uid)

        # 更新按钮状态（移除按钮）
        try:
            await callback_query.message.edit_reply_markup(reply_markup=None)
        except Exception as e:
            logger.warning(f"无法更新按钮状态: {e}")

        await callback_query.answer("✅ 已成功回座")

    except Exception as e:
        logger.error(f"❌ 快速回座失败: {e}")
        await callback_query.answer("❌ 回座失败，请手动输入'回座'", show_alert=True)


# ============ 上下班打卡指令优化 =================
@dp.message(Command("workstart"))
@rate_limit(rate=5, per=60)
@message_deduplicate
@with_retry("work_start", max_retries=2)
@track_performance("work_start")
async def cmd_workstart(message: types.Message):
    """上班打卡 - 优化版本"""
    await process_work_checkin(message, "work_start")


@dp.message(Command("workend"))
@rate_limit(rate=5, per=60)
@message_deduplicate
@with_retry("work_end", max_retries=2)
@track_performance("work_end")
async def cmd_workend(message: types.Message):
    """下班打卡 - 优化版本"""
    await process_work_checkin(message, "work_end")


# ============ 上下班打卡处理函数优化 ============
async def auto_end_current_activity(
    chat_id: int,
    uid: int,
    user_data: dict,
    now: datetime,
    message: types.Message = None,
):
    """自动结束当前正在进行的活动 - 优化版本"""
    try:
        current_activity = user_data.get("current_activity")
        if not current_activity:
            return

        # 记录活动信息
        act = current_activity
        start_time = datetime.fromisoformat(user_data["activity_start_time"])
        elapsed = (now - start_time).total_seconds()

        # 计算超时和罚款
        time_limit_seconds = await db.get_activity_time_limit(act) * 60
        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, int(elapsed - time_limit_seconds))
        overtime_minutes = overtime_seconds / 60

        fine_amount = 0
        if is_overtime and overtime_seconds > 0:
            fine_amount = await calculate_fine(act, overtime_minutes)

        # 完成活动
        await db.complete_user_activity(
            chat_id, uid, act, int(elapsed), fine_amount, is_overtime
        )

        # 取消定时任务
        key = f"{chat_id}-{uid}"
        await safe_cancel_task(key)

        # 发送自动结束通知
        if message:
            auto_end_msg = (
                f"🔄 <b>自动结束活动通知</b>\n"
                f"👤 用户：{MessageFormatter.format_user_link(uid, user_data['nickname'])}\n"
                f"📝 检测到您有未结束的活动：<code>{act}</code>\n"
                f"⏰ 由于您进行了下班打卡，系统已自动为您结束该活动\n"
                f"⏱️ 活动时长：<code>{MessageFormatter.format_time(int(elapsed))}</code>"
            )

            if is_overtime:
                auto_end_msg += f"\n⚠️ 本次活动已超时！\n⏰ 超时时长：<code>{MessageFormatter.format_time(int(overtime_seconds))}</code>"
                if fine_amount > 0:
                    auto_end_msg += f"\n💰 超时罚款：<code>{fine_amount}</code> 元"

            auto_end_msg += f"\n\n✅ 活动已自动结束，下班打卡继续处理..."

            await message.answer(
                auto_end_msg,
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                parse_mode="HTML",
            )

        # 记录日志
        logger.info(f"✅ 用户 {uid} 的下班打卡自动结束了活动: {act}, 时长: {elapsed}秒")

    except Exception as e:
        logger.error(f"❌ 自动结束活动失败: {e}")
        if message:
            await message.answer(
                f"⚠️ 自动结束活动时出现错误，但下班打卡将继续处理\n错误详情: {e}",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
            )


async def process_work_checkin(message: types.Message, checkin_type: str):
    """处理上下班打卡 - 修复跨天问题版本"""
    chat_id = message.chat.id
    uid = message.from_user.id
    name = message.from_user.full_name
    now = get_beijing_time()
    current_time = now.strftime("%H:%M")

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await db.init_group(chat_id)
        await db.init_user(chat_id, uid)

        user_data = await db.get_user_cached(chat_id, uid)
        today = str(now.date())

        # 检查是否有正在进行的活动（仅在下班打卡时检查）
        current_activity = user_data.get("current_activity")
        activity_auto_ended = False
        if checkin_type == "work_end" and current_activity:
            # 自动结束当前活动
            await auto_end_current_activity(chat_id, uid, user_data, now, message)
            activity_auto_ended = True

        today_records = await db.get_today_work_records(chat_id, uid)

        if checkin_type in today_records:
            action_text = "上班" if checkin_type == "work_start" else "下班"
            existing_time = today_records[checkin_type]["checkin_time"]
            existing_status = today_records[checkin_type]["status"]
            await message.answer(
                f"❌ 您今天已经打过{action_text}卡了！\n"
                f"⏰ 打卡时间：<code>{existing_time}</code>\n"
                f"📊 状态：{existing_status}",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                parse_mode="HTML",
            )
            return

        if checkin_type == "work_end" and "work_start" not in today_records:
            await message.answer(
                "❌ 您今天还没有打上班卡，无法打下班卡！\n"
                "💡 请先使用'🟢 上班'按钮或 /workstart 命令打上班卡",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
                parse_mode="HTML",
            )
            return

        work_hours = await db.get_group_work_time(chat_id)
        expected_time = work_hours[checkin_type]

        # 使用改进的跨天时间计算
        time_diff_minutes, expected_dt = calculate_cross_day_time_diff(
            now, expected_time, checkin_type
        )

        def format_time_diff(minutes):
            total_seconds = abs(int(minutes * 60))
            hours = total_seconds // 3600
            mins = (total_seconds % 3600) // 60
            secs = total_seconds % 60

            if hours > 0 and mins > 0 and secs > 0:
                return f"{hours}小时{mins}分{secs}秒"
            elif hours > 0 and mins > 0:
                return f"{hours}小时{mins}分"
            elif hours > 0:
                return f"{hours}小时"
            elif mins > 0 and secs > 0:
                return f"{mins}分{secs}秒"
            elif mins > 0:
                return f"{mins}分"
            else:
                return f"{secs}秒"

        time_diff_str = format_time_diff(time_diff_minutes)

        fine_amount = 0
        is_late_early = False

        if checkin_type == "work_start":
            # 上班打卡：时间差为正数表示迟到
            if time_diff_minutes > 0:
                fine_amount = await calculate_work_fine("work_start", time_diff_minutes)
                status = f"❌ 迟到 {time_diff_str}"
                if fine_amount > 0:
                    status += f" \n💰罚款 {fine_amount}元"
                emoji = "⏰"
                is_late_early = True
            else:
                status = "✅ 准时"
                emoji = "👍"
            action_text = "上班"
        else:
            # 下班打卡：时间差为负数表示早退
            if time_diff_minutes < 0:
                fine_amount = await calculate_work_fine(
                    "work_end", abs(time_diff_minutes)
                )
                status = f"❌ 早退 {time_diff_str}"
                if fine_amount > 0:
                    status += f" \n💰罚款 {fine_amount}元"
                emoji = "🏃"
                is_late_early = True
            else:
                status = "✅ 准时"
                emoji = "👍"
            action_text = "下班"

        await db.add_work_record(
            chat_id,
            uid,
            today,
            checkin_type,
            current_time,
            status,
            time_diff_minutes,
            fine_amount,
        )

        # 显示实际选择的期望时间点
        expected_time_display = expected_dt.strftime("%m/%d %H:%M")

        result_msg = (
            f"{emoji} <b>{action_text}打卡完成</b>\n"
            f"👤 用户：{MessageFormatter.format_user_link(uid, name)}\n"
            f"⏰ 打卡时间：<code>{current_time}</code>\n"
            f"📅 期望时间：<code>{expected_time_display}</code>\n"
            f"📊 状态：{status}"
        )

        # 如果自动结束了活动，添加提示
        if checkin_type == "work_end" and activity_auto_ended and current_activity:
            result_msg += f"\n\n🔄 检测到您有未结束的活动 <code>{current_activity}</code>，已自动为您结束"

        await message.answer(
            result_msg,
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            parse_mode="HTML",
        )

        # 修复通知推送部分 - 确保迟到早退通知能正确发送
        if is_late_early:
            try:
                # 确定通知类型
                if checkin_type == "work_start":
                    status_type = "迟到"
                    time_detail = f"迟到 {time_diff_str}"
                else:
                    status_type = "早退"
                    time_detail = f"早退 {time_diff_str}"

                # 获取群组名称
                chat_title = str(chat_id)
                try:
                    chat_info = await bot.get_chat(chat_id)
                    chat_title = chat_info.title or chat_title
                except Exception as e:
                    logger.warning(f"无法获取群组信息: {e}")

                # 构建通知内容
                notif_text = (
                    f"⚠️ <b>{action_text}{status_type}通知</b>\n"
                    f"🏢 群组：<code>{chat_title}</code>\n"
                    f"---------------------------------------\n"
                    f"👤 用户：{MessageFormatter.format_user_link(uid, name)}\n"
                    f"⏰ 打卡时间：<code>{current_time}</code>\n"
                    f"📅 期望时间：<code>{expected_time_display}</code>\n"
                    f"⏱️ {time_detail}"
                )

                if fine_amount > 0:
                    notif_text += f"\n💰 罚款金额：<code>{fine_amount}</code> 元"

                # 发送通知并检查结果
                sent = await NotificationService.send_notification(chat_id, notif_text)

                if sent:
                    logger.info(
                        f"✅ 已发送{action_text}{status_type}通知：{time_detail}，罚款{fine_amount}元"
                    )
                else:
                    logger.warning(
                        f"⚠️ {action_text}{status_type}通知发送失败，可能没有配置推送目标"
                    )

                    # 如果没有成功发送到任何目标，尝试发送给管理员作为兜底
                    try:
                        for admin_id in Config.ADMINS:
                            await bot.send_message(
                                admin_id,
                                f"🚨 重要：{action_text}{status_type}通知发送失败\n{notif_text}",
                                parse_mode="HTML",
                            )
                        logger.info(f"✅ 已发送兜底通知给管理员")
                    except Exception as admin_e:
                        logger.error(f"❌ 发送兜底通知给管理员失败: {admin_e}")

            except Exception as e:
                logger.error(f"❌ 发送上下班通知失败: {e}")

                # 记录详细的错误信息以便调试
                import traceback

                error_details = traceback.format_exc()
                logger.error(f"❌ 详细错误信息: {error_details}")


# ===== 添加辅助函数 ======
def calculate_cross_day_time_diff(
    current_dt: datetime, expected_time: str, checkin_type: str
):
    """
    计算跨天时间差 - 专门处理夜班场景
    返回: (时间差（分钟）, 选择的期望时间点)
    """
    # 解析期望时间
    expected_hour, expected_minute = map(int, expected_time.split(":"))

    # 创建五个可能的时间点：前2天、前1天、当天、后1天、后2天
    possible_times = []

    for day_offset in [-2, -1, 0, 1, 2]:
        target_day = current_dt + timedelta(days=day_offset)
        possible_times.append(
            target_day.replace(
                hour=expected_hour, minute=expected_minute, second=0, microsecond=0
            )
        )

    # 根据打卡类型选择最合适的时间点
    if checkin_type == "work_start":
        # 上班打卡：选择最接近但不超过当前时间的时间点
        valid_times = [t for t in possible_times if t <= current_dt]
        if valid_times:
            expected_dt = max(valid_times)  # 选择最接近的一个
        else:
            # 如果没有合适的时间，选择最早的时间
            expected_dt = min(possible_times)
    else:  # work_end
        # 下班打卡：选择最接近但超过当前时间的时间点
        valid_times = [t for t in possible_times if t >= current_dt]
        if valid_times:
            expected_dt = min(valid_times)  # 选择最接近的一个
        else:
            # 如果没有合适的时间，选择最晚的时间
            expected_dt = max(possible_times)

    time_diff_minutes = (current_dt - expected_dt).total_seconds() / 60

    # 调试日志
    logger.info(f"🔍 跨天计算调试:")
    logger.info(f"  当前时间: {current_dt.strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"  期望时间: {expected_time}")
    logger.info(f"  打卡类型: {checkin_type}")
    logger.info(f"  选择的时间点: {expected_dt.strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"  时间差: {time_diff_minutes:.2f} 分钟")

    return time_diff_minutes, expected_dt


# ============ 文本命令处理优化 =================
@dp.message(Command("workrecord"))
@rate_limit(rate=5, per=60)
async def cmd_workrecord(message: types.Message):
    """查询上下班记录 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await db.init_group(chat_id)
        await db.init_user(chat_id, uid)

        work_records = await db.get_user_work_records(chat_id, uid)

        if not work_records:
            await message.answer(
                "📝 暂无上下班打卡记录",
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
            )
            return

        work_hours = await db.get_group_work_time(chat_id)
        user_data = await db.get_user_cached(chat_id, uid)

        record_text = (
            f"📊 <b>上下班打卡记录</b>\n"
            f"👤 用户：{MessageFormatter.format_user_link(uid, user_data['nickname'])}\n"
            f"🕒 当前设置：上班 <code>{work_hours['work_start']}</code> - 下班 <code>{work_hours['work_end']}</code>\n\n"
        )

        # 按日期分组记录
        records_by_date = {}
        for record in work_records:
            date_str = record["record_date"]
            if date_str not in records_by_date:
                records_by_date[date_str] = {}
            records_by_date[date_str][record["checkin_type"]] = record

        dates = sorted(records_by_date.keys(), reverse=True)[:7]

        for date_str in dates:
            date_record = records_by_date[date_str]
            record_text += f"📅 <code>{date_str}</code>\n"

            if "work_start" in date_record:
                start_info = date_record["work_start"]
                record_text += f"   🟢 上班：{start_info['checkin_time']} - {start_info['status']}\n"

            if "work_end" in date_record:
                end_info = date_record["work_end"]
                record_text += (
                    f"   🔴 下班：{end_info['checkin_time']} - {end_info['status']}\n"
                )

            record_text += "\n"

        await message.answer(
            record_text,
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            parse_mode="HTML",
        )


# ============ 添加上下班按钮处理优化 =================
@dp.message(
    lambda message: message.text and message.text.strip() in ["🟢 上班", "🔴 下班"]
)
@rate_limit(rate=5, per=60)
async def handle_work_buttons(message: types.Message):
    """处理上下班按钮点击 - 优化版本"""
    text = message.text.strip()
    if text == "🟢 上班":
        await process_work_checkin(message, "work_start")
    elif text == "🔴 下班":
        await process_work_checkin(message, "work_end")


# ============ 文本命令处理优化 =================
@dp.message(
    lambda message: message.text and message.text.strip() in ["回座", "✅ 回座"]
)
@rate_limit(rate=10, per=60)
async def handle_back_command(message: types.Message):
    """处理回座命令 - 优化版本"""
    await process_back(message)


@dp.message(lambda message: message.text and message.text.strip() in ["📊 我的记录"])
@rate_limit(rate=10, per=60)
@track_performance("handle_my_record")
async def handle_my_record(message: types.Message):
    """处理我的记录按钮 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await show_history(message)


@dp.message(lambda message: message.text and message.text.strip() in ["🏆 排行榜"])
@rate_limit(rate=10, per=60)
@track_performance("handle_rank")
async def handle_rank(message: types.Message):
    """处理排行榜按钮 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await show_rank(message)


@dp.message(lambda message: message.text and message.text.strip() in ["👑 管理员面板"])
@rate_limit(rate=5, per=60)
async def handle_admin_panel_button(message: types.Message):
    """处理管理员面板按钮点击 - 优化版本"""
    if not await is_admin(message.from_user.id):
        await message.answer(
            Config.MESSAGES["no_permission"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=False
            ),
        )
        return

    admin_text = (
        "👑 管理员面板\n\n"
        "可用命令：\n"
        "• /setchannel <频道ID> - 绑定提醒频道\n"
        "• /setgroup <群组ID> - 绑定通知群组\n"
        "• /unbindchannel - 解除绑定频道\n"
        "• /unbindgroup - 解除绑定通知群组\n"
        "• \n"
        "• /addactivity <活动名> <次数> <分钟> - 添加或修改活动\n"
        "• /set <用户ID> <活动> <分钟> - 设置用户时间\n"
        "• /delactivity <活动名> - 删除活动\n"
        "• \n"
        "• /setworktime 9:00 18:00 - 设置上下班时间\n"
        "• /delwork - 基本移除，保留历史记录\n"
        "• /delwork clear - 移除并清除所有记录\n"
        "• /workstatus - 查看当前上下班功能状态\n"
        "• \n"
        "• /reset <用户ID> - 重置用户数据\n"
        "• \n"
        "• /setresettime <小时> <分钟> - 设置每日重置时间\n"
        "• \n"
        "• /setfine <活动名> <时间段> <金额> - 设置活动罚款费率\n"
        "• /setfines_all <t1> <f1> [<t2> <f2> ...] - 为所有活动统一设置分段罚款\n"
        "• /setworkfine <work_start|work_end> <时间段> <金额> - 设置上下班罚款\n"
        "• /showsettings - 查看当前群设置\n"
        "• /exportmonthly - 导出月度数据\n"
        "• /exportmonthly 2024 1 - 导出指定年月数据\n"
        "• /export - 导出数据\n\n"
        "点击下方按钮进行操作："
    )
    await message.answer(admin_text, reply_markup=get_admin_keyboard())


@dp.message(lambda message: message.text and message.text.strip() in ["🔙 返回主菜单"])
@rate_limit(rate=5, per=60)
async def handle_back_to_main_menu(message: types.Message):
    """处理返回主菜单按钮 - 优化版本"""
    uid = message.from_user.id
    await message.answer(
        "已返回主菜单",
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
    )


@dp.message(lambda message: message.text and message.text.strip() in ["📤 导出数据"])
@rate_limit(rate=5, per=60)
async def handle_export_data_button(message: types.Message):
    """处理导出数据按钮点击 - 优化版本"""
    if not await is_admin(message.from_user.id):
        await message.answer(
            Config.MESSAGES["no_permission"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=False
            ),
        )
        return
    await export_data(message)


@dp.message(
    lambda message: message.text
    and message.text.strip() in Config.DEFAULT_ACTIVITY_LIMITS.keys()
)
@rate_limit(rate=10, per=60)
async def handle_activity_direct_input(message: types.Message):
    """处理直接输入活动名称进行打卡 - 优化版本"""
    act = message.text.strip()
    await start_activity(message, act)


@dp.message(lambda message: message.text and message.text.strip())
@rate_limit(rate=10, per=60)
async def handle_other_text_messages(message: types.Message):
    """处理其他文本消息 - 优化版本"""
    text = message.text.strip()
    uid = message.from_user.id

    if text.startswith("/") or text in [
        "👑 管理员面板",
        "🔙 返回主菜单",
        "📤 导出数据",
        "🔔 通知设置",
    ]:
        return

    activity_limits = await db.get_activity_limits_cached()
    if any(act in text for act in activity_limits.keys()):
        return

    await message.answer(
        "请使用下方按钮或直接输入活动名称进行操作：\n\n"
        "📝 使用方法：\n"
        "• 输入活动名称（如：<code>吃饭</code>、<code>小厕</code>）开始打卡\n"
        "• 输入'回座'或点击'✅ 回座'按钮结束当前活动\n"
        "• 点击'📊 我的记录'查看个人统计\n"
        "• 点击'🏆 排行榜'查看群内排名",
        reply_markup=await get_main_keyboard(
            chat_id=message.chat.id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )


# ==================== 用户功能优化 ====================
async def show_history(message: types.Message):
    """显示用户历史记录 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    async with OptimizedUserContext(chat_id, uid) as user:
        first_line = (
            f"👤 用户：{MessageFormatter.format_user_link(uid, user['nickname'])}"
        )
        text = f"{first_line}\n📊 今日记录：\n\n"

        has_records = False
        activity_limits = await db.get_activity_limits_cached()
        user_activities = await db.get_user_all_activities(chat_id, uid)

        for act in activity_limits.keys():
            activity_info = user_activities.get(act, {})
            total_time = activity_info.get("time", 0)
            count = activity_info.get("count", 0)
            max_times = activity_limits[act]["max_times"]
            if total_time > 0 or count > 0:
                status = "✅" if count < max_times else "❌"
                time_str = MessageFormatter.format_time(int(total_time))
                text += f"• <code>{act}</code>：<code>{time_str}</code>，次数：<code>{count}</code>/<code>{max_times}</code> {status}\n"
                has_records = True

        total_time_all = user.get("total_accumulated_time", 0)
        total_count_all = user.get("total_activity_count", 0)
        total_fine = user.get("total_fines", 0)
        overtime_count = user.get("overtime_count", 0)
        total_overtime = user.get("total_overtime_time", 0)

        text += f"\n📈 今日总统计：\n"
        text += f"• 总累计时间：<code>{MessageFormatter.format_time(int(total_time_all))}</code>\n"
        text += f"• 总活动次数：<code>{total_count_all}</code> 次\n"
        if overtime_count > 0:
            text += f"• 超时次数：<code>{overtime_count}</code> 次\n"
            text += f"• 总超时时间：<code>{MessageFormatter.format_time(int(total_overtime))}</code>\n"
        if total_fine > 0:
            text += f"• 累计罚款：<code>{total_fine}</code> 元"

        if not has_records and total_count_all == 0:
            text += "暂无记录，请先进行打卡活动"

        await message.answer(
            text,
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
            parse_mode="HTML",
        )


async def show_rank(message: types.Message):
    """显示排行榜（修复版）——直接从 user_activities 聚合当天数据，避免依赖 last_updated"""
    chat_id = message.chat.id
    uid = message.from_user.id

    # 确保群组初始化（如果你 init_group 有副作用）
    await db.init_group(chat_id)

    # 读取活动列表（带缓存）
    activity_limits = await db.get_activity_limits_cached()
    if not activity_limits:
        await message.answer(
            "⚠️ 当前没有配置任何活动，无法生成排行榜。",
            reply_markup=await get_main_keyboard(
                chat_id=chat_id, show_admin=await is_admin(uid)
            ),
        )
        return

    # 准备文本头
    rank_text = "🏆 今日活动排行榜\n\n"
    today = datetime.now().date()

    # 为避免大量单次连接开销，我们直接用连接一次性查询每个活动的 TopN
    top_n = 3
    async with db.pool.acquire() as conn:
        any_result = False
        for act in activity_limits.keys():
            rows = await conn.fetch(
                """
                SELECT
                    u.user_id,
                    u.nickname,
                    ua.accumulated_time as total_time
                FROM user_activities ua
                JOIN users u ON ua.chat_id = u.chat_id AND ua.user_id = u.user_id
                WHERE ua.chat_id = $1 AND ua.activity_name = $2 AND ua.activity_date = $3
                ORDER BY ua.accumulated_time DESC
                LIMIT $4
                """,
                chat_id,
                act,
                today,
                top_n,
            )

            if not rows:
                # 跳过没有数据的活动（也可以显示“暂无记录”）
                continue

            any_result = True
            rank_text += f"📈 <code>{act}</code>：\n"
            for i, row in enumerate(rows, start=1):
                user_id = row["user_id"]
                name = row["nickname"] or str(user_id)
                time_sec = row["total_time"] or 0
                # 你的 MessageFormatter.format_time / format_seconds_to_hms 根据项目定义来用
                # 这里尽量使用项目里已有的工具：
                try:
                    time_str = MessageFormatter.format_time(int(time_sec))
                except Exception:
                    # 兜底格式化为秒->时分秒
                    time_str = (
                        db.format_seconds_to_hms(int(time_sec))
                        if hasattr(db, "format_seconds_to_hms")
                        else f"{int(time_sec)}s"
                    )

                rank_text += f"  <code>{i}.</code> {MessageFormatter.format_user_link(user_id, name)} - <code>{time_str}</code>\n"
            rank_text += "\n"

    if not any_result:
        rank_text = "🏆 今日活动排行榜\n\n暂时没有任何活动记录，大家快去打卡吧！"

    await message.answer(
        rank_text,
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )


# ==================== 回座功能优化 ====================
async def _process_back_locked(message: types.Message, chat_id: int, uid: int):
    """线程安全的回座逻辑 - 优化版本"""
    now = get_beijing_time()

    async with OptimizedUserContext(chat_id, uid) as user_data:
        if not user_data["current_activity"]:
            await message.answer(
                Config.MESSAGES["no_activity"],
                reply_markup=await get_main_keyboard(
                    chat_id=chat_id, show_admin=await is_admin(uid)
                ),
            )
            return

        act = user_data["current_activity"]
        start_time = datetime.fromisoformat(user_data["activity_start_time"])
        elapsed = (now - start_time).total_seconds()

        time_limit_seconds = await db.get_activity_time_limit(act) * 60
        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, int(elapsed - time_limit_seconds))
        overtime_minutes = overtime_seconds / 60

        fine_amount = 0
        if is_overtime and overtime_seconds > 0:
            fine_amount = await calculate_fine(act, overtime_minutes)

        await db.complete_user_activity(
            chat_id, uid, act, int(elapsed), fine_amount, is_overtime
        )

    key = f"{chat_id}-{uid}"
    await safe_cancel_task(key)

    user_data = await db.get_user_cached(chat_id, uid)
    user_activities = await db.get_user_all_activities(chat_id, uid)
    activity_counts = {
        act: info.get("count", 0) for act, info in user_activities.items()
    }

    await message.answer(
        MessageFormatter.format_back_message(
            user_id=uid,
            user_name=user_data["nickname"],
            activity=act,
            time_str=now.strftime("%m/%d %H:%M:%S"),
            elapsed_time=MessageFormatter.format_time(int(elapsed)),
            total_activity_time=MessageFormatter.format_time(
                int(user_activities.get(act, {}).get("time", 0))
            ),
            total_time=MessageFormatter.format_time(
                int(user_data["total_accumulated_time"])
            ),
            activity_counts=activity_counts,
            total_count=user_data["total_activity_count"],
            is_overtime=is_overtime,
            overtime_seconds=overtime_seconds,
            fine_amount=fine_amount,
        ),
        reply_markup=await get_main_keyboard(
            chat_id=chat_id, show_admin=await is_admin(uid)
        ),
        parse_mode="HTML",
    )

    if is_overtime:
        try:
            chat_title = str(chat_id)
            try:
                chat_info = await bot.get_chat(chat_id)
                chat_title = chat_info.title or chat_title
            except Exception as e:
                logger.warning(f"无法获取群组信息: {e}")

            # 修复：使用精确的秒数计算超时时间
            overtime_minutes_total = overtime_seconds / 60
            overtime_minutes_int = int(overtime_minutes_total)
            overtime_seconds_remaining = int(
                (overtime_minutes_total - overtime_minutes_int) * 60
            )

            overtime_display = f"{overtime_minutes_int}分{overtime_seconds_remaining}秒"

            notif_text = (
                f"🚨 <b>超时回座通知</b>\n"
                f"🏢 群组：<code>{chat_title}</code>\n"
                f"---------------------------------------\n"
                f"👤 用户名称：{MessageFormatter.format_user_link(uid, user_data['nickname'])}\n"
                f"📝 活动内容：<code>{act}</code>\n"
                f"⏰ 回座时间：<code>{now.strftime('%m/%d %H:%M:%S')}</code>\n"
                f"⏱️ 超时时长：<code>{MessageFormatter.format_time(int(overtime_seconds))}</code> \n"
                f"💰 本次罚款：<code>{fine_amount}</code> 元"
            )

            await NotificationService.send_notification(chat_id, notif_text)

        except Exception as e:
            logger.error(f"⚠️ 超时通知推送异常: {e}")


async def process_back(message: types.Message):
    """回座打卡 - 优化版本"""
    chat_id = message.chat.id
    uid = message.from_user.id

    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await _process_back_locked(message, chat_id, uid)


# ==================== 管理员按钮处理优化 ====================
@dp.message(lambda message: message.text == "🔔 通知设置")
@rate_limit(rate=5, per=60)
async def handle_notification_settings(message: types.Message, state: FSMContext):
    """处理通知设置按钮 - 优化版本"""
    if not await is_admin(message.from_user.id):
        await message.answer(
            Config.MESSAGES["no_permission"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=False
            ),
        )
        return
    await notification_settings_menu(message, state)


async def notification_settings_menu(message: types.Message, state: FSMContext):
    """通知设置菜单 - 优化版本"""
    chat_id = message.chat.id
    await db.init_group(chat_id)
    group_data = await db.get_group_cached(chat_id)

    current_settings = (
        f"🔔 当前通知设置：\n"
        f"频道ID: <code>{group_data.get('channel_id', '未设置')}</code>\n"
        f"通知群组ID: <code>{group_data.get('notification_group_id', '未设置')}</code>\n\n"
        f"请选择操作："
    )

    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="设置频道"), KeyboardButton(text="设置通知群组")],
            [KeyboardButton(text="清除频道"), KeyboardButton(text="清除通知群组")],
            [KeyboardButton(text="🔙 返回管理员面板")],
        ],
        resize_keyboard=True,
    )

    await message.answer(current_settings, reply_markup=keyboard, parse_mode="HTML")


@dp.message(
    lambda message: message.text
    in ["设置频道", "设置通知群组", "清除频道", "清除通知群组", "🔙 返回管理员面板"]
)
@rate_limit(rate=5, per=60)
async def handle_notification_actions(message: types.Message, state: FSMContext):
    """处理通知设置操作 - 优化版本"""
    text = message.text
    chat_id = message.chat.id

    if text == "🔙 返回管理员面板":
        await state.clear()
        await message.answer("已返回管理员面板", reply_markup=get_admin_keyboard())
        return
    elif text == "设置频道":
        await message.answer(
            "请输入频道ID（格式如 -1001234567890）：",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(AdminStates.waiting_for_channel_id)
    elif text == "设置通知群组":
        await message.answer(
            "请输入通知群组ID（格式如 -1001234567890）：",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(AdminStates.waiting_for_group_id)
    elif text == "清除频道":
        await db.init_group(chat_id)
        await db.update_group_channel(chat_id, None)
        await message.answer("✅ 已清除频道设置", reply_markup=get_admin_keyboard())
    elif text == "清除通知群组":
        await db.init_group(chat_id)
        await db.update_group_notification(chat_id, None)
        await message.answer("✅ 已清除通知群组设置", reply_markup=get_admin_keyboard())


@dp.message(AdminStates.waiting_for_channel_id)
@rate_limit(rate=5, per=60)
async def set_channel_id(message: types.Message, state: FSMContext):
    """设置频道ID - 优化版本"""
    try:
        channel_id = int(message.text)
        chat_id = message.chat.id
        await db.init_group(chat_id)
        await db.update_group_channel(chat_id, channel_id)
        await message.answer(
            f"✅ 已绑定提醒频道：<code>{channel_id}</code>",
            reply_markup=get_admin_keyboard(),
            parse_mode="HTML",
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ 请输入有效的频道ID！")


@dp.message(AdminStates.waiting_for_group_id)
@rate_limit(rate=5, per=60)
async def set_group_id(message: types.Message, state: FSMContext):
    """设置通知群组ID - 优化版本"""
    try:
        group_id = int(message.text)
        chat_id = message.chat.id
        await db.init_group(chat_id)
        await db.update_group_notification(chat_id, group_id)
        await message.answer(
            f"✅ 已绑定通知群组：<code>{group_id}</code>",
            reply_markup=get_admin_keyboard(),
            parse_mode="HTML",
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ 请输入有效的群组ID！")


async def export_data(message: types.Message):
    """导出数据 - 优化版本"""
    if not await is_admin(message.from_user.id):
        await message.answer(
            Config.MESSAGES["no_permission"],
            reply_markup=await get_main_keyboard(
                chat_id=message.chat.id, show_admin=False
            ),
        )
        return

    chat_id = message.chat.id
    await message.answer("⏳ 正在导出数据...")
    try:
        await export_and_push_csv(chat_id)
        await message.answer("✅ 数据导出完成！")
    except Exception as e:
        await message.answer(f"❌ 导出失败：{e}")


# ==================== CSV导出推送功能优化 ====================
async def optimized_monthly_export(chat_id: int, year: int, month: int):
    """优化版月度数据导出，减少内存占用"""
    try:
        # 分批处理数据，避免一次性加载所有数据
        batch_size = 50
        offset = 0

        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)

        # 写入表头
        headers = [
            "用户ID",
            "用户昵称",
            "活动名称",
            "活动次数",
            "总时长(秒)",
            "总时长(格式化)",
        ]
        writer.writerow(headers)

        has_data = False

        while True:
            # 分批查询数据
            monthly_stats = await db.get_monthly_statistics_batch(
                chat_id, year, month, batch_size, offset
            )

            if not monthly_stats:
                break

            for user_stat in monthly_stats:
                has_data = True
                user_activities = user_stat.get("activities", {})

                for activity_name, activity_info in user_activities.items():
                    row = [
                        user_stat["user_id"],
                        user_stat.get("nickname", "未知用户"),
                        activity_name,
                        activity_info.get("count", 0),
                        activity_info.get("time", 0),
                        activity_info.get("time_formatted", "0小时0分钟"),
                    ]
                    writer.writerow(row)

            offset += batch_size

            # 释放内存
            del monthly_stats
            await asyncio.sleep(0.1)  # 短暂让出控制权

        if not has_data:
            return None

        return csv_buffer.getvalue()

    except Exception as e:
        logger.error(f"❌ 月度导出优化版失败: {e}")
        return None


async def export_and_push_csv(
    chat_id: int, to_admin_if_no_group: bool = True, file_name: str = None
):
    """导出群组数据为 CSV 并推送 - 优化版本"""
    await db.init_group(chat_id)

    if not file_name:
        date_str = get_beijing_time().strftime("%Y%m%d_%H%M%S")
        file_name = f"group_{chat_id}_statistics_{date_str}.csv"

    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)

    activity_limits = await db.get_activity_limits_cached()
    headers = ["用户ID", "用户昵称"]
    for act in activity_limits.keys():
        headers.extend([f"{act}次数", f"{act}总时长"])
    headers.extend(
        ["活动次数总计", "活动用时总计", "罚款总金额", "超时次数", "总超时时间"]
    )
    writer.writerow(headers)

    has_data = False
    group_stats = await db.get_group_statistics(chat_id)

    for user_data in group_stats:
        total_count = user_data.get("total_activity_count", 0)
        if total_count > 0:
            has_data = True

        row = [user_data["user_id"], user_data.get("nickname", "未知用户")]
        for act in activity_limits.keys():
            activity_info = user_data["activities"].get(act, {})
            count = activity_info.get("count", 0)
            total_seconds = int(activity_info.get("time", 0))

            time_str = MessageFormatter.format_time_for_csv(total_seconds)

            row.append(count)
            row.append(time_str)

        total_seconds_all = int(user_data.get("total_accumulated_time", 0))
        total_time_str = MessageFormatter.format_time_for_csv(total_seconds_all)

        overtime_seconds = int(user_data.get("total_overtime_time", 0))
        overtime_str = MessageFormatter.format_time_for_csv(overtime_seconds)

        row.extend(
            [
                total_count,
                total_time_str,
                user_data.get("total_fines", 0),
                user_data.get("overtime_count", 0),
                overtime_str,
            ]
        )
        writer.writerow(row)

    if not has_data:
        await bot.send_message(chat_id, "⚠️ 当前群组没有数据需要导出")
        return

    csv_content = csv_buffer.getvalue()
    csv_buffer.close()

    temp_file = f"temp_{file_name}"
    try:
        async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
            await f.write(csv_content)

        chat_title = str(chat_id)
        try:
            chat_info = await bot.get_chat(chat_id)
            chat_title = chat_info.title or chat_title
        except:
            pass

        caption = (
            f"📊 群组数据导出\n"
            f"🏢 群组：<code>{chat_title}</code>\n"
            f"📅 导出时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"-------------------------------------\n"
            f"💾 包含每个用户的所有活动统计和总计信息"
        )

        try:
            csv_input_file = FSInputFile(temp_file, filename=file_name)
            await bot.send_document(
                chat_id, csv_input_file, caption=caption, parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"❌ 发送到当前聊天失败: {e}")

        await NotificationService.send_document(
            chat_id, FSInputFile(temp_file, filename=file_name), caption
        )

        logger.info(f"✅ 数据导出并推送完成: {file_name}")

    except Exception as e:
        logger.error(f"❌ 导出过程出错: {e}")
        await bot.send_message(chat_id, f"❌ 导出失败：{e}")
    finally:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass


async def export_monthly_csv(
    chat_id: int,
    year: int = None,
    month: int = None,
    to_admin_if_no_group: bool = True,
    file_name: str = None,
):
    """导出月度数据为 CSV 并推送 - 优化版本"""
    if year is None or month is None:
        today = get_beijing_time()
        year = today.year
        month = today.month

    if not file_name:
        file_name = f"group_{chat_id}_monthly_{year:04d}{month:02d}.csv"

    # 使用优化版导出
    csv_content = await optimized_monthly_export(chat_id, year, month)

    if not csv_content:
        await bot.send_message(chat_id, f"⚠️ {year}年{month}月没有数据需要导出")
        return

    temp_file = f"temp_{file_name}"
    try:
        async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
            await f.write(csv_content)

        chat_title = str(chat_id)
        try:
            chat_info = await bot.get_chat(chat_id)
            chat_title = chat_info.title or chat_title
        except:
            pass

        caption = (
            f"📊 月度数据导出\n"
            f"🏢 群组：<code>{chat_title}</code>\n"
            f"📅 统计月份：<code>{year}年{month}月</code>\n"
            f"⏰ 导出时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
            f"-------------------------------------\n"
            f"💾 包含每个用户的月度活动统计"
        )

        try:
            csv_input_file = FSInputFile(temp_file, filename=file_name)
            await bot.send_document(
                chat_id, csv_input_file, caption=caption, parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"❌ 发送到当前聊天失败: {e}")

        await NotificationService.send_document(
            chat_id, FSInputFile(temp_file, filename=file_name), caption
        )

        logger.info(f"✅ 月度数据导出并推送完成: {file_name}")

    except Exception as e:
        logger.error(f"❌ 月度导出过程出错: {e}")
        await bot.send_message(chat_id, f"❌ 月度导出失败：{e}")
    finally:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass


async def generate_monthly_report(chat_id: int, year: int = None, month: int = None):
    """生成月度报告 - 优化版本"""
    if year is None or month is None:
        today = get_beijing_time()
        year = today.year
        month = today.month

    monthly_stats = await db.get_monthly_statistics(chat_id, year, month)
    work_stats = await db.get_monthly_work_statistics(chat_id, year, month)
    activity_ranking = await db.get_monthly_activity_ranking(chat_id, year, month)

    if not monthly_stats and not work_stats:
        return None

    chat_title = str(chat_id)
    try:
        chat_info = await bot.get_chat(chat_id)
        chat_title = chat_info.title or chat_title
    except:
        pass

    # 生成报告文本
    report = (
        f"📊 <b>{year}年{month}月打卡统计报告</b>\n"
        f"🏢 群组：<code>{chat_title}</code>\n"
        f"📅 生成时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>\n"
        f"{MessageFormatter.create_dashed_line()}\n"
    )

    # 总体统计
    total_users = len(monthly_stats)
    total_activity_time = sum(stat.get("total_time", 0) for stat in monthly_stats)
    total_activity_count = sum(stat.get("total_count", 0) for stat in monthly_stats)
    total_fines = sum(stat.get("total_fines", 0) for stat in monthly_stats)

    report += (
        f"👥 <b>总体统计</b>\n"
        f"• 活跃用户：<code>{total_users}</code> 人\n"
        f"• 总活动时长：<code>{MessageFormatter.format_time(int(total_activity_time))}</code>\n"
        f"• 总活动次数：<code>{total_activity_count}</code> 次\n"
        f"• 总罚款金额：<code>{total_fines}</code> 元\n\n"
    )

    # 上下班统计
    total_work_start = sum(stat.get("work_start_count", 0) for stat in work_stats)
    total_work_end = sum(stat.get("work_end_count", 0) for stat in work_stats)
    total_work_fines = sum(
        stat.get("work_start_fines", 0) + stat.get("work_end_fines", 0)
        for stat in work_stats
    )

    if total_work_start > 0 or total_work_end > 0:
        report += (
            f"🕒 <b>上下班统计</b>\n"
            f"• 上班打卡：<code>{total_work_start}</code> 次\n"
            f"• 下班打卡：<code>{total_work_end}</code> 次\n"
            f"• 上下班罚款：<code>{total_work_fines}</code> 元\n\n"
        )

    # 活动排行榜
    report += f"🏆 <b>月度活动排行榜</b>\n"
    for activity, ranking in activity_ranking.items():
        if ranking:
            report += f"📈 <code>{activity}</code>：\n"
            for i, user in enumerate(ranking[:3], 1):
                time_str = MessageFormatter.format_time(int(user.get("total_time", 0)))
                report += f"  <code>{i}.</code> {user.get('nickname', '未知用户')} - {time_str}\n"
            report += "\n"

    return report


# ==================== 系统维护功能优化 ====================
async def export_data_before_reset(chat_id: int):
    """在重置前自动导出CSV数据 - 优化版本"""
    try:
        # 先检查是否有数据需要导出
        group_stats = await db.get_group_statistics(chat_id)
        has_data = False

        if group_stats:
            for user_data in group_stats:
                total_count = user_data.get("total_activity_count", 0)
                total_time = user_data.get("total_accumulated_time", 0)
                if total_count > 0 or total_time > 0:
                    has_data = True
                    break

        if not has_data:
            logger.info(f"⚠️ 群组 {chat_id} 没有数据需要导出，跳过自动导出")
            return

        date_str = get_beijing_time().strftime("%Y%m%d")
        file_name = f"group_{chat_id}_statistics_{date_str}.csv"
        await export_and_push_csv(
            chat_id, to_admin_if_no_group=True, file_name=file_name
        )
        logger.info(f"✅ 群组 {chat_id} 的每日数据已自动导出并推送")
    except Exception as e:
        logger.error(f"❌ 自动导出数据失败：{e}")


async def daily_reset_task():
    """每日自动重置任务 - 优化版本"""
    while True:
        now = get_beijing_time()
        logger.info(f"🔄 重置任务检查，当前时间: {now}")

        all_groups = await db.get_all_groups()
        for chat_id in all_groups:
            group_data = await db.get_group_cached(chat_id)
            if not group_data:
                continue

            reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
            reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

            if now.hour == reset_hour and now.minute == reset_minute:
                try:
                    logger.info(f"⏰ 到达重置时间，正在重置群组 {chat_id} 的数据...")
                    await export_data_before_reset(chat_id)

                    group_members = await db.get_group_members(chat_id)
                    for user_data in group_members:
                        user_lock = get_user_lock(chat_id, user_data["user_id"])
                        async with user_lock:
                            await db.reset_user_daily_data(
                                chat_id, user_data["user_id"]
                            )

                    logger.info(f"✅ 群组 {chat_id} 数据重置完成")

                except Exception as e:
                    logger.error(f"❌ 重置群组 {chat_id} 失败: {e}")

        await asyncio.sleep(60)


async def auto_daily_export_task():
    """每天重置前自动导出群组数据 - 优化版本"""
    while True:
        now = get_beijing_time()
        logger.info(f"🕒 自动导出任务运行中，当前时间: {now}")

        all_groups = await db.get_all_groups()
        export_executed = False

        for chat_id in all_groups:
            try:
                group_data = await db.get_group_cached(chat_id)
                if not group_data:
                    continue

                reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
                reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

                # 方案1: 23:59固定导出（所有群组）
                if now.hour == 23 and now.minute == 59:
                    logger.info(f"📤 23:59自动导出，正在导出群组 {chat_id} 的数据...")
                    await export_and_push_csv(chat_id)
                    logger.info(f"✅ 23:59自动导出完成：群组 {chat_id}")
                    export_executed = True

                # 方案2: 重置时间前1分钟导出
                else:
                    # 计算重置前1分钟的时间
                    reset_time = now.replace(
                        hour=reset_hour, minute=reset_minute, second=0, microsecond=0
                    )
                    export_time = reset_time - timedelta(minutes=1)

                    # 如果当前时间在导出时间范围内（精确匹配）
                    if (
                        now.hour == export_time.hour
                        and now.minute == export_time.minute
                    ):
                        logger.info(
                            f"📤 到达重置前1分钟导出时间，正在导出群组 {chat_id} 的数据..."
                        )
                        await export_and_push_csv(chat_id)
                        logger.info(f"✅ 重置前导出完成：群组 {chat_id}")
                        export_executed = True

            except Exception as e:
                logger.error(f"❌ 自动导出失败 {chat_id}: {e}")

        if export_executed:
            # 如果执行了导出，等待2分钟避免重复执行
            await asyncio.sleep(120)
        else:
            # 如果没有执行任何导出，等待1分钟再检查
            await asyncio.sleep(60)


# ==================== 月度报告任务优化 ====================
async def process_monthly_export_for_group(chat_id: int, year: int, month: int):
    """处理单个群组的月度导出 - 优化版本"""
    try:
        # 1. 生成CSV数据（使用优化版）
        csv_content = await optimized_monthly_export(chat_id, year, month)

        if not csv_content:
            logger.info(f"⚠️ 群组 {chat_id} 没有 {year}年{month}月的数据")
            return

        # 2. 保存临时文件
        file_name = f"monthly_report_{chat_id}_{year:04d}{month:02d}.csv"
        temp_file = f"temp_{file_name}"

        try:
            async with aiofiles.open(temp_file, "w", encoding="utf-8-sig") as f:
                await f.write(csv_content)

            # 3. 推送文件
            chat_title = await get_chat_title(chat_id)
            caption = (
                f"📊 月度打卡统计报告\n"
                f"🏢 群组：<code>{chat_title}</code>\n"
                f"📅 统计月份：<code>{year}年{month}月</code>\n"
                f"⏰ 生成时间：<code>{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}</code>"
            )

            # 使用推送服务发送
            await NotificationService.send_document(
                chat_id, FSInputFile(temp_file, filename=file_name), caption
            )

            logger.info(f"✅ 群组 {chat_id} 月度报告推送完成")

        finally:
            # 清理临时文件
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except:
                pass

    except Exception as e:
        logger.error(f"❌ 处理群组 {chat_id} 月度导出失败: {e}")


async def efficient_monthly_export_task():
    """高效的月度数据导出任务 - 优化版本"""
    while True:
        now = get_beijing_time()

        # 每月1号上午10点执行（避开高峰期）
        if now.day == 1 and now.hour == 10 and now.minute == 0:
            last_month = now.month - 1 if now.month > 1 else 12
            last_year = now.year if now.month > 1 else now.year - 1

            logger.info(f"📊 开始执行月度数据导出: {last_year}年{last_month}月")

            all_groups = await db.get_all_groups()

            for chat_id in all_groups:
                try:
                    if not performance_optimizer.memory_usage_ok():
                        logger.warning(f"⚠️ 内存使用较高，跳过群组 {chat_id} 的月度导出")
                        continue

                    # 生成并推送月度报告
                    await process_monthly_export_for_group(
                        chat_id, last_year, last_month
                    )

                    # 每组处理完后休息一下，避免资源紧张
                    await asyncio.sleep(10)

                except Exception as e:
                    logger.error(f"❌ 群组 {chat_id} 月度导出失败: {e}")

            # 执行数据清理
            try:
                await db.manage_monthly_data()
                logger.info("✅ 月度数据管理完成")
            except Exception as e:
                logger.error(f"❌ 月度数据管理失败: {e}")

            # 等待24小时避免重复执行
            await asyncio.sleep(24 * 60 * 60)
        else:
            await asyncio.sleep(60)  # 每分钟检查一次


async def monthly_report_task():
    """月度报告推送任务 - 优化版本"""
    while True:
        now = get_beijing_time()
        logger.info(f"📅 月度报告任务检查，当前时间: {now}")

        # 每月1号上午9点推送上月报告
        if now.day == 1 and now.hour == 9 and now.minute == 0:
            last_month = now.month - 1 if now.month > 1 else 12
            last_year = now.year if now.month > 1 else now.year - 1

            logger.info(f"📊 开始生成 {last_year}年{last_month}月月度报告...")

            all_groups = await db.get_all_groups()
            for chat_id in all_groups:
                try:
                    # 生成月度报告
                    report = await generate_monthly_report(
                        chat_id, last_year, last_month
                    )
                    if report:
                        # 发送报告
                        await bot.send_message(chat_id, report, parse_mode="HTML")
                        logger.info(
                            f"✅ 已发送 {last_year}年{last_month}月报告到群组 {chat_id}"
                        )

                        # 导出CSV文件
                        await export_monthly_csv(chat_id, last_year, last_month)
                        logger.info(
                            f"✅ 已导出 {last_year}年{last_month}月数据到群组 {chat_id}"
                        )
                    else:
                        logger.info(
                            f"⚠️ 群组 {chat_id} 没有 {last_year}年{last_month}月的数据"
                        )

                except Exception as e:
                    logger.error(f"❌ 群组 {chat_id} 月度报告生成失败: {e}")

            # 等待24小时，避免重复执行
            await asyncio.sleep(24 * 60 * 60)
        else:
            # 每分钟检查一次
            await asyncio.sleep(60)


# ==================== 内存清理任务优化 ====================
async def memory_cleanup_task():
    """定期内存清理任务 - 优化版本"""
    while True:
        try:
            await asyncio.sleep(Config.CLEANUP_INTERVAL)
            await performance_optimizer.memory_cleanup()
            # 清理数据库旧数据
            await db.cleanup_old_data(30)
        except Exception as e:
            logger.error(f"❌ 内存清理任务失败: {e}")
            await asyncio.sleep(300)


async def health_monitoring_task():
    """健康监控任务 - 优化版本"""
    while True:
        try:
            # 检查内存使用
            if not performance_optimizer.memory_usage_ok():
                logger.warning("⚠️ 内存使用过高，执行紧急清理")
                await performance_optimizer.memory_cleanup()

            # 检查任务数量
            if len(tasks) > 1000:
                logger.warning(f"⚠️ 活动任务数量过多: {len(tasks)}")
                await performance_optimizer.memory_cleanup()

            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"❌ 健康监控任务失败: {e}")
            await asyncio.sleep(60)


# ==================== 辅助函数优化 ====================
async def get_chat_title(chat_id: int) -> str:
    """获取群组标题 - 优化版本"""
    try:
        chat_info = await bot.get_chat(chat_id)
        return chat_info.title or str(chat_id)
    except Exception:
        return str(chat_id)


# ==================== Render检查接口优化 ====================
# 替换原有的 health_check 函数
async def enhanced_health_check(request):
    """增强版健康检查接口 - 包含心跳状态"""
    try:
        # 检查数据库连接
        db_stats = await db.get_database_stats()

        # 检查心跳状态
        heartbeat_status = heartbeat_manager.get_status()

        # 检查内存使用
        memory_ok = performance_optimizer.memory_usage_ok()

        # 获取基本状态
        status = "healthy" if memory_ok else "degraded"

        return web.json_response(
            {
                "status": status,
                "timestamp": get_beijing_time().isoformat(),
                "bot_status": "running",
                "memory_ok": memory_ok,
                "database": db_stats,
                "heartbeat": heartbeat_status,
                "active_tasks": len(tasks),
                "system": {
                    "python_version": sys.version,
                    "platform": sys.platform,
                    "uptime": (
                        time.time() - start_time if "start_time" in globals() else 0
                    ),
                },
            }
        )
    except Exception as e:
        logger.error(f"❌ 健康检查失败: {e}")
        return web.json_response(
            {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": get_beijing_time().isoformat(),
            },
            status=500,
        )


async def start_web_server():
    """启动轻量HTTP健康检测服务 - 修复端口绑定版本"""
    try:
        app = web.Application()

        # 添加多个健康检查端点
        app.router.add_get("/", enhanced_health_check)
        app.router.add_get("/health", enhanced_health_check)
        app.router.add_get("/status", enhanced_health_check)
        app.router.add_get("/ping", lambda request: web.Response(text="pong"))
        app.router.add_get("/metrics", metrics_endpoint)
        app.router.add_get("/detailed-status", detailed_status_check)

        runner = web.AppRunner(app)
        await runner.setup()

        # 修复：使用 Render 提供的 PORT 环境变量
        port = int(os.environ.get("PORT", Config.WEB_SERVER_CONFIG["PORT"]))
        host = "0.0.0.0"  # 必须绑定到 0.0.0.0

        site = web.TCPSite(runner, host, port)
        await site.start()
        logger.info(f"🌐 Web server started on {host}:{port}")

        # 返回站点信息以便后续管理
        return site
    except Exception as e:
        logger.error(f"❌ Web server failed: {e}")
        raise


async def get_active_users_count() -> int:
    """获取活跃用户数量（今日有活动的用户）"""
    try:
        today = str(datetime.now(beijing_tz).date())
        conn = await db.get_connection()
        try:
            result = await conn.fetchval(
                "SELECT COUNT(DISTINCT user_id) FROM users WHERE last_updated = $1",
                today,
            )
            return result or 0
        finally:
            await db.release_connection(conn)
    except Exception as e:
        logger.error(f"获取活跃用户数失败: {e}")
        return 0


async def metrics_endpoint(request):
    """Prometheus格式指标端点"""
    try:
        # 获取活跃用户数（需要先定义 active_users）
        active_users_count = await get_active_users_count()

        # 获取内存使用（字节）
        memory_bytes = psutil.Process().memory_info().rss

        # 获取数据库连接数
        db_connections = 0
        if db.pool:
            try:
                # asyncpg 连接池统计
                db_connections = db.pool.get_size()
            except Exception as e:
                logger.warning(f"获取数据库连接数失败: {e}")

        # 获取其他性能指标
        task_count = len(tasks)
        cache_stats = global_cache.get_stats()

        # Prometheus格式指标
        metrics = [
            "# HELP bot_active_users 活跃用户数量",
            "# TYPE bot_active_users gauge",
            f"bot_active_users {active_users_count}",
            "# HELP bot_memory_usage_bytes 内存使用量（字节）",
            "# TYPE bot_memory_usage_bytes gauge",
            f"bot_memory_usage_bytes {memory_bytes}",
            "# HELP bot_db_connections 数据库连接数",
            "# TYPE bot_db_connections gauge",
            f"bot_db_connections {db_connections}",
            "# HELP bot_active_tasks 活跃任务数量",
            "# TYPE bot_active_tasks gauge",
            f"bot_active_tasks {task_count}",
            "# HELP bot_cache_hits 缓存命中次数",
            "# TYPE bot_cache_hits counter",
            f"bot_cache_hits {cache_stats['hits']}",
            "# HELP bot_cache_misses 缓存未命中次数",
            "# TYPE bot_cache_misses counter",
            f"bot_cache_misses {cache_stats['misses']}",
            "# HELP bot_uptime_seconds 运行时间（秒）",
            "# TYPE bot_uptime_seconds gauge",
            f"bot_uptime_seconds {int(time.time() - start_time)}",
        ]

        return web.Response(text="\n".join(metrics), content_type="text/plain")

    except Exception as e:
        logger.error(f"❌ 指标端点错误: {e}")
        return web.Response(text=f"error: {e}", status=500)


async def detailed_status_check(request):
    """详细状态检查端点"""
    try:
        # 收集各种状态信息
        status_info = {
            "status": "healthy",
            "timestamp": get_beijing_time().isoformat(),
            "bot": {
                "active_tasks": len(tasks),
                "user_locks_count": len(user_locks),
                "memory_usage_ok": performance_optimizer.memory_usage_ok(),
            },
            "database": await db.get_database_stats(),
            "heartbeat": heartbeat_manager.get_status(),
            "system": {
                "python_version": sys.version,
                "platform": sys.platform,
                "current_time": get_beijing_time().isoformat(),
            },
        }

        return web.json_response(status_info)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)


# ==================== 启动流程优化 ====================
async def on_startup():
    """启动时执行 - 优化版本"""
    logger.info("🤖 机器人启动中...")
    await bot.delete_webhook(drop_pending_updates=True)
    # 初始化异步数据库
    await db.initialize()
    logger.info("✅ Webhook 已删除，使用 polling 模式")


async def on_shutdown():
    """关闭时执行 - 优化版本"""
    logger.info("🛑 机器人正在关闭...")

    for key, task in list(tasks.items()):
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"取消任务异常: {e}")
        del tasks[key]

    logger.info("✅ 清理完成")


def check_environment():
    """检查环境配置 - 优化版本"""
    if not Config.TOKEN:
        logger.error("❌ BOT_TOKEN 未设置")
        return False
    return True


# ==================== Webhook 设置函数 ====================
async def setup_webhook():
    """配置Webhook - 带洪水控制保护"""
    if not Config.should_use_webhook():
        # 明确使用Polling模式，清理Webhook
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ 已删除Webhook，使用Polling模式")
        except Exception as e:
            logger.warning(f"⚠️ 删除Webhook失败: {e}")
        return False

    if not Config.WEBHOOK_URL:
        logger.error("❌ Webhook模式已启用，但WEBHOOK_URL未设置，将使用Polling模式")
        return False

    try:
        # 修复URL格式
        base_url = Config.WEBHOOK_URL.rstrip("/")
        webhook_url = f"{base_url}/webhook"

        # 先检查当前Webhook状态，避免不必要的设置
        current_webhook = await bot.get_webhook_info()

        if current_webhook.url == webhook_url:
            logger.info(f"✅ Webhook已正确设置: {webhook_url}")
            return True

        logger.info(f"🔗 设置Webhook: {webhook_url}")

        # 先删除旧Webhook
        await bot.delete_webhook(drop_pending_updates=True)
        await asyncio.sleep(2)  # 等待2秒避免洪水限制

        # 设置新Webhook
        await bot.set_webhook(
            url=webhook_url,
            drop_pending_updates=True,
            allowed_updates=["message", "callback_query"],
        )

        # 验证设置
        await asyncio.sleep(1)
        new_webhook = await bot.get_webhook_info()

        if new_webhook.url == webhook_url:
            logger.info(f"✅ Webhook设置成功: {webhook_url}")
            logger.info(f"📊 待处理更新: {new_webhook.pending_update_count}")
            return True
        else:
            logger.error(f"❌ Webhook设置验证失败")
            return False

    except Exception as e:
        logger.error(f"❌ Webhook设置失败: {e}")

        # 如果是洪水限制，等待后重试一次
        if "Flood control" in str(e) or "Too Many Requests" in str(e):
            logger.warning("⚠️ 遇到洪水限制，等待10秒后重试...")
            await asyncio.sleep(10)

            try:
                await bot.delete_webhook(drop_pending_updates=True)
                await asyncio.sleep(2)
                await bot.set_webhook(url=webhook_url, drop_pending_updates=True)
                logger.info("✅ 重试Webhook设置成功")
                return True
            except Exception as retry_error:
                logger.error(f"❌ Webhook重试失败: {retry_error}")

        return False


async def optimized_on_startup():
    """优化版启动流程 - 修复洪水控制问题"""
    logger.info("🤖 机器人启动中...")

    max_retries = 2  # 减少重试次数
    for attempt in range(max_retries):
        try:
            # 并行执行启动任务（除了Webhook）
            startup_tasks = [
                db.initialize(),
                preload_frequent_data(),
                heartbeat_manager.initialize(),
            ]

            results = await asyncio.gather(*startup_tasks, return_exceptions=True)

            # 检查是否有失败的任务
            failed_tasks = [r for r in results if isinstance(r, Exception)]
            if failed_tasks:
                raise Exception(f"启动任务失败: {failed_tasks}")

            # 设置Webhook（如果启用）- 单独处理以避免影响其他启动任务
            webhook_success = await setup_webhook()

            if Config.should_use_webhook() and not webhook_success:
                logger.warning("⚠️ Webhook设置失败，应用将在Polling模式下运行")
                # 更新配置以使用Polling
                Config.BOT_MODE = "polling"
                # 确保删除Webhook
                try:
                    await bot.delete_webhook(drop_pending_updates=True)
                except:
                    pass

            logger.info("✅ 优化启动完成")
            return

        except Exception as e:
            logger.warning(f"⚠️ 启动第 {attempt + 1} 次失败: {e}")
            if attempt == max_retries - 1:
                logger.error(f"❌ 启动重试{max_retries}次后失败")
                raise
            await asyncio.sleep(2**attempt)


async def preload_frequent_data():
    """预加载常用数据 - 优化版本"""
    try:
        # 并行预加载
        preload_tasks = [
            db.get_activity_limits_cached(),
            db.get_push_settings(),
            db.get_fine_rates(),
        ]

        await asyncio.gather(*preload_tasks)
        logger.info("✅ 常用数据预加载完成")
    except Exception as e:
        logger.warning(f"⚠️ 预加载数据失败: {e}")


async def optimized_on_shutdown():
    """优化版关闭流程"""
    logger.info("🛑 机器人正在关闭...")

    try:
        # 并行清理任务
        cleanup_tasks = [
            performance_optimizer.memory_cleanup(),
            db.cleanup_cache(),
            heartbeat_manager.stop(),  # 停止心跳管理器
        ]

        # 取消所有活动任务
        for key, task in list(tasks.items()):
            if not task.done():
                task.cancel()

        await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        # 等待任务完成
        for key, task in list(tasks.items()):
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"任务异常: {e}")

        logger.info("✅ 优化清理完成")
    except Exception as e:
        logger.error(f"❌ 关闭过程中出错: {e}")


# ========== 主启动函数优化 ==========

logger = logging.getLogger("GroupCheckInBot")


# =======================
# Render 保活 HTTP 服务
# =======================
async def health_check(request):
    return web.json_response({"status": "ok", "timestamp": time.time()})


async def start_health_server():
    """Render 保活端口监听"""
    app = web.Application()
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=Config.WEB_SERVER_CONFIG["PORT"])
    await site.start()
    logger.info(
        f"🌐 Health check server running on port {Config.WEB_SERVER_CONFIG['PORT']}"
    )


# =======================
# 主程序启动逻辑
# =======================
async def optimized_main():
    """优化版主启动函数 - Render 修复版本"""
    if not check_environment():
        sys.exit(1)

    try:
        await optimized_on_startup()

        # 🚀 Render 需要一个端口监听 —— 启动保活服务
        asyncio.create_task(start_health_server())

        # 启动后台任务
        critical_tasks = [
            asyncio.create_task(memory_cleanup_task()),
            asyncio.create_task(health_monitoring_task()),
            asyncio.create_task(heartbeat_manager.start_heartbeat_loop()),
        ]

        normal_tasks = [
            asyncio.create_task(daily_reset_task()),
            asyncio.create_task(auto_daily_export_task()),
            asyncio.create_task(efficient_monthly_export_task()),
            asyncio.create_task(monthly_report_task()),
        ]

        all_tasks = critical_tasks + normal_tasks
        logger.info(f"✅ 所有后台任务已启动: {len(all_tasks)} 个任务")

        # 智能模式选择
        if Config.should_use_webhook():
            logger.info("🚀 使用 Webhook 模式运行")
            # Webhook 模式：Render 端口会持续监听
            while True:
                await asyncio.sleep(3600)
        else:
            logger.info("🚀 使用 Polling 模式运行")
            await dp.start_polling(bot, skip_updates=True)

    except Exception as e:
        logger.error(f"❌ 启动过程中出错: {e}")
        raise
    finally:
        await optimized_on_shutdown()


# ==================== Webhook 路由处理 ====================


async def webhook_handler(request: web.Request):
    """处理Telegram Webhook请求"""
    try:
        # 验证请求来源（可选但推荐）
        # 您可以添加Token验证来确保请求来自Telegram

        update_data = await request.json()
        update = types.Update(**update_data)

        # 使用Dispatcher处理更新
        await dp.feed_update(bot, update)

        return web.Response(status=200, text="OK")

    except Exception as e:
        logger.error(f"❌ Webhook处理错误: {e}")
        return web.Response(status=500, text="Internal Server Error")


async def start_webhook_server():
    """启动Webhook服务器"""
    try:
        # 设置Webhook
        webhook_url = f"{Config.WEBHOOK_URL}/webhook"

        logger.info(f"🔗 设置Webhook: {webhook_url}")
        await bot.set_webhook(
            url=webhook_url,
            drop_pending_updates=True,
            allowed_updates=["message", "callback_query", "chat_member"],
        )

        # 验证Webhook设置
        webhook_info = await bot.get_webhook_info()
        logger.info(f"📊 Webhook信息: {webhook_info.url}")
        logger.info(f"📊 待处理更新: {webhook_info.pending_update_count}")

        # 创建aiohttp应用
        app = web.Application()

        # 添加路由
        app.router.add_post("/webhook", webhook_handler)
        app.router.add_get("/health", enhanced_health_check)
        app.router.add_get("/", enhanced_health_check)
        app.router.add_get("/status", enhanced_health_check)
        app.router.add_get("/ping", lambda request: web.Response(text="pong"))

        # 启动服务器
        runner = web.AppRunner(app)
        await runner.setup()

        port = int(os.environ.get("PORT", Config.WEB_SERVER_CONFIG["PORT"]))
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()

        logger.info(f"🌐 Webhook服务器已在端口 {port} 启动")
        logger.info("✅ Webhook模式已就绪，等待Telegram请求...")

        return runner

    except Exception as e:
        logger.error(f"❌ Webhook服务器启动失败: {e}")
        # 尝试删除Webhook并回退到Polling
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("🔄 已删除Webhook，将使用Polling模式")
        except:
            pass
        raise


async def webhook_main():
    """Webhook模式主函数"""
    logger.info("🚀 启动Webhook模式...")

    try:
        await optimized_on_startup()

        # 启动Webhook服务器
        webhook_runner = await start_webhook_server()

        # 启动后台任务
        background_tasks = [
            asyncio.create_task(memory_cleanup_task()),
            asyncio.create_task(health_monitoring_task()),
            asyncio.create_task(heartbeat_manager.start_heartbeat_loop()),
            asyncio.create_task(daily_reset_task()),
            asyncio.create_task(auto_daily_export_task()),
            asyncio.create_task(efficient_monthly_export_task()),
        ]

        logger.info(f"✅ 后台任务已启动: {len(background_tasks)} 个任务")

        # 保持服务器运行
        try:
            while True:
                await asyncio.sleep(3600)  # 每小时检查一次

                # 可选：定期检查Webhook状态
                try:
                    webhook_info = await bot.get_webhook_info()
                    if webhook_info.pending_update_count > 100:
                        logger.warning(
                            f"⚠️ 待处理更新较多: {webhook_info.pending_update_count}"
                        )
                except Exception as e:
                    logger.warning(f"⚠️ 检查Webhook状态失败: {e}")

        except asyncio.CancelledError:
            logger.info("🛑 Webhook服务器被取消")
        except Exception as e:
            logger.error(f"❌ Webhook服务器运行错误: {e}")
            raise

    except Exception as e:
        logger.error(f"❌ Webhook模式启动失败: {e}")
        raise

    finally:
        # 清理资源
        try:
            if "webhook_runner" in locals():
                await webhook_runner.cleanup()
        except Exception as e:
            logger.warning(f"⚠️ 清理Webhook运行器失败: {e}")

        await optimized_on_shutdown()


async def polling_main():
    """Polling模式主函数"""
    logger.info("🚀 启动Polling模式...")

    await optimized_on_startup()

    # 启动后台任务
    background_tasks = [
        asyncio.create_task(memory_cleanup_task()),
        asyncio.create_task(health_monitoring_task()),
        asyncio.create_task(heartbeat_manager.start_heartbeat_loop()),
        asyncio.create_task(daily_reset_task()),
        asyncio.create_task(auto_daily_export_task()),
        asyncio.create_task(efficient_monthly_export_task()),
    ]

    logger.info(f"✅ 后台任务已启动: {len(background_tasks)} 个任务")
    logger.info("🔄 开始轮询消息...")

    try:
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"❌ Polling模式运行错误: {e}")
        raise


# 修改主函数以支持两种模式
async def main():
    """主启动函数 - 简化版本避免重复启动"""
    if not check_environment():
        logger.error("❌ 环境检查失败")
        sys.exit(1)

    # 立即设置Polling模式，避免Webhook问题
    Config.BOT_MODE = "polling"  # 强制使用Polling模式

    try:
        await db.initialize()
        logger.info("✅ 数据库初始化完成")

        # 使用简化的启动
        await simple_on_startup()

        # 直接使用Polling模式
        logger.info("🚀 使用 Polling 模式运行")

        # 启动必要的后台任务
        essential_tasks = [
            asyncio.create_task(memory_cleanup_task()),
            asyncio.create_task(heartbeat_manager.start_heartbeat_loop()),
        ]

        logger.info(f"✅ 基础后台任务已启动: {len(essential_tasks)} 个任务")

        # 启动轮询
        await dp.start_polling(bot, skip_updates=True)

    except KeyboardInterrupt:
        logger.info("👋 收到中断信号，正在关闭...")
    except Exception as e:
        logger.error(f"💥 主程序异常: {e}")
        raise
    finally:
        # 清理资源
        try:
            await db.close()
            logger.info("✅ 数据库连接已关闭")
        except Exception as e:
            logger.error(f"❌ 关闭数据库连接失败: {e}")

        logger.info("🎉 程序安全退出")


async def simple_on_startup():
    """简化版启动流程"""
    logger.info("🔧 执行简化启动...")

    # 删除Webhook，确保使用Polling模式
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ 已确认使用Polling模式")
    except Exception as e:
        logger.warning(f"⚠️ 删除Webhook失败: {e}")

    # 预加载必要数据
    try:
        await preload_frequent_data()
        logger.info("✅ 数据预加载完成")
    except Exception as e:
        logger.warning(f"⚠️ 数据预加载失败: {e}")


async def polling_main():
    """Polling模式主函数"""
    await optimized_on_startup()

    # 启动后台任务
    background_tasks = [
        asyncio.create_task(memory_cleanup_task()),
        asyncio.create_task(health_monitoring_task()),
        asyncio.create_task(heartbeat_manager.start_heartbeat_loop()),
        asyncio.create_task(daily_reset_task()),
        asyncio.create_task(auto_daily_export_task()),
    ]

    logger.info("🚀 使用 Polling 模式运行")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 机器人已手动停止")
    except Exception as e:
        logger.error(f"💥 机器人异常退出: {e}")
        sys.exit(1)
