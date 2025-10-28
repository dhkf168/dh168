import asyncio
import json
import os
import csv
import sys
from io import StringIO
from datetime import datetime, timedelta, timezone
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiohttp import web
from dotenv import load_dotenv
from dotenv import load_dotenv
from collections import defaultdict
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    FSInputFile
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

import aiofiles
import logging
from functools import wraps

# 设置时区为北京时间 (UTC+8)
beijing_tz = timezone(timedelta(hours=8))

# 日志配置
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==================== 集中配置区域 ====================
class Config:
    # Bot 配置
    TOKEN = os.getenv("BOT_TOKEN", "7982069377:AAHbxr5MGtg1sfIW4BrsmbeSljnsA8fgQQY")

    # 文件配置
    DATA_FILE = "group_data.json"
    ACTIVITY_FILE = "activities.json"
    PUSH_SETTINGS_FILE = "push_settings.json"

    # 管理员配置
    ADMINS = [8356418002, 87654321]

    # 默认活动配置
    DEFAULT_ACTIVITY_LIMITS = {
        "吃饭": {"max_times": 2, "time_limit": 30},
        "小厕": {"max_times": 5, "time_limit": 5},
        "大厕": {"max_times": 2, "time_limit": 15},
        "抽烟": {"max_times": 5, "time_limit": 10},
    }

    # 默认罚款
    DEFAULT_FINE_RATES = {
        "吃饭": {"10": 100, "30": 300},
        "小厕": {"5": 50, "10": 100},
        "大厕": {"15": 80, "30": 200},
        "抽烟": {"10": 200, "30": 500},
    }

    # 自动导出推送开关配置
    AUTO_EXPORT_SETTINGS = {
        "enable_channel_push": True,
        "enable_group_push": True,
        "enable_admin_push": True,
    }

    # 每日重置时间配置
    DAILY_RESET_HOUR = 0
    DAILY_RESET_MINUTE = 0

    # 消息模板
    MESSAGES = {
        "welcome": "欢迎使用群打卡机器人！请点击下方按钮或直接输入活动名称打卡：",
        "no_activity": "❌ 没有找到正在进行的活动，请先打卡活动再回座。",
        "has_activity": "❌ 您当前有活动【{}】正在进行中，请先回座后才能开始新活动！",
        "no_permission": "❌ 你没有权限执行此操作",
        "max_times_reached": "❌ 您今日的{}次数已达到上限（{}次），无法再次打卡",
        "setchannel_usage": "❌ 用法：/setchannel <频道ID>\n频道ID格式如 -1001234567890",
        "setgroup_usage": "❌ 用法：/setgroup <群组ID>\n用于接收超时通知的群组ID",
        "set_usage": "❌ 用法：/set <用户ID> <活动> <时长分钟>",
        "reset_usage": "❌ 用法：/reset <用户ID>",
        "addactivity_usage": "❌ 用法：/addactivity <活动名> <max次数> <time_limit分钟>",
        "setresettime_usage": "❌ 用法：/setresettime <小时> <分钟>\n例如：/setresettime 0 0 表示每天0点重置",
        "setfine_usage": "❌ 用法：/setfine <活动名> <时间段> <金额>\n例如：/setfine 抽烟 10 200",
        "setfines_all_usage": "❌ 用法：/setfines_all <t1> <f1> [<t2> <f2> ...]\n例如：/setfines_all 10 100 30 300 60 1000",
        "setpush_usage": "❌ 用法：/setpush <channel|group|admin> <on|off>"
    }

# ==================== 装饰器和工具类 ====================
def admin_required(func):
    """管理员权限检查装饰器"""
    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        if not is_admin(message.from_user.id):
            await message.answer(config.MESSAGES["no_permission"], reply_markup=get_main_keyboard())
            return
        return await func(message, *args, **kwargs)
    return wrapper

class UserContext:
    """用户上下文管理器"""
    def __init__(self, chat_id: int, uid: int):
        self.chat_id = chat_id
        self.uid = uid
        
    async def __aenter__(self):
        init_group(self.chat_id)
        init_user(self.chat_id, self.uid)
        await reset_daily_data_if_needed(self.chat_id, self.uid)
        return group_data[str(self.chat_id)]["成员"][str(self.uid)]
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await schedule_save_data()

class NotificationService:
    """统一推送服务"""
    @staticmethod
    async def send_notification(chat_id: int, text: str, notification_type: str = "all"):
        """发送通知到绑定的频道和群组"""
        sent = False
        
        # 发送到频道
        channel_id = group_data.get(str(chat_id), {}).get("频道ID")
        if config.AUTO_EXPORT_SETTINGS.get("enable_channel_push") and channel_id:
            try:
                await bot.send_message(channel_id, text, parse_mode="HTML")
                sent = True
                logging.info(f"✅ 已发送到频道: {channel_id}")
            except Exception as e:
                logging.error(f"❌ 发送到频道失败: {e}")

        # 发送到通知群组
        group_id = group_data.get(str(chat_id), {}).get("通知群组ID")
        if config.AUTO_EXPORT_SETTINGS.get("enable_group_push") and group_id:
            try:
                await bot.send_message(group_id, text, parse_mode="HTML")
                sent = True
                logging.info(f"✅ 已发送到通知群组: {group_id}")
            except Exception as e:
                logging.error(f"❌ 发送到通知群组失败: {e}")

        # 管理员兜底推送
        if not sent and config.AUTO_EXPORT_SETTINGS.get("enable_admin_push"):
            for admin_id in config.ADMINS:
                try:
                    await bot.send_message(admin_id, text, parse_mode="HTML")
                    logging.info(f"✅ 已发送给管理员: {admin_id}")
                except Exception as e:
                    logging.error(f"❌ 发送给管理员失败: {e}")
        
        return sent

    @staticmethod
    async def send_document(chat_id: int, document: FSInputFile, caption: str = ""):
        """发送文档到绑定的频道和群组"""
        sent = False
        
        # 发送到频道
        channel_id = group_data.get(str(chat_id), {}).get("频道ID")
        if config.AUTO_EXPORT_SETTINGS.get("enable_channel_push") and channel_id:
            try:
                await bot.send_document(channel_id, document, caption=caption, parse_mode="HTML")
                sent = True
                logging.info(f"✅ 已发送文档到频道: {channel_id}")
            except Exception as e:
                logging.error(f"❌ 发送文档到频道失败: {e}")

        # 发送到通知群组
        group_id = group_data.get(str(chat_id), {}).get("通知群组ID")
        if config.AUTO_EXPORT_SETTINGS.get("enable_group_push") and group_id:
            try:
                await bot.send_document(group_id, document, caption=caption, parse_mode="HTML")
                sent = True
                logging.info(f"✅ 已发送文档到通知群组: {group_id}")
            except Exception as e:
                logging.error(f"❌ 发送文档到通知群组失败: {e}")

        # 管理员兜底推送
        if not sent and config.AUTO_EXPORT_SETTINGS.get("enable_admin_push"):
            for admin_id in config.ADMINS:
                try:
                    await bot.send_document(admin_id, document, caption=caption, parse_mode="HTML")
                    logging.info(f"✅ 已发送文档给管理员: {admin_id}")
                except Exception as e:
                    logging.error(f"❌ 发送文档给管理员失败: {e}")
        
        return sent

class MessageFormatter:
    """消息格式化工具类"""
    @staticmethod
    def format_time(seconds: int):
        """格式化时间显示"""
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        if h > 0:
            return f"{h}小时{m}分钟"
        elif m > 0:
            return f"{m}分钟{s}秒"
        else:
            return f"{s}秒"

    @staticmethod
    def format_time_for_export(seconds: int):
        """为导出数据格式化时间显示"""
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        if h > 0:
            return f"{h}时{m}分{s}秒"
        elif m > 0:
            return f"{m}分{s}秒"
        else:
            return f"{s}秒"

    @staticmethod
    def format_user_link(user_id: int, user_name: str):
        """格式化用户链接"""
        if not user_name:
            user_name = f"用户{user_id}"
        clean_name = str(user_name).replace('<', '').replace('>', '').replace('&', '').replace('"', '')
        return f'<a href="tg://user?id={user_id}">{clean_name}</a>'

    @staticmethod
    def create_dashed_line():
        """创建短虚线分割线"""
        return "--------------------------------------------"

    @staticmethod
    def format_copyable_text(text: str):
        """格式化可复制文本"""
        return f"<code>{text}</code>"

    @staticmethod
    def format_activity_message(user_id: int, user_name: str, activity: str, time_str: str,
                               count: int, max_times: int, time_limit: int):
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
    def format_back_message(user_id: int, user_name: str, activity: str, time_str: str, elapsed_time: str,
                           total_activity_time: str, total_time: str, activity_counts: dict, total_count: int,
                           is_overtime: bool = False, overtime_seconds: int = 0, fine_amount: int = 0):
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
                message += f"罚款：{MessageFormatter.format_copyable_text(str(fine_amount))} 元\n"

        dashed_line = MessageFormatter.create_dashed_line()
        message += f"{dashed_line}\n"

        for act, count in activity_counts.items():
            if count > 0:
                message += f"🔢 本日{MessageFormatter.format_copyable_text(act)}次数：{MessageFormatter.format_copyable_text(str(count))} 次\n"

        message += f"\n📊 今日总活动次数：{MessageFormatter.format_copyable_text(str(total_count))} 次"

        return message

# ==================== 并发安全机制 ====================
user_locks = defaultdict(asyncio.Lock)

def get_user_lock(chat_id: int, uid: int) -> asyncio.Lock:
    """获取用户级锁"""
    key = f"{chat_id}-{uid}"
    return user_locks[key]

# ==================== 状态机类 ====================
class AdminStates(StatesGroup):
    waiting_for_channel_id = State()
    waiting_for_group_id = State()

# ==================== 初始化配置 ====================
config = Config()
storage = MemoryStorage()

# 内存数据
group_data = {}
activity_limits = config.DEFAULT_ACTIVITY_LIMITS.copy()
fine_rates = config.DEFAULT_FINE_RATES.copy()
tasks = {}  # 定时任务：chat_id-uid → asyncio.Task

bot = Bot(token=config.TOKEN)
dp = Dispatcher(storage=storage)

# ==================== 工具函数 ====================
def get_beijing_time():
    """获取北京时间"""
    return datetime.now(beijing_tz)

def is_admin(uid):
    """检查用户是否为管理员"""
    return uid in config.ADMINS

def init_group(chat_id: int):
    """初始化群组数据"""
    if str(chat_id) not in group_data:
        group_data[str(chat_id)] = {
            "频道ID": None,
            "通知群组ID": None,
            "每日重置时间": {
                "hour": config.DAILY_RESET_HOUR, 
                "minute": config.DAILY_RESET_MINUTE
            },
            "成员": {}
        }

def init_user(chat_id: int, uid: int):
    """初始化用户数据"""
    init_group(chat_id)
    if str(uid) not in group_data[str(chat_id)]["成员"]:
        group_data[str(chat_id)]["成员"][str(uid)] = {
            "活动": None,
            "开始时间": None,
            "累计": {},
            "次数": {},
            "最后更新": str(get_beijing_time().date()),
            "昵称": None,
            "用户ID": uid,
            "总累计时间": 0,
            "总次数": 0,
            "累计罚款": 0,
            "超时次数": 0,
            "总超时时间": 0
        }

async def reset_daily_data_if_needed(chat_id: int, uid: int):
    """优化版每日数据重置 - 只在需要时重置"""
    today = str(get_beijing_time().date())
    user_data = group_data[str(chat_id)]["成员"][str(uid)]
    
    if user_data["最后更新"] != today:
        user_data["累计"] = {}
        user_data["次数"] = {}
        user_data["总累计时间"] = 0
        user_data["总次数"] = 0
        user_data["累计罚款"] = 0
        user_data["超时次数"] = 0
        user_data["总超时时间"] = 0
        user_data["最后更新"] = today
        await schedule_save_data()

async def check_activity_limit(chat_id: int, uid: int, act: str):
    """检查活动次数是否达到上限"""
    init_user(chat_id, uid)
    await reset_daily_data_if_needed(chat_id, uid)
    
    current_count = group_data[str(chat_id)]["成员"][str(uid)]["次数"].get(act, 0)
    max_times = activity_limits[act]["max_times"]

    return current_count < max_times, current_count, max_times

def has_active_activity(chat_id: int, uid: int):
    """检查用户是否有活动正在进行"""
    init_user(chat_id, uid)
    user_data = group_data[str(chat_id)]["成员"][str(uid)]
    return user_data["活动"] is not None, user_data["活动"]

def load_data():
    """加载数据文件"""
    global group_data, activity_limits, fine_rates
    if os.path.exists(config.DATA_FILE):
        with open(config.DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            for chat_id, chat_data in data.items():
                if "每日重置时间" not in chat_data:
                    chat_data["每日重置时间"] = {"hour": config.DAILY_RESET_HOUR, "minute": config.DAILY_RESET_MINUTE}
            group_data = data

    if os.path.exists(config.ACTIVITY_FILE):
        with open(config.ACTIVITY_FILE, "r", encoding="utf-8") as f:
            activity_data = json.load(f)
            if isinstance(activity_data, dict) and "activities" in activity_data:
                activity_limits = activity_data.get("activities", config.DEFAULT_ACTIVITY_LIMITS.copy())
                fine_rates = activity_data.get("fines", config.DEFAULT_FINE_RATES.copy())
            else:
                activity_limits = activity_data
                fine_rates = config.DEFAULT_FINE_RATES.copy()

async def save_data():
    """保存数据到文件"""
    try:
        async with aiofiles.open(config.DATA_FILE, "w", encoding="utf-8") as f:
            await f.write(json.dumps(group_data, ensure_ascii=False, indent=2))
        
        activity_data = {
            "activities": activity_limits,
            "fines": fine_rates
        }
        async with aiofiles.open(config.ACTIVITY_FILE, "w", encoding="utf-8") as f:
            await f.write(json.dumps(activity_data, ensure_ascii=False, indent=2))
        
        logging.info("✅ 数据保存完成")
    except Exception as e:
        logging.error(f"❌ 数据保存失败: {e}")

def save_push_settings():
    """保存推送设置到文件"""
    try:
        with open(config.PUSH_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(config.AUTO_EXPORT_SETTINGS, f, ensure_ascii=False, indent=2)
        logging.info("✅ 推送设置已保存")
    except Exception as e:
        logging.error(f"❌ 保存推送设置失败：{e}")

def load_push_settings():
    """从文件加载推送设置"""
    try:
        if os.path.exists(config.PUSH_SETTINGS_FILE):
            with open(config.PUSH_SETTINGS_FILE, "r", encoding="utf-8") as f:
                saved_settings = json.load(f)
                for key, value in saved_settings.items():
                    if key in config.AUTO_EXPORT_SETTINGS:
                        config.AUTO_EXPORT_SETTINGS[key] = value
            logging.info("✅ 推送设置已加载")
    except Exception as e:
        logging.error(f"❌ 加载推送设置失败：{e}")

# ==================== 保存机制 ====================
save_lock = asyncio.Lock()
last_save_time = 0

async def schedule_save_data():
    """延迟保存数据"""
    global last_save_time
    current_time = asyncio.get_event_loop().time()
    
    if current_time - last_save_time < 3:
        return
    
    last_save_time = current_time
    
    try:
        await save_data()
    except Exception as e:
        logging.error(f"保存数据失败: {e}")

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
        del tasks[key]

def calculate_fine(activity: str, overtime_minutes: float) -> int:
    """计算罚款金额 - 分段罚款"""
    if activity not in fine_rates:
        return 0

    fine_segments = fine_rates[activity]
    segments = sorted([int(time) for time in fine_segments.keys()])

    applicable_fine = 0
    for segment in segments:
        if overtime_minutes <= segment:
            applicable_fine = fine_segments[str(segment)]
            break

    if applicable_fine == 0 and segments:
        applicable_fine = fine_segments[str(segments[-1])]

    return applicable_fine

# ==================== 回复键盘 ====================
def get_main_keyboard(show_admin=False):
    """获取主回复键盘"""
    activity_buttons = []
    row = []
    for i, act in enumerate(activity_limits.keys(), 1):
        emoji_label = act
        row.append(KeyboardButton(text=emoji_label))
        if len(row) >= 3:
            activity_buttons.append(row)
            row = []
    if row:
        activity_buttons.append(row)

    function_buttons = []
    
    if show_admin:
        function_buttons.append([
            KeyboardButton(text="👑 管理员面板"),
            KeyboardButton(text="📊 我的记录"), 
            KeyboardButton(text="🏆 排行榜")
        ])
    else:
        function_buttons.append([
            KeyboardButton(text="📊 我的记录"), 
            KeyboardButton(text="🏆 排行榜")
        ])
    
    function_buttons.append([KeyboardButton(text="✅ 回座")])

    keyboard = activity_buttons + function_buttons

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="请选择操作或输入活动名称..."
    )

def get_admin_keyboard():
    """管理员专用键盘"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="👑 管理员面板"),
                KeyboardButton(text="📤 导出数据")
            ],
            [
                KeyboardButton(text="🔔 通知设置")
            ],
            [
                KeyboardButton(text="🔙 返回主菜单")
            ]
        ],
        resize_keyboard=True
    )

# ==================== 活动定时提醒 ====================
async def activity_timer(chat_id: int, uid: int, act: str, limit: int):
    """优化的活动定时提醒任务"""
    try:
        key = f"{chat_id}-{uid}"
        one_minute_warning_sent = False
        timeout_immediate_sent = False
        timeout_5min_sent = False
        last_reminder_minute = 0
        
        while True:
            # 使用锁保护数据访问
            user_lock = get_user_lock(chat_id, uid)
            async with user_lock:
                # 检查用户是否还在活动中
                if (str(chat_id) not in group_data or 
                    str(uid) not in group_data[str(chat_id)]["成员"] or
                    group_data[str(chat_id)]["成员"][str(uid)]["活动"] != act):
                    break
                    
                user_data = group_data[str(chat_id)]["成员"][str(uid)]
                start_time = datetime.fromisoformat(user_data["开始时间"])
                elapsed = (get_beijing_time() - start_time).total_seconds()
                remaining = limit * 60 - elapsed
                
                nickname = user_data.get('昵称', str(uid))

            # 1分钟前警告（还未超时）
            if 0 < remaining <= 60 and not one_minute_warning_sent:
                warning_msg = f"⏳ <b>即将超时警告</b>：您本次{MessageFormatter.format_copyable_text(act)}还有 <code>1</code> 分钟即将超时！\n💡 请及时回座，避免超时罚款"
                await bot.send_message(chat_id, warning_msg, parse_mode="HTML")
                one_minute_warning_sent = True
            
            # 超时提醒
            if remaining <= 0:
                overtime_minutes = int(-remaining // 60)
                
                # 超时立即提醒
                if overtime_minutes == 0 and not timeout_immediate_sent:
                    timeout_msg = (
                        f"⚠️ <b>超时警告</b>\n"
                        f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                        f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经<code>超时</code>！\n"
                        f"💢 请立即回座，避免产生罚款！"
                    )
                    await bot.send_message(chat_id, timeout_msg, parse_mode="HTML")
                    timeout_immediate_sent = True
                    last_reminder_minute = 0
                
                # 超时5分钟提醒
                elif overtime_minutes == 5 and not timeout_5min_sent:
                    timeout_msg = (
                        f"🔔 <b>超时警告</b>\n"
                        f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                        f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经超时 <code>5</code> 分钟！\n"
                        f"💢 请立即回座，避免罚款增加！"
                    )
                    await bot.send_message(chat_id, timeout_msg, parse_mode="HTML")
                    timeout_5min_sent = True
                    last_reminder_minute = 5
                
                # 后续每10分钟提醒一次
                elif overtime_minutes >= 10 and overtime_minutes % 10 == 0 and overtime_minutes > last_reminder_minute:
                    timeout_msg = (
                        f"🚨 <b>严重超时警告</b>\n"
                        f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                        f"❌ 您的 {MessageFormatter.format_copyable_text(act)} 已经超时 <code>{overtime_minutes}</code> 分钟！\n"
                        f"💢 请立即回座！严重超时将记录并通知管理员！"
                    )
                    await bot.send_message(chat_id, timeout_msg, parse_mode="HTML")
                    last_reminder_minute = overtime_minutes
                    
                    # 如果超时超过30分钟，发送更强烈的警告
                    if overtime_minutes >= 30:
                        urgent_msg = (
                            f"🚨 <b>紧急超时警告</b>\n"
                            f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                            f"⚠️ 您的 {MessageFormatter.format_copyable_text(act)} 已经超时 <code>{overtime_minutes}</code> 分钟！\n"
                            f"💢 请立即回座！严重超时将记录并通知管理员！"
                        )
                        await bot.send_message(chat_id, urgent_msg, parse_mode="HTML")
                        
                        # 同时发送到通知频道/群组
                        try:
                            notif_text = (
                                f"🚨 <b>严重超时警报</b>\n"
                                f"🏢 群组：<code>{chat_id}</code>\n"
                                f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                                f"📝 活动：<code>{act}</code>\n"
                                f"⏰ 超时时长：<code>{overtime_minutes}</code> 分钟\n"
                                f"🕒 开始时间：<code>{start_time.strftime('%m/%d %H:%M:%S')}</code>"
                            )
                            await NotificationService.send_notification(chat_id, notif_text)
                                
                        except Exception as e:
                            logging.error(f"发送严重超时通知失败: {e}")
            
            # 检查超时强制回座（有锁保护）
            user_lock = get_user_lock(chat_id, uid)
            async with user_lock:
                if (str(chat_id) in group_data and 
                    str(uid) in group_data[str(chat_id)]["成员"] and
                    group_data[str(chat_id)]["成员"][str(uid)]["活动"] == act):
                    
                    # 如果超时超过2小时，自动强制回座
                    if remaining <= -120 * 60:
                        auto_back_msg = (
                            f"🛑 <b>自动安全回座</b>\n"
                            f"👤 用户：{MessageFormatter.format_user_link(uid, nickname)}\n"
                            f"📝 活动：<code>{act}</code>\n"
                            f"⚠️ 由于超时超过2小时，系统已自动为您回座\n"
                            f"⏰ 超时时长：<code>120</code> 分钟\n"
                            f"💢 请检查是否忘记回座！"
                        )
                        await bot.send_message(chat_id, auto_back_msg, parse_mode="HTML")
                        
                        # 执行自动回座
                        user_data = group_data[str(chat_id)]["成员"][str(uid)]
                        user_data["活动"] = None
                        user_data["开始时间"] = None
                        await schedule_save_data()
                        await safe_cancel_task(key)
                        break
                
            await asyncio.sleep(30)
            
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.error(f"定时器错误: {e}")

# ==================== CSV导出推送功能 ====================
async def export_and_push_csv(chat_id: int, to_admin_if_no_group: bool = True, file_name: str = None):
    """导出群组数据为 CSV 并推送"""
    init_group(chat_id)
    
    if not file_name:
        date_str = get_beijing_time().strftime('%Y%m%d_%H%M%S')
        file_name = f"group_{chat_id}_statistics_{date_str}.csv"

    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    
    headers = ["用户ID", "用户昵称"]
    for act in activity_limits.keys():
        headers.extend([f"{act}次数", f"{act}总时长(秒)"])
    headers.extend(["活动次数总计", "活动用时总计(秒)", "罚款总金额", "超时次数", "总超时时间(秒)"])
    writer.writerow(headers)
    
    has_data = False
    
    for uid, user_data in group_data[str(chat_id)]["成员"].items():
        total_count = user_data.get("总次数", 0)
        if total_count > 0:
            has_data = True
        
        row = [uid, user_data.get("昵称", "未知用户")]
        for act in activity_limits.keys():
            row.append(user_data["次数"].get(act, 0))
            row.append(int(user_data["累计"].get(act, 0)))
        
        row.extend([
            total_count,
            int(user_data.get("总累计时间", 0)),
            user_data.get("累计罚款", 0),
            user_data.get("超时次数", 0),
            int(user_data.get("总超时时间", 0))
        ])
        writer.writerow(row)
    
    if not has_data:
        await bot.send_message(chat_id, "⚠️ 当前群组没有数据需要导出")
        return

    csv_content = csv_buffer.getvalue()
    csv_buffer.close()
    
    temp_file = f"temp_{file_name}"
    try:
        with open(temp_file, 'w', encoding='utf-8-sig') as f:
            f.write(csv_content)
        
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
            f"--------------------------------------------\n"
            f"💾 包含每个用户的所有活动统计和总计信息"
        )

        # 发送给当前聊天
        try:
            csv_input_file = FSInputFile(temp_file, filename=file_name)
            await bot.send_document(chat_id, csv_input_file, 
                                  caption=caption,
                                  parse_mode="HTML")
        except Exception as e:
            logging.error(f"❌ 发送到当前聊天失败: {e}")

        # 使用推送服务发送到绑定的频道和群组
        await NotificationService.send_document(chat_id, FSInputFile(temp_file, filename=file_name), caption)

        logging.info(f"✅ 数据导出并推送完成: {file_name}")

    except Exception as e:
        logging.error(f"❌ 导出过程出错: {e}")
        await bot.send_message(chat_id, f"❌ 导出失败：{e}")
    finally:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass

# ==================== 核心打卡功能 ====================
async def _start_activity_locked(message: types.Message, act: str, chat_id: int, uid: int):
    """线程安全的打卡逻辑"""
    name = message.from_user.full_name
    now = get_beijing_time()

    if act not in activity_limits:
        await message.answer(
            f"❌ 活动 '{act}' 不存在，请使用下方按钮选择活动",
            reply_markup=get_main_keyboard(show_admin=is_admin(uid))
        )
        return

    has_active, current_act = has_active_activity(chat_id, uid)
    if has_active:
        await message.answer(
            config.MESSAGES["has_activity"].format(current_act),
            reply_markup=get_main_keyboard(show_admin=is_admin(uid))
        )
        return

    async with UserContext(chat_id, uid) as user_data:
        can_start, current_count, max_times = await check_activity_limit(chat_id, uid, act)
        
        if not can_start:
            await message.answer(
                config.MESSAGES["max_times_reached"].format(act, max_times),
                reply_markup=get_main_keyboard(show_admin=is_admin(uid))
            )
            return

        # 原子性操作序列
        user_data["昵称"] = name
        count = current_count + 1
        user_data["活动"] = act
        user_data["开始时间"] = str(now)
        user_data["次数"][act] = count

    key = f"{chat_id}-{uid}"
    await safe_cancel_task(key)
    
    time_limit = activity_limits[act]["time_limit"]
    tasks[key] = asyncio.create_task(activity_timer(chat_id, uid, act, time_limit))

    await message.answer(
        MessageFormatter.format_activity_message(uid, name, act, now.strftime('%m/%d %H:%M:%S'),
                                count, max_times, time_limit),
        reply_markup=get_main_keyboard(show_admin=is_admin(uid)),
        parse_mode="HTML"
    )

async def start_activity(message: types.Message, act: str):
    """开始活动打卡"""
    chat_id = message.chat.id
    uid = message.from_user.id
    
    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await _start_activity_locked(message, act, chat_id, uid)

# ==================== 消息处理器 ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """开始命令"""
    uid = message.from_user.id
    await message.answer(
        config.MESSAGES["welcome"],
        reply_markup=get_main_keyboard(show_admin=is_admin(uid))
    )

@dp.message(Command("menu"))
async def cmd_menu(message: types.Message):
    """显示主菜单"""
    uid = message.from_user.id
    await message.answer("📋 主菜单", reply_markup=get_main_keyboard(show_admin=is_admin(uid)))

@dp.message(Command("admin"))
@admin_required
async def cmd_admin(message: types.Message):
    """管理员命令"""
    await message.answer("👑 管理员面板", reply_markup=get_admin_keyboard())

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    """帮助命令"""
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
        "📊 查看记录：\n"
        "• 点击 <code>📊 我的记录</code> 查看个人统计\n"
        "• 点击 <code>🏆 排行榜</code> 查看群内排名\n\n"
        "🔧 其他命令：\n"
        "• <code>/start</code> - 开始使用机器人\n"
        "• <code>/menu</code> - 显示主菜单\n"
        "• <code>/help</code> - 显示此帮助信息\n\n"
        "⏰ 注意事项：\n"
        "• 每个活动有每日次数限制和时间限制\n"
        "• 超时会产生罚款\n"
        "• 活动完成后请及时回座\n"
        "• 每日数据会在指定时间自动重置"
    )
    
    await message.answer(
        help_text,
        reply_markup=get_main_keyboard(show_admin=is_admin(uid)),
        parse_mode="HTML"
    )

# ==================== 管理员命令功能 ====================
@dp.message(Command("setchannel"))
@admin_required
async def cmd_setchannel(message: types.Message):
    """绑定提醒频道"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await message.answer(config.MESSAGES["setchannel_usage"], reply_markup=get_main_keyboard(show_admin=True))
        return

    try:
        channel_id = int(args[1].strip())
        init_group(chat_id)
        group_data[str(chat_id)]["频道ID"] = channel_id
        await schedule_save_data()
        await message.answer(f"✅ 已绑定超时提醒推送频道：<code>{channel_id}</code>", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")
    except ValueError:
        await message.answer("❌ 频道ID必须是数字", reply_markup=get_main_keyboard(show_admin=True))

@dp.message(Command("setgroup"))
@admin_required
async def cmd_setgroup(message: types.Message):
    """绑定通知群组"""
    chat_id = message.chat.id
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await message.answer(config.MESSAGES["setgroup_usage"], reply_markup=get_main_keyboard(show_admin=True))
        return

    try:
        group_id = int(args[1].strip())
        init_group(chat_id)
        group_data[str(chat_id)]["通知群组ID"] = group_id
        await schedule_save_data()

        await message.answer(f"✅ 已绑定超时通知群组：<code>{group_id}</code>", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")
    except ValueError:
        await message.answer("❌ 群组ID必须是数字", reply_markup=get_main_keyboard(show_admin=True))

@dp.message(Command("unbindchannel"))
@admin_required
async def cmd_unbind_channel(message: types.Message):
    """解除绑定频道"""
    chat_id = message.chat.id
    init_group(chat_id)
    group_data[str(chat_id)]["频道ID"] = None
    await schedule_save_data()

    await message.answer("✅ 已解除绑定的提醒频道", reply_markup=get_main_keyboard(show_admin=True))

@dp.message(Command("unbindgroup"))
@admin_required
async def cmd_unbind_group(message: types.Message):
    """解除绑定通知群组"""
    chat_id = message.chat.id
    init_group(chat_id)
    group_data[str(chat_id)]["通知群组ID"] = None
    await schedule_save_data()

    await message.answer("✅ 已解除绑定的通知群组", reply_markup=get_main_keyboard(show_admin=True))

@dp.message(Command("addactivity"))
@admin_required
async def cmd_addactivity(message: types.Message):
    """添加新活动"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(config.MESSAGES["addactivity_usage"], reply_markup=get_main_keyboard(show_admin=True))
        return

    try:
        act, max_times, time_limit = args[1], int(args[2]), int(args[3])
        existed = act in activity_limits
        activity_limits[act] = {"max_times": max_times, "time_limit": time_limit}
        if act not in fine_rates:
            fine_rates[act] = {}
        await schedule_save_data()

        if existed:
            await message.answer(f"✅ 已修改活动 <code>{act}</code>，次数上限 <code>{max_times}</code>，时间限制 <code>{time_limit}</code> 分钟", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")
        else:
            await message.answer(f"✅ 已添加新活动 <code>{act}</code>，次数上限 <code>{max_times}</code>，时间限制 <code>{time_limit}</code> 分钟", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ 添加/修改活动失败：{e}", reply_markup=get_main_keyboard(show_admin=True))

@dp.message(Command("delactivity"))
@admin_required
async def cmd_delactivity(message: types.Message):
    """删除活动"""
    args = message.text.split()
    if len(args) != 2:
        await message.answer("❌ 用法：/delactivity <活动名>", reply_markup=get_main_keyboard(show_admin=True))
        return
    act = args[1]
    if act not in activity_limits:
        await message.answer(f"❌ 活动 <code>{act}</code> 不存在", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")
        return
    activity_limits.pop(act, None)
    fine_rates.pop(act, None)
    await schedule_save_data()

    await message.answer(f"✅ 活动 <code>{act}</code> 已删除", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")

@dp.message(Command("set"))
@admin_required
async def cmd_set(message: types.Message):
    """设置用户数据"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(config.MESSAGES["set_usage"], reply_markup=get_main_keyboard(show_admin=True))
        return

    try:
        uid, act, minutes = args[1], args[2], args[3]
        chat_id = message.chat.id

        init_user(chat_id, int(uid))
        group_data[str(chat_id)]["成员"][str(uid)]["累计"][act] = int(minutes) * 60
        group_data[str(chat_id)]["成员"][str(uid)]["次数"][act] = int(minutes) // 30
        await schedule_save_data()

        await message.answer(f"✅ 已设置用户 <code>{uid}</code> 的 <code>{act}</code> 累计时间为 <code>{minutes}</code> 分钟", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ 设置失败：{e}", reply_markup=get_main_keyboard(show_admin=True))

@dp.message(Command("reset"))
@admin_required
async def cmd_reset(message: types.Message):
    """重置用户数据"""
    args = message.text.split()
    if len(args) != 2:
        await message.answer(config.MESSAGES["reset_usage"], reply_markup=get_main_keyboard(show_admin=True))
        return

    try:
        uid = args[1]
        chat_id = message.chat.id

        init_user(chat_id, int(uid))
        group_data[str(chat_id)]["成员"][str(uid)]["累计"] = {}
        group_data[str(chat_id)]["成员"][str(uid)]["次数"] = {}
        group_data[str(chat_id)]["成员"][str(uid)]["总累计时间"] = 0
        group_data[str(chat_id)]["成员"][str(uid)]["总次数"] = 0
        group_data[str(chat_id)]["成员"][str(uid)]["累计罚款"] = 0
        group_data[str(chat_id)]["成员"][str(uid)]["超时次数"] = 0
        group_data[str(chat_id)]["成员"][str(uid)]["总超时时间"] = 0
        await schedule_save_data()

        await message.answer(f"✅ 已重置用户 <code>{uid}</code> 的今日数据", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ 重置失败：{e}", reply_markup=get_main_keyboard(show_admin=True))

@dp.message(Command("setresettime"))
@admin_required
async def cmd_setresettime(message: types.Message):
    """设置每日重置时间"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(config.MESSAGES["setresettime_usage"], reply_markup=get_main_keyboard(show_admin=True))
        return

    try:
        hour = int(args[1])
        minute = int(args[2])

        if 0 <= hour <= 23 and 0 <= minute <= 59:
            chat_id = message.chat.id
            init_group(chat_id)
            group_data[str(chat_id)]["每日重置时间"] = {"hour": hour, "minute": minute}
            await schedule_save_data()

            await message.answer(f"✅ 每日重置时间已设置为：<code>{hour:02d}:{minute:02d}</code>", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")
        else:
            await message.answer("❌ 小时必须在0-23之间，分钟必须在0-59之间！", reply_markup=get_main_keyboard(show_admin=True))
    except ValueError:
        await message.answer("❌ 请输入有效的数字！", reply_markup=get_main_keyboard(show_admin=True))

@dp.message(Command("setfine"))
@admin_required
async def cmd_setfine(message: types.Message):
    """设置活动罚款费率"""
    args = message.text.split()
    if len(args) != 4:
        await message.answer(config.MESSAGES["setfine_usage"], reply_markup=get_main_keyboard(show_admin=True))
        return

    try:
        act = args[1]
        time_segment = args[2]
        fine_amount = int(args[3])

        if act not in activity_limits:
            await message.answer(f"❌ 活动 '<code>{act}</code>' 不存在！", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")
            return

        if fine_amount < 0:
            await message.answer("❌ 罚款金额不能为负数！", reply_markup=get_main_keyboard(show_admin=True))
            return

        if act not in fine_rates:
            fine_rates[act] = {}

        fine_rates[act][time_segment] = fine_amount
        await schedule_save_data()

        await message.answer(f"✅ 已设置活动 '<code>{act}</code>' 在 <code>{time_segment}</code> 分钟内的罚款费率为 <code>{fine_amount}</code> 元", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")
    except ValueError:
        await message.answer("❌ 请输入有效的数字！", reply_markup=get_main_keyboard(show_admin=True))
    except Exception as e:
        await message.answer(f"❌ 设置失败：{e}", reply_markup=get_main_keyboard(show_admin=True))

@dp.message(Command("setfines_all"))
@admin_required
async def cmd_setfines_all(message: types.Message):
    """为所有活动统一设置分段罚款"""
    args = message.text.split()
    if len(args) < 3 or (len(args) - 1) % 2 != 0:
        await message.answer(config.MESSAGES["setfines_all_usage"], reply_markup=get_main_keyboard(show_admin=True))
        return

    try:
        pairs = args[1:]
        segments = {}
        for i in range(0, len(pairs), 2):
            t = int(pairs[i])
            f = int(pairs[i+1])
            if t <= 0 or f < 0:
                await message.answer("❌ 时间段必须为正整数，罚款金额不能为负数", reply_markup=get_main_keyboard(show_admin=True))
                return
            segments[str(t)] = f

        for act in activity_limits.keys():
            fine_rates[act] = segments.copy()

        await schedule_save_data()

        segments_text = " ".join([f"<code>{t}</code>:<code>{f}</code>" for t, f in segments.items()])
        await message.answer(f"✅ 已为所有活动设置分段罚款：{segments_text}", reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ 设置失败：{e}", reply_markup=get_main_keyboard(show_admin=True))

@dp.message(Command("showsettings"))
@admin_required
async def cmd_showsettings(message: types.Message):
    """显示目前的设置"""
    chat_id = message.chat.id
    init_group(chat_id)
    cfg = group_data[str(chat_id)]

    text = f"🔧 当前群设置（群 {chat_id}）\n"
    text += f"• 绑定频道ID: {cfg.get('频道ID', '未设置')}\n"
    text += f"• 通知群组ID: {cfg.get('通知群组ID', '未设置')}\n"
    rt = cfg.get("每日重置时间", {"hour": config.DAILY_RESET_HOUR, "minute": config.DAILY_RESET_MINUTE})
    text += f"• 每日重置时间: {rt.get('hour',0):02d}:{rt.get('minute',0):02d}\n\n"

    text += "📋 活动设置：\n"
    for act, v in activity_limits.items():
        text += f"• {act}：次数上限 {v['max_times']}，时间限制 {v['time_limit']} 分钟\n"

    text += "\n💰 当前各活动罚款分段：\n"
    for act, fr in fine_rates.items():
        text += f"• {act}：{fr}\n"

    await message.answer(text, reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")

# ==================== 推送开关管理命令 ====================
@dp.message(Command("setpush"))
@admin_required
async def cmd_setpush(message: types.Message):
    """设置推送开关"""
    args = message.text.split()
    if len(args) != 3:
        await message.answer(config.MESSAGES["setpush_usage"], reply_markup=get_main_keyboard(show_admin=True))
        return

    push_type = args[1].lower()
    status = args[2].lower()

    if push_type not in ["channel", "group", "admin"]:
        await message.answer("❌ 类型错误，请使用 channel、group 或 admin", reply_markup=get_main_keyboard(show_admin=True))
        return

    if status not in ["on", "off"]:
        await message.answer("❌ 状态错误，请使用 on 或 off", reply_markup=get_main_keyboard(show_admin=True))
        return

    if push_type == "channel":
        config.AUTO_EXPORT_SETTINGS["enable_channel_push"] = (status == "on")
        status_text = "开启" if status == "on" else "关闭"
        await message.answer(f"✅ 已{status_text}频道推送", reply_markup=get_main_keyboard(show_admin=True))
    elif push_type == "group":
        config.AUTO_EXPORT_SETTINGS["enable_group_push"] = (status == "on")
        status_text = "开启" if status == "on" else "关闭"
        await message.answer(f"✅ 已{status_text}群组推送", reply_markup=get_main_keyboard(show_admin=True))
    elif push_type == "admin":
        config.AUTO_EXPORT_SETTINGS["enable_admin_push"] = (status == "on")
        status_text = "开启" if status == "on" else "关闭"
        await message.answer(f"✅ 已{status_text}管理员推送", reply_markup=get_main_keyboard(show_admin=True))

    save_push_settings()

@dp.message(Command("showpush"))
@admin_required
async def cmd_showpush(message: types.Message):
    """显示推送设置"""
    settings = config.AUTO_EXPORT_SETTINGS
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
    await message.answer(text, reply_markup=get_main_keyboard(show_admin=True), parse_mode="HTML")

@dp.message(Command("testpush"))
@admin_required
async def cmd_testpush(message: types.Message):
    """测试推送功能"""
    chat_id = message.chat.id
    try:
        test_file_name = f"test_push_{get_beijing_time().strftime('%H%M%S')}.txt"
        with open(test_file_name, "w", encoding="utf-8") as f:
            f.write("这是一个推送测试文件\n")
            f.write(f"测试时间：{get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("如果收到此文件，说明推送功能正常")

        caption = "🧪 推送功能测试\n这是一个测试文件，用于验证自动导出推送功能是否正常工作。"

        success_count = 0
        
        if config.AUTO_EXPORT_SETTINGS["enable_group_push"]:
            notification_group_id = group_data.get(str(chat_id), {}).get("通知群组ID")
            if notification_group_id:
                try:
                    await bot.send_document(
                        notification_group_id,
                        FSInputFile(test_file_name),
                        caption=caption,
                        parse_mode="HTML"
                    )
                    success_count += 1
                    await message.answer(f"✅ 测试文件已发送到通知群组: {notification_group_id}")
                except Exception as e:
                    await message.answer(f"❌ 通知群组推送测试失败: {e}")

        if config.AUTO_EXPORT_SETTINGS["enable_channel_push"]:
            channel_id = group_data.get(str(chat_id), {}).get("频道ID")
            if channel_id:
                try:
                    await bot.send_document(
                        channel_id,
                        FSInputFile(test_file_name),
                        caption=caption,
                        parse_mode="HTML"
                    )
                    success_count += 1
                    await message.answer(f"✅ 测试文件已发送到频道: {channel_id}")
                except Exception as e:
                    await message.answer(f"❌ 频道推送测试失败: {e}")

        os.remove(test_file_name)

        if success_count == 0:
            await message.answer("⚠️ 没有成功发送任何测试推送，请检查推送设置和绑定状态")
        else:
            await message.answer(f"✅ 推送测试完成，成功发送 {success_count} 个测试文件")

    except Exception as e:
        await message.answer(f"❌ 推送测试失败：{e}")

@dp.message(Command("export"))
@admin_required
async def cmd_export(message: types.Message):
    """管理员手动导出群组数据"""
    chat_id = message.chat.id
    await message.answer("⏳ 正在导出数据，请稍候...")
    try:
        await export_and_push_csv(chat_id)
        await message.answer("✅ 数据已导出并推送到绑定的群组或频道！")
    except Exception as e:
        await message.answer(f"❌ 导出失败：{e}")

# ==================== 简化版指令 ====================
@dp.message(Command("ci"))
async def cmd_ci(message: types.Message):
    """指令打卡：/ci 活动名"""
    args = message.text.split(maxsplit=1)
    if len(args) != 2:
        await message.answer("❌ 用法：/ci <活动名>", reply_markup=get_main_keyboard(show_admin=is_admin(message.from_user.id)))
        return
    act = args[1].strip()
    if act not in activity_limits:
        await message.answer(f"❌ 活动 '<code>{act}</code>' 不存在，请先使用 /addactivity 添加或检查拼写", reply_markup=get_main_keyboard(show_admin=is_admin(message.from_user.id)), parse_mode="HTML")
        return
    await start_activity(message, act)

@dp.message(Command("at"))
async def cmd_at(message: types.Message):
    """指令回座：/at"""
    await process_back(message)

# ============ 文本命令处理 =================
@dp.message(lambda message: message.text and message.text.strip() in ["回座", "✅ 回座"])
async def handle_back_command(message: types.Message):
    """处理回座命令"""
    await process_back(message)

@dp.message(lambda message: message.text and message.text.strip() in ["📊 我的记录"])
async def handle_my_record(message: types.Message):
    """处理我的记录按钮"""
    chat_id = message.chat.id
    uid = message.from_user.id
    
    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await show_history(message)

@dp.message(lambda message: message.text and message.text.strip() in ["🏆 排行榜"])
async def handle_rank(message: types.Message):
    """处理排行榜按钮"""
    chat_id = message.chat.id
    uid = message.from_user.id
    
    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await show_rank(message)

@dp.message(lambda message: message.text and message.text.strip() in ["👑 管理员面板"])
async def handle_admin_panel_button(message: types.Message):
    """处理管理员面板按钮点击"""
    if not is_admin(message.from_user.id):
        await message.answer(config.MESSAGES["no_permission"], reply_markup=get_main_keyboard())
        return

    admin_text = (
        "👑 管理员面板\n\n"
        "可用命令：\n"
        "• /setchannel <频道ID> - 绑定提醒频道\n"
        "• /setgroup <群组ID> - 绑定通知群组\n"
        "• /unbindchannel - 解除绑定频道\n"
        "• /unbindgroup - 解除绑定通知群组\n"
        "• /addactivity <活动名> <次数> <分钟> - 添加或修改活动\n"
        "• /delactivity <活动名> - 删除活动\n"
        "• /set <用户ID> <活动> <分钟> - 设置用户时间\n"
        "• /reset <用户ID> - 重置用户数据\n"
        "• /setresettime <小时> <分钟> - 设置每日重置时间\n"
        "• /setfine <活动名> <时间段> <金额> - 设置活动罚款费率\n"
        "• /setfines_all <t1> <f1> [<t2> <f2> ...] - 为所有活动统一设置分段罚款\n"
        "• /showsettings - 查看当前群设置\n"
        "• /export - 导出数据\n\n"
        "点击下方按钮进行操作："
    )
    await message.answer(admin_text, reply_markup=get_admin_keyboard())

@dp.message(lambda message: message.text and message.text.strip() in ["🔙 返回主菜单"])
async def handle_back_to_main_menu(message: types.Message):
    """处理返回主菜单按钮"""
    uid = message.from_user.id
    await message.answer("已返回主菜单", reply_markup=get_main_keyboard(show_admin=is_admin(uid)))

@dp.message(lambda message: message.text and message.text.strip() in ["📤 导出数据"])
async def handle_export_data_button(message: types.Message):
    """处理导出数据按钮点击"""
    if not is_admin(message.from_user.id):
        await message.answer(config.MESSAGES["no_permission"], reply_markup=get_main_keyboard())
        return
    await export_data(message)

@dp.message(lambda message: message.text and message.text.strip() in activity_limits.keys())
async def handle_activity_direct_input(message: types.Message):
    """处理直接输入活动名称进行打卡"""
    act = message.text.strip()
    await start_activity(message, act)

@dp.message(lambda message: message.text and message.text.strip())
async def handle_other_text_messages(message: types.Message):
    """处理其他文本消息"""
    text = message.text.strip()
    uid = message.from_user.id
    
    if (text.startswith("/") or 
        text in ["👑 管理员面板", "🔙 返回主菜单", "📤 导出数据", "🔔 通知设置"]):
        return
    
    if any(act in text for act in activity_limits.keys()):
        return
    
    await message.answer(
        "请使用下方按钮或直接输入活动名称进行操作：\n\n"
        "📝 使用方法：\n"
        "• 输入活动名称（如：<code>吃饭</code>、<code>小厕</code>）开始打卡\n"
        "• 输入'回座'或点击'✅ 回座'按钮结束当前活动\n"
        "• 点击'📊 我的记录'查看个人统计\n"
        "• 点击'🏆 排行榜'查看群内排名",
        reply_markup=get_main_keyboard(show_admin=is_admin(uid)),
        parse_mode="HTML"
    )

# ==================== 用户功能 ====================
async def show_history(message: types.Message):
    """显示用户历史记录"""
    chat_id = message.chat.id
    uid = message.from_user.id
    
    async with UserContext(chat_id, uid) as user:
        first_line = f"👤 用户：{MessageFormatter.format_user_link(uid, user['昵称'])}"
        text = f"{first_line}\n📊 今日记录：\n\n"

        has_records = False
        for act in activity_limits.keys():
            total_time = user["累计"].get(act, 0)
            count = user["次数"].get(act, 0)
            max_times = activity_limits[act]["max_times"]
            if total_time > 0 or count > 0:
                status = "✅" if count < max_times else "❌"
                time_str = MessageFormatter.format_time(int(total_time))
                text += f"• <code>{act}</code>：<code>{time_str}</code>，次数：<code>{count}</code>/<code>{max_times}</code> {status}\n"
                has_records = True

        total_time_all = user.get("总累计时间", 0)
        total_count_all = user.get("总次数", 0)
        total_fine = user.get("累计罚款", 0)
        overtime_count = user.get("超时次数", 0)
        total_overtime = user.get("总超时时间", 0)

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

        await message.answer(text, reply_markup=get_main_keyboard(show_admin=is_admin(uid)), parse_mode="HTML")

async def show_rank(message: types.Message):
    """显示排行榜"""
    chat_id = message.chat.id
    uid = message.from_user.id
    init_group(chat_id)

    rank_text = "🏆 今日活动排行榜\n\n"

    # 优化的数据收集方式
    user_times_by_activity = {}
    for act in activity_limits.keys():
        user_times = []
        for user_uid, user_data in group_data[str(chat_id)]["成员"].items():
            total_time = user_data["累计"].get(act, 0)
            if total_time > 0:
                user_times.append((user_data['昵称'], user_data['用户ID'], total_time))

        user_times.sort(key=lambda x: x[2], reverse=True)
        user_times_by_activity[act] = user_times[:3]

    # 生成排行榜文本
    for act, user_times in user_times_by_activity.items():
        if user_times:
            rank_text += f"📈 <code>{act}</code>：\n"
            for i, (name, user_id, time_sec) in enumerate(user_times, 1):
                time_str = MessageFormatter.format_time(int(time_sec))
                rank_text += f"  <code>{i}.</code> {MessageFormatter.format_user_link(user_id, name)} - <code>{time_str}</code>\n"
            rank_text += "\n"

    await message.answer(rank_text, reply_markup=get_main_keyboard(show_admin=is_admin(uid)), parse_mode="HTML")

# ==================== 回座功能 ====================
async def _process_back_locked(message: types.Message, chat_id: int, uid: int):
    """线程安全的回座逻辑"""
    now = get_beijing_time()

    async with UserContext(chat_id, uid) as user_data:
        if not user_data["活动"]:
            await message.answer(config.MESSAGES["no_activity"], reply_markup=get_main_keyboard(show_admin=is_admin(uid)))
            return

        act = user_data["活动"]
        start_time = datetime.fromisoformat(user_data["开始时间"])
        elapsed = (now - start_time).total_seconds()
        total_activity_time = user_data["累计"].get(act, 0) + elapsed

        # 检查是否超时
        time_limit_seconds = activity_limits[act]["time_limit"] * 60
        is_overtime = elapsed > time_limit_seconds
        overtime_seconds = max(0, int(elapsed - time_limit_seconds))
        overtime_minutes = overtime_seconds / 60

        # 计算罚款（分段罚款）
        fine_amount = 0
        if is_overtime and overtime_seconds > 0:
            fine_amount = calculate_fine(act, overtime_minutes)
            user_data["累计罚款"] = user_data.get("累计罚款", 0) + fine_amount
            
            # 记录超时统计
            user_data["超时次数"] = user_data.get("超时次数", 0) + 1
            user_data["总超时时间"] = user_data.get("总超时时间", 0) + overtime_seconds

        # 更新用户数据
        user_data["累计"][act] = total_activity_time
        current_count = user_data["次数"].get(act, 0)
        user_data["总累计时间"] = user_data.get("总累计时间", 0) + elapsed
        user_data["总次数"] = user_data.get("总次数", 0) + 1
        user_data["活动"] = None
        user_data["开始时间"] = None

    # 取消定时任务
    key = f"{chat_id}-{uid}"
    await safe_cancel_task(key)

    # 构建活动次数字典
    activity_counts = {a: user_data["次数"].get(a, 0) for a in activity_limits.keys()}

    # 发送回座消息
    await message.answer(
        MessageFormatter.format_back_message(
            user_id=uid,
            user_name=user_data['昵称'],
            activity=act,
            time_str=now.strftime('%m/%d %H:%M:%S'),
            elapsed_time=MessageFormatter.format_time(int(elapsed)),
            total_activity_time=MessageFormatter.format_time(int(total_activity_time)),
            total_time=MessageFormatter.format_time(int(user_data["总累计时间"])),
            activity_counts=activity_counts,
            total_count=user_data["总次数"],
            is_overtime=is_overtime,
            overtime_seconds=overtime_seconds,
            fine_amount=fine_amount
        ),
        reply_markup=get_main_keyboard(show_admin=is_admin(uid)),
        parse_mode="HTML"
    )

    # 超时通知推送
    if is_overtime:
        try:
            chat_title = str(chat_id)
            try:
                chat_info = await bot.get_chat(chat_id)
                chat_title = chat_info.title or chat_title
            except Exception as e:
                logging.warning(f"无法获取群组信息: {e}")

            overtime_minutes_int = int(overtime_minutes)
            notif_text = (
                f"🚨 <b>超时回座通知</b>\n"
                f"🏢 群组：<code>{chat_title}</code>\n"
                f"--------------------------------------------\n"
                f"👤 用户：{MessageFormatter.format_user_link(uid, user_data['昵称'])}\n"
                f"📝 活动：<code>{act}</code>\n"
                f"⏰ 回座时间：<code>{now.strftime('%m/%d %H:%M:%S')}</code>\n"
                f"⏱️ 超时时长：<code>{MessageFormatter.format_time(int(overtime_seconds))}</code> ({overtime_minutes_int}分钟)\n"
                f"💰 本次罚款：<code>{fine_amount}</code> 元"
            )

            await NotificationService.send_notification(chat_id, notif_text)

        except Exception as e:
            logging.error(f"⚠️ 超时通知推送异常: {e}")

async def process_back(message: types.Message):
    """回座打卡"""
    chat_id = message.chat.id
    uid = message.from_user.id
    
    user_lock = get_user_lock(chat_id, uid)
    async with user_lock:
        await _process_back_locked(message, chat_id, uid)

# ==================== 管理员按钮处理 ====================
@dp.message(lambda message: message.text == "🔔 通知设置")
async def handle_notification_settings(message: types.Message, state: FSMContext):
    """处理通知设置按钮"""
    if not is_admin(message.from_user.id):
        await message.answer(config.MESSAGES["no_permission"], reply_markup=get_main_keyboard())
        return
    await notification_settings_menu(message, state)

async def notification_settings_menu(message: types.Message, state: FSMContext):
    """通知设置菜单"""
    chat_id = message.chat.id
    init_group(chat_id)
    group_config = group_data[str(chat_id)]

    current_settings = (
        f"🔔 当前通知设置：\n"
        f"频道ID: <code>{group_config.get('频道ID', '未设置')}</code>\n"
        f"通知群组ID: <code>{group_config.get('通知群组ID', '未设置')}</code>\n\n"
        f"请选择操作："
    )

    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="设置频道"), KeyboardButton(text="设置通知群组")],
            [KeyboardButton(text="清除频道"), KeyboardButton(text="清除通知群组")],
            [KeyboardButton(text="🔙 返回管理员面板")]
        ],
        resize_keyboard=True
    )

    await message.answer(current_settings, reply_markup=keyboard, parse_mode="HTML")

@dp.message(lambda message: message.text in ["设置频道", "设置通知群组", "清除频道", "清除通知群组", "🔙 返回管理员面板"])
async def handle_notification_actions(message: types.Message, state: FSMContext):
    """处理通知设置操作"""
    text = message.text
    chat_id = message.chat.id

    if text == "🔙 返回管理员面板":
        await state.clear()
        await message.answer("已返回管理员面板", reply_markup=get_admin_keyboard())
        return
    elif text == "设置频道":
        await message.answer("请输入频道ID（格式如 -1001234567890）：", reply_markup=ReplyKeyboardRemove())
        await state.set_state(AdminStates.waiting_for_channel_id)
    elif text == "设置通知群组":
        await message.answer("请输入通知群组ID（格式如 -1001234567890）：", reply_markup=ReplyKeyboardRemove())
        await state.set_state(AdminStates.waiting_for_group_id)
    elif text == "清除频道":
        init_group(chat_id)
        group_data[str(chat_id)]["频道ID"] = None
        await schedule_save_data()
        await message.answer("✅ 已清除频道设置", reply_markup=get_admin_keyboard())
    elif text == "清除通知群组":
        init_group(chat_id)
        group_data[str(chat_id)]["通知群组ID"] = None
        await schedule_save_data()
        await message.answer("✅ 已清除通知群组设置", reply_markup=get_admin_keyboard())

@dp.message(AdminStates.waiting_for_channel_id)
async def set_channel_id(message: types.Message, state: FSMContext):
    """设置频道ID"""
    try:
        channel_id = int(message.text)
        chat_id = message.chat.id
        init_group(chat_id)
        group_data[str(chat_id)]["频道ID"] = channel_id
        await schedule_save_data()
        await message.answer(f"✅ 已绑定提醒频道：<code>{channel_id}</code>", reply_markup=get_admin_keyboard(), parse_mode="HTML")
        await state.clear()
    except ValueError:
        await message.answer("❌ 请输入有效的频道ID！")

@dp.message(AdminStates.waiting_for_group_id)
async def set_group_id(message: types.Message, state: FSMContext):
    """设置通知群组ID"""
    try:
        group_id = int(message.text)
        chat_id = message.chat.id
        init_group(chat_id)
        group_data[str(chat_id)]["通知群组ID"] = group_id
        await schedule_save_data()
        await message.answer(f"✅ 已绑定通知群组：<code>{group_id}</code>", reply_markup=get_admin_keyboard(), parse_mode="HTML")
        await state.clear()
    except ValueError:
        await message.answer("❌ 请输入有效的群组ID！")

async def export_data(message: types.Message):
    """导出数据"""
    if not is_admin(message.from_user.id):
        await message.answer(config.MESSAGES["no_permission"], reply_markup=get_main_keyboard())
        return
        
    chat_id = message.chat.id
    await message.answer("⏳ 正在导出数据...")
    try:
        await export_and_push_csv(chat_id)
        await message.answer("✅ 数据导出完成！")
    except Exception as e:
        await message.answer(f"❌ 导出失败：{e}")

# ==================== 系统维护功能 ====================
async def export_data_before_reset(chat_id: int):
    """在重置前自动导出CSV数据"""
    try:
        date_str = get_beijing_time().strftime('%Y%m%d')
        file_name = f"group_{chat_id}_statistics_{date_str}.csv"
        await export_and_push_csv(chat_id, to_admin_if_no_group=True, file_name=file_name)
        logging.info(f"✅ 群组 {chat_id} 的每日数据已自动导出并推送")
    except Exception as e:
        logging.error(f"❌ 自动导出数据失败：{e}")

async def daily_reset_task():
    """每日自动重置任务"""
    while True:
        now = get_beijing_time()
        logging.info(f"🔄 重置任务检查，当前时间: {now}")

        for chat_id_str, chat_data in group_data.items():
            reset_time = chat_data.get("每日重置时间", {"hour": config.DAILY_RESET_HOUR, "minute": config.DAILY_RESET_MINUTE})
            reset_hour = reset_time["hour"]
            reset_minute = reset_time["minute"]

            if now.hour == reset_hour and now.minute == reset_minute:
                try:
                    chat_id = int(chat_id_str)
                    logging.info(f"⏰ 到达重置时间，正在重置群组 {chat_id} 的数据...")
                    
                    await export_data_before_reset(chat_id)
                    
                    # 使用锁保护重置操作
                    for uid in list(chat_data["成员"].keys()):
                        user_lock = get_user_lock(int(chat_id_str), int(uid))
                        async with user_lock:
                            user_data = chat_data["成员"][uid]
                            user_data["累计"] = {}
                            user_data["次数"] = {}
                            user_data["总累计时间"] = 0
                            user_data["总次数"] = 0
                            user_data["累计罚款"] = 0
                            user_data["超时次数"] = 0
                            user_data["总超时时间"] = 0
                            user_data["最后更新"] = str(now.date())

                    await schedule_save_data()
                    logging.info(f"✅ 群组 {chat_id} 数据重置完成")
                    
                except Exception as e:
                    logging.error(f"❌ 重置群组 {chat_id_str} 失败: {e}")

        await asyncio.sleep(60)

async def auto_daily_export_task():
    """每天重置前自动导出群组数据"""
    while True:
        now = get_beijing_time()
        logging.info(f"🕒 自动导出任务运行中，当前时间: {now}")
        
        if now.hour == 23 and now.minute == 50:
            logging.info("⏰ 到达自动导出时间，开始导出所有群组数据...")
            for chat_id_str in list(group_data.keys()):
                try:
                    chat_id = int(chat_id_str)
                    logging.info(f"📤 正在导出群组 {chat_id} 的数据...")
                    await export_and_push_csv(chat_id)
                    logging.info(f"✅ 自动导出完成：群组 {chat_id}")
                except Exception as e:
                    logging.error(f"❌ 自动导出失败 {chat_id_str}: {e}")
            await asyncio.sleep(60)
        else:
            await asyncio.sleep(60)

# ==================== Render检查接口 ====================
async def health_check(request):
    """Render健康检查接口"""
    return web.json_response({
        "status": "healthy",
        "timestamp": get_beijing_time().isoformat(),
        "bot_status": "running",
        "users_count": sum(len(chat_data["成员"]) for chat_data in group_data.values()),
        "active_tasks": len(tasks),
        "memory_usage": "normal"
    })

async def start_web_server():
    """启动轻量HTTP健康检测服务"""
    try:
        app = web.Application()
        app.router.add_get("/", health_check)
        app.router.add_get("/health", health_check)
        app.router.add_get("/status", health_check)

        runner = web.AppRunner(app)
        await runner.setup()

        port = int(os.environ.get("PORT", 8080))
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logging.info(f"🌐 Web server started on port {port}")
    except Exception as e:
        logging.error(f"❌ Web server failed: {e}")

# ==================== 启动流程 ====================
async def on_startup():
    """启动时执行"""
    logging.info("🤖 机器人启动中...")
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("✅ Webhook 已删除，使用 polling 模式")

async def on_shutdown():
    """关闭时执行"""
    logging.info("🛑 机器人正在关闭...")
    
    # 安全取消所有定时任务
    for key, task in list(tasks.items()):
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        del tasks[key]
    
    try:
        # 最后一次保存
        await save_data()
        save_push_settings()
        logging.info("✅ 数据保存完成")
    except Exception as e:
        logging.error(f"❌ 保存失败: {e}")

def check_environment():
    """检查环境配置"""
    if not config.TOKEN:
        logging.error("❌ BOT_TOKEN 未设置")
        return False
    return True

# ========== 主启动函数 ==========
async def main():
    """主启动函数"""
    if not check_environment():
        sys.exit(1)

    # 启动健康检查服务
    asyncio.create_task(start_web_server())
    
    # 初始化配置
    await on_startup()
    
    try:
        load_data()
        load_push_settings()
        logging.info("✅ 数据加载完成")
    except Exception as e:
        logging.error(f"❌ 数据加载失败: {e}")
        global group_data
        group_data = {}

    # 启动定时任务
    asyncio.create_task(auto_daily_export_task())
    asyncio.create_task(daily_reset_task())
    logging.info("✅ 定时任务已启动")

    try:
        logging.info("🚀 开始轮询消息...")
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logging.error(f"❌ 机器人运行错误: {e}")
        raise
    finally:
        await on_shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("👋 机器人已手动停止")
    except Exception as e:
        logging.error(f"💥 机器人异常退出: {e}")
        sys.exit(1)