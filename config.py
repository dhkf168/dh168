import os
import logging
from datetime import datetime, time
from typing import Dict, Any, List
from pytz import timezone

# 时区配置
beijing_tz = timezone("Asia/Shanghai")

# 添加 dotenv 加载
try:
    from dotenv import load_dotenv

    load_dotenv()
    print("✅ .env 文件已加载")
except ImportError:
    print("❌ python-dotenv 未安装，将使用系统环境变量")


class Config:
    """配置文件 - 纯双班模式"""

    # Bot配置
    TOKEN = os.getenv("BOT_TOKEN", "")
    ADMINS = (
        list(map(int, os.getenv("ADMINS", "").split(",")))
        if os.getenv("ADMINS")
        else []
    )

    # 数据库配置
    DATABASE_URL = os.getenv("DATABASE_URL", "")
    DB_MIN_CONNECTIONS = int(os.getenv("DB_MIN_CONNECTIONS", "3"))
    DB_MAX_CONNECTIONS = int(os.getenv("DB_MAX_CONNECTIONS", "10"))
    DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))
    DB_CONNECTION_TIMEOUT = int(os.getenv("DB_CONNECTION_TIMEOUT", "60"))
    DB_HEALTH_CHECK_INTERVAL = int(os.getenv("DB_HEALTH_CHECK_INTERVAL", "30"))

    # 运行模式
    BOT_MODE = os.getenv("BOT_MODE", "polling")
    WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
    WEBHOOK_PATH = "/webhook"

    # Web服务器配置
    WEB_SERVER_CONFIG = {"HOST": "0.0.0.0", "PORT": int(os.getenv("PORT", "8080"))}

    # 时间配置（仅保留双班模式的配置）
    DAILY_RESET_HOUR = int(os.getenv("DAILY_RESET_HOUR", "0"))
    DAILY_RESET_MINUTE = int(os.getenv("DAILY_RESET_MINUTE", "0"))

    # 默认活动配置
    DEFAULT_ACTIVITY_LIMITS = {
        "小厕": {"max_times": 10, "time_limit": 5},
        "大厕": {"max_times": 5, "time_limit": 15},
        "吃饭": {"max_times": 3, "time_limit": 35},
        "抽烟或休息": {"max_times": 5, "time_limit": 10},
    }

    # 默认罚款配置
    DEFAULT_FINE_RATES = {
        "小厕": {"30": 100, "60": 200, "120": 500},
        "大厕": {"30": 100, "60": 200, "120": 500},
        "吃饭": {"30": 100, "60": 200, "120": 500},
        "抽烟或休息": {"30": 100, "60": 200, "120": 500},
    }

    # 默认上下班罚款配置
    DEFAULT_WORK_FINE_RATES = {
        "work_start": {"1": 10, "10": 20, "30": 50},
        "work_end": {"1": 10, "10": 20, "30": 50},
    }

    # 默认上下班时间
    DEFAULT_WORK_HOURS = {"work_start": "09:00", "work_end": "18:00"}
    SHIFT_MANAGEMENT_CONFIG = {
        "export_delay_hours": 3,
        "check_interval_minutes": 5,
        "grace_period_minutes": 15,
    }

    # 双班模式默认配置（统一使用这些）
    DEFAULT_GRACE_BEFORE = 120
    DEFAULT_GRACE_AFTER = 360
    DEFAULT_WORKEND_GRACE_BEFORE = 120
    DEFAULT_WORKEND_GRACE_AFTER = 360

    # 班次类型
    SHIFT_DAY = "day"
    SHIFT_NIGHT = "night"

    # 换班功能开关
    HANDOVER_ENABLED = os.getenv("HANDOVER_ENABLED", "true").lower() == "true"

    # 换班日夜班开始时间（默认21:00）
    HANDOVER_NIGHT_START = os.getenv("HANDOVER_NIGHT_START", "21:00")

    # 换班日白班开始时间（默认09:00）
    HANDOVER_DAY_START = os.getenv("HANDOVER_DAY_START", "09:00")

    # 换班日夜班总时长（小时）
    HANDOVER_NIGHT_HOURS = int(os.getenv("HANDOVER_NIGHT_HOURS", "18"))

    # 换班日白班总时长（小时）
    HANDOVER_DAY_HOURS = int(os.getenv("HANDOVER_DAY_HOURS", "18"))

    # 正常夜班时长（小时）
    NORMAL_NIGHT_HOURS = int(os.getenv("NORMAL_NIGHT_HOURS", "12"))

    # 正常白班时长（小时）
    NORMAL_DAY_HOURS = int(os.getenv("NORMAL_DAY_HOURS", "12"))

    # 换班日计数重置阈值（小时）
    HANDOVER_RESET_THRESHOLD_HOURS = int(
        os.getenv("HANDOVER_RESET_THRESHOLD_HOURS", "12")
    )

    # 默认活动时间限制（分钟）
    DEFAULT_ACTIVITY_LIMIT_MINUTES = 120

    # 数据保留天数
    DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "30"))
    MONTHLY_DATA_RETENTION_DAYS = int(os.getenv("MONTHLY_DATA_RETENTION_DAYS", "40"))

    # 月度导出配置
    MONTHLY_EXPORT_ENABLED = (
        os.getenv("MONTHLY_EXPORT_ENABLED", "true").lower() == "true"
    )
    MONTHLY_EXPORT_HOUR = int(os.getenv("MONTHLY_EXPORT_HOUR", "15"))
    MONTHLY_EXPORT_MINUTE = int(os.getenv("MONTHLY_EXPORT_MINUTE", "0"))

    # 自动清理配置
    AUTO_CLEANUP_ENABLED = os.getenv("AUTO_CLEANUP_ENABLED", "true").lower() == "true"
    CLEANUP_HOUR = int(os.getenv("CLEANUP_HOUR", "3"))
    CLEANUP_MINUTE = int(os.getenv("CLEANUP_MINUTE", "0"))

    # 清理间隔（秒）
    CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "600"))
    MEMORY_CLEANUP_INTERVAL = 300

    # 自动导出设置
    AUTO_EXPORT_SETTINGS = {
        "enable_channel_push": True,
        "enable_group_push": True,
        "enable_admin_push": True,
    }

    # 保活配置
    KEEPALIVE_ENABLED = os.getenv("KEEPALIVE_ENABLED", "true").lower() == "true"
    KEEPALIVE_INTERVAL = int(os.getenv("KEEPALIVE_INTERVAL", "600"))
    EXTERNAL_MONITORING_URLS = os.getenv("EXTERNAL_MONITORING_URLS", "").split(",")

    # 消息模板
    MESSAGES = {
        "welcome": "👋 欢迎使用打卡机器人！\n\n请使用下方按钮开始打卡活动。",
        "no_permission": "❌ 您没有管理员权限！",
        "has_activity": "❌ 您当前正在进行活动：{}，请先回座！",
        "max_times_reached": "❌ 您今日的{}次数已达到上限（{}次）！",
        "no_activity": "❌ 您当前没有活动在进行！",
        "setchannel_usage": "❌ 用法：/setchannel <频道ID>",
        "setgroup_usage": "❌ 用法：/setgroup <群组ID>",
        "addactivity_usage": "❌ 用法：/addactivity <活动名> <次数> <分钟>",
        "set_usage": "❌ 用法：/set <用户ID> <活动> <分钟>",
        "reset_usage": "❌ 用法：/reset <用户ID>",
        "setresettime_usage": "❌ 用法：/setresettime <小时> <分钟>",
        "setfine_usage": "❌ 用法：/setfine <活动名> <时间段> <金额>",
        "setpush_usage": "❌ 用法：/setpush <channel|group|admin> <on|off>",
        "setfines_all_usage": "❌ 用法：/setfines_all <时间段1> <金额1> [<时间段2> <金额2> ...]\n例如：/setfines_all 30 5 60 10 120 20",
    }

    # 新增配置项
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    ENABLE_PERFORMANCE_MONITOR = (
        os.getenv("ENABLE_PERFORMANCE_MONITOR", "true").lower() == "true"
    )
    MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", "4096"))

    @classmethod
    def should_use_webhook(cls):
        """检查是否使用Webhook模式"""
        return cls.BOT_MODE.lower() == "webhook" and cls.WEBHOOK_URL

    @classmethod
    def validate_config(cls):
        """验证配置"""
        errors = []

        if not cls.TOKEN:
            errors.append("BOT_TOKEN 未设置")

        if not cls.DATABASE_URL:
            errors.append("DATABASE_URL 未设置")

        if cls.should_use_webhook() and not cls.WEBHOOK_URL:
            errors.append("Webhook模式已启用，但WEBHOOK_URL未设置")

        if not cls.ADMINS:
            errors.append("至少需要设置一个管理员ID")
        else:
            for admin_id in cls.ADMINS:
                if not isinstance(admin_id, int) or admin_id <= 0:
                    errors.append(f"无效的管理员ID: {admin_id}")

        if not (0 <= cls.DAILY_RESET_HOUR <= 23):
            errors.append("DAILY_RESET_HOUR 必须在 0-23 范围内")
        if not (0 <= cls.DAILY_RESET_MINUTE <= 59):
            errors.append("DAILY_RESET_MINUTE 必须在 0-59 范围内")

        # ===== 把换班配置验证放在这里 =====
        # 验证换班配置
        if cls.HANDOVER_NIGHT_HOURS <= 0 or cls.HANDOVER_NIGHT_HOURS > 24:
            errors.append(
                f"HANDOVER_NIGHT_HOURS 必须在 1-24 之间，当前值: {cls.HANDOVER_NIGHT_HOURS}"
            )

        if cls.HANDOVER_DAY_HOURS <= 0 or cls.HANDOVER_DAY_HOURS > 24:
            errors.append(
                f"HANDOVER_DAY_HOURS 必须在 1-24 之间，当前值: {cls.HANDOVER_DAY_HOURS}"
            )

        if cls.NORMAL_NIGHT_HOURS <= 0 or cls.NORMAL_NIGHT_HOURS > 24:
            errors.append(
                f"NORMAL_NIGHT_HOURS 必须在 1-24 之间，当前值: {cls.NORMAL_NIGHT_HOURS}"
            )

        if cls.NORMAL_DAY_HOURS <= 0 or cls.NORMAL_DAY_HOURS > 24:
            errors.append(
                f"NORMAL_DAY_HOURS 必须在 1-24 之间，当前值: {cls.NORMAL_DAY_HOURS}"
            )

        if (
            cls.HANDOVER_RESET_THRESHOLD_HOURS <= 0
            or cls.HANDOVER_RESET_THRESHOLD_HOURS > 24
        ):
            errors.append(
                f"HANDOVER_RESET_THRESHOLD_HOURS 必须在 1-24 之间，当前值: {cls.HANDOVER_RESET_THRESHOLD_HOURS}"
            )

        # 时间格式验证
        import re

        time_pattern = re.compile(r"^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$")
        if not time_pattern.match(cls.HANDOVER_NIGHT_START):
            errors.append(
                f"HANDOVER_NIGHT_START 格式错误: {cls.HANDOVER_NIGHT_START}，应为 HH:MM 格式"
            )

        if not time_pattern.match(cls.HANDOVER_DAY_START):
            errors.append(
                f"HANDOVER_DAY_START 格式错误: {cls.HANDOVER_DAY_START}，应为 HH:MM 格式"
            )
        # ===== 验证结束 =====

        # 最后检查是否有错误
        if errors:
            error_msg = "配置错误:\n• " + "\n• ".join(errors)
            logging.error(error_msg)
            raise ValueError(error_msg)


# 验证配置
try:
    Config.validate_config()
except ValueError as e:
    logging.error(f"配置验证失败: {e}")
    raise
