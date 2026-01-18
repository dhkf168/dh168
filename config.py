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
    """配置文件"""

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
    BOT_MODE = os.getenv("BOT_MODE", "polling")  # polling 或 webhook
    WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
    WEBHOOK_PATH = "/webhook"

    # Web服务器配置
    WEB_SERVER_CONFIG = {"HOST": "0.0.0.0", "PORT": int(os.getenv("PORT", "8080"))}

    # 时间配置
    DAILY_RESET_HOUR = int(os.getenv("DAILY_RESET_HOUR", "0"))
    DAILY_RESET_MINUTE = int(os.getenv("DAILY_RESET_MINUTE", "0"))

    # 默认活动配置
    DEFAULT_ACTIVITY_LIMITS = {
        "小厕": {"max_times": 10, "time_limit": 5},
        "大厕": {"max_times": 5, "time_limit": 15},
        "吃饭": {"max_times": 3, "time_limit": 35},
        "抽烟": {"max_times": 5, "time_limit": 10},
    }

    # 默认罚款配置
    DEFAULT_FINE_RATES = {
        "小厕": {"30min": 100, "60min": 200, "120min": 500},
        "大厕": {"30min": 100, "60min": 200, "120min": 500},
        "吃饭": {"30min": 100, "60min": 200, "120min": 500},
        "抽烟": {"30min": 100, "60min": 200, "120min": 500},
    }

    # 默认上下班罚款配置
    DEFAULT_WORK_FINE_RATES = {
        "work_start": {"1": 10, "10": 20, "30": 50},
        "work_end": {"1": 10, "10": 20, "30": 50},
    }

    # 默认上下班时间
    DEFAULT_WORK_HOURS = {"work_start": "09:00", "work_end": "18:00"}

    # 默认活动时间限制（分钟）
    DEFAULT_ACTIVITY_LIMIT_MINUTES = 120

    # 日常数据保留天数
    DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "30"))
    # 月度数据配置
    MONTHLY_DATA_RETENTION_DAYS = int(os.getenv("MONTHLY_DATA_RETENTION_DAYS", "40"))

    # 清理间隔（秒）
    CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "600"))
    MEMORY_CLEANUP_INTERVAL = 300

    # 自动导出设置
    AUTO_EXPORT_SETTINGS = {
        "enable_channel_push": True,
        "enable_group_push": True,
        "enable_admin_push": True,
    }

    # 🆕 保活配置
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
        "setfines_all_usage": "❌ 用法：/setfines_all <t1> <f1> [t2 f2...]",
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

        # 验证管理员ID
        if not cls.ADMINS:
            errors.append("至少需要设置一个管理员ID")
        else:
            for admin_id in cls.ADMINS:
                if not isinstance(admin_id, int) or admin_id <= 0:
                    errors.append(f"无效的管理员ID: {admin_id}")

        # 验证重置时间
        if not (0 <= cls.DAILY_RESET_HOUR <= 23):
            errors.append("DAILY_RESET_HOUR 必须在 0-23 范围内")
        if not (0 <= cls.DAILY_RESET_MINUTE <= 59):
            errors.append("DAILY_RESET_MINUTE 必须在 0-59 范围内")

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