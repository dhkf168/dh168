# config.py - 完整优化版本
import os
from datetime import timedelta, timezone
from typing import Dict, Any, List

# 时区配置
beijing_tz = timezone(timedelta(hours=8))


class Config:
    # Bot 配置
    TOKEN = os.getenv("BOT_TOKEN", "")

    # 数据库配置
    DATABASE_URL = os.getenv("DATABASE_URL", "")

    # 性能优化配置
    PERFORMANCE_CONFIG = {
        "ENABLE_QUERY_CACHE": True,
        "CACHE_TTL": 60,
        "MAX_RETRY_ATTEMPTS": 3,
        "RETRY_BACKOFF_BASE": 1.0,
        "MEMORY_CLEANUP_THRESHOLD_MB": 250,
        "BATCH_PROCESSING_SIZE": 50,
        "MAX_CONCURRENT_DB_QUERIES": 20,
        "ENABLE_COMPRESSION": True,
        "GC_COLLECTION_THRESHOLD": (700, 10, 10),
    }

    # === 新增的运行模式配置 ===
    BOT_MODE = os.getenv("BOT_MODE", "auto")  # auto, webhook, polling
    WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")  # Webhook完整URL

    # 修改 should_use_webhook 方法
    @classmethod
    def should_use_webhook(cls):
        """判断是否应该使用Webhook模式 - 修复版本"""
        mode = cls.BOT_MODE.lower()

        if mode == "webhook":
            if not cls.WEBHOOK_URL:
                print("⚠️ 警告: Webhook模式已启用但WEBHOOK_URL未设置")
            return True
        elif mode == "polling":
            return False
        else:  # auto模式
            # 在Render等云平台默认使用Polling，除非明确配置Webhook
            if cls.is_development():
                return bool(cls.WEBHOOK_URL)
            else:
                # 生产环境：只有明确配置了WEBHOOK_URL才使用Webhook
                return bool(cls.WEBHOOK_URL) and cls.WEBHOOK_URL.strip()

    @classmethod
    def is_development(cls):
        """判断是否是开发环境"""
        return cls.get_environment() == "development"

    # 数据库连接池高级配置
    DB_MIN_CONNECTIONS = int(os.getenv("DB_MIN_CONNECTIONS", "2"))
    DB_MAX_CONNECTIONS = int(os.getenv("DB_MAX_CONNECTIONS", "20"))
    DB_CONNECTION_TIMEOUT = int(os.getenv("DB_CONNECTION_TIMEOUT", "30"))
    DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))

    # 数据库健康检查配置
    DB_HEALTH_CHECK_ENABLED = (
        os.getenv("DB_HEALTH_CHECK_ENABLED", "true").lower() == "true"
    )
    DB_HEARTBEAT_INTERVAL = int(os.getenv("DB_HEARTBEAT_INTERVAL", "300"))
    DB_CONNECTION_MAX_AGE = int(os.getenv("DB_CONNECTION_MAX_AGE", "3600"))

    # 异步数据库连接池配置
    DATABASE_POOL_SETTINGS = {
        "min_size": DB_MIN_CONNECTIONS,
        "max_size": DB_MAX_CONNECTIONS,
        "command_timeout": DB_CONNECTION_TIMEOUT,
        "max_inactive_connection_lifetime": DB_POOL_RECYCLE,
        "health_check_enabled": DB_HEALTH_CHECK_ENABLED,
        "heartbeat_interval": DB_HEARTBEAT_INTERVAL,
        "connection_max_age": DB_CONNECTION_MAX_AGE,
    }

    # 文件配置
    BACKUP_DIR = "backups"
    os.makedirs(BACKUP_DIR, exist_ok=True)

    # 管理员配置
    ADMIN_IDS = os.getenv("ADMIN_IDS", "8356418002,6607669683")
    ADMINS = [int(x.strip()) for x in ADMIN_IDS.split(",") if x.strip()]

    # 性能配置优化
    SAVE_DELAY = 3.0
    MAX_CONCURRENT_LOCKS = 5000
    MAX_MEMORY_USERS = 10000
    CLEANUP_INTERVAL = 3600

    # 数据保留配置
    DATA_RETENTION_DAYS = 35
    MONTHLY_BACKUP_DAYS = 365

    # 默认配置
    DEFAULT_WORK_HOURS = {"work_start": "09:00", "work_end": "18:00"}

    # 心跳机制配置
    HEARTBEAT_CONFIG = {
        "ENABLED": True,  # 启用心跳
        "INTERVAL": 10,  # 心跳间隔（分钟）
        "PING_URLS": [  # 要ping的URL列表
            "https://api.telegram.org",
            "https://www.google.com",
            "https://www.cloudflare.com",
        ],
        "SELF_PING_ENABLED": True,  # 自ping启用
        "SELF_PING_INTERVAL": 5,  # 自ping间隔（分钟）
    }

    # Web 服务器配置
    WEB_SERVER_CONFIG = {
        "HOST": "0.0.0.0",
        "PORT": int(os.environ.get("PORT", 8080)),
        "ENABLED": True,
    }

    DEFAULT_ACTIVITY_LIMITS = {
        "吃饭": {"max_times": 2, "time_limit": 30},
        "小厕": {"max_times": 5, "time_limit": 5},
        "大厕": {"max_times": 2, "time_limit": 15},
        "抽烟": {"max_times": 5, "time_limit": 10},
    }

    DEFAULT_FINE_RATES = {
        "吃饭": {"10": 100, "30": 300},
        "小厕": {"5": 50, "10": 100},
        "大厕": {"15": 80, "30": 200},
        "抽烟": {"10": 200, "30": 500},
    }

    DEFAULT_WORK_FINE_RATES = {
        "work_start": {"60": 50, "120": 100, "180": 200, "240": 300, "max": 500},
        "work_end": {"60": 50, "120": 100, "180": 200, "240": 300, "max": 500},
    }

    AUTO_EXPORT_SETTINGS = {
        "enable_channel_push": True,
        "enable_group_push": True,
        "enable_admin_push": True,
        "monthly_auto_export": True,
    }

    # 时间配置
    DAILY_RESET_HOUR = 0
    DAILY_RESET_MINUTE = 0

    # 异步任务配置
    ASYNC_TASK_CONFIG = {
        "max_concurrent_tasks": 100,
        "task_timeout": 300,
        "retry_attempts": 3,
        "retry_delay": 5,
    }

    # 内存管理配置
    MEMORY_MANAGEMENT = {
        "max_memory_mb": 400,
        "gc_threshold": (700, 10, 10),
        "cleanup_batch_size": 100,
    }

    # 日志配置
    LOGGING_CONFIG = {
        "level": "INFO",
        "format": "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
        "max_file_size_mb": 10,
        "backup_count": 5,
    }

    # 导出配置
    EXPORT_CONFIG = {
        "batch_size": 50,
        "max_file_size_mb": 20,
        "temp_file_cleanup_delay": 300,
    }

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
        "setpush_usage": "❌ 用法：/setpush <channel|group|admin> <on|off>",
        "setworkfine_usage": "❌ 用法：/setworkfine <work_start|work_end> <时间段> <金额>",
        "async_processing": "⏳ 正在处理中，请稍候...",
        "async_timeout": "⏰ 处理超时，请稍后重试",
        "async_error": "❌ 处理过程中出现错误，请稍后重试",
        "db_connection_error": "❌ 数据库连接失败，请检查配置",
        "db_query_timeout": "⏰ 数据库查询超时，请稍后重试",
        "export_started": "📤 开始导出数据...",
        "export_completed": "✅ 数据导出完成",
        "export_failed": "❌ 数据导出失败",
        "export_no_data": "⚠️ 没有数据需要导出",
        "monthly_report_generating": "📊 正在生成月度报告...",
        "monthly_report_completed": "✅ 月度报告生成完成",
        "monthly_report_no_data": "⚠️ 该月份没有数据需要报告",
        "maintenance_cleanup": "🧹 正在清理系统数据...",
        "maintenance_completed": "✅ 系统维护完成",
    }

    # 错误代码
    ERROR_CODES = {
        "DB_CONNECTION_FAILED": 1001,
        "DB_QUERY_TIMEOUT": 1002,
        "DB_TRANSACTION_FAILED": 1003,
        "ASYNC_TASK_TIMEOUT": 2001,
        "ASYNC_TASK_CANCELLED": 2002,
        "MEMORY_LIMIT_EXCEEDED": 3001,
        "FILE_OPERATION_FAILED": 4001,
        "NETWORK_ERROR": 5001,
        "PERMISSION_DENIED": 6001,
    }

    # 功能开关
    FEATURE_FLAGS = {
        "enable_async_processing": True,
        "enable_memory_management": True,
        "enable_auto_cleanup": True,
        "enable_performance_monitoring": True,
        "enable_error_tracking": True,
        "enable_health_checks": True,
    }

    # 健康检查配置
    HEALTH_CHECK_CONFIG = {
        "check_interval": 60,
        "timeout": 10,
        "retry_count": 3,
        "critical_memory_usage": 0.8,
    }

    # 性能监控配置
    PERFORMANCE_MONITORING = {
        "enable_metrics": True,
        "metrics_interval": 60,
        "slow_query_threshold": 5.0,
        "high_memory_threshold": 0.7,
    }


# 🆕 在类定义后添加时区参数 - 使用 Config.DATABASE_URL
if Config.DATABASE_URL and "timezone=Asia/Shanghai" not in Config.DATABASE_URL:
    if "?" in Config.DATABASE_URL:
        Config.DATABASE_URL += "&timezone=Asia/Shanghai"
    else:
        Config.DATABASE_URL += "?timezone=Asia/Shanghai"
    print(f"✅ 数据库URL已添加时区参数: {Config.DATABASE_URL}")


# 在Config类中添加
DATABASE_RETRY_CONFIG = {
    "MAX_RETRIES": 3,
    "BASE_DELAY": 1.0,
    "MAX_DELAY": 10.0,
    "JITTER": 0.1,
}


# 配置验证
try:
    if not Config.TOKEN:
        raise ValueError("BOT_TOKEN 未设置")
    if not Config.ADMINS:
        raise ValueError("ADMIN_IDS 未设置有效的管理员ID")

    if Config.DATABASE_URL and Config.DATABASE_URL.startswith("postgresql"):
        required_parts = ["postgresql://", "@", "/"]
        for part in required_parts:
            if part not in Config.DATABASE_URL:
                raise ValueError(f"PostgreSQL 数据库URL格式不正确，缺少: {part}")
    elif Config.DATABASE_URL and Config.DATABASE_URL.startswith("sqlite:///"):
        db_path = Config.DATABASE_URL.replace("sqlite:///", "")
        if not db_path:
            raise ValueError("SQLite 数据库路径不能为空")

    if Config.DB_MIN_CONNECTIONS < 1:
        raise ValueError("数据库连接池最小连接数必须大于0")
    if Config.DB_MAX_CONNECTIONS < Config.DB_MIN_CONNECTIONS:
        raise ValueError("数据库连接池最大连接数必须大于等于最小连接数")

    print("✅ 配置验证通过")

except ValueError as e:
    print(f"❌ 配置错误: {e}")
    exit(1)
except Exception as e:
    print(f"❌ 配置验证过程中出现未知错误: {e}")
    exit(1)


# 环境工具类
class EnvUtils:
    @staticmethod
    def is_production():
        return os.getenv("ENVIRONMENT", "development").lower() == "production"

    @staticmethod
    def is_development():
        return os.getenv("ENVIRONMENT", "development").lower() == "development"

    @staticmethod
    def get_environment():
        return os.getenv("ENVIRONMENT", "development")

    @staticmethod
    def should_enable_debug():
        return os.getenv("DEBUG", "false").lower() == "true"

    @staticmethod
    def get_log_level():
        if EnvUtils.should_enable_debug():
            return "DEBUG"
        return Config.LOGGING_CONFIG["level"]


# 性能配置工具
class PerformanceConfig:
    @staticmethod
    def get_database_pool_settings():
        base_settings = Config.DATABASE_POOL_SETTINGS.copy()

        if EnvUtils.is_production():
            base_settings["min_size"] = max(5, Config.DB_MIN_CONNECTIONS)
            base_settings["max_size"] = max(30, Config.DB_MAX_CONNECTIONS)
        elif EnvUtils.is_development():
            base_settings["min_size"] = min(1, Config.DB_MIN_CONNECTIONS)
            base_settings["max_size"] = min(10, Config.DB_MAX_CONNECTIONS)

        return base_settings

    @staticmethod
    def get_async_task_config():
        base_config = Config.ASYNC_TASK_CONFIG.copy()

        if EnvUtils.is_production():
            base_config["max_concurrent_tasks"] = 200
        elif EnvUtils.is_development():
            base_config["max_concurrent_tasks"] = 50

        return base_config

    @staticmethod
    def get_memory_limits():
        base_limits = Config.MEMORY_MANAGEMENT.copy()
        env_memory = os.getenv("MAX_MEMORY_MB")
        if env_memory:
            try:
                base_limits["max_memory_mb"] = int(env_memory)
            except ValueError:
                print(f"⚠️ 无效的内存限制配置: {env_memory}，使用默认值")
        return base_limits


# 启动配置打印
def print_startup_config():
    print("🚀 机器人启动配置:")
    print(f"   环境: {EnvUtils.get_environment()}")
    print(f"   调试模式: {EnvUtils.should_enable_debug()}")
    print(f"   日志级别: {EnvUtils.get_log_level()}")
    print(
        f"   数据库类型: {'PostgreSQL' if Config.DATABASE_URL and Config.DATABASE_URL.startswith('postgresql') else 'SQLite'}"
    )
    print(f"   管理员数量: {len(Config.ADMINS)}")
    print(f"   活动数量: {len(Config.DEFAULT_ACTIVITY_LIMITS)}")


@classmethod
def should_use_polling(cls):
    """智能判断是否应该使用Polling模式"""
    return not cls.should_use_webhook()


if __name__ == "__main__":
    print_startup_config()
else:
    import sys

    if "gunicorn" not in sys.modules and "uwsgi" not in sys.modules:
        print_startup_config()
