# config.py - 完整优化版本
"""
机器人配置文件
包含数据库配置、性能优化、运行模式、管理员设置等所有配置项
"""

import os
from datetime import timedelta, timezone
from typing import Dict, Any, List

# 时区配置 - 使用北京时间 (UTC+8)
beijing_tz = timezone(timedelta(hours=8))


class Config:
    """主配置类，包含机器人的所有配置参数"""

    # === Bot 基础配置 ===
    # Telegram Bot Token，从环境变量获取，默认使用测试token
    TOKEN = os.getenv("BOT_TOKEN", "")

    # === 数据库配置 ===
    # 数据库连接URL，支持PostgreSQL和SQLite
    DATABASE_URL = os.getenv(
        "DATABASE_URL", ""
    )

    # === 性能优化配置 ===
    PERFORMANCE_CONFIG = {
        "ENABLE_QUERY_CACHE": True,  # 启用查询缓存
        "CACHE_TTL": 60,  # 缓存生存时间(秒)
        "MAX_RETRY_ATTEMPTS": 3,  # 最大重试次数
        "RETRY_BACKOFF_BASE": 1.0,  # 重试退避基数
        "MEMORY_CLEANUP_THRESHOLD_MB": 250,  # 内存清理阈值(MB)
        "BATCH_PROCESSING_SIZE": 50,  # 批处理大小
        "MAX_CONCURRENT_DB_QUERIES": 20,  # 最大并发数据库查询数
        "ENABLE_COMPRESSION": True,  # 启用数据压缩
        "GC_COLLECTION_THRESHOLD": (700, 10, 10),  # GC回收阈值
    }

    # === 运行模式配置 ===
    BOT_MODE = os.getenv("BOT_MODE", "auto")  # 运行模式: auto, webhook, polling
    WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")  # Webhook完整URL

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

    # === 数据库连接池高级配置 ===
    DB_MIN_CONNECTIONS = int(os.getenv("DB_MIN_CONNECTIONS", "2"))  # 最小连接数
    DB_MAX_CONNECTIONS = int(os.getenv("DB_MAX_CONNECTIONS", "20"))  # 最大连接数
    DB_CONNECTION_TIMEOUT = int(
        os.getenv("DB_CONNECTION_TIMEOUT", "30")
    )  # 连接超时时间(秒)
    DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))  # 连接池回收时间(秒)

    # === 数据库健康检查配置 ===
    DB_HEALTH_CHECK_ENABLED = (
        os.getenv("DB_HEALTH_CHECK_ENABLED", "true").lower() == "true"  # 启用健康检查
    )
    DB_HEARTBEAT_INTERVAL = int(
        os.getenv("DB_HEARTBEAT_INTERVAL", "300")
    )  # 心跳间隔(秒)
    DB_CONNECTION_MAX_AGE = int(
        os.getenv("DB_CONNECTION_MAX_AGE", "3600")
    )  # 连接最大年龄(秒)

    # === 异步数据库连接池配置 ===
    DATABASE_POOL_SETTINGS = {
        "min_size": DB_MIN_CONNECTIONS,  # 最小连接数
        "max_size": DB_MAX_CONNECTIONS,  # 最大连接数
        "command_timeout": DB_CONNECTION_TIMEOUT,  # 命令超时时间
        "max_inactive_connection_lifetime": DB_POOL_RECYCLE,  # 非活跃连接生存时间
        "health_check_enabled": DB_HEALTH_CHECK_ENABLED,  # 启用健康检查
        "heartbeat_interval": DB_HEARTBEAT_INTERVAL,  # 心跳间隔
        "connection_max_age": DB_CONNECTION_MAX_AGE,  # 连接最大年龄
    }

    # === 文件配置 ===
    BACKUP_DIR = "backups"  # 备份文件目录
    os.makedirs(BACKUP_DIR, exist_ok=True)  # 确保备份目录存在

    # === 管理员配置 ===
    ADMIN_IDS = os.getenv("ADMIN_IDS", "8356418002,6607669683")  # 管理员ID列表
    ADMINS = [int(x.strip()) for x in ADMIN_IDS.split(",") if x.strip()]  # 解析管理员ID

    # === 性能配置优化 ===
    SAVE_DELAY = 3.0  # 保存延迟时间(秒)
    MAX_CONCURRENT_LOCKS = 5000  # 最大并发锁数量
    MAX_MEMORY_USERS = 10000  # 内存中最大用户数
    CLEANUP_INTERVAL = 3600  # 清理间隔(秒)

    # === 数据保留配置 ===
    DATA_RETENTION_DAYS = 35  # 数据保留天数
    MONTHLY_BACKUP_DAYS = 365  # 月度备份保留天数

    # === 默认配置 ===
    DEFAULT_WORK_HOURS = {"work_start": "09:00", "work_end": "18:00"}  # 默认工作时间

    # === 心跳机制配置 ===
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

    # === Web 服务器配置 ===
    WEB_SERVER_CONFIG = {
        "HOST": "0.0.0.0",  # 服务器主机
        "PORT": int(os.environ.get("PORT", 8080)),  # 服务器端口
        "ENABLED": True,  # 启用Web服务器
    }

    # === 默认活动限制配置 ===
    DEFAULT_ACTIVITY_LIMITS = {
        "吃饭": {"max_times": 2, "time_limit": 30},  # 吃饭：最多2次，每次30分钟
        "小厕": {"max_times": 5, "time_limit": 5},  # 小厕：最多5次，每次5分钟
        "大厕": {"max_times": 2, "time_limit": 15},  # 大厕：最多2次，每次15分钟
        "抽烟": {"max_times": 5, "time_limit": 10},  # 抽烟：最多5次，每次10分钟
    }

    # === 默认罚款费率配置 ===
    DEFAULT_FINE_RATES = {
        "吃饭": {"10": 100, "30": 300},  # 吃饭：10分钟100，30分钟300
        "小厕": {"5": 50, "10": 100},  # 小厕：5分钟50，10分钟100
        "大厕": {"15": 80, "30": 200},  # 大厕：15分钟80，30分钟200
        "抽烟": {"10": 200, "30": 500},  # 抽烟：10分钟200，30分钟500
    }

    # === 默认工作罚款费率配置 ===
    DEFAULT_WORK_FINE_RATES = {
        "work_start": {
            "60": 50,
            "120": 100,
            "180": 200,
            "240": 300,
            "max": 500,
        },  # 上班迟到罚款
        "work_end": {
            "60": 50,
            "120": 100,
            "180": 200,
            "240": 300,
            "max": 500,
        },  # 下班早退罚款
    }

    # === 自动导出设置 ===
    AUTO_EXPORT_SETTINGS = {
        "enable_channel_push": True,  # 启用频道推送
        "enable_group_push": True,  # 启用群组推送
        "enable_admin_push": True,  # 启用管理员推送
        "monthly_auto_export": True,  # 启用月度自动导出
    }

    # === 时间配置 ===
    DAILY_RESET_HOUR = 0  # 每日重置小时
    DAILY_RESET_MINUTE = 0  # 每日重置分钟

    # === 异步任务配置 ===
    ASYNC_TASK_CONFIG = {
        "max_concurrent_tasks": 100,  # 最大并发任务数
        "task_timeout": 300,  # 任务超时时间(秒)
        "retry_attempts": 3,  # 重试次数
        "retry_delay": 5,  # 重试延迟(秒)
    }

    # === 内存管理配置 ===
    MEMORY_MANAGEMENT = {
        "max_memory_mb": 400,  # 最大内存使用(MB)
        "gc_threshold": (700, 10, 10),  # GC阈值
        "cleanup_batch_size": 100,  # 清理批处理大小
    }

    # === 日志配置 ===
    LOGGING_CONFIG = {
        "level": "INFO",  # 日志级别
        "format": "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",  # 日志格式
        "max_file_size_mb": 10,  # 最大日志文件大小(MB)
        "backup_count": 5,  # 备份文件数量
    }

    # === 导出配置 ===
    EXPORT_CONFIG = {
        "batch_size": 50,  # 批处理大小
        "max_file_size_mb": 20,  # 最大文件大小(MB)
        "temp_file_cleanup_delay": 300,  # 临时文件清理延迟(秒)
    }

    # === 消息模板配置 ===
    MESSAGES = {
        "welcome": "欢迎使用群打卡机器人！请点击下方按钮或直接输入活动名称打卡：",  # 欢迎消息
        "no_activity": "❌ 没有找到正在进行的活动，请先打卡活动再回座。",  # 无活动错误
        "has_activity": "❌ 您当前有活动【{}】正在进行中，请先回座后才能开始新活动！",  # 有活动错误
        "no_permission": "❌ 你没有权限执行此操作",  # 无权限错误
        "max_times_reached": "❌ 您今日的{}次数已达到上限（{}次），无法再次打卡",  # 次数上限错误
        "setchannel_usage": "❌ 用法：/setchannel <频道ID>\n频道ID格式如 -1001234567890",  # 设置频道用法
        "setgroup_usage": "❌ 用法：/setgroup <群组ID>\n用于接收超时通知的群组ID",  # 设置群组用法
        "set_usage": "❌ 用法：/set <用户ID> <活动> <时长分钟>",  # 设置用法
        "reset_usage": "❌ 用法：/reset <用户ID>",  # 重置用法
        "addactivity_usage": "❌ 用法：/addactivity <活动名> <max次数> <time_limit分钟>",  # 添加活动用法
        "setresettime_usage": "❌ 用法：/setresettime <小时> <分钟>\n例如：/setresettime 0 0 表示每天0点重置",  # 设置重置时间用法
        "setfine_usage": "❌ 用法：/setfine <活动名> <时间段> <金额>\n例如：/setfine 抽烟 10 200",  # 设置罚款用法
        "setfines_all_usage": "❌ 用法：/setfines_all <t1> <f1> [<t2> <f2> ...]\n例如：/setfines_all 10 100 30 300 60 1000",  # 设置所有罚款用法
        "setpush_usage": "❌ 用法：/setpush <channel|group|admin> <on|off>",  # 设置推送用法
        "setworkfine_usage": "❌ 用法：/setworkfine <work_start|work_end> <时间段> <金额>",  # 设置工作罚款用法
        "async_processing": "⏳ 正在处理中，请稍候...",  # 异步处理中消息
        "async_timeout": "⏰ 处理超时，请稍后重试",  # 异步超时消息
        "async_error": "❌ 处理过程中出现错误，请稍后重试",  # 异步错误消息
        "db_connection_error": "❌ 数据库连接失败，请检查配置",  # 数据库连接错误
        "db_query_timeout": "⏰ 数据库查询超时，请稍后重试",  # 数据库查询超时
        "export_started": "📤 开始导出数据...",  # 导出开始消息
        "export_completed": "✅ 数据导出完成",  # 导出完成消息
        "export_failed": "❌ 数据导出失败",  # 导出失败消息
        "export_no_data": "⚠️ 没有数据需要导出",  # 无数据导出消息
        "monthly_report_generating": "📊 正在生成月度报告...",  # 月度报告生成中
        "monthly_report_completed": "✅ 月度报告生成完成",  # 月度报告完成
        "monthly_report_no_data": "⚠️ 该月份没有数据需要报告",  # 月度报告无数据
        "maintenance_cleanup": "🧹 正在清理系统数据...",  # 维护清理中
        "maintenance_completed": "✅ 系统维护完成",  # 维护完成
    }

    # === 错误代码配置 ===
    ERROR_CODES = {
        "DB_CONNECTION_FAILED": 1001,  # 数据库连接失败
        "DB_QUERY_TIMEOUT": 1002,  # 数据库查询超时
        "DB_TRANSACTION_FAILED": 1003,  # 数据库事务失败
        "ASYNC_TASK_TIMEOUT": 2001,  # 异步任务超时
        "ASYNC_TASK_CANCELLED": 2002,  # 异步任务取消
        "MEMORY_LIMIT_EXCEEDED": 3001,  # 内存限制超出
        "FILE_OPERATION_FAILED": 4001,  # 文件操作失败
        "NETWORK_ERROR": 5001,  # 网络错误
        "PERMISSION_DENIED": 6001,  # 权限拒绝
    }

    # === 功能开关配置 ===
    FEATURE_FLAGS = {
        "enable_async_processing": True,  # 启用异步处理
        "enable_memory_management": True,  # 启用内存管理
        "enable_auto_cleanup": True,  # 启用自动清理
        "enable_performance_monitoring": True,  # 启用性能监控
        "enable_error_tracking": True,  # 启用错误跟踪
        "enable_health_checks": True,  # 启用健康检查
    }

    # === 健康检查配置 ===
    HEALTH_CHECK_CONFIG = {
        "check_interval": 60,  # 检查间隔(秒)
        "timeout": 10,  # 超时时间(秒)
        "retry_count": 3,  # 重试次数
        "critical_memory_usage": 0.8,  # 关键内存使用率阈值
    }

    # === 性能监控配置 ===
    PERFORMANCE_MONITORING = {
        "enable_metrics": True,  # 启用指标收集
        "metrics_interval": 60,  # 指标收集间隔(秒)
        "slow_query_threshold": 5.0,  # 慢查询阈值(秒)
        "high_memory_threshold": 0.7,  # 高内存使用率阈值
    }

    # === 缺失的方法补充 ===
    @classmethod
    def get_environment(cls):
        """获取当前环境 - 之前缺失的方法"""
        return os.getenv("ENVIRONMENT", "development")

    @classmethod
    def should_use_polling(cls):
        """智能判断是否应该使用Polling模式 - 之前位置不正确的方法"""
        return not cls.should_use_webhook()


# === 数据库重试配置 ===
DATABASE_RETRY_CONFIG = {
    "MAX_RETRIES": 3,  # 最大重试次数
    "BASE_DELAY": 1.0,  # 基础延迟(秒)
    "MAX_DELAY": 10.0,  # 最大延迟(秒)
    "JITTER": 0.1,  # 抖动因子
}


# === 配置验证 ===
try:
    # 验证必要配置项
    if not Config.TOKEN:
        raise ValueError("BOT_TOKEN 未设置")
    if not Config.ADMINS:
        raise ValueError("ADMIN_IDS 未设置有效的管理员ID")

    # 验证数据库URL格式
    if Config.DATABASE_URL and Config.DATABASE_URL.startswith("postgresql"):
        required_parts = ["postgresql://", "@", "/"]
        for part in required_parts:
            if part not in Config.DATABASE_URL:
                raise ValueError(f"PostgreSQL 数据库URL格式不正确，缺少: {part}")
    elif Config.DATABASE_URL and Config.DATABASE_URL.startswith("sqlite:///"):
        db_path = Config.DATABASE_URL.replace("sqlite:///", "")
        if not db_path:
            raise ValueError("SQLite 数据库路径不能为空")

    # 验证数据库连接池配置
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


# === 环境工具类 ===
class EnvUtils:
    """环境相关的工具类"""

    @staticmethod
    def is_production():
        """判断是否是生产环境"""
        return os.getenv("ENVIRONMENT", "development").lower() == "production"

    @staticmethod
    def is_development():
        """判断是否是开发环境"""
        return os.getenv("ENVIRONMENT", "development").lower() == "development"

    @staticmethod
    def get_environment():
        """获取当前环境"""
        return os.getenv("ENVIRONMENT", "development")

    @staticmethod
    def should_enable_debug():
        """判断是否启用调试模式"""
        return os.getenv("DEBUG", "false").lower() == "true"

    @staticmethod
    def get_log_level():
        """获取日志级别"""
        if EnvUtils.should_enable_debug():
            return "DEBUG"
        return Config.LOGGING_CONFIG["level"]


# === 性能配置工具类 ===
class PerformanceConfig:
    """性能配置相关的工具类"""

    @staticmethod
    def get_database_pool_settings():
        """获取数据库连接池设置（根据环境调整）"""
        base_settings = Config.DATABASE_POOL_SETTINGS.copy()

        if EnvUtils.is_production():
            # 生产环境使用更大的连接池
            base_settings["min_size"] = max(5, Config.DB_MIN_CONNECTIONS)
            base_settings["max_size"] = max(30, Config.DB_MAX_CONNECTIONS)
        elif EnvUtils.is_development():
            # 开发环境使用较小的连接池
            base_settings["min_size"] = min(1, Config.DB_MIN_CONNECTIONS)
            base_settings["max_size"] = min(10, Config.DB_MAX_CONNECTIONS)

        return base_settings

    @staticmethod
    def get_async_task_config():
        """获取异步任务配置（根据环境调整）"""
        base_config = Config.ASYNC_TASK_CONFIG.copy()

        if EnvUtils.is_production():
            base_config["max_concurrent_tasks"] = 200  # 生产环境支持更多并发任务
        elif EnvUtils.is_development():
            base_config["max_concurrent_tasks"] = 50  # 开发环境限制并发任务数

        return base_config

    @staticmethod
    def get_memory_limits():
        """获取内存限制配置"""
        base_limits = Config.MEMORY_MANAGEMENT.copy()
        env_memory = os.getenv("MAX_MEMORY_MB")
        if env_memory:
            try:
                base_limits["max_memory_mb"] = int(env_memory)
            except ValueError:
                print(f"⚠️ 无效的内存限制配置: {env_memory}，使用默认值")
        return base_limits


# === 启动配置打印函数 ===
def print_startup_config():
    """打印启动配置信息"""
    print("🚀 机器人启动配置:")
    print(f"   环境: {EnvUtils.get_environment()}")
    print(f"   调试模式: {EnvUtils.should_enable_debug()}")
    print(f"   日志级别: {EnvUtils.get_log_level()}")
    print(
        f"   数据库类型: {'PostgreSQL' if Config.DATABASE_URL and Config.DATABASE_URL.startswith('postgresql') else 'SQLite'}"
    )
    print(f"   管理员数量: {len(Config.ADMINS)}")
    print(f"   活动数量: {len(Config.DEFAULT_ACTIVITY_LIMITS)}")


# === 主程序入口 ===
if __name__ == "__main__":
    # 直接运行脚本时打印配置信息
    print_startup_config()
else:
    # 作为模块导入时，如果不是在gunicorn或uwsgi中运行，也打印配置信息
    import sys

    if "gunicorn" not in sys.modules and "uwsgi" not in sys.modules:
        print_startup_config()
