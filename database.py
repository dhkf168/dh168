# database.py - 纯 PostgreSQL 版本（最终完整版）
import logging
import asyncio
import time
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional
from config import Config, beijing_tz
import asyncpg
from asyncpg.pool import Pool
from datetime import date, datetime

logger = logging.getLogger("GroupCheckInBot")


class PostgreSQLDatabase:
    """纯 PostgreSQL 数据库管理器"""

    def __init__(self, database_url: str = None):
        self.database_url = database_url or Config.DATABASE_URL
        self.pool: Optional[Pool] = None
        self._initialized = False
        self._cache = {}
        self._cache_ttl = {}

    # ========== 时区相关方法 ==========
    def get_beijing_time(self):
        """获取北京时间"""
        return datetime.now(beijing_tz)

    def get_beijing_date(self):
        """获取北京日期"""
        return self.get_beijing_time().date()
    
    # ========== 统一业务日期 ==========
    async def get_business_date(self, chat_id: int) -> date:
        """
        获取当前的业务日期（考虑自定义重置时间）
        如果当前时间 < 重置时间，则算作前一天
        """
        now = self.get_beijing_time()

        # 尝试获取群组设置
        try:
            group_data = await self.get_group_cached(chat_id)
            if group_data:
                reset_hour = group_data.get('reset_hour', Config.DAILY_RESET_HOUR)
                reset_minute = group_data.get('reset_minute', Config.DAILY_RESET_MINUTE)
            else:
                reset_hour = Config.DAILY_RESET_HOUR
                reset_minute = Config.DAILY_RESET_MINUTE
        except Exception:
            # 兜底逻辑
            reset_hour = Config.DAILY_RESET_HOUR
            reset_minute = Config.DAILY_RESET_MINUTE

        # 构造当天的重置时间点
        reset_time_today = now.replace(
            hour=reset_hour,
            minute=reset_minute,
            second=0,
            microsecond=0
        )

        # 核心判断：如果还没过重置点，就归属到昨天
        if now < reset_time_today:
            return (now - timedelta(days=1)).date()
        else:
            return now.date()

    # ========== 初始化方法 ==========
    async def initialize(self):
        """带重试的数据库初始化"""
        if self._initialized:
            return

        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(
                    f"🔗 尝试连接 PostgreSQL 数据库 (尝试 {attempt + 1}/{max_retries})"
                )
                await self._initialize_impl()
                logger.info("✅ PostgreSQL 数据库初始化完成")

                # 🆕 设置时区应该在初始化成功后
                async with self.pool.acquire() as conn:
                    await conn.execute("SET timezone = 'Asia/Shanghai'")
                    logger.info("✅ 数据库会话时区已设置为 Asia/Shanghai")

                self._initialized = True
                return

            except Exception as e:
                logger.warning(f"⚠️ 数据库初始化第 {attempt + 1} 次失败: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"❌ 数据库初始化重试{max_retries}次后失败: {e}")
                    raise
                retry_delay = 2**attempt
                logger.info(f"⏳ {retry_delay}秒后重试数据库初始化...")
                await asyncio.sleep(retry_delay)

    async def _initialize_impl(self):
        """实际的数据库初始化实现"""
        try:
            # 创建连接池
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=Config.DB_MIN_CONNECTIONS,
                max_size=Config.DB_MAX_CONNECTIONS,
                max_inactive_connection_lifetime=Config.DB_POOL_RECYCLE,
                command_timeout=Config.DB_CONNECTION_TIMEOUT,
                statement_cache_size=0,
            )
            logger.info("✅ PostgreSQL 连接池创建成功")

            # 测试连接并获取数据库信息
            async with self.pool.acquire() as conn:
                db_version = await conn.fetchval("SELECT version()")
                db_name = await conn.fetchval("SELECT current_database()")
                active_connections = await conn.fetchval(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
                )

                logger.info("📊 数据库连接信息:")
                logger.info(f"   - 数据库: {db_name}")
                logger.info(f"   - 版本: {str(db_version).split(',')[0]}")
                logger.info(f"   - 当前连接数: {active_connections}")

            # 创建表和索引
            await self._create_tables()
            await self._create_indexes()
            await self._initialize_default_data()

        except Exception as e:
            logger.error(f"❌ PostgreSQL 连接失败: {e}")
            if "connection" in str(e).lower() or "authentication" in str(e).lower():
                logger.error("💡 请检查 DATABASE_URL 环境变量是否正确配置")
                logger.error("💡 请检查数据库服务是否正常运行")
                logger.error("💡 请检查网络连接和防火墙设置")
            raise

    async def _create_tables(self):
        """创建所有必要的表"""
        async with self.pool.acquire() as conn:
            tables = [
                """
                CREATE TABLE IF NOT EXISTS groups (
                    chat_id BIGINT PRIMARY KEY,
                    channel_id BIGINT,
                    notification_group_id BIGINT,
                    reset_hour INTEGER DEFAULT 0,
                    reset_minute INTEGER DEFAULT 0,
                    work_start_time TEXT DEFAULT '09:00',
                    work_end_time TEXT DEFAULT '18:00',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    nickname TEXT,
                    current_activity TEXT,
                    activity_start_time TEXT,
                    total_accumulated_time INTEGER DEFAULT 0,
                    total_activity_count INTEGER DEFAULT 0,
                    total_fines INTEGER DEFAULT 0,
                    overtime_count INTEGER DEFAULT 0,
                    total_overtime_time INTEGER DEFAULT 0,
                    last_updated DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS user_activities (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    activity_date DATE,
                    activity_name TEXT,
                    activity_count INTEGER DEFAULT 0,
                    accumulated_time INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, activity_date, activity_name)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS work_records (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    record_date DATE,
                    checkin_type TEXT,
                    checkin_time TEXT,
                    status TEXT,
                    time_diff_minutes REAL,
                    fine_amount INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, record_date, checkin_type)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS activity_configs (
                    activity_name TEXT PRIMARY KEY,
                    max_times INTEGER,
                    time_limit INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS fine_configs (
                    id SERIAL PRIMARY KEY,
                    activity_name TEXT,
                    time_segment TEXT,
                    fine_amount INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(activity_name, time_segment)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS work_fine_configs (
                    id SERIAL PRIMARY KEY,
                    checkin_type TEXT,
                    time_segment TEXT,
                    fine_amount INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(checkin_type, time_segment)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS push_settings (
                    setting_key TEXT PRIMARY KEY,
                    setting_value INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS monthly_statistics (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    statistic_date DATE,  -- 统计日期（年月）
                    activity_name TEXT,
                    activity_count INTEGER DEFAULT 0,
                    accumulated_time INTEGER DEFAULT 0,
                    work_days INTEGER DEFAULT 0,      -- 新增：工作天数
                    work_hours INTEGER DEFAULT 0,     -- 新增：工作时长（秒）
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, statistic_date, activity_name)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS daily_statistics(
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    statistic_date DATE,
                    activity_name TEXT,
                    activity_count INTEGER DEFAULT 0,
                    accumulated_time INTEGER DEFAULT 0,
                    is_soft_reset BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, statistic_date, activity_name, is_soft_reset)
                )
                """,
                
            ]

            for table_sql in tables:
                await conn.execute(table_sql)

            logger.info("✅ 数据库表创建完成")

    async def _create_indexes(self):
        """创建性能索引"""
        async with self.pool.acquire() as conn:
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_user_activities_main ON user_activities (chat_id, user_id, activity_date)",
                "CREATE INDEX IF NOT EXISTS idx_user_activities_activity ON user_activities (activity_name)",
                "CREATE INDEX IF NOT EXISTS idx_work_records_main ON work_records (chat_id, user_id, record_date)",
                "CREATE INDEX IF NOT EXISTS idx_users_main ON users (chat_id, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_users_updated ON users (last_updated)",
                "CREATE INDEX IF NOT EXISTS idx_user_activities_date ON user_activities (activity_date)",
                "CREATE INDEX IF NOT EXISTS idx_work_records_date ON work_records (record_date)",
                "CREATE INDEX IF NOT EXISTS idx_monthly_stats_main ON monthly_statistics (chat_id, user_id, statistic_date)",
                "CREATE INDEX IF NOT EXISTS idx_monthly_stats_activity ON monthly_statistics (activity_name)",
                "CREATE INDEX IF NOT EXISTS idx_monthly_stats_date ON monthly_statistics (statistic_date)",
                 "CREATE INDEX IF NOT EXISTS idx_daily_stats_main ON daily_statistics (chat_id, user_id, statistic_date)",
                 "CREATE INDEX IF NOT EXISTS idx_daily_stats_activity ON daily_statistics (activity_name)",
                 "CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_statistics (statistic_date)",
                 "CREATE INDEX IF NOT EXISTS idx_daily_stats_soft_reset ON daily_statistics (is_soft_reset)",
            ]

            for index_sql in indexes:
                try:
                    await conn.execute(index_sql)
                except Exception as e:
                    logger.warning(f"创建索引失败: {e}")

            logger.info("✅ 数据库索引创建完成")

    async def _initialize_default_data(self):
        """初始化默认数据"""
        async with self.pool.acquire() as conn:
            # 初始化活动配置
            for activity, limits in Config.DEFAULT_ACTIVITY_LIMITS.items():
                await conn.execute(
                    "INSERT INTO activity_configs (activity_name, max_times, time_limit) VALUES ($1, $2, $3) ON CONFLICT (activity_name) DO NOTHING",
                    activity,
                    limits["max_times"],
                    limits["time_limit"],
                )

            # 初始化罚款配置
            for activity, fines in Config.DEFAULT_FINE_RATES.items():
                for time_segment, amount in fines.items():
                    await conn.execute(
                        "INSERT INTO fine_configs (activity_name, time_segment, fine_amount) VALUES ($1, $2, $3) ON CONFLICT (activity_name, time_segment) DO NOTHING",
                        activity,
                        time_segment,
                        amount,
                    )

            # 初始化上下班罚款配置
            for checkin_type, fines in Config.DEFAULT_WORK_FINE_RATES.items():
                for time_segment, amount in fines.items():
                    await conn.execute(
                        "INSERT INTO work_fine_configs (checkin_type, time_segment, fine_amount) VALUES ($1, $2, $3) ON CONFLICT (checkin_type, time_segment) DO NOTHING",
                        checkin_type,
                        time_segment,
                        amount,
                    )

            # 初始化推送设置
            for key, value in Config.AUTO_EXPORT_SETTINGS.items():
                await conn.execute(
                    "INSERT INTO push_settings (setting_key, setting_value) VALUES ($1, $2) ON CONFLICT (setting_key) DO NOTHING",
                    key,
                    1 if value else 0,
                )

            logger.info("✅ 默认数据初始化完成")

    # ========== 数据库连接管理 ==========
    async def get_connection(self):
        """获取数据库连接"""
        if not self.pool:
            raise RuntimeError("数据库连接池尚未初始化")
        return await self.pool.acquire()

    async def release_connection(self, conn):
        """释放数据库连接"""
        await self.pool.release(conn)

    async def close(self):
        """安全关闭数据库连接池"""
        try:
            if self.pool:
                await self.pool.close()
                logger.info("✅ PostgreSQL 连接池已安全关闭")
        except Exception as e:
            logger.warning(f"⚠️ 关闭数据库连接时出现异常: {e}")

    # ========== 缓存管理 ==========
    def _get_cached(self, key: str):
        """获取缓存数据"""
        if key in self._cache_ttl and time.time() < self._cache_ttl[key]:
            return self._cache.get(key)
        else:
            # 清理过期缓存
            if key in self._cache:
                del self._cache[key]
            if key in self._cache_ttl:
                del self._cache_ttl[key]
            return None

    def _set_cached(self, key: str, value: Any, ttl: int = 60):
        """设置缓存数据"""
        self._cache[key] = value
        self._cache_ttl[key] = time.time() + ttl

    async def cleanup_cache(self):
        """清理缓存"""
        current_time = time.time()
        expired_keys = [
            key for key, expiry in self._cache_ttl.items() if current_time >= expiry
        ]
        for key in expired_keys:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)

        if expired_keys:
            logger.debug(f"清理了 {len(expired_keys)} 个过期缓存")

    # 🆕 新增：强制刷新活动配置缓存
    async def force_refresh_activity_cache(self):
        """强制刷新活动配置缓存"""
        # 清理活动相关的所有缓存
        cache_keys_to_remove = ["activity_limits", "push_settings", "fine_rates"]

        for key in cache_keys_to_remove:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)

        # 重新加载活动配置
        await self.get_activity_limits()
        await self.get_fine_rates()

        logger.info("🔄 活动配置缓存已强制刷新")

        # ========== 群组相关操作 ==========

    async def init_group(self, chat_id: int):
        """初始化群组"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO groups (chat_id) VALUES ($1) ON CONFLICT (chat_id) DO NOTHING",
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def get_group(self, chat_id: int) -> Optional[Dict]:
        """获取群组配置"""
        cache_key = f"group:{chat_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM groups WHERE chat_id = $1", chat_id
            )
            if row:
                result = dict(row)
                self._set_cached(cache_key, result, 300)
                return result
            return None

    async def update_group_channel(self, chat_id: int, channel_id: int):
        """更新群组频道ID"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET channel_id = $1, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $2",
                channel_id,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def update_group_notification(self, chat_id: int, group_id: int):
        """更新群组通知群组ID"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET notification_group_id = $1, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $2",
                group_id,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def update_group_reset_time(self, chat_id: int, hour: int, minute: int):
        """更新群组重置时间"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET reset_hour = $1, reset_minute = $2, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $3",
                hour,
                minute,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def update_group_work_time(
        self, chat_id: int, work_start: str, work_end: str
    ):
        """更新群组上下班时间"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET work_start_time = $1, work_end_time = $2, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $3",
                work_start,
                work_end,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def get_group_work_time(self, chat_id: int) -> Dict[str, str]:
        """获取群组上下班时间"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT work_start_time, work_end_time FROM groups WHERE chat_id = $1",
                chat_id,
            )
            if row and row["work_start_time"] and row["work_end_time"]:
                return {
                    "work_start": row["work_start_time"],
                    "work_end": row["work_end_time"],
                }
            return Config.DEFAULT_WORK_HOURS.copy()

    async def has_work_hours_enabled(self, chat_id: int) -> bool:
        """检查是否启用了上下班功能"""
        work_hours = await self.get_group_work_time(chat_id)
        return (
            work_hours["work_start"] != Config.DEFAULT_WORK_HOURS["work_start"]
            or work_hours["work_end"] != Config.DEFAULT_WORK_HOURS["work_end"]
        )

    # ========== 用户相关操作 ==========
    async def init_user(self, chat_id: int, user_id: int, nickname: str = None):
        """初始化用户"""
        today = await self.get_business_date(chat_id)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users (chat_id, user_id, nickname, last_updated) 
                VALUES ($1, $2, $3, $4) 
                ON CONFLICT (chat_id, user_id) 
                DO UPDATE SET 
                    nickname = COALESCE($3, users.nickname),
                    last_updated = $4,
                    updated_at = CURRENT_TIMESTAMP
                """,
                chat_id,
                user_id,
                nickname,
                today,
            )
            self._cache.pop(f"user:{chat_id}:{user_id}", None)

    async def cleanup_inactive_users(self, days: int = 30):
        """清理长期未活动用户及其记录（安全版）"""

        cutoff_date = (self.get_beijing_time() - timedelta(days=days)).date()

        async with self.pool.acquire() as conn:
            async with conn.transaction():

                # 找出要删除的用户列表（避免直接删）
                users_to_delete = await conn.fetch(
                    """
                    SELECT user_id 
                    FROM users
                    WHERE last_updated < $1
                    AND NOT EXISTS (
                        SELECT 1 FROM monthly_statistics 
                        WHERE monthly_statistics.chat_id = users.chat_id 
                        AND monthly_statistics.user_id = users.user_id
                    )
                    """,
                    cutoff_date,
                )

                user_ids = [u["user_id"] for u in users_to_delete]

                if not user_ids:
                    logger.info("🧹 无需清理用户")
                    return 0

                # 删除用户的日常记录
                await conn.execute(
                    "DELETE FROM user_activities WHERE user_id = ANY($1)",
                    user_ids,
                )

                # 删除上下班记录（如果你需要）
                await conn.execute(
                    "DELETE FROM work_records WHERE user_id = ANY($1)",
                    user_ids,
                )

                # 最后删除用户
                deleted_count = await conn.execute(
                    "DELETE FROM users WHERE user_id = ANY($1)",
                    user_ids,
                )

        logger.info(f"🧹 清理了 {deleted_count} 个长期未活动的用户以及他们的所有记录")
        return deleted_count

    async def get_user(self, chat_id: int, user_id: int) -> Optional[Dict]:
        """获取用户数据"""
        cache_key = f"user:{chat_id}:{user_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE chat_id = $1 AND user_id = $2",
                chat_id,
                user_id,
            )
            if row:
                result = dict(row)
                self._set_cached(cache_key, result, 30)
                return result
            return None

    async def get_user_cached(self, chat_id: int, user_id: int) -> Optional[Dict]:
        """带缓存的获取用户数据"""
        return await self.get_user(chat_id, user_id)

    async def get_group_cached(self, chat_id: int) -> Optional[Dict]:
        """带缓存的获取群组配置"""
        return await self.get_group(chat_id)

    async def update_user_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        start_time: str,
        nickname: str = None,
    ):
        """更新用户活动状态"""
        async with self.pool.acquire() as conn:
            if nickname:
                await conn.execute(
                    "UPDATE users SET current_activity = $1, activity_start_time = $2, nickname = $3, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $4 AND user_id = $5",
                    activity,
                    start_time,
                    nickname,
                    chat_id,
                    user_id,
                )
            else:
                await conn.execute(
                    "UPDATE users SET current_activity = $1, activity_start_time = $2, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $3 AND user_id = $4",
                    activity,
                    start_time,
                    chat_id,
                    user_id,
                )
            self._cache.pop(f"user:{chat_id}:{user_id}", None)



    # ========== 核心打卡活动写入 ==========

    async def complete_user_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        elapsed_time: int,
        fine_amount: int = 0,
        is_overtime: bool = False,
    ):
        """完成用户活动 - 实现四表同步写入（修改现有方法）"""
        # 🧭 时间体系统一入口
        business_today = await self.get_business_date(chat_id)   # 业务日
        real_today = self.get_beijing_date()                     # 物理日
        statistic_date = real_today.replace(day=1)               # 月度统计归属自然月

        logger.info(
            f"🔍 [四表同步写入] 用户{user_id} 活动{activity} "
            f"时长{elapsed_time}s"
        )

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # ✅ 1️⃣. 写入 daily_statistics 表（硬重置前的数据，is_soft_reset=FALSE）
                await conn.execute(
                    """
                    INSERT INTO daily_statistics 
                    (chat_id, user_id, statistic_date, activity_name, activity_count, accumulated_time, is_soft_reset)
                    VALUES ($1, $2, $3, $4, 1, $5, FALSE)
                    ON CONFLICT (chat_id, user_id, statistic_date, activity_name, is_soft_reset) 
                    DO UPDATE SET 
                        activity_count = daily_statistics.activity_count + 1,
                        accumulated_time = daily_statistics.accumulated_time + EXCLUDED.accumulated_time,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    chat_id,
                    user_id,
                    business_today,
                    activity,
                    elapsed_time,
                )

                # ✅ 2️⃣. 写入 user_activities 表
                await conn.execute(
                    """
                    INSERT INTO user_activities 
                    (chat_id, user_id, activity_date, activity_name, activity_count, accumulated_time)
                    VALUES ($1, $2, $3, $4, 1, $5)
                    ON CONFLICT (chat_id, user_id, activity_date, activity_name) 
                    DO UPDATE SET 
                        activity_count = user_activities.activity_count + 1,
                        accumulated_time = user_activities.accumulated_time + EXCLUDED.accumulated_time,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    chat_id,
                    user_id,
                    business_today,
                    activity,
                    elapsed_time,
                )

                # ✅ 3️⃣. 写入 monthly_statistics 表
                await conn.execute(
                    """
                    INSERT INTO monthly_statistics 
                    (chat_id, user_id, statistic_date, activity_name, activity_count, accumulated_time)
                    VALUES ($1, $2, $3, $4, 1, $5)
                    ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                    DO UPDATE SET 
                        activity_count = monthly_statistics.activity_count + 1,
                        accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    chat_id,
                    user_id,
                    statistic_date,
                    activity,
                    elapsed_time,
                )

                # ✅ 4️⃣. 罚款记录到 daily_statistics 表
                if fine_amount > 0:
                    await conn.execute(
                        """
                        INSERT INTO daily_statistics 
                        (chat_id, user_id, statistic_date, activity_name, activity_count, accumulated_time, is_soft_reset)
                        VALUES ($1, $2, $3, 'total_fines', 1, $4, FALSE)
                        ON CONFLICT (chat_id, user_id, statistic_date, activity_name, is_soft_reset) 
                        DO UPDATE SET 
                            activity_count = daily_statistics.activity_count + 1,
                            accumulated_time = daily_statistics.accumulated_time + EXCLUDED.accumulated_time,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        chat_id,
                        user_id,
                        business_today,
                        fine_amount,
                    )

                # ✅ 5️⃣. 月度罚款统计
                if fine_amount > 0:
                    await conn.execute(
                        """
                        INSERT INTO monthly_statistics 
                        (chat_id, user_id, statistic_date, activity_name, accumulated_time)
                        VALUES ($1, $2, $3, 'total_fines', $4)
                        ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                        DO UPDATE SET 
                            accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        chat_id,
                        user_id,
                        statistic_date,
                        fine_amount,
                    )

                # ✅ 6️⃣. 超时记录到 daily_statistics 表
                overtime_seconds = 0
                if is_overtime:
                    time_limit = await self.get_activity_time_limit(activity)
                    overtime_seconds = max(0, elapsed_time - (time_limit * 60))

                    # 超时次数
                    await conn.execute(
                        """
                        INSERT INTO daily_statistics 
                        (chat_id, user_id, statistic_date, activity_name, activity_count, is_soft_reset)
                        VALUES ($1, $2, $3, 'overtime_count', 1, FALSE)
                        ON CONFLICT (chat_id, user_id, statistic_date, activity_name, is_soft_reset) 
                        DO UPDATE SET 
                            activity_count = daily_statistics.activity_count + 1,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        chat_id,
                        user_id,
                        business_today,
                    )

                    # 超时时长
                    await conn.execute(
                        """
                        INSERT INTO daily_statistics 
                        (chat_id, user_id, statistic_date, activity_name, accumulated_time, is_soft_reset)
                        VALUES ($1, $2, $3, 'overtime_time', $4, FALSE)
                        ON CONFLICT (chat_id, user_id, statistic_date, activity_name, is_soft_reset) 
                        DO UPDATE SET 
                            accumulated_time = daily_statistics.accumulated_time + EXCLUDED.accumulated_time,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        chat_id,
                        user_id,
                        business_today,
                        overtime_seconds,
                    )

                # ✅ 7️⃣. 更新 users 表
                update_fields = [
                    "total_accumulated_time = total_accumulated_time + $1",
                    "total_activity_count = total_activity_count + 1",
                    "current_activity = NULL",
                    "activity_start_time = NULL",
                    "last_updated = $2",
                ]
                params = [elapsed_time, real_today]

                if fine_amount > 0:
                    update_fields.append("total_fines = total_fines + $3")
                    params.append(fine_amount)

                if is_overtime:
                    update_fields.append("overtime_count = overtime_count + 1")
                    update_fields.append("total_overtime_time = total_overtime_time + $4")
                    params.append(overtime_seconds)

                update_fields.append("updated_at = CURRENT_TIMESTAMP")
                params.extend([chat_id, user_id])

                query = f"UPDATE users SET {', '.join(update_fields)} WHERE chat_id = ${len(params)-1} AND user_id = ${len(params)}"
                await conn.execute(query, *params)

        # 清理缓存
        self._cache.pop(f"user:{chat_id}:{user_id}", None)

        logger.info(f"✅ [四表同步写入完成] 用户{user_id} 活动{activity}")



    # ========== 每日用户数据重置(硬重置) ==========
    async def reset_user_daily_data(
        self,
        chat_id: int,
        user_id: int | None = None,
        target_date: date | None = None
    ):
        """
        🧬 硬重置用户数据 - 完整融合优化版
        支持：
        - 单用户硬重置（传入 user_id）
        - 群组硬重置（user_id=None，针对整个群组）

        功能：
        1. 自动结算当前跨天活动并持久化至月度统计（仅单用户）
        2. 物理删除 daily_statistics, user_activities, work_records 三表记录
        3. 重置 users 表所有状态字段（含 checkin_message_id）
        4. 智能日期校验与多级缓存清理
        """
        try:
            # ─────────────── ① 基础准备与日期校验 ───────────────
            current_biz_date = await self.get_business_date(chat_id)
            if target_date is None:
                target_date = current_biz_date
            elif not isinstance(target_date, date):
                raise ValueError(f"target_date必须是date类型，得到: {type(target_date)}")

            new_last_updated = max(target_date, current_biz_date)

            async with self.pool.acquire() as conn:
                async with conn.transaction():

                    if user_id:  # 单用户重置
                        # 获取重置前状态
                        user_before = await self.get_user(chat_id, user_id)
                        activities_before = await self.get_user_all_activities(chat_id, user_id)
                        cross_day = {"activity": None, "duration": 0, "fine": 0}

                        # ─────────────── 跨天活动结算 ───────────────
                        if user_before and user_before.get("current_activity"):
                            act = user_before["current_activity"]
                            start_str = user_before.get("activity_start_time")
                            if start_str:
                                try:
                                    start_time = datetime.fromisoformat(start_str)
                                    now = self.get_beijing_time()
                                    elapsed = int((now - start_time).total_seconds())
                                    limit_min = await self.get_activity_time_limit(act)
                                    limit_sec = limit_min * 60
                                    overtime_sec = max(0, elapsed - limit_sec)
                                    fine = 0
                                    if overtime_sec > 0:
                                        rates = await self.get_fine_rates_for_activity(act)
                                        if rates:
                                            segments = sorted(
                                                [int(str(k).lower().replace("min",""))
                                                 for k in rates if str(k).replace("min","").isdigit()]
                                            )
                                            over_min = overtime_sec / 60
                                            for s in segments:
                                                if over_min <= s:
                                                    fine = rates.get(str(s), rates.get(f"{s}min",0))
                                                    break
                                            if fine == 0 and segments:
                                                m = segments[-1]
                                                fine = rates.get(str(m), rates.get(f"{m}min",0))
                                    # 写入月度统计
                                    activity_month = start_time.date().replace(day=1)
                                    await conn.execute("""
                                        INSERT INTO monthly_statistics
                                        (chat_id, user_id, statistic_date, activity_name, activity_count, accumulated_time)
                                        VALUES ($1,$2,$3,$4,1,$5)
                                        ON CONFLICT (chat_id, user_id, statistic_date, activity_name)
                                        DO UPDATE SET
                                            activity_count = monthly_statistics.activity_count + 1,
                                            accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                                            updated_at = CURRENT_TIMESTAMP
                                    """, chat_id, user_id, activity_month, act, elapsed)
                                    # 累加罚款/超时
                                    if fine > 0 or overtime_sec > 0:
                                        await conn.execute("""
                                            UPDATE users SET
                                                total_fines = total_fines + $1,
                                                overtime_count = overtime_count + CASE WHEN $2 > 0 THEN 1 ELSE 0 END,
                                                total_overtime_time = total_overtime_time + $2
                                            WHERE chat_id=$3 AND user_id=$4
                                        """, fine, overtime_sec, chat_id, user_id)
                                    cross_day.update({"activity": act, "duration": elapsed, "fine": fine})
                                except Exception as e:
                                    logger.error(f"❌ 跨天结算失败: {e}")

                        # ─────────────── 删除三表记录 ───────────────
                        daily_stats_res = await conn.execute(
                            "DELETE FROM daily_statistics WHERE chat_id=$1 AND user_id=$2 AND statistic_date=$3",
                            chat_id, user_id, target_date
                        )
                        activities_res = await conn.execute(
                            "DELETE FROM user_activities WHERE chat_id=$1 AND user_id=$2 AND activity_date=$3",
                            chat_id, user_id, target_date
                        )
                        work_res = await conn.execute(
                            "DELETE FROM work_records WHERE chat_id=$1 AND user_id=$2 AND record_date=$3",
                            chat_id, user_id, target_date
                        )

                        # ─────────────── 重置用户状态 ───────────────
                        users_res = await conn.execute("""
                            UPDATE users SET
                                total_activity_count=0,
                                total_accumulated_time=0,
                                total_fines=0,
                                total_overtime_time=0,
                                overtime_count=0,
                                current_activity=NULL,
                                activity_start_time=NULL,
                                checkin_message_id=NULL,
                                last_updated=$3,
                                updated_at=CURRENT_TIMESTAMP
                            WHERE chat_id=$1 AND user_id=$2
                            AND (
                                total_activity_count>0 OR total_accumulated_time>0 OR
                                total_fines>0 OR current_activity IS NOT NULL OR
                                checkin_message_id IS NOT NULL
                            )
                        """, chat_id, user_id, new_last_updated)

                        # ─────────────── 清理缓存 ───────────────
                        for key in (f"user:{chat_id}:{user_id}", f"group:{chat_id}", "activity_limits"):
                            self._cache.pop(key, None)
                            self._cache_ttl.pop(key, None)

                        # ─────────────── 日志 ───────────────
                        def parse(res): return int(res.split()[-1]) if res and " " in res else 0
                        del_count = parse(daily_stats_res) + parse(activities_res) + parse(work_res)
                        upd_count = parse(users_res)

                        log = f"✅ [硬重置完成] 用户:{user_id} 群:{chat_id} 日期:{target_date}\n" \
                              f"🗑️ 物理删除: {del_count} 条 (流水/打卡/统计)\n" \
                              f"🔄 状态更新: {upd_count} 次\n"
                        if cross_day["activity"]:
                            log += f"🌙 跨天活动: {cross_day['activity']} ({self.format_seconds_to_hms(cross_day['duration'])}) 已存入月度统计\n"
                        logger.info(log)
                        return True

                    else:  # 群组重置
                        # 删除三表所有记录
                        await conn.execute(
                            "DELETE FROM daily_statistics WHERE chat_id=$1 AND statistic_date=$2",
                            chat_id, target_date
                        )
                        await conn.execute(
                            "DELETE FROM user_activities WHERE chat_id=$1 AND activity_date=$2",
                            chat_id, target_date
                        )
                        await conn.execute(
                            "DELETE FROM work_records WHERE chat_id=$1 AND record_date=$2",
                            chat_id, target_date
                        )

                        # 重置用户状态（含 checkin_message_id）
                        await conn.execute("""
                            UPDATE users SET
                                total_activity_count=0,
                                total_accumulated_time=0,
                                total_fines=0,
                                total_overtime_time=0,
                                overtime_count=0,
                                current_activity=NULL,
                                activity_start_time=NULL,
                                checkin_message_id=NULL,
                                last_updated=$2,
                                updated_at=CURRENT_TIMESTAMP
                            WHERE chat_id=$1
                        """, chat_id, new_last_updated)

                        # 清缓存
                        for key in (f"group:{chat_id}", "activity_limits"):
                            self._cache.pop(key, None)
                            self._cache_ttl.pop(key, None)

                        logger.info(f"✅ [硬重置完成] 群组:{chat_id} 日期:{target_date}")
                        return True

        except Exception as e:
            if user_id:
                logger.error(f"❌ 硬重置失败 {chat_id}-{user_id}: {e}")
            else:
                logger.error(f"❌ 群组硬重置失败 {chat_id}: {e}")
            return False

    # ========== 用户数据二次重置(软重置) ===========

    async def soft_reset_group(self, chat_id: int, mode: str = "soft"):
        """
        🧬 统一群组数据重置 (软重置/硬重置整合版)
        :param mode: "soft" (标记统计记录) 或 "hard" (物理删除统计记录)
        """
        try:
            today = await self.get_business_date(chat_id)
            
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # 1️⃣ 处理统计表 (daily_statistics)
                    if mode == "soft":
                        # 软重置：保留记录但打上标记，方便后续回溯
                        await conn.execute("""
                            UPDATE daily_statistics 
                            SET is_soft_reset = TRUE, updated_at = CURRENT_TIMESTAMP
                            WHERE chat_id = $1 AND statistic_date = $2
                        """, chat_id, today)
                    else:
                        # 硬重置：物理删除当日统计
                        await conn.execute("""
                            DELETE FROM daily_statistics 
                            WHERE chat_id = $1 AND statistic_date = $2
                        """, chat_id, today)

                    # 2️⃣ 物理清理当日流水 (活动记录与打卡记录)
                    # 无论软硬重置，流水必须清理，用户才能重新开始当日打卡
                    await conn.execute("""
                        DELETE FROM user_activities 
                        WHERE chat_id = $1 AND activity_date = $2
                    """, chat_id, today)
                    
                    await conn.execute("""
                        DELETE FROM work_records 
                        WHERE chat_id = $1 AND record_date = $2
                    """, chat_id, today)

                    # 3️⃣ 重置用户表实时状态 (针对该群所有用户)
                    await conn.execute("""
                        UPDATE users SET 
                            total_activity_count = 0,
                            total_accumulated_time = 0,
                            total_fines = 0,
                            total_overtime_time = 0,
                            overtime_count = 0,
                            current_activity = NULL,
                            activity_start_time = NULL,
                            checkin_message_id = NULL,
                            last_updated = $2,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE chat_id = $1
                    """, chat_id, today)

            # 4️⃣ 智能缓存清理 (包含该群所有用户及群设置缓存)
            cache_keys = list(self._cache.keys())
            for key in cache_keys:
                if f"user:{chat_id}:" in key or f"group:{chat_id}" in key:
                    self._cache.pop(key, None)
                    self._cache_ttl.pop(key, None)
            
            # 同时清理活动限制缓存，确保状态刷新
            self._cache.pop("activity_limits", None)
            self._cache_ttl.pop("activity_limits", None)

            logger.info(f"✅ 群组 {chat_id} [{mode}重置] 完成，业务日期: {today}")
            return True

        except Exception as e:
            logger.error(f"❌ 群组重置失败 {chat_id}: {e}")
            return False
        
    # ========== 清理 daily_statistics 方法 =========
    async def clear_daily_statistics(self, chat_id: int, date_obj: date = None):
        """
        清空 daily_statistics 表（导出后使用）
        """
        try:
            if date_obj is None:
                date_obj = await self.get_business_date(chat_id)
            
            async with self.pool.acquire() as conn:
                deleted = await conn.execute(
                    """
                    DELETE FROM daily_statistics 
                    WHERE chat_id = $1 AND statistic_date = $2
                    """,
                    chat_id,
                    date_obj,
                )
            
                logger.info(f"🗑️ 清空 daily_statistics: 群组{chat_id} 日期{date_obj} 删除{deleted}条")
                return True
            
        except Exception as e:
            logger.error(f"❌ 清空 daily_statistics 失败 {chat_id}: {e}")
            return False
        

    async def get_business_date(self, chat_id: int):
        """
        占位函数：获取群组业务日期
        你原始实现逻辑自行填充
        """
        pass


    async def update_user_last_updated(
        self, chat_id: int, user_id: int, date_obj: date
    ):
        """
        更新用户最后更新时间
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE users 
                    SET last_updated = $1, updated_at = CURRENT_TIMESTAMP 
                    WHERE chat_id = $2 AND user_id = $3
                    """,
                    date_obj,
                    chat_id,
                    user_id,
                )

            # 清理用户缓存
            self._cache.pop(f"user:{chat_id}:{user_id}", None)
            logger.debug(f"✅ 更新最后更新时间: {chat_id}-{user_id} -> {date_obj}")

        except Exception as e:
            logger.error(f"❌ 更新最后更新时间失败 {chat_id}-{user_id}: {e}")

    async def get_user_activity_count(
        self, chat_id: int, user_id: int, activity: str
    ) -> int:
        """获取用户今日活动次数"""
        today = await self.get_business_date(chat_id)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT activity_count FROM user_activities WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3 AND activity_name = $4",
                chat_id,
                user_id,
                today,
                activity,
            )
            count = row["activity_count"] if row else 0
            logger.debug(f"📊 获取活动计数: 用户{user_id} 活动{activity} 计数{count}")
            return count

    async def get_user_activity_time(
        self, chat_id: int, user_id: int, activity: str
    ) -> int:
        """获取用户今日活动累计时间"""
        today = await self.get_business_date(chat_id)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT accumulated_time FROM user_activities WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3 AND activity_name = $4",
                chat_id,
                user_id,
                today,
                activity,
            )
            return row["accumulated_time"] if row else 0

    # ========= 我的记录获取数据 =========
    async def get_user_all_activities(
        self, chat_id: int, user_id: int
    ) -> Dict[str, Dict]:
        """获取用户所有活动数据 - 从 user_activities 表获取"""
        today = await self.get_business_date(chat_id)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT activity_name, activity_count, accumulated_time FROM user_activities WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3",
                chat_id,
                user_id,
                today,
            )

            activities = {}
            for row in rows:
                activities[row["activity_name"]] = {
                    "count": row["activity_count"],
                    "time": row["accumulated_time"],
                    "time_formatted": self.format_seconds_to_hms(
                        row["accumulated_time"]
                    ),
                }
            return activities
        
    # ========= 排行榜获取数据 =========
    async def get_daily_rank_data(self, chat_id: int, activity: str) -> List[Dict]:
        """获取每日排行榜数据 - 从 daily_statistics 表获取"""
        today = await self.get_business_date(chat_id)
        
        async with self.pool.acquire() as conn:
            # 获取已完成活动的用户排名
            completed_rows = await conn.fetch(
                """
                SELECT 
                    ds.user_id,
                    u.nickname,
                    ds.accumulated_time as total_time,
                    ds.activity_count as total_count
                FROM daily_statistics ds
                LEFT JOIN users u ON ds.chat_id = u.chat_id AND ds.user_id = u.user_id
                WHERE ds.chat_id = $1 
                    AND ds.statistic_date = $2 
                    AND ds.activity_name = $3
                ORDER BY ds.accumulated_time DESC
                LIMIT 5
                """,
                chat_id,
                today,
                activity,
            )
            
            # 获取进行中的活动用户
            active_rows = await conn.fetch(
                """
                SELECT 
                    u.user_id,
                    u.nickname,
                    u.activity_start_time
                FROM users u
                WHERE u.chat_id = $1 
                    AND u.current_activity = $2
                """,
                chat_id,
                activity,
            )
            
            # 合并结果
            result = []
            
            # 添加已完成活动的用户
            for row in completed_rows:
                result.append({
                    "user_id": row["user_id"],
                    "nickname": row["nickname"],
                    "total_time": row["total_time"] or 0,
                    "total_count": row["total_count"] or 0,
                    "status": "completed"
                })
            
            # 添加进行中的用户
            for row in active_rows:
                result.append({
                    "user_id": row["user_id"],
                    "nickname": row["nickname"],
                    "total_time": 0,
                    "total_count": 0,
                    "status": "active",
                    "activity_start_time": row["activity_start_time"]
                })
            
            return result

    # 占位函数：获取群组业务日期
    async def get_business_date(self, chat_id: int):
        """
        占位函数：获取群组业务日期
        你原始实现逻辑自行填充
        """
        pass


    # ========== 上下班记录操作 ==========

    async def add_work_record(
        self,
        chat_id: int,
        user_id: int,
        record_date,
        checkin_type: str,
        checkin_time: str,
        status: str,
        time_diff_minutes: float,
        fine_amount: int = 0,
    ):
        """添加上下班记录 - 方案A最终版（完整功能 + 四表同步）"""

        # 统一 record_date 类型
        if isinstance(record_date, str):
            record_date = datetime.strptime(record_date, "%Y-%m-%d").date()
        elif isinstance(record_date, datetime):
            record_date = record_date.date()

        statistic_date = record_date.replace(day=1)  # 月度统计使用月初日期

        async with self.pool.acquire() as conn:
            async with conn.transaction():

                # 1️⃣ work_records 表（保持原有逻辑，完全不变）
                await conn.execute(
                    """
                    INSERT INTO work_records 
                    (chat_id, user_id, record_date, checkin_type, checkin_time, status, time_diff_minutes, fine_amount)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (chat_id, user_id, record_date, checkin_type) 
                    DO UPDATE SET 
                        checkin_time = EXCLUDED.checkin_time,
                        status = EXCLUDED.status,
                        time_diff_minutes = EXCLUDED.time_diff_minutes,
                        fine_amount = EXCLUDED.fine_amount,
                        created_at = CURRENT_TIMESTAMP
                    """,
                    chat_id,
                    user_id,
                    record_date,
                    checkin_type,
                    checkin_time,
                    status,
                    time_diff_minutes,
                    fine_amount,
                )

                # 2️⃣ daily_statistics：记录工作相关罚款（新增能力）
                if fine_amount > 0:
                    await conn.execute(
                        """
                        INSERT INTO daily_statistics 
                        (chat_id, user_id, statistic_date, activity_name, activity_count, accumulated_time, is_soft_reset)
                        VALUES ($1, $2, $3, 'work_fine', 1, $4, FALSE)
                        ON CONFLICT (chat_id, user_id, statistic_date, activity_name, is_soft_reset) 
                        DO UPDATE SET 
                            activity_count = daily_statistics.activity_count + 1,
                            accumulated_time = daily_statistics.accumulated_time + EXCLUDED.accumulated_time,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        chat_id,
                        user_id,
                        record_date,
                        fine_amount,
                    )

                # 3️⃣ 完整工作日判定 + 工作天数 & 工作时长统计
                if checkin_type == "work_end":

                    # 是否存在同一天的 work_start
                    has_work_start = await conn.fetchval(
                        """
                        SELECT 1 FROM work_records 
                        WHERE chat_id = $1 
                          AND user_id = $2 
                          AND record_date = $3 
                          AND checkin_type = 'work_start'
                        """,
                        chat_id,
                        user_id,
                        record_date,
                    )

                    if has_work_start:
                        # 是否已经统计过该工作日
                        existing = await conn.fetchval(
                            """
                            SELECT 1 FROM monthly_statistics 
                            WHERE chat_id = $1 
                              AND user_id = $2 
                              AND statistic_date = $3 
                              AND activity_name = 'work_days'
                            """,
                            chat_id,
                            user_id,
                            statistic_date,
                        )

                        if not existing:
                            # work_days +1
                            await conn.execute(
                                """
                                INSERT INTO monthly_statistics 
                                (chat_id, user_id, statistic_date, activity_name, work_days)
                                VALUES ($1, $2, $3, 'work_days', 1)
                                ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                                DO UPDATE SET 
                                    work_days = monthly_statistics.work_days + 1,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                chat_id,
                                user_id,
                                statistic_date,
                            )

                            logger.info(
                                f"✅ 工作天数统计: 用户{user_id} 日期{record_date} 完成完整工作日"
                            )

                        # 计算并更新工作时长（核心能力，保留）
                        await self._calculate_daily_work_hours(
                            conn, chat_id, user_id, record_date, statistic_date
                        )

                # 4️⃣ 罚款统计（users + monthly_statistics）
                if fine_amount > 0:
                    # users 表
                    await conn.execute(
                        """
                        UPDATE users 
                        SET total_fines = total_fines + $1 
                        WHERE chat_id = $2 AND user_id = $3
                        """,
                        fine_amount,
                        chat_id,
                        user_id,
                    )

                    # monthly_statistics 表
                    await conn.execute(
                        """
                        INSERT INTO monthly_statistics 
                        (chat_id, user_id, statistic_date, activity_name, accumulated_time)
                        VALUES ($1, $2, $3, 'total_fines', $4)
                        ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                        DO UPDATE SET 
                            accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        chat_id,
                        user_id,
                        statistic_date,
                        fine_amount,
                    )

                    logger.info(
                        f"💰 罚款统计: 用户{user_id} 金额{fine_amount} 类型{checkin_type}"
                    )

            # 5️⃣ 缓存清理（保持原有行为）
            self._cache.pop(f"user:{chat_id}:{user_id}", None)

    async def _calculate_daily_work_hours(self, conn, chat_id, user_id, record_date, statistic_date):
        """
        计算并更新每日工作时长的核心函数
        这里保留接口占位，内部逻辑根据你原始实现自行填充
        """
        pass


    async def get_user_work_records(
        self, chat_id: int, user_id: int, limit: int = 7
    ) -> List[Dict]:
        """获取用户上下班记录"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM work_records WHERE chat_id = $1 AND user_id = $2 ORDER BY record_date DESC, checkin_type LIMIT $3",
                chat_id,
                user_id,
                limit * 2,
            )

            result = []
            for row in rows:
                record = dict(row)
                if record["time_diff_minutes"]:
                    record["time_diff_formatted"] = self.format_minutes_to_hm(
                        record["time_diff_minutes"]
                    )
                else:
                    record["time_diff_formatted"] = "0小时0分钟"
                result.append(record)

            return result

    async def has_work_record_today(
        self, chat_id: int, user_id: int, checkin_type: str
    ) -> bool:
        """
        🆕 修复版：检查在当前工作周期内是否有指定类型的上下班记录
        考虑跨天情况，基于管理员设定的重置时间
        """
        now = self.get_beijing_time()

        # 获取群组重置时间设置
        group_data = await self.get_group_cached(chat_id)
        if not group_data:
            # 如果群组不存在，使用默认重置时间
            reset_hour = Config.DAILY_RESET_HOUR
            reset_minute = Config.DAILY_RESET_MINUTE
        else:
            reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
            reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

        # 计算当前重置周期开始时间（与 reset_daily_data_if_needed 逻辑一致）
        reset_time_today = now.replace(
            hour=reset_hour, minute=reset_minute, second=0, microsecond=0
        )

        if now < reset_time_today:
            # 当前时间还没到今天的重置点 → 当前周期起点是昨天的重置时间
            current_period_start = reset_time_today - timedelta(days=1)
        else:
            # 已经过了今天的重置点 → 当前周期起点为今天的重置时间
            current_period_start = reset_time_today

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM work_records WHERE chat_id = $1 AND user_id = $2 AND record_date >= $3 AND checkin_type = $4",
                chat_id,
                user_id,
                current_period_start.date(),  # 🆕 改为 >= 当前周期开始日期
                checkin_type,
            )
            return row is not None

    async def get_today_work_records(
        self, chat_id: int, user_id: int
    ) -> Dict[str, Dict]:
        """获取用户今天的上下班记录"""
        today = await self.get_business_date(chat_id)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM work_records WHERE chat_id = $1 AND user_id = $2 AND record_date = $3",
                chat_id,
                user_id,
                today,
            )

            records = {}
            for row in rows:
                record = dict(row)
                if record["time_diff_minutes"]:
                    record["time_diff_formatted"] = self.format_minutes_to_hm(
                        record["time_diff_minutes"]
                    )
                else:
                    record["time_diff_formatted"] = "0小时0分钟"
                records[row["checkin_type"]] = record
            return records

    # ========== 活动配置操作 ==========
    async def get_activity_limits(self) -> Dict:
        """获取所有活动限制"""
        cache_key = "activity_limits"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM activity_configs")

            limits = {
                row["activity_name"]: {
                    "max_times": row["max_times"],
                    "time_limit": row["time_limit"],
                }
                for row in rows
            }
            self._set_cached(cache_key, limits, 300)
            return limits

    # 🆕 在这里添加缺失的辅助方法
    async def _calculate_daily_work_hours(
        self, conn, chat_id: int, user_id: int, work_date: date, statistic_date: date
    ):
        """计算单日工作时长并更新月度统计"""
        try:
            # 获取当天的上下班记录
            records = await conn.fetch(
                """
                SELECT checkin_type, checkin_time 
                FROM work_records 
                WHERE chat_id = $1 AND user_id = $2 AND record_date = $3
                ORDER BY checkin_time
                """,
                chat_id,
                user_id,
                work_date,
            )

            work_seconds = 0
            work_start_time = None

            # 计算工作时长
            for record in records:
                if record["checkin_type"] == "work_start":
                    work_start_time = record["checkin_time"]
                elif record["checkin_type"] == "work_end" and work_start_time:
                    try:
                        # 解析时间字符串
                        start_dt = datetime.strptime(work_start_time, "%H:%M")
                        end_dt = datetime.strptime(record["checkin_time"], "%H:%M")

                        # 计算时间差（秒）
                        time_diff = end_dt - start_dt
                        if time_diff.total_seconds() > 0:
                            work_seconds += int(time_diff.total_seconds())

                        work_start_time = None  # 重置开始时间
                    except ValueError as e:
                        logger.warning(f"解析工作时间失败: {e}")
                        continue

            # 更新月度统计中的工作时长（大于0才更新）
            if work_seconds > 0:
                await conn.execute(
                    """
                    INSERT INTO monthly_statistics 
                    (chat_id, user_id, statistic_date, activity_name, accumulated_time)
                    VALUES ($1, $2, $3, 'work_hours', $4)
                    ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                    DO UPDATE SET 
                        accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    chat_id,
                    user_id,
                    statistic_date,
                    work_seconds,
                )
                logger.debug(
                    f"✅ 更新工作时长: 用户{user_id} 日期{work_date} 时长{work_seconds}秒"
                )

        except Exception as e:
            logger.error(f"❌ 计算工作时长失败 {chat_id}-{user_id}: {e}")

    # 🆕 如果需要，还可以添加其他辅助方法
    async def _safe_update_monthly_fines(
        self, conn, chat_id: int, user_id: int, statistic_date: date, fine_amount: int
    ):
        """安全更新月度罚款统计"""
        try:
            await conn.execute(
                """
                INSERT INTO monthly_statistics 
                (chat_id, user_id, statistic_date, activity_name, accumulated_time)
                VALUES ($1, $2, $3, 'total_fines', $4)
                ON CONFLICT (chat_id, user_id, statistic_date, activity_name) 
                DO UPDATE SET 
                    accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time
                """,
                chat_id,
                user_id,
                statistic_date,
                fine_amount,
            )
        except Exception as e:
            logger.error(f"❌ 更新月度罚款统计失败: {e}")

    async def get_activity_limits_cached(self) -> Dict:
        """带缓存的获取活动限制"""
        return await self.get_activity_limits()

    async def get_activity_time_limit(self, activity: str) -> int:
        """获取活动时间限制"""
        limits = await self.get_activity_limits()
        return limits.get(activity, {}).get("time_limit", 0)

    async def get_activity_max_times(self, activity: str) -> int:
        """获取活动最大次数"""
        limits = await self.get_activity_limits()
        return limits.get(activity, {}).get("max_times", 0)

    async def activity_exists(self, activity: str) -> bool:
        """检查活动是否存在 - 修复版本"""
        # 先检查缓存
        cache_key = "activity_limits"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return activity in cached

        # 如果缓存不存在，直接从数据库查询
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM activity_configs WHERE activity_name = $1", activity
            )
            return row is not None

    async def update_activity_config(
        self, activity: str, max_times: int, time_limit: int
    ):
        """更新活动配置 - 修复新增活动无法打卡问题"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # 更新或新增活动配置
                await conn.execute(
                    """
                    INSERT INTO activity_configs (activity_name, max_times, time_limit)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (activity_name) 
                    DO UPDATE SET 
                        max_times = EXCLUDED.max_times,
                        time_limit = EXCLUDED.time_limit,
                        created_at = CURRENT_TIMESTAMP
                    """,
                    activity,
                    max_times,
                    time_limit,
                )

                # ✅ 初始化默认罚款配置，避免新增活动无法打卡
                default_fines = getattr(Config, "DEFAULT_FINE_RATES", {}).get(
                    "default", {}
                )
                if not default_fines:
                    default_fines = {"30min": 5, "60min": 10, "120min": 20}

                # 批量插入罚款配置
                values = [(activity, ts, amt) for ts, amt in default_fines.items()]
                await conn.executemany(
                    """
                    INSERT INTO fine_configs (activity_name, time_segment, fine_amount)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (activity_name, time_segment) DO NOTHING
                    """,
                    values,
                )

            # 清理缓存
            self._cache.pop("activity_limits", None)
            logger.info(f"✅ 活动配置更新完成: {activity}，并初始化罚款配置")

    async def delete_activity_config(self, activity: str):
        """删除活动配置"""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM activity_configs WHERE activity_name = $1", activity
                )
                await conn.execute(
                    "DELETE FROM fine_configs WHERE activity_name = $1", activity
                )
        self._cache.pop("activity_limits", None)
        logger.info(f"🗑 已删除活动配置及罚款: {activity}")

    # ========== 罚款配置操作 ==========
    async def get_fine_rates(self) -> Dict:
        """获取所有罚款费率"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM fine_configs")
            fines = {}
            for row in rows:
                activity = row["activity_name"]
                if activity not in fines:
                    fines[activity] = {}
                fines[activity][row["time_segment"]] = row["fine_amount"]
            return fines

    async def get_fine_rates_for_activity(self, activity: str) -> Dict:
        """获取指定活动的罚款费率"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT time_segment, fine_amount FROM fine_configs WHERE activity_name = $1",
                activity,
            )
            return {row["time_segment"]: row["fine_amount"] for row in rows}

    async def update_fine_config(
        self, activity: str, time_segment: str, fine_amount: int
    ):
        """更新罚款配置"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO fine_configs (activity_name, time_segment, fine_amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (activity_name, time_segment) 
                DO UPDATE SET 
                    fine_amount = EXCLUDED.fine_amount,
                    created_at = CURRENT_TIMESTAMP
            """,
                activity,
                time_segment,
                fine_amount,
            )

    async def get_work_fine_rates(self) -> Dict:
        """获取上下班罚款费率"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM work_fine_configs")
            fines = {}
            for row in rows:
                checkin_type = row["checkin_type"]
                if checkin_type not in fines:
                    fines[checkin_type] = {}
                fines[checkin_type][row["time_segment"]] = row["fine_amount"]
            return fines

    async def get_work_fine_rates_for_type(self, checkin_type: str) -> Dict:
        """获取指定类型的上下班罚款费率"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT time_segment, fine_amount FROM work_fine_configs WHERE checkin_type = $1",
                checkin_type,
            )
            return {row["time_segment"]: row["fine_amount"] for row in rows}

    async def update_work_fine_rate(
        self, checkin_type: str, time_segment: str, fine_amount: int
    ):
        """插入或更新上下班罚款规则"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO work_fine_configs (checkin_type, time_segment, fine_amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (checkin_type, time_segment)
                DO UPDATE SET fine_amount = EXCLUDED.fine_amount
                """,
                checkin_type,
                time_segment,
                fine_amount,
            )
            logger.info(
                f"✅ 已更新罚款配置: 类型={checkin_type}, 阈值={time_segment}, 金额={fine_amount}"
            )

    async def update_work_fine_config(
        self, checkin_type: str, time_segment: str, fine_amount: int
    ):
        """更新上下班罚款配置"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO work_fine_configs (checkin_type, time_segment, fine_amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (checkin_type, time_segment) 
                DO UPDATE SET 
                    fine_amount = EXCLUDED.fine_amount,
                    created_at = CURRENT_TIMESTAMP
            """,
                checkin_type,
                time_segment,
                fine_amount,
            )

    async def clear_work_fine_rates(self, checkin_type: str):
        """清空指定类型的上下班罚款配置"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM work_fine_configs WHERE checkin_type = $1",
                checkin_type,
            )
            logger.info(f"🧹 已清空 {checkin_type} 的旧罚款配置")

    # ========== 推送设置操作 ==========
    async def get_push_settings(self) -> Dict:
        """获取推送设置"""
        cache_key = "push_settings"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM push_settings")
            settings = {row["setting_key"]: bool(row["setting_value"]) for row in rows}
            self._set_cached(cache_key, settings, 300)
            return settings

    async def update_push_setting(self, key: str, value: bool):
        """更新推送设置"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO push_settings (setting_key, setting_value)
                VALUES ($1, $2)
                ON CONFLICT (setting_key) 
                DO UPDATE SET 
                    setting_value = EXCLUDED.setting_value,
                    created_at = CURRENT_TIMESTAMP
            """,
                key,
                1 if value else 0,
            )
            self._cache.pop("push_settings", None)

    # ========== 统计和导出相关 ==========

    async def get_group_statistics(
        self, chat_id: int, target_date: Optional[date] = None
    ) -> List[Dict]:
        """
        📊 从 daily_statistics 表获取合并数据 (含软重置和硬重置数据)
        用于群组统计展示和数据导出。
        """
        # 1. 基础准备：获取业务日期
        if target_date is None:
            target_date = await self.get_business_date(chat_id)

        async with self.pool.acquire() as conn:
            # 2. 获取所有活动统计数据 (聚合 SUM)
            # 通过 SUM 自动合并了同一天内多次“软重置”产生的多条记录
            daily_stats = await conn.fetch(
                """
                SELECT 
                    ds.user_id,
                    u.nickname,
                    ds.activity_name,
                    SUM(ds.activity_count) as total_activity_count,
                    SUM(ds.accumulated_time) as total_accumulated_time
                FROM daily_statistics ds
                LEFT JOIN users u ON ds.chat_id = u.chat_id AND ds.user_id = u.user_id
                WHERE ds.chat_id = $1 AND ds.statistic_date = $2
                GROUP BY ds.user_id, u.nickname, ds.activity_name
                ORDER BY ds.user_id, ds.activity_name
                """,
                chat_id,
                target_date,
            )
            
            # 获取当前所有活动配置，用于后续补全“0次”活动的展示
            activity_limits = await self.get_activity_limits()
            
            # 3. 按用户分组处理基础活动数据
            user_stats = {}
            for row in daily_stats:
                user_id = row["user_id"]
                if user_id not in user_stats:
                    user_stats[user_id] = {
                        "user_id": user_id,
                        "nickname": row["nickname"] or f"用户{user_id}",
                        "activities": {},
                        "total_accumulated_time": 0,
                        "total_activity_count": 0,
                        "work_days": 0,      # 初始化工作天数
                        "work_hours": 0,     # 初始化工作时长
                        "total_fines": 0,    # 初始化罚款
                        "overtime_count": 0,  # 初始化超时次数
                        "total_overtime_time": 0 # 初始化超时时间
                    }
                
                activity_name = row["activity_name"]
                
                # 🆕 分支 A: 处理工作天数
                if activity_name == "work_days":
                    user_stats[user_id]["work_days"] = row["total_activity_count"] or 0
                
                # 🆕 分支 B: 处理工作时长
                elif activity_name == "work_hours":
                    user_stats[user_id]["work_hours"] = row["total_accumulated_time"] or 0
                
                # 🆕 分支 C: 处理罚款 (支持 total_fines 和 work_fine 两种 key)
                elif activity_name in ["total_fines", "work_fine"]:
                    user_stats[user_id]["total_fines"] += row["total_accumulated_time"] or 0
                
                # 🆕 分支 D: 处理超时次数
                elif activity_name == "overtime_count":
                    user_stats[user_id]["overtime_count"] = row["total_activity_count"] or 0
                
                # 🆕 分支 E: 处理超时时长
                elif activity_name == "overtime_time":
                    user_stats[user_id]["total_overtime_time"] = row["total_accumulated_time"] or 0
                
                # 🆕 分支 F: 处理普通活动 (排除掉上述所有特殊 Key)
                elif activity_name not in ["work_days", "work_hours", "total_fines", "work_fine", "overtime_count", "overtime_time"]:
                    if activity_name not in user_stats[user_id]["activities"]:
                        user_stats[user_id]["activities"][activity_name] = {
                            "count": 0,
                            "time": 0,
                        }
                    
                    count_val = row["total_activity_count"] or 0
                    time_val = row["total_accumulated_time"] or 0
                    
                    user_stats[user_id]["activities"][activity_name]["count"] += count_val
                    user_stats[user_id]["activities"][activity_name]["time"] += time_val
                    
                    # 只有普通打卡活动才累加进总计时长和总次数
                    user_stats[user_id]["total_accumulated_time"] += time_val
                    user_stats[user_id]["total_activity_count"] += count_val
            
            # 4. 确保所有配置的活动都在 activities 字典中 (补全 0 数据)
            for user_id, stats in user_stats.items():
                for act in activity_limits.keys():
                    if act not in stats["activities"]:
                        stats["activities"][act] = {
                            "count": 0,
                            "time": 0,
                            "time_formatted": "0秒"
                        }
                    else:
                        # 格式化已存在的活动时间
                        stats["activities"][act]["time_formatted"] = self.format_seconds_to_hms(
                            stats["activities"][act]["time"]
                        )
            
            # 5. 转换为列表并确保字段完整存在 (防错处理)
            result = []
            for user_id, user_data in user_stats.items():
                final_data = {
                    "user_id": user_id,
                    "nickname": user_data["nickname"],
                    "activities": user_data["activities"],
                    "total_accumulated_time": user_data.get("total_accumulated_time", 0),
                    "total_activity_count": user_data.get("total_activity_count", 0),
                    "total_fines": user_data.get("total_fines", 0),
                    "overtime_count": user_data.get("overtime_count", 0),
                    "total_overtime_time": user_data.get("total_overtime_time", 0),
                    "work_days": user_data.get("work_days", 0),
                    "work_hours": user_data.get("work_hours", 0),
                }
                
                # 注入总时长的格式化字符串
                final_data["total_accumulated_time_formatted"] = (
                    self.format_seconds_to_hms(final_data["total_accumulated_time"])
                )
                final_data["total_overtime_time_formatted"] = self.format_seconds_to_hms(
                    final_data["total_overtime_time"]
                )
                
                result.append(final_data)
            
            return result

    # 占位函数：格式化秒数为时分秒
    def format_seconds_to_hms(self, seconds: int) -> str:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    async def get_business_date(self, chat_id: int):
        """
        占位函数：获取群组业务日期
        你原始实现逻辑自行填充
        """
        pass


    async def get_all_groups(self, retries: int = 3, delay: float = 2.0) -> List[int]:
        """
        获取所有群组ID（带超时与自愈机制）
        """
        for attempt in range(1, retries + 1):
            try:
                async with self.pool.acquire() as conn:
                    # ✅ 增加超时保护（最多等待10秒）
                    rows = await asyncio.wait_for(
                        conn.fetch("SELECT chat_id FROM groups"), timeout=10
                    )
                    return [row["chat_id"] for row in rows]

            except (
                asyncpg.InterfaceError,
                asyncpg.PostgresConnectionError,
                asyncio.TimeoutError,
            ) as e:
                logger.warning(f"⚠️ 第 {attempt} 次获取群组失败: {e}")

                # ✅ 使用新的重连机制替换旧的连接池重置
                reconnect_success = await self.reconnect()

                if reconnect_success and attempt < retries:
                    sleep_time = delay * attempt  # 指数退避
                    logger.info(f"⏳ {sleep_time:.1f}s 后重试（第 {attempt} 次）...")
                    await asyncio.sleep(sleep_time)
                else:
                    logger.error("❌ 重试次数耗尽或重连失败，放弃操作。")
                    return []

            except Exception as e:
                logger.error(f"💥 未知错误（get_all_groups）：{e}")
                return []

    async def get_group_members(self, chat_id: int) -> List[Dict]:
        """获取群组成员"""
        today = await self.get_business_date(chat_id)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT user_id, nickname, current_activity, activity_start_time, total_accumulated_time, total_activity_count, total_fines, overtime_count, total_overtime_time FROM users WHERE chat_id = $1 AND last_updated = $2",
                chat_id,
                today,
            )

            result = []
            for row in rows:
                user_data = dict(row)
                user_data["total_accumulated_time_formatted"] = (
                    self.format_seconds_to_hms(user_data["total_accumulated_time"])
                )
                user_data["total_overtime_time_formatted"] = self.format_seconds_to_hms(
                    user_data["total_overtime_time"]
                )
                result.append(user_data)

            return result

    # ========== 月度统计 ==========
    async def get_monthly_statistics(
        self, chat_id: int, year: int = None, month: int = None
    ) -> List[Dict]:
        """最终版：完全基于月度表统计（昵称来自 users，不依赖 users 活动字段）"""

        # 自动获取年月
        if year is None or month is None:
            today = self.get_beijing_time()
            year = today.year
            month = today.month

        statistic_date = date(year, month, 1)

        async with self.pool.acquire() as conn:

            # =====================================================
            # 📌 1. 月度汇总（完全来自 monthly_statistics）
            # =====================================================
            monthly_stats = await conn.fetch(
                """
                SELECT
                    ms.user_id,

                    -- 昵称（安全：昵称不会被 reset）
                    u.nickname,

                    -- 活动总时长（普通活动）
                    COALESCE(SUM(
                        CASE 
                            WHEN ms.activity_name NOT IN (
                                'work_days', 'work_hours', 
                                'total_fines', 'overtime_count', 'overtime_time'
                            )
                            THEN ms.accumulated_time
                            ELSE 0 
                        END
                    ), 0) AS total_accumulated_time,

                    -- 活动总次数（普通活动）
                    COALESCE(SUM(
                        CASE 
                            WHEN ms.activity_name NOT IN (
                                'work_days', 'work_hours', 
                                'total_fines', 'overtime_count', 'overtime_time'
                            )
                            THEN ms.activity_count
                            ELSE 0 
                        END
                    ), 0) AS total_activity_count,

                    -- 🎯 罚款
                    COALESCE(SUM(
                        CASE WHEN ms.activity_name = 'total_fines'
                        THEN ms.accumulated_time ELSE 0 END
                    ), 0) AS total_fines,

                    -- 🎯 超时次数
                    COALESCE(SUM(
                        CASE WHEN ms.activity_name = 'overtime_count'
                        THEN ms.activity_count ELSE 0 END
                    ), 0) AS overtime_count,

                    -- 🎯 超时时间
                    COALESCE(SUM(
                        CASE WHEN ms.activity_name = 'overtime_time'
                        THEN ms.accumulated_time ELSE 0 END
                    ), 0) AS total_overtime_time,

                    -- 🎯 工作天数
                    COALESCE(SUM(
                        CASE WHEN ms.activity_name = 'work_days'
                        THEN ms.activity_count ELSE 0 END
                    ), 0) AS work_days,

                    -- 🎯 工作时长
                    COALESCE(SUM(
                        CASE WHEN ms.activity_name = 'work_hours'
                        THEN ms.accumulated_time ELSE 0 END
                    ), 0) AS work_hours

                FROM monthly_statistics ms
                JOIN users u ON u.chat_id = ms.chat_id AND u.user_id = ms.user_id
                WHERE ms.chat_id = $1 AND ms.statistic_date = $2
                GROUP BY ms.user_id, u.nickname
                ORDER BY total_accumulated_time DESC
                """,
                chat_id,
                statistic_date,
            )

            result = []

            # 提取用户ID列表
            user_ids = [row["user_id"] for row in monthly_stats]

            # =====================================================
            # 📌 2. 批量获取活动详情（普通活动）
            # =====================================================
            activity_map = {}
            if user_ids:
                activity_rows = await conn.fetch(
                    """
                    SELECT 
                        user_id,
                        activity_name,
                        activity_count,
                        accumulated_time
                    FROM monthly_statistics
                    WHERE chat_id = $1 AND user_id = ANY($2) AND statistic_date = $3
                    AND activity_name NOT IN (
                        'work_days', 'work_hours', 
                        'total_fines', 'overtime_count', 'overtime_time'
                    )
                    ORDER BY user_id, activity_name
                    """,
                    chat_id,
                    user_ids,
                    statistic_date,
                )
                for row in activity_rows:
                    uid = row["user_id"]
                    activity_map.setdefault(uid, {})
                    seconds = row["accumulated_time"] or 0
                    activity_map[uid][row["activity_name"]] = {
                        "count": row["activity_count"] or 0,
                        "time": seconds,
                        "time_formatted": self.format_seconds_to_hms(seconds),
                    }

            # =====================================================
            # 📌 3. 批量上下班统计（work_records）
            # =====================================================
            work_stats_map = {}
            if user_ids:
                work_rows = await conn.fetch(
                    """
                    SELECT 
                        user_id,
                        checkin_type,
                        COUNT(*) AS count,
                        SUM(fine_amount) AS fines
                    FROM work_records
                    WHERE chat_id = $1 AND user_id = ANY($2)
                    AND record_date >= $3 
                    AND record_date < $3 + INTERVAL '1 month'
                    GROUP BY user_id, checkin_type
                    """,
                    chat_id,
                    user_ids,
                    statistic_date,
                )
                for row in work_rows:
                    uid = row["user_id"]
                    work_stats_map.setdefault(uid, {})
                    work_stats_map[uid][row["checkin_type"]] = {
                        "count": row["count"],
                        "fines": row["fines"] or 0,
                    }

            # =====================================================
            # 📌 4. 合成最终输出
            # =====================================================
            for row in monthly_stats:
                uid = row["user_id"]

                user_data = {
                    "user_id": uid,
                    "nickname": row["nickname"],
                    "total_accumulated_time": row["total_accumulated_time"],
                    "total_activity_count": row["total_activity_count"],
                    "total_fines": row["total_fines"],
                    "overtime_count": row["overtime_count"],
                    "total_overtime_time": row["total_overtime_time"],
                    "work_days": row["work_days"],
                    "work_hours": row["work_hours"],
                    "activities": activity_map.get(uid, {}),
                    "work_stats": work_stats_map.get(uid, {}),
                }

                # 格式化时间
                user_data["total_accumulated_time_formatted"] = (
                    self.format_seconds_to_hms(user_data["total_accumulated_time"])
                )
                user_data["total_overtime_time_formatted"] = self.format_seconds_to_hms(
                    user_data["total_overtime_time"]
                )
                user_data["work_hours_formatted"] = self.format_seconds_to_hms(
                    user_data["work_hours"]
                )

                result.append(user_data)

            return result

    async def get_monthly_statistics_batch(
        self, chat_id: int, year: int, month: int, limit: int, offset: int
    ) -> List[Dict]:
        """分批获取月度统计信息 - 修复日期格式"""
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, month + 1, 1)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    u.user_id,
                    u.nickname,
                    ua.activity_name,
                    SUM(ua.activity_count) as total_count,
                    SUM(ua.accumulated_time) as total_time
                FROM users u
                JOIN user_activities ua ON u.chat_id = ua.chat_id AND u.user_id = ua.user_id
                WHERE u.chat_id = $1 
                    AND ua.activity_date >= $2::date  -- 🆕 添加 ::date 转换
                    AND ua.activity_date < $3::date   -- 🆕 添加 ::date 转换
                GROUP BY u.user_id, u.nickname, ua.activity_name
                ORDER BY u.user_id, ua.activity_name
                LIMIT $4 OFFSET $5
                """,
                chat_id,
                start_date,
                end_date,
                limit,
                offset,
            )

            # 按用户分组数据
            user_stats = {}
            for row in rows:
                user_id = row["user_id"]
                if user_id not in user_stats:
                    user_stats[user_id] = {
                        "user_id": user_id,
                        "nickname": row["nickname"],
                        "activities": {},
                    }

                user_stats[user_id]["activities"][row["activity_name"]] = {
                    "count": row["total_count"] or 0,
                    "time": row["total_time"] or 0,
                    "time_formatted": self.format_seconds_to_hms(
                        row["total_time"] or 0
                    ),
                }

            return list(user_stats.values())

    async def get_monthly_work_statistics(
        self, chat_id: int, year: int = None, month: int = None
    ) -> List[Dict]:
        """获取月度上下班统计"""
        if year is None or month is None:
            today = self.get_beijing_time()
            year = today.year
            month = today.month

        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, month + 1, 1)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    wr.user_id,
                    u.nickname,
                    COUNT(CASE WHEN wr.checkin_type = 'work_start' THEN 1 END) as work_start_count,
                    COUNT(CASE WHEN wr.checkin_type = 'work_end' THEN 1 END) as work_end_count,
                    SUM(CASE WHEN wr.checkin_type = 'work_start' THEN wr.fine_amount ELSE 0 END) as work_start_fines,
                    SUM(CASE WHEN wr.checkin_type = 'work_end' THEN wr.fine_amount ELSE 0 END) as work_end_fines,
                    AVG(CASE WHEN wr.checkin_type = 'work_start' THEN wr.time_diff_minutes ELSE NULL END) as avg_work_start_late,
                    AVG(CASE WHEN wr.checkin_type = 'work_end' THEN wr.time_diff_minutes ELSE NULL END) as avg_work_end_early
                FROM work_records wr
                JOIN users u ON wr.chat_id = u.chat_id AND wr.user_id = u.user_id
                WHERE wr.chat_id = $1 AND wr.record_date >= $2 AND wr.record_date < $3
                GROUP BY wr.user_id, u.nickname
                ORDER BY work_start_count DESC, work_end_count DESC
            """,
                chat_id,
                start_date,
                end_date,
            )

            result = []
            for row in rows:
                user_data = dict(row)
                user_data["avg_work_start_late"] = user_data["avg_work_start_late"] or 0
                user_data["avg_work_end_early"] = user_data["avg_work_end_early"] or 0
                user_data["avg_work_start_late_formatted"] = self.format_minutes_to_hm(
                    user_data["avg_work_start_late"]
                )
                user_data["avg_work_end_early_formatted"] = self.format_minutes_to_hm(
                    user_data["avg_work_end_early"]
                )
                result.append(user_data)

            return result

    # ========== 月度工作统计 ==========
    async def get_monthly_activity_ranking(
        self, chat_id: int, year: int = None, month: int = None
    ) -> Dict[str, List]:
        """获取月度活动排行榜 - 基于新的 monthly_statistics 表"""
        if year is None or month is None:
            today = self.get_beijing_time()
            year = today.year
            month = today.month

        statistic_date = date(year, month, 1)

        async with self.pool.acquire() as conn:
            activity_limits = await self.get_activity_limits()
            rankings = {}

            for activity in activity_limits.keys():
                # 🆕 关键修改：从 monthly_statistics 表获取排行榜数据
                rows = await conn.fetch(
                    """
                    SELECT 
                        ms.user_id,
                        u.nickname,
                        ms.accumulated_time as total_time,
                        ms.activity_count as total_count
                    FROM monthly_statistics ms
                    JOIN users u ON ms.chat_id = u.chat_id AND ms.user_id = u.user_id
                    WHERE ms.chat_id = $1 AND ms.activity_name = $2 
                        AND ms.statistic_date = $3
                    ORDER BY ms.accumulated_time DESC
                    LIMIT 10
                    """,
                    chat_id,
                    activity,
                    statistic_date,
                )

                formatted_rows = []
                for row in rows:
                    user_data = dict(row)
                    user_data["total_time"] = user_data["total_time"] or 0
                    user_data["total_time_formatted"] = self.format_seconds_to_hms(
                        user_data["total_time"]
                    )
                    formatted_rows.append(user_data)

                rankings[activity] = formatted_rows

            return rankings

    # === 获取月度统计数据 - 横向格式专用 ===

    async def get_monthly_statistics_horizontal(
        self, chat_id: int, year: int, month: int
    ):
        """获取月度统计数据 - 横向格式专用"""
        from datetime import date

        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, month + 1, 1)

        async with self.pool.acquire() as conn:
            # 获取用户基本统计
            user_stats = await conn.fetch(
                """
                SELECT 
                    u.user_id,
                    u.nickname,
                    SUM(COALESCE(ua.accumulated_time, 0)) as total_time,
                    SUM(COALESCE(ua.activity_count, 0)) as total_count,
                    SUM(COALESCE(u.total_fines, 0)) as total_fines,
                    SUM(COALESCE(u.overtime_count, 0)) as total_overtime_count,
                    SUM(COALESCE(u.total_overtime_time, 0)) as total_overtime_time
                FROM users u
                LEFT JOIN user_activities ua ON u.chat_id = ua.chat_id AND u.user_id = ua.user_id
                    AND ua.activity_date >= $1 AND ua.activity_date < $2
                WHERE u.chat_id = $3
                GROUP BY u.user_id, u.nickname
                """,
                start_date,
                end_date,
                chat_id,
            )

            result = []
            for stat in user_stats:
                user_data = dict(stat)

                # 获取用户每项活动的详细统计
                activity_details = await conn.fetch(
                    """
                    SELECT 
                        activity_name,
                        SUM(activity_count) as activity_count,
                        SUM(accumulated_time) as accumulated_time
                    FROM user_activities
                    WHERE chat_id = $1 AND user_id = $2 AND activity_date >= $3 AND activity_date < $4
                    GROUP BY activity_name
                    """,
                    chat_id,
                    user_data["user_id"],
                    start_date,
                    end_date,
                )

                user_data["activities"] = {}
                for row in activity_details:
                    activity_time = row["accumulated_time"] or 0
                    user_data["activities"][row["activity_name"]] = {
                        "count": row["activity_count"] or 0,
                        "time": activity_time,
                        "time_formatted": self.format_seconds_to_hms(activity_time),
                    }

                result.append(user_data)

            return result

    # ========== 数据清理 ==========
    async def cleanup_old_data(self, days: int = 30):
        """清理旧数据 - 修复版（防止 str 传入 asyncpg），包含月度统计清理"""
        try:
            cutoff_date = (self.get_beijing_time() - timedelta(days=days)).date()
            logger.info(
                f"🔄 开始清理 {days} 天前的数据，截止日期: {cutoff_date.isoformat()}"
            )

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # ✅ 清理 user_activities 表（超过指定天数的日常数据）
                    user_activities_deleted = await conn.execute(
                        "DELETE FROM user_activities WHERE activity_date < $1::date",
                        cutoff_date,
                    )

                    # ✅ 清理 work_records 表（超过指定天数的打卡记录）
                    work_records_deleted = await conn.execute(
                        "DELETE FROM work_records WHERE record_date < $1::date",
                        cutoff_date,
                    )

                    # ✅ 清理 users 表（超过指定天数未更新的用户）
                    users_deleted = await conn.execute(
                        "DELETE FROM users WHERE last_updated < $1::date", cutoff_date
                    )

                    # 🆕 新增：清理月度统计数据（保留最近3个月的数据）
                    monthly_cutoff = (
                        (self.get_beijing_time() - timedelta(days=90))
                        .date()
                        .replace(day=1)
                    )
                    monthly_deleted = await conn.execute(
                        "DELETE FROM monthly_statistics WHERE statistic_date < $1::date",
                        monthly_cutoff,
                    )

            logger.info(
                f"✅ 成功清理超过 {days} 天的数据:\n"
                f"   - 日常活动记录: {user_activities_deleted}\n"
                f"   - 上下班记录: {work_records_deleted}\n"
                f"   - 用户数据: {users_deleted}\n"
                f"   - 月度统计: {monthly_deleted} (保留最近3个月)"
            )

        except Exception as e:
            logger.error(f"❌ 清理旧数据失败: {e}")
            raise

    async def safe_cleanup_old_data(self, days: int = 30) -> bool:
        """安全清理旧数据 - 不会抛出异常，适合在定时任务中使用"""
        try:
            await self.cleanup_old_data(days)
            logger.info(f"✅ 安全清理完成: 清理了超过 {days} 天的数据")
            return True
        except Exception as e:
            logger.warning(f"⚠️ 安全清理数据失败（不影响主要功能）: {e}")
            return False

    async def cleanup_monthly_data(self, target_date: date = None):
        """清理指定月份的月度统计数据"""
        try:
            if target_date is None:
                # 默认清理3个月前的数据
                today = self.get_beijing_time()
                monthly_cutoff = (today - timedelta(days=90)).date().replace(day=1)
                target_date = monthly_cutoff
            elif not isinstance(target_date, date):
                raise ValueError(
                    f"target_date必须是date类型，得到: {type(target_date)}"
                )
            else:
                # 确保target_date是月初日期
                target_date = target_date.replace(day=1)

            async with self.pool.acquire() as conn:
                # 获取要删除的记录数（用于日志）
                count_before = await conn.fetchval(
                    "SELECT COUNT(*) FROM monthly_statistics WHERE statistic_date < $1",
                    target_date,
                )

                # 执行删除
                result = await conn.execute(
                    "DELETE FROM monthly_statistics WHERE statistic_date < $1",
                    target_date,
                )

                # 解析删除的记录数
                deleted_count = (
                    int(result.split()[-1])
                    if result and result.startswith("DELETE")
                    else 0
                )

            logger.info(
                f"🗑️ 月度统计清理完成:\n"
                f"   - 清理截止: {target_date.strftime('%Y年%m月')}\n"
                f"   - 删除记录: {deleted_count} 条\n"
                f"   - 剩余记录: {count_before - deleted_count} 条"
            )

            return deleted_count

        except Exception as e:
            logger.error(f"❌ 清理月度数据失败: {e}")
            raise

    async def manage_monthly_data(self):
        """月度数据管理 - 包含月度统计清理"""
        try:
            # 清理日常数据（保留30天）
            await self.cleanup_old_data(Config.DATA_RETENTION_DAYS)

            # 🆕 新增：清理月度统计数据（保留3个月）
            await self.cleanup_monthly_data()

            logger.info(
                f"✅ 月度数据管理完成:\n"
                f"   - 日常数据保留: {Config.DATA_RETENTION_DAYS} 天\n"
                f"   - 月度统计保留: 3 个月"
            )

        except Exception as e:
            logger.error(f"❌ 月度数据管理失败: {e}")

    async def should_create_monthly_archive(self) -> bool:
        """检查是否应该创建月度归档"""
        today = self.get_beijing_time()
        return today.day == 1

    async def cleanup_specific_month(self, year: int, month: int):
        """清理指定年月的月度统计数据"""
        try:
            target_date = date(year, month, 1)

            async with self.pool.acquire() as conn:
                # 获取要删除的记录数
                count_before = await conn.fetchval(
                    "SELECT COUNT(*) FROM monthly_statistics WHERE statistic_date = $1",
                    target_date,
                )

                # 执行删除
                result = await conn.execute(
                    "DELETE FROM monthly_statistics WHERE statistic_date = $1",
                    target_date,
                )

                deleted_count = (
                    int(result.split()[-1])
                    if result and result.startswith("DELETE")
                    else 0
                )

            logger.info(
                f"🗑️ 指定月份统计清理完成:\n"
                f"   - 清理月份: {year}年{month:02d}月\n"
                f"   - 删除记录: {deleted_count} 条"
            )

            return deleted_count

        except Exception as e:
            logger.error(f"❌ 清理指定月份数据失败 {year}-{month}: {e}")
            raise

    # ========== 数据库统计 ==========
    async def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        return {
            "type": "postgresql",
            "initialized": self._initialized,
            "cache_size": len(self._cache),
        }

    async def get_database_size(self) -> int:
        """获取数据库大小"""
        async with self.pool.acquire() as conn:
            # 提取数据库名
            db_name = self.database_url.split("/")[-1]
            row = await conn.fetchrow("SELECT pg_database_size($1)", db_name)
            return row[0] if row else 0

    # ========== 工具方法 ==========
    @staticmethod
    def format_seconds_to_hms(seconds: int) -> str:
        """将秒数格式化为小时:分钟:秒的字符串"""
        if not seconds:
            return "0秒"

        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours}小时{minutes}分{secs}秒"
        elif minutes > 0:
            return f"{minutes}分{secs}秒"
        else:
            return f"{secs}秒"

    # ========== 健康检查与监控 ==========
    async def connection_health_check(self) -> bool:
        """
        ✅ 数据库连接健康检查（优化版）
        - 自动重试1次（防止瞬时断连）
        - 更高效的轻量查询
        - 统一日志风格
        - 精确异常区分
        """
        if not self.pool:
            logger.warning("⚠️ [DB] 健康检查失败：连接池未初始化")
            return False

        for attempt in range(2):  # ✅ 增加1次自动重试
            try:
                async with self.pool.acquire() as conn:
                    # ✅ 使用更标准的PostgreSQL查询（移除分号）
                    result = await conn.fetchval("SELECT 1")
                    if result == 1:
                        if attempt > 0:
                            logger.info("✅ [DB] 重试后连接恢复正常")
                        else:
                            logger.debug("✅ [DB] 连接正常")
                        return True
                    else:
                        logger.error(f"❌ [DB] 健康检查返回异常值: {result}")
                        return False

            except (asyncio.TimeoutError, ConnectionError) as e:
                logger.warning(
                    f"⚠️ [DB] 健康检查网络异常 ({e.__class__.__name__})，正在重试... ({attempt+1}/2)"
                )
                if attempt == 0:  # ✅ 只在第一次重试时等待
                    await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"❌ [DB] 健康检查失败: {type(e).__name__}: {e}")
                return False

        logger.error("❌ [DB] 健康检查多次失败，数据库可能断开连接")
        return False

    async def reconnect(self, max_retries: int = 3) -> bool:
        """
        重新连接数据库
        返回: True-成功, False-失败
        """
        logger.warning("🔄 尝试重新连接数据库...")

        for attempt in range(1, max_retries + 1):
            try:
                # 关闭现有连接池
                if self.pool:
                    await self.pool.close()
                    logger.debug("✅ 旧连接池已关闭")

                # 重置状态
                self.pool = None
                self._initialized = False
                self._cache.clear()
                self._cache_ttl.clear()

                # 重新初始化
                await self.initialize()

                # 验证重新连接是否成功
                if await self.connection_health_check():
                    logger.info(f"✅ 数据库重连成功 (第{attempt}次尝试)")
                    return True
                else:
                    logger.warning(f"⚠️ 重连后健康检查失败 (第{attempt}次尝试)")

            except Exception as e:
                logger.error(f"❌ 数据库重连第{attempt}次尝试失败: {e}")

                if attempt < max_retries:
                    retry_delay = 2**attempt  # 指数退避
                    logger.info(f"⏳ {retry_delay}秒后重试...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"💥 数据库重连{max_retries}次后彻底失败")
                    return False

        return False

    @staticmethod
    def format_minutes_to_hm(minutes: float) -> str:
        """将分钟数格式化为小时:分钟的字符串"""
        if not minutes:
            return "0小时0分钟"

        total_seconds = int(minutes * 60)
        hours = total_seconds // 3600
        mins = (total_seconds % 3600) // 60
        secs = total_seconds % 60

        if hours > 0:
            return f"{hours}小时{mins}分{secs}秒"
        elif mins > 0:
            return f"{mins}分{secs}秒"
        else:
            return f"{secs}秒"

    @staticmethod
    def format_time_for_csv(seconds: int) -> str:
        """为 CSV 导出格式化时间显示"""
        if not seconds:
            return "0分0秒"

        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours}时{minutes}分{secs}秒"
        else:
            return f"{minutes}分{secs}秒"

    async def init_activity_limit_table(self):
        """初始化活动人数限制表"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS activity_user_limits (
                    activity_name TEXT PRIMARY KEY,
                    max_users INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

    async def set_activity_user_limit(self, activity: str, max_users: int):
        """设置活动人数限制"""
        await self.init_activity_limit_table()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO activity_user_limits (activity_name, max_users)
                VALUES ($1, $2)
                ON CONFLICT (activity_name)
                DO UPDATE SET 
                    max_users = EXCLUDED.max_users,
                    updated_at = CURRENT_TIMESTAMP
            """,
                activity,
                max_users,
            )

        # 清理缓存
        self._cache.pop(f"activity_limit:{activity}", None)
        logger.info(f"✅ 设置活动人数限制: {activity} -> {max_users}人")

    async def get_activity_user_limit(self, activity: str) -> int:
        """获取活动人数限制"""
        cache_key = f"activity_limit:{activity}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        await self.init_activity_limit_table()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT max_users FROM activity_user_limits WHERE activity_name = $1",
                activity,
            )
            limit = row["max_users"] if row else 0
            self._set_cached(cache_key, limit, 60)
            return limit

    async def get_current_activity_users(self, chat_id: int, activity: str) -> int:
        """获取当前正在进行指定活动的用户数量"""
        async with self.pool.acquire() as conn:
            count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM users 
                WHERE chat_id = $1 AND current_activity = $2
            """,
                chat_id,
                activity,
            )
            return count or 0

    async def remove_activity_user_limit(self, activity: str):
        """移除活动人数限制"""
        await self.init_activity_limit_table()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM activity_user_limits WHERE activity_name = $1", activity
            )

        self._cache.pop(f"activity_limit:{activity}", None)
        logger.info(f"🗑️ 已移除活动人数限制: {activity}")

    async def get_all_activity_limits(self) -> Dict[str, int]:
        """获取所有活动的人数限制"""
        await self.init_activity_limit_table()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT activity_name, max_users FROM activity_user_limits"
            )
            return {row["activity_name"]: row["max_users"] for row in rows}


# 全局数据库实例
db = PostgreSQLDatabase()
