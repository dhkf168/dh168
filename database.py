# database.py - 纯 PostgreSQL 版本（最终完整版）
import logging
import asyncio
import time
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional
from config import Config
import asyncpg
from asyncpg.pool import Pool

logger = logging.getLogger("GroupCheckInBot")


class PostgreSQLDatabase:
    """纯 PostgreSQL 数据库管理器"""

    def __init__(self, database_url: str = None):
        self.database_url = database_url or Config.DATABASE_URL
        self.pool: Optional[Pool] = None
        self._initialized = False
        self._cache = {}
        self._cache_ttl = {}

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
        today = datetime.now().date()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (chat_id, user_id, nickname, last_updated) VALUES ($1, $2, $3, $4) ON CONFLICT (chat_id, user_id) DO NOTHING",
                chat_id,
                user_id,
                nickname,
                today,
            )
            self._cache.pop(f"user:{chat_id}:{user_id}", None)

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

    async def complete_user_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        elapsed_time: int,
        fine_amount: int = 0,
        is_overtime: bool = False,
    ):
        """完成用户活动 - 修复计数问题版本"""
        today = datetime.now().date()

        logger.info(
            f"🔍 [数据库操作开始] 用户{user_id} 活动{activity} 时长{elapsed_time}s"
        )

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # 确保用户记录存在并更新日期
                await conn.execute(
                    """
                    INSERT INTO users (chat_id, user_id, last_updated) 
                    VALUES ($1, $2, $3)
                    ON CONFLICT (chat_id, user_id) 
                    DO UPDATE SET last_updated = EXCLUDED.last_updated
                    """,
                    chat_id,
                    user_id,
                    today,
                )

                # 使用 ON CONFLICT 原子更新活动计数
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
                    today,
                    activity,
                    elapsed_time,
                )

                # 更新用户总体统计
                update_fields = [
                    "total_accumulated_time = total_accumulated_time + $1",
                    "total_activity_count = total_activity_count + 1",
                    "current_activity = NULL",
                    "activity_start_time = NULL",
                    "last_updated = $2",
                ]
                params = [elapsed_time, today]

                if fine_amount > 0:
                    update_fields.append("total_fines = total_fines + $3")
                    params.append(fine_amount)

                if is_overtime:
                    update_fields.append("overtime_count = overtime_count + 1")
                    time_limit = await self.get_activity_time_limit(activity)
                    overtime_seconds = max(0, elapsed_time - (time_limit * 60))
                    update_fields.append(
                        "total_overtime_time = total_overtime_time + $4"
                    )
                    params.append(overtime_seconds)

                update_fields.append("updated_at = CURRENT_TIMESTAMP")
                params.extend([chat_id, user_id])

                placeholders = ", ".join(update_fields)
                query = f"UPDATE users SET {placeholders} WHERE chat_id = ${len(params)-1} AND user_id = ${len(params)}"
                await conn.execute(query, *params)

            self._cache.pop(f"user:{chat_id}:{user_id}", None)

        logger.info(f"🔍 [数据库操作完成] 用户{user_id} 活动{activity} 完成更新")

    async def reset_user_daily_data(self, chat_id: int, user_id: int):
        """
        重置用户的每日统计数据：
        - 清空 user_activities 当日计数
        - 保留累计总时间和总次数
        - 更新 last_updated 为当前日期
        """
        async with self.pool.acquire() as conn:
            # 删除 user_activities 表中的当日记录（当天起点之后的记录算下一天）
            await conn.execute(
                """
                DELETE FROM user_activities
                WHERE chat_id = $1 AND user_id = $2
                """,
                chat_id,
                user_id,
            )

            # 清空用户表的当日计数类字段（但不影响历史累计）
            await conn.execute(
                """
                UPDATE users
                SET
                    total_activity_count = 0,
                    total_accumulated_time = 0,
                    total_overtime_time = 0,
                    overtime_count = 0,
                    last_updated = CURRENT_DATE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE chat_id = $1 AND user_id = $2
                """,
                chat_id,
                user_id,
            )

        # 清理缓存
        self._cache.pop(f"user:{chat_id}:{user_id}", None)
        logger.info(f"✅ 已重置用户 {user_id} 的每日统计数据（chat_id={chat_id}）")

    async def get_user_activity_count(
        self, chat_id: int, user_id: int, activity: str
    ) -> int:
        """获取用户今日活动次数"""
        today = datetime.now().date()
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
        today = datetime.now().date()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT accumulated_time FROM user_activities WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3 AND activity_name = $4",
                chat_id,
                user_id,
                today,
                activity,
            )
            return row["accumulated_time"] if row else 0

    async def get_user_all_activities(
        self, chat_id: int, user_id: int
    ) -> Dict[str, Dict]:
        """获取用户所有活动数据"""
        today = datetime.now().date()
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

    # ========== 上下班记录操作 ==========
    async def add_work_record(
        self,
        chat_id: int,
        user_id: int,
        record_date,  # 移除类型注解，让Python自动处理
        checkin_type: str,
        checkin_time: str,
        status: str,
        time_diff_minutes: float,
        fine_amount: int = 0,
    ):
        """添加上下班记录"""
        if isinstance(record_date, str):
            record_date = datetime.strptime(record_date, "%Y-%m-%d").date()
        elif isinstance(record_date, datetime):
            record_date = record_date.date()

        async with self.pool.acquire() as conn:
            async with conn.transaction():
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

                # 更新用户罚款总额
                if fine_amount > 0:
                    await conn.execute(
                        "UPDATE users SET total_fines = total_fines + $1 WHERE chat_id = $2 AND user_id = $3",
                        fine_amount,
                        chat_id,
                        user_id,
                    )

            self._cache.pop(f"user:{chat_id}:{user_id}", None)

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
        """检查今天是否有指定类型的上下班记录"""
        today = datetime.now().date()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM work_records WHERE chat_id = $1 AND user_id = $2 AND record_date = $3 AND checkin_type = $4",
                chat_id,
                user_id,
                today,
                checkin_type,
            )
            return row is not None

    async def get_today_work_records(
        self, chat_id: int, user_id: int
    ) -> Dict[str, Dict]:
        """获取用户今天的上下班记录"""
        today = datetime.now().date()
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
        """检查活动是否存在"""
        limits = await self.get_activity_limits()
        return activity in limits

    async def update_activity_config(
        self, activity: str, max_times: int, time_limit: int
    ):
        """更新活动配置"""
        async with self.pool.acquire() as conn:
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
            self._cache.pop("activity_limits", None)

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
    async def get_group_statistics(self, chat_id: int) -> List[Dict]:
        """获取群组统计信息"""
        today = datetime.now().date()
        async with self.pool.acquire() as conn:
            users = await conn.fetch(
                "SELECT user_id, nickname, total_accumulated_time, total_activity_count, total_fines, overtime_count, total_overtime_time FROM users WHERE chat_id = $1 AND last_updated = $2",
                chat_id,
                today,
            )

            result = []
            for user in users:
                user_data = dict(user)
                user_data["total_accumulated_time_formatted"] = (
                    self.format_seconds_to_hms(user_data["total_accumulated_time"])
                )
                user_data["total_overtime_time_formatted"] = self.format_seconds_to_hms(
                    user_data["total_overtime_time"]
                )

                # 获取用户的活动详情
                activities = await conn.fetch(
                    "SELECT activity_name, activity_count, accumulated_time FROM user_activities WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3",
                    chat_id,
                    user["user_id"],
                    today,
                )

                user_data["activities"] = {}
                for row in activities:
                    user_data["activities"][row["activity_name"]] = {
                        "count": row["activity_count"],
                        "time": row["accumulated_time"],
                        "time_formatted": self.format_seconds_to_hms(
                            row["accumulated_time"]
                        ),
                    }
                result.append(user_data)

            return result

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
        today = datetime.now().date()
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
        """获取月度统计信息"""
        if year is None or month is None:
            today = datetime.now()
            year = today.year
            month = today.month

        start_date = f"{year:04d}-{month:02d}-01"
        if month == 12:
            end_date = f"{year+1:04d}-01-01"
        else:
            end_date = f"{year:04d}-{month+1:02d}-01"

        async with self.pool.acquire() as conn:
            monthly_stats = await conn.fetch(
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
                ORDER BY total_time DESC
            """,
                start_date,
                end_date,
                chat_id,
            )

            result = []
            for stat in monthly_stats:
                user_data = dict(stat)
                user_data["total_time"] = user_data["total_time"] or 0
                user_data["total_overtime_time"] = user_data["total_overtime_time"] or 0
                user_data["total_time_formatted"] = self.format_seconds_to_hms(
                    user_data["total_time"]
                )
                user_data["total_overtime_time_formatted"] = self.format_seconds_to_hms(
                    user_data["total_overtime_time"]
                )

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
                        "time": self.format_seconds_to_hms(activity_time),
                    }

                result.append(user_data)

            return result

    async def get_monthly_statistics_batch(
        self, chat_id: int, year: int, month: int, limit: int, offset: int
    ) -> List[Dict]:
        """分批获取月度统计信息"""
        start_date = f"{year:04d}-{month:02d}-01"
        if month == 12:
            end_date = f"{year+1:04d}-01-01"
        else:
            end_date = f"{year:04d}-{month+1:02d}-01"

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
                    AND ua.activity_date >= $2 
                    AND ua.activity_date < $3
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
            today = datetime.now()
            year = today.year
            month = today.month

        start_date = f"{year:04d}-{month:02d}-01"
        if month == 12:
            end_date = f"{year+1:04d}-01-01"
        else:
            end_date = f"{year:04d}-{month+1:02d}-01"

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

    async def get_monthly_activity_ranking(
        self, chat_id: int, year: int = None, month: int = None
    ) -> Dict[str, List]:
        """获取月度活动排行榜"""
        if year is None or month is None:
            today = datetime.now()
            year = today.year
            month = today.month

        start_date = f"{year:04d}-{month:02d}-01"
        if month == 12:
            end_date = f"{year+1:04d}-01-01"
        else:
            end_date = f"{year:04d}-{month+1:02d}-01"

        async with self.pool.acquire() as conn:
            activity_limits = await self.get_activity_limits()
            rankings = {}

            for activity in activity_limits.keys():
                rows = await conn.fetch(
                    """
                    SELECT 
                        u.user_id,
                        u.nickname,
                        SUM(COALESCE(ua.accumulated_time, 0)) as total_time,
                        SUM(COALESCE(ua.activity_count, 0)) as total_count
                    FROM user_activities ua
                    JOIN users u ON ua.chat_id = u.chat_id AND ua.user_id = u.user_id
                    WHERE ua.chat_id = $1 AND ua.activity_name = $2 AND ua.activity_date >= $3 AND ua.activity_date < $4
                    GROUP BY u.user_id, u.nickname
                    ORDER BY total_time DESC
                    LIMIT 10
                """,
                    chat_id,
                    activity,
                    start_date,
                    end_date,
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

    # ========== 数据清理 ==========

    async def cleanup_old_data(self, days: int = 30):
        """清理旧数据 - 修复版（防止 str 传入 asyncpg）"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).date()
            logger.info(
                f"🔄 开始清理 {days} 天前的数据，截止日期: {cutoff_date.isoformat()}"
            )

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # ✅ 关键修复：传入 cutoff_date（date对象），不再用字符串
                    await conn.execute(
                        "DELETE FROM user_activities WHERE activity_date < $1::date",
                        cutoff_date,
                    )
                    await conn.execute(
                        "DELETE FROM work_records WHERE record_date < $1::date",
                        cutoff_date,
                    )
                    await conn.execute(
                        "DELETE FROM users WHERE last_updated < $1::date", cutoff_date
                    )

            logger.info(f"✅ 成功清理超过 {days} 天的数据")
        except Exception as e:
            logger.error(f"❌ 清理旧数据失败: {e}")
            raise

    async def safe_cleanup_old_data(self, days: int = 30) -> bool:
        """安全清理旧数据 - 不会抛出异常，适合在定时任务中使用"""
        try:
            await self.cleanup_old_data(days)
            return True
        except Exception as e:
            logger.warning(f"⚠️ 安全清理数据失败（不影响主要功能）: {e}")
            return False

    async def manage_monthly_data(self):
        """月度数据管理"""
        try:
            await self.cleanup_old_data(Config.DATA_RETENTION_DAYS)
            logger.info(
                f"✅ 月度数据管理完成，保留 {Config.DATA_RETENTION_DAYS} 天数据"
            )
        except Exception as e:
            logger.error(f"❌ 月度数据管理失败: {e}")

    async def should_create_monthly_archive(self) -> bool:
        """检查是否应该创建月度归档"""
        today = datetime.now()
        return today.day == 1

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
        数据库连接层面的健康检查
        返回: True-连接健康, False-连接异常
        """
        if not self.pool:
            logger.warning("⚠️ 数据库健康检查: 连接池未初始化")
            return False

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                is_healthy = result == 1
                if not is_healthy:
                    logger.error("❌ 数据库健康检查: 查询返回异常结果")
                else:
                    logger.debug("✅ 数据库健康检查: 连接正常")
                return is_healthy
        except Exception as e:
            logger.error(f"❌ 数据库健康检查失败: {e}")
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


# 全局数据库实例
db = PostgreSQLDatabase()
