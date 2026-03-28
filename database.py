import logging
import asyncio
import time
import json
import random
from datetime import datetime, timedelta, date
from config import beijing_tz
from typing import Dict, Any, List, Optional, Union
from config import Config, beijing_tz
import asyncpg
from asyncpg.pool import Pool
from fault_tolerance import with_deadlock_retry, db_circuit_breaker, Watchdog
from asyncpg.exceptions import SerializationError, PostgresError

logger = logging.getLogger("GroupCheckInBot")


class PostgreSQLDatabase:
    """PostgreSQL数据库管理器 - 纯双班模式"""

    def __init__(self, database_url: str = None):
        # 基础连接配置
        self.database_url = database_url or Config.DATABASE_URL
        self.pool: Optional[Pool] = None
        self._initialized = False

        # 一级缓存 (L1 Cache) 属性
        self._cache = {}
        self._cache_ttl = {}
        self._cache_max_size = 1000
        self._cache_access_order = []

        # 并发控制：防击穿与命名锁
        self._pending_queries = {}  # 用于 Singleflight 模式
        self._locks: Dict[str, asyncio.Lock] = {}
        self._locks_lock = asyncio.Lock()  # 保护 _locks 字典本身的互斥锁

        # 锁清理与统计属性
        self._lock_cleanup_interval = 3600  # 1小时清理一次空闲锁
        self._last_lock_cleanup = time.time()
        self._lock_stats = {"total_created": 0, "peak_count": 0}

        # 连接池健康监测
        self._last_pool_check = 0
        self._pool_check_interval = 60

        # 指数退避重连机制
        self._last_connection_check = 0
        self._connection_check_interval = 30
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        self._reconnect_base_delay = 1.0

        # 后台维护任务句柄
        self._maintenance_running = False
        self._maintenance_task = None
        self._connection_maintenance_task = None

    # ========== 重连机制 ==========
    async def _ensure_healthy_connection(self):
        """确保连接健康"""
        current_time = time.time()
        if current_time - self._last_connection_check < self._connection_check_interval:
            return True

        try:
            is_healthy = await self.connection_health_check()
            if not is_healthy:
                logger.warning("数据库连接不健康，尝试重连...")
                await self._reconnect()  # 修复后的 _reconnect 会设置 _initialized=True
                # 重连后再次验证
                is_healthy = await self.connection_health_check()
                if not is_healthy:
                    logger.error("重连后数据库仍然不健康")
                    return False

            self._last_connection_check = current_time
            return True

        except Exception as e:
            logger.error(f"数据库连接检查失败: {e}")
            # 重要：标记为未初始化
            self._initialized = False
            return False

    async def _reconnect(self):
        """重新建立数据库连接池（修复版）"""
        self._reconnect_attempts += 1

        if self._reconnect_attempts > self._max_reconnect_attempts:
            logger.error(
                f"数据库重连尝试次数超过上限 ({self._max_reconnect_attempts})，停止重连"
            )
            raise ConnectionError("数据库重连失败")

        try:
            delay = self._reconnect_base_delay * (2 ** (self._reconnect_attempts - 1))
            logger.info(f"{delay}秒后尝试第{self._reconnect_attempts}次数据库重连...")
            await asyncio.sleep(delay)

            # ⚠️ 重要：先标记为未初始化，防止新请求进入
            self._initialized = False

            # 关闭旧连接池（如果有）
            if self.pool:
                old_pool = self.pool
                self.pool = None  # 先置空，防止其他操作使用
                try:
                    await old_pool.close()
                except Exception as close_error:
                    logger.warning(f"关闭旧连接池时出错: {close_error}")

            # 创建新连接池
            await self._initialize_impl()

            self._initialized = True

            self._reconnect_attempts = 0
            logger.info("✅ 数据库重连成功")

        except Exception as e:
            logger.error(f"数据库第{self._reconnect_attempts}次重连失败: {e}")
            if self._reconnect_attempts >= self._max_reconnect_attempts:
                logger.critical("数据库重连最终失败，服务可能无法正常工作")
            raise

    async def get_pool_stats(self) -> Dict[str, Any]:
        """获取连接池统计信息"""
        stats = {
            "initialized": self._initialized,
            "pool_exists": self.pool is not None,
            "reconnect_attempts": self._reconnect_attempts,
        }

        if self.pool and hasattr(self.pool, "_holders"):
            try:
                holders = self.pool._holders
                stats["total_connections"] = len(holders)
                stats["active_connections"] = sum(1 for h in holders if h._holder)
                stats["idle_connections"] = (
                    stats["total_connections"] - stats["active_connections"]
                )
            except Exception as e:
                stats["pool_stats_error"] = str(e)

        return stats

    async def execute_with_retry(
        self,
        operation_name: str,
        query: str,
        *args,
        fetch: bool = False,
        fetchrow: bool = False,
        fetchval: bool = False,
        max_retries: int = 2,
        timeout: int = 30,
        slow_threshold: float = 1.0,
    ):
        """带重试和超时的查询执行（修复连接释放问题）"""

        watchdog = Watchdog(timeout=timeout + 5, name=operation_name)

        async def _execute():
            if not await self._ensure_healthy_connection():
                raise ConnectionError("数据库连接不健康")

            if sum([fetch, fetchrow, fetchval]) > 1:
                raise ValueError("只能指定一种查询类型: fetch, fetchrow 或 fetchval")

            for attempt in range(max_retries + 1):
                conn = None
                start_time = time.time()
                try:
                    # 获取连接
                    conn = await self.pool.acquire()
                    watchdog.feed()

                    # 设置语句超时
                    await conn.execute(f"SET statement_timeout = {timeout * 1000}")

                    # 执行查询
                    if fetch:
                        result = await conn.fetch(query, *args)
                    elif fetchrow:
                        result = await conn.fetchrow(query, *args)
                    elif fetchval:
                        result = await conn.fetchval(query, *args)
                    else:
                        result = await conn.execute(query, *args)

                    execution_time = time.time() - start_time
                    watchdog.feed()

                    # 慢查询监控
                    if execution_time > slow_threshold:
                        log_level = (
                            logging.WARNING if execution_time < 5.0 else logging.ERROR
                        )
                        logger.log(
                            log_level,
                            f"⏱️ 慢查询: {operation_name} 耗时 {execution_time:.3f}秒 "
                            f"(SQL: {query[:100]}{'...' if len(query) > 100 else ''})",
                        )

                    return result

                except asyncpg.DeadlockDetectedError as e:
                    if attempt == max_retries:
                        logger.error(
                            f"{operation_name} 死锁重试{max_retries}次后失败: {e}"
                        )
                        raise

                    retry_delay = min(0.1 * (2**attempt) * (1 + random.random()), 5)
                    logger.warning(
                        f"🔄 检测到死锁，{retry_delay:.2f}秒后第{attempt + 1}次重试: {operation_name}"
                    )
                    await asyncio.sleep(retry_delay)

                except (
                    asyncpg.PostgresConnectionError,
                    asyncpg.ConnectionDoesNotExistError,
                    asyncpg.InterfaceError,
                    ConnectionError,
                ) as e:
                    if attempt == max_retries:
                        logger.error(
                            f"{operation_name} 数据库重试{max_retries}次后失败: {e}"
                        )
                        raise

                    retry_delay = min(1 * (2**attempt), 5)
                    logger.warning(
                        f"{operation_name} 数据库连接异常，{retry_delay}秒后第{attempt + 1}次重试: {e}"
                    )

                    # ⚠️ 重要：不要在这里调用 _reconnect()！
                    # 让连接池自己处理坏连接
                    await asyncio.sleep(retry_delay)

                except asyncpg.QueryCanceledError:
                    logger.error(
                        f"{operation_name} 查询超时被取消 (超时设置: {timeout}秒)"
                    )
                    if attempt == max_retries:
                        raise
                    await asyncio.sleep(1)

                except Exception as e:
                    if "database" in str(e).lower() or "sql" in str(e).lower():
                        logger.error(f"{operation_name} 数据库操作失败: {e}")
                    else:
                        logger.error(f"{operation_name} 操作失败: {e}")
                    raise

                finally:
                    # 确保连接被释放
                    if conn:
                        try:
                            await self.pool.release(conn)
                        except Exception as release_error:
                            logger.warning(f"释放连接失败: {release_error}")

        return await db_circuit_breaker.call(_execute)

    # ========== 定期维护任务 ==========
    async def start_connection_maintenance(self):
        """启动连接维护任务"""
        if hasattr(self, "_maintenance_running") and self._maintenance_running:
            logger.info("连接维护任务已在运行")
            return

        self._maintenance_running = True
        self._maintenance_task = asyncio.create_task(
            self._connection_maintenance_loop()
        )
        logger.info("✅ 数据库连接维护任务已启动")

    async def stop_connection_maintenance(self):
        """停止连接维护任务"""
        self._maintenance_running = False
        if hasattr(self, "_maintenance_task") and self._maintenance_task:
            self._maintenance_task.cancel()
            try:
                await self._maintenance_task
            except asyncio.CancelledError:
                pass
            self._maintenance_task = None
        logger.info("数据库连接维护任务已停止")

    async def _monitor_pool_health(self):
        """监控数据库健康状态（修复版）"""
        if not self.pool:
            return

        try:
            # ===== 1. 基础连接测试 =====
            try:
                async with self.pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                logger.debug("✅ 数据库连接正常")
            except Exception as e:
                logger.error(f"❌ 数据库连接异常: {e}")
                self._last_connection_check = 0
                return

            # ===== 2. 检查是否有死锁 =====
            try:
                async with self.pool.acquire() as conn:
                    deadlocks = await conn.fetch(
                        """
                        SELECT pid, query, age(now(), query_start) as duration,
                               datname, usename, application_name
                        FROM pg_stat_activity
                        WHERE state = 'active' 
                          AND wait_event_type = 'Lock'
                          AND pid != pg_backend_pid()
                    """
                    )

                    if deadlocks:
                        logger.error(f"🔒 检测到 {len(deadlocks)} 个死锁:")
                        for dl in deadlocks:
                            logger.error(
                                f"  • PID: {dl['pid']}, 用户: {dl['usename']}, "
                                f"时长: {dl['duration']}, 查询: {dl['query'][:100]}..."
                            )
            except Exception as e:
                # 权限不足时降级
                if "permission denied" in str(e).lower():
                    logger.debug("死锁检测需要更高权限，已跳过")
                else:
                    logger.debug(f"死锁检查失败: {e}")

            # ===== 3. 检查长时间运行的事务 =====
            try:
                async with self.pool.acquire() as conn:
                    long_tx = await conn.fetch(
                        """
                        SELECT pid, query, age(now(), xact_start) as duration
                        FROM pg_stat_activity
                        WHERE state = 'active' 
                          AND xact_start < now() - interval '5 minutes'
                          AND pid != pg_backend_pid()
                        LIMIT 10
                    """
                    )

                    if long_tx:
                        logger.warning(f"⏱️ 检测到 {len(long_tx)} 个长时间运行的事务:")
                        for tx in long_tx[:3]:
                            logger.warning(
                                f"  • PID: {tx['pid']}, 时长: {tx['duration']}"
                            )
            except Exception as e:
                logger.debug(f"长时间事务检查失败: {e}")

            # ===== 4. 获取连接统计（安全方式）=====
            try:
                async with self.pool.acquire() as conn:
                    # 使用 SQL 查询获取连接统计，完全不依赖内部属性
                    stats = await conn.fetchrow(
                        """
                        SELECT 
                            COUNT(*) as total_connections,
                            COUNT(*) FILTER (WHERE state = 'active') as active_connections,
                            COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
                            COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
                            COUNT(*) FILTER (WHERE wait_event_type = 'Lock') as waiting_connections,
                            MAX(EXTRACT(EPOCH FROM (now() - query_start))) FILTER (WHERE state = 'active') as max_query_seconds
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                    """
                    )

                    if stats:
                        total = stats["total_connections"] or 0
                        active = stats["active_connections"] or 0
                        idle = stats["idle_connections"] or 0
                        idle_tx = stats["idle_in_transaction"] or 0
                        waiting = stats["waiting_connections"] or 0
                        max_query_sec = stats["max_query_seconds"] or 0

                        # 记录统计信息
                        logger.debug(
                            f"📊 数据库连接状态:\n"
                            f"    ├─ 总计: {total}\n"
                            f"    ├─ 活跃: {active}\n"
                            f"    ├─ 空闲: {idle}\n"
                            f"    ├─ 空闲事务: {idle_tx}\n"
                            f"    ├─ 等待锁: {waiting}\n"
                            f"    └─ 最长查询: {max_query_sec:.1f}秒"
                        )

                        # 阈值告警
                        if active > Config.DB_MAX_CONNECTIONS * 0.8:
                            logger.warning(
                                f"⚠️ 活跃连接数过高: {active}/{Config.DB_MAX_CONNECTIONS}"
                            )

                        if waiting > 0:
                            logger.warning(f"⚠️ 有 {waiting} 个查询在等待锁")

                        if idle_tx > 5:
                            logger.warning(f"⚠️ 有 {idle_tx} 个连接在事务中空闲")

                        if max_query_sec > 30:
                            logger.warning(f"⚠️ 存在超过30秒的慢查询")

            except Exception as e:
                logger.debug(f"获取连接统计失败: {e}")

            # ===== 5. 可选：获取连接池配置信息（如果可用）=====
            try:
                if hasattr(self.pool, "_holders") and isinstance(
                    self.pool._holders, list
                ):
                    # _holders 是列表，可以获取长度
                    holder_count = len(self.pool._holders)
                    logger.debug(f"📦 连接池内部: 持有 {holder_count} 个连接")
            except Exception as e:
                # 忽略内部属性访问错误
                pass

        except Exception as e:
            # 最外层异常处理
            logger.error(f"连接池监控失败: {e}")

    async def _connection_maintenance_loop(self):
        """企业级数据库连接维护循环"""

        logger.info("🚀 Database maintenance loop started")

        base_interval = 60
        monitor_interval = 300
        cache_cleanup_interval = 600

        last_monitor_time = 0
        last_cache_cleanup = time.time()
        last_cleanup_hour = -1

        consecutive_failures = 0
        max_backoff = 600

        # ✅ 添加：记录上次成功时间
        last_success_time = time.time()

        while self._maintenance_running:

            try:

                # ===== 动态 sleep（指数退避）=====
                sleep_time = min(base_interval * (2**consecutive_failures), max_backoff)
                await asyncio.sleep(sleep_time)

                current_time = time.time()

                # ===== 1 健康检查 =====
                try:
                    healthy = await self.connection_health_check()
                except Exception as e:
                    logger.error(f"❌ Health check error: {e}")
                    healthy = False

                if not healthy:

                    logger.warning("⚠️ Database unhealthy")

                    try:
                        # ✅ 确保使用正确的方法名
                        if hasattr(self, "_safe_reconnect"):
                            await self._safe_reconnect()
                        else:
                            await self._reconnect()

                        # 再次验证
                        if not await self.connection_health_check():
                            raise RuntimeError("Reconnect validation failed")

                        logger.info("✅ Database reconnected successfully")

                        consecutive_failures = 0
                        last_success_time = current_time
                        continue

                    except Exception as e:

                        consecutive_failures += 1

                        logger.error(
                            f"❌ Reconnect failed "
                            f"(failure #{consecutive_failures}): {e}"
                        )

                        # ✅ 如果长时间无法连接，可以触发告警
                        if current_time - last_success_time > 3600:  # 1小时
                            logger.critical("🚨 Database offline for over 1 hour!")

                        continue

                else:
                    consecutive_failures = 0
                    last_success_time = current_time

                # ===== 2 缓存清理 =====
                if current_time - last_cache_cleanup >= cache_cleanup_interval:

                    try:

                        cache_before = len(self._cache)

                        await self.cleanup_cache()

                        cache_after = len(self._cache)

                        if cache_before != cache_after:
                            logger.debug(
                                f"🧹 Cache cleanup {cache_before} -> {cache_after}"
                            )

                        last_cache_cleanup = current_time

                    except Exception as e:
                        logger.error(f"❌ Cache cleanup failed: {e}")

                # ===== 3 连接池监控 =====
                if current_time - last_monitor_time >= monitor_interval:

                    try:
                        await self._monitor_pool_health()
                    except Exception as e:
                        logger.error(f"❌ Pool monitoring failed: {e}")

                    last_monitor_time = current_time

                # ===== 4 每小时数据维护 =====
                current_hour = datetime.now().hour

                if current_hour != last_cleanup_hour:

                    last_cleanup_hour = current_hour

                    try:

                        deleted = await self.cleanup_old_data(
                            days=Config.DATA_RETENTION_DAYS
                        )

                        logs_deleted = await self.cleanup_old_reset_logs(days=90)

                        if deleted or logs_deleted:

                            logger.info(
                                f"🧹 Hourly cleanup "
                                f"data={deleted}, logs={logs_deleted}"
                            )

                    except Exception as e:
                        logger.error(f"❌ Hourly cleanup failed: {e}")

            except asyncio.CancelledError:

                logger.info("🛑 Maintenance loop cancelled")

                break

            except Exception as e:

                logger.exception(f"❌ Maintenance loop error: {e}")

                consecutive_failures += 1

        logger.info("🏁 Database maintenance loop stopped")

    async def _cleanup_cache_if_needed(self):
        """按需清理缓存"""
        cache_before = len(self._cache)
        # 调用核心清理方法，内部通常实现为遍历过期时间戳
        await self.cleanup_cache()
        cache_after = len(self._cache)

        # 仅在有实际清理动作时记录 debug 日志，保持日志整洁
        if cache_before != cache_after:
            logger.debug(f"🧹 缓存清理: {cache_before} -> {cache_after} (项)")

    async def _perform_hourly_cleanup(self):
        """执行每小时清理任务"""
        try:
            # 清理过期的业务数据（如 30 天前的临时记录）
            daily_deleted = await self.cleanup_old_data(days=Config.DATA_RETENTION_DAYS)
            # 清理过期的重置日志（如 90 天前的管理操作记录）
            logs_deleted = await self.cleanup_old_reset_logs(days=90)

            if daily_deleted > 0 or logs_deleted > 0:
                logger.info(
                    f"🧹 每小时清理完成: "
                    f"业务数据={daily_deleted}, 重置日志={logs_deleted}"
                )
        except Exception as e:
            logger.error(f"❌ 每小时数据清理失败: {e}")

    # ========== 时区相关方法 ==========
    def get_beijing_time(self):
        """获取北京时间"""
        return datetime.now(beijing_tz)

    def get_beijing_date(self):
        """获取北京日期"""
        return self.get_beijing_time().date()

    async def get_business_date_range(
        self, chat_id: int, current_dt: datetime = None
    ) -> Dict[str, date]:
        """获取业务日期范围（双班模式专用）"""
        if current_dt is None:
            current_dt = self.get_beijing_time()

        business_today = await self.get_business_date(chat_id, current_dt)
        business_yesterday = business_today - timedelta(days=1)
        business_day_before = business_today - timedelta(days=2)

        natural_today = current_dt.date()

        logger.debug(
            f"📅 业务日期范围:\n"
            f"   • 自然今天: {natural_today}\n"
            f"   • 业务今天: {business_today}\n"
            f"   • 业务昨天: {business_yesterday}"
        )

        return {
            "business_today": business_today,
            "business_yesterday": business_yesterday,
            "business_day_before": business_day_before,
            "natural_today": natural_today,
        }

    # ========== 初始化方法 ==========
    async def initialize(self):
        """初始化数据库"""
        if self._initialized:
            return

        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"连接PostgreSQL数据库 (尝试 {attempt + 1}/{max_retries})")
                await self._initialize_impl()
                logger.info("PostgreSQL数据库初始化完成")
                self._initialized = True
                return
            except Exception as e:
                logger.warning(f"数据库初始化第 {attempt + 1} 次失败: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"数据库初始化重试{max_retries}次后失败: {e}")
                    try:
                        await self._force_recreate_tables()
                        self._initialized = True
                        logger.info("✅ 数据库表强制重建成功")
                        return
                    except Exception as rebuild_error:
                        logger.error(f"数据库表强制重建失败: {rebuild_error}")
                        raise e
                await asyncio.sleep(2**attempt)

    async def _initialize_impl(self):
        """实际的数据库初始化实现"""
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=Config.DB_MIN_CONNECTIONS,
            max_size=Config.DB_MAX_CONNECTIONS,
            max_inactive_connection_lifetime=Config.DB_POOL_RECYCLE,
            command_timeout=Config.DB_CONNECTION_TIMEOUT,
        )
        logger.info("PostgreSQL连接池创建成功")

        async with self.pool.acquire() as conn:
            await conn.execute("SET statement_timeout = 30000")
            await conn.execute("SET idle_in_transaction_session_timeout = 60000")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self._create_tables()
                await self._create_indexes()
                await self._initialize_default_data()
                logger.info("✅ 数据库表初始化完成")
                break
            except Exception as e:
                logger.warning(f"数据库表初始化第 {attempt + 1} 次失败: {e}")
                if attempt == max_retries - 1:
                    logger.error("数据库表初始化最终失败，尝试强制重建...")
                    await self._force_recreate_tables()
                await asyncio.sleep(1)

    async def _force_recreate_tables(self):
        """强制重新创建所有表"""
        logger.warning("🔄 强制重新创建数据库表...")

        async with self.pool.acquire() as conn:
            tables = [
                "monthly_statistics",
                "activity_user_limits",
                "push_settings",
                "work_fine_configs",
                "fine_configs",
                "activity_configs",
                "work_records",
                "user_activities",
                "users",
                "groups",
            ]

            for table in tables:
                try:
                    await conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    logger.info(f"✅ 删除表: {table}")
                except Exception as e:
                    logger.warning(f"删除表 {table} 失败: {e}")

            await self._create_tables()
            await self._create_indexes()
            await self._initialize_default_data()
            logger.info("🎉 数据库表强制重建完成")

    def _extract_table_name(self, table_sql: str) -> str:
        """安全提取表名"""
        try:
            words = table_sql.upper().split()
            if "TABLE" in words:
                table_index = words.index("TABLE") + 1
                if table_index < len(words) and words[table_index] == "IF":
                    table_index += 3
                elif table_index < len(words) and words[table_index] == "NOT":
                    table_index += 2
                return words[table_index] if table_index < len(words) else "unknown"
        except Exception:
            pass
        return "unknown"

    async def _create_tables(self):
        """创建所有必要的表（纯双班模式，移除软重置相关字段）"""
        async with self.pool.acquire() as conn:
            tables = [
                # 1. groups表
                """
                CREATE TABLE IF NOT EXISTS groups (
                    chat_id BIGINT PRIMARY KEY,
                    channel_id BIGINT,
                    notification_group_id BIGINT,
                    extra_work_notification_group BIGINT,
                    reset_hour INTEGER DEFAULT 0,
                    reset_minute INTEGER DEFAULT 0,
                    work_start_time TEXT DEFAULT '09:00',
                    work_end_time TEXT DEFAULT '18:00',
                    dual_mode BOOLEAN DEFAULT TRUE,
                    dual_day_start TEXT DEFAULT '09:00',
                    dual_day_end TEXT DEFAULT '21:00',
                    shift_grace_before INTEGER DEFAULT 120,
                    shift_grace_after INTEGER DEFAULT 360,
                    workend_grace_before INTEGER DEFAULT 120,
                    workend_grace_after INTEGER DEFAULT 360,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                # 2. users表
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    nickname TEXT,
                    current_activity TEXT,
                    activity_start_time TEXT,
                    shift TEXT DEFAULT 'day',
                    checkin_message_id BIGINT DEFAULT NULL,
                    last_user_activity_message_id BIGINT DEFAULT NULL,
                    last_bot_reply_message_id BIGINT DEFAULT NULL,
                    checkin_message_id BIGINT DEFAULT NULL,
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
                # 3. user_activities表
                """
                CREATE TABLE IF NOT EXISTS user_activities (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    activity_date DATE,
                    activity_name TEXT,
                    activity_count INTEGER DEFAULT 0,
                    accumulated_time INTEGER DEFAULT 0,
                    shift TEXT DEFAULT 'day',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, activity_date, activity_name, shift)
                )
                """,
                # 4. work_records表
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
                    shift TEXT DEFAULT 'day',
                    shift_detail TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, record_date, checkin_type, shift)
                )
                """,
                # 5. activity_configs表
                """
                CREATE TABLE IF NOT EXISTS activity_configs (
                    activity_name TEXT PRIMARY KEY,
                    max_times INTEGER,
                    time_limit INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                # 6. fine_configs表
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
                # 7. work_fine_configs表
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
                # 8. push_settings表
                """
                CREATE TABLE IF NOT EXISTS push_settings (
                    setting_key TEXT PRIMARY KEY,
                    setting_value INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                # 9. monthly_statistics表
                """
                CREATE TABLE IF NOT EXISTS monthly_statistics(
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    statistic_date DATE,
                    shift TEXT DEFAULT 'day',
                    activity_count INTEGER DEFAULT 0,
                    accumulated_time INTEGER DEFAULT 0,
                    fine_amount INTEGER DEFAULT 0,
                    overtime_count INTEGER DEFAULT 0,
                    overtime_time INTEGER DEFAULT 0,
                    work_days INTEGER DEFAULT 0,
                    work_hours INTEGER DEFAULT 0,
                    work_start_count INTEGER DEFAULT 0,  
                    work_end_count INTEGER DEFAULT 0,    
                    work_start_fines INTEGER DEFAULT 0,  
                    work_end_fines INTEGER DEFAULT 0,    
                    late_count INTEGER DEFAULT 0,        
                    early_count INTEGER DEFAULT 0,      
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, statistic_date, shift) 
                )
                """,
                # 10. activity_user_limits表
                """
                CREATE TABLE IF NOT EXISTS activity_user_limits (
                    activity_name TEXT PRIMARY KEY,
                    max_users INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                # 11. daily_statistics表
                """
                CREATE TABLE IF NOT EXISTS daily_statistics(
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT,
                    user_id BIGINT,
                    record_date DATE,
                    shift TEXT DEFAULT 'day',
                    activity_count INTEGER DEFAULT 0,
                    accumulated_time INTEGER DEFAULT 0,
                    fine_amount INTEGER DEFAULT 0,
                    overtime_count INTEGER DEFAULT 0,
                    overtime_time INTEGER DEFAULT 0,
                    work_days INTEGER DEFAULT 0,
                    work_hours INTEGER DEFAULT 0,
                    work_start_count INTEGER DEFAULT 0,
                    work_end_count INTEGER DEFAULT 0,
                    work_start_fines INTEGER DEFAULT 0,
                    work_end_fines INTEGER DEFAULT 0,
                    late_count INTEGER DEFAULT 0,
                    early_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, record_date, shift)
                )
                """,
                # 12. group_shift_state表
                """
                CREATE TABLE IF NOT EXISTS group_shift_state (
                    chat_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    shift TEXT NOT NULL,
                    record_date DATE NOT NULL,
                    shift_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (chat_id, user_id, shift)
                )
                """,
                # 13. shift_handover_configs表
                """
                CREATE TABLE IF NOT EXISTS shift_handover_configs (
                    chat_id BIGINT PRIMARY KEY,
                    handover_enabled BOOLEAN DEFAULT TRUE,
                    handover_day INTEGER DEFAULT 31,
                    handover_month INTEGER DEFAULT 0,
                    night_start_time TEXT DEFAULT '21:00',
                    day_start_time TEXT DEFAULT '09:00',
                    handover_night_hours INTEGER DEFAULT 18,
                    handover_day_hours INTEGER DEFAULT 18,
                    normal_night_hours INTEGER DEFAULT 12,
                    normal_day_hours INTEGER DEFAULT 12,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
                # 14. user_handover_cycles表
                """
                CREATE TABLE IF NOT EXISTS user_handover_cycles (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    handover_date DATE NOT NULL,
                    shift_type TEXT NOT NULL,
                    cycle_number INTEGER DEFAULT 1,
                    cycle_start_time TIMESTAMP,
                    total_work_seconds INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, user_id, handover_date, shift_type, cycle_number)
                )
                """,
                # 15. reset_logs表
                """
                CREATE TABLE IF NOT EXISTS reset_logs (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    reset_date DATE NOT NULL,
                    completed_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, reset_date)
                )
                """,
            ]

            for table_sql in tables:
                try:
                    await conn.execute(table_sql)
                    table_name = self._extract_table_name(table_sql)
                    logger.info(f"✅ 创建表: {table_name}")
                except Exception as e:
                    logger.error(f"❌ 创建表失败: {e}")
                    logger.error(f"失败的SQL: {table_sql[:100]}...")
                    raise

            logger.info("🚀 数据库所有表及字段初始化完成")

    async def _create_indexes(self):
        """优化的索引方案 - 确保统计与清理的高效执行"""
        async with self.pool.acquire() as conn:
            indexes = [
                # 原有索引（核心业务路径）
                "CREATE INDEX IF NOT EXISTS idx_users_primary ON users (chat_id, user_id)",
                "CREATE INDEX IF NOT EXISTS idx_users_current_activity ON users (chat_id, current_activity) WHERE current_activity IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_users_checkin_message ON users (chat_id, checkin_message_id) WHERE checkin_message_id IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_user_activities_main ON user_activities (chat_id, user_id, activity_date, shift)",
                "CREATE INDEX IF NOT EXISTS idx_user_activities_cleanup ON user_activities (chat_id, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_work_records_main ON work_records (chat_id, user_id, record_date, shift)",
                "CREATE INDEX IF NOT EXISTS idx_work_records_night ON work_records (chat_id, user_id, shift, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_work_records_cleanup ON work_records (chat_id, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_daily_stats_main ON daily_statistics (chat_id, record_date, user_id, shift)",
                "CREATE INDEX IF NOT EXISTS idx_monthly_stats_main ON monthly_statistics (chat_id, statistic_date, user_id, shift)",
                "CREATE INDEX IF NOT EXISTS idx_groups_config ON groups (chat_id, dual_mode, reset_hour, reset_minute)",
                "CREATE INDEX IF NOT EXISTS idx_fine_configs_lookup ON fine_configs (activity_name, time_segment)",
                "CREATE INDEX IF NOT EXISTS idx_shift_state ON group_shift_state (chat_id, shift, shift_start_time)",
                # ===== 新增索引：针对统计提速与日志清理 =====
                # 优化 get_group_statistics 中的聚合计算
                "CREATE INDEX IF NOT EXISTS idx_daily_stats_shift ON daily_statistics(chat_id, record_date, shift)",
                # 优化 work_records 的批量范围查询
                "CREATE INDEX IF NOT EXISTS idx_work_records_shift ON work_records(chat_id, record_date, shift)",
                # 优化 cleanup_old_reset_logs 的删除性能
                "CREATE INDEX IF NOT EXISTS idx_reset_logs_date ON reset_logs(chat_id, reset_date)",
            ]

            created_count = 0
            for index_sql in indexes:
                try:
                    await conn.execute(index_sql)
                    created_count += 1
                    # 动态解析索引名称用于日志记录
                    parts = index_sql.split()
                    idx_name = parts[5] if len(parts) > 5 else "unknown_idx"
                    logger.debug(f"✅ 检查/创建索引: {idx_name}")
                except Exception as e:
                    logger.warning(f"⚠️ 创建索引失败 ({index_sql[:30]}...): {e}")

            logger.info(f"🚀 数据库索引优化完成，共处理 {created_count} 个索引项")

    async def _initialize_default_data(self):
        """初始化默认数据"""
        async with self.pool.acquire() as conn:
            for activity, limits in Config.DEFAULT_ACTIVITY_LIMITS.items():
                await conn.execute(
                    "INSERT INTO activity_configs (activity_name, max_times, time_limit) VALUES ($1, $2, $3) ON CONFLICT (activity_name) DO NOTHING",
                    activity,
                    limits["max_times"],
                    limits["time_limit"],
                )
                logger.info(f"✅ 初始化活动配置: {activity}")

            for activity, fines in Config.DEFAULT_FINE_RATES.items():
                for time_segment, amount in fines.items():
                    await conn.execute(
                        "INSERT INTO fine_configs (activity_name, time_segment, fine_amount) VALUES ($1, $2, $3) ON CONFLICT (activity_name, time_segment) DO NOTHING",
                        activity,
                        time_segment,
                        amount,
                    )
                logger.info(f"✅ 初始化罚款配置: {activity}")

            for key, value in Config.AUTO_EXPORT_SETTINGS.items():
                await conn.execute(
                    "INSERT INTO push_settings (setting_key, setting_value) VALUES ($1, $2) ON CONFLICT (setting_key) DO NOTHING",
                    key,
                    1 if value else 0,
                )
                logger.info(f"✅ 初始化推送设置: {key}")

            logger.info("默认数据初始化完成")

    async def health_check(self) -> bool:
        """完整的数据库健康检查"""
        if not self.pool or not self._initialized:
            logger.warning("数据库未初始化")
            return False

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                if result != 1:
                    return False

                critical_tables = ["users", "groups", "activity_configs"]
                for table in critical_tables:
                    try:
                        await conn.fetchval(f"SELECT 1 FROM {table} LIMIT 1")
                    except Exception as e:
                        logger.error(f"❌ 关键表 {table} 访问失败: {e}")
                        return False

                return True
        except Exception as e:
            logger.error(f"❌ 数据库健康检查失败: {e}")
            return False

    # ========== 连接管理 ==========
    def _ensure_pool_initialized(self):
        """确保连接池已初始化"""
        if not self.pool or not self._initialized:
            raise RuntimeError("数据库连接池尚未初始化，请先调用 initialize() 方法")

    async def get_connection(self):
        """获取数据库连接"""
        self._ensure_pool_initialized()
        return await self.pool.acquire()

    async def release_connection(self, conn):
        """释放数据库连接"""
        if self.pool:
            await self.pool.release(conn)

    async def close(self):
        """关闭数据库连接"""
        try:
            if self.pool:
                await self.pool.close()
                logger.info("PostgreSQL连接池已关闭")
        except Exception as e:
            logger.warning(f"关闭数据库连接时出现异常: {e}")
        finally:
            # ✅ 1. 重置初始化标志
            self._initialized = False
            # ✅ 2. 清空连接池引用
            self.pool = None
            # ✅ 3. 重置重连计数（可选）
            self._reconnect_attempts = 0
            logger.debug("数据库连接状态已重置")

    # ========== 缓存管理 ==========
    def _get_cached(self, key: str):
        """增强的缓存获取 - 带访问计数"""
        import time

        current_time = time.time()

        # 检查是否存在且未过期
        if key in self._cache_ttl and current_time < self._cache_ttl[key]:
            # 更新访问次数（用于LRU淘汰）
            if hasattr(self, "_cache_access_count"):
                self._cache_access_count[key] = self._cache_access_count.get(key, 0) + 1

            # 更新最后访问时间
            if hasattr(self, "_cache_last_access"):
                self._cache_last_access[key] = current_time

            return self._cache.get(key)

        # 缓存过期或不存在
        if key in self._cache:
            del self._cache[key]
        if key in self._cache_ttl:
            del self._cache_ttl[key]
        if hasattr(self, "_cache_access_count") and key in self._cache_access_count:
            del self._cache_access_count[key]
        if hasattr(self, "_cache_last_access") and key in self._cache_last_access:
            del self._cache_last_access[key]

        return None

    def _set_cached(self, key: str, value: Any, ttl: int = 30):
        """增强的缓存设置 - 带大小限制"""
        import time

        current_time = time.time()

        # 初始化缓存统计属性
        if not hasattr(self, "_cache_access_count"):
            self._cache_access_count = {}
        if not hasattr(self, "_cache_last_access"):
            self._cache_last_access = {}

        # 检查缓存大小，如果过大则执行LRU淘汰
        max_cache_size = 1000  # 最多缓存1000个用户
        if len(self._cache) >= max_cache_size:
            self._evict_lru_cache()

        self._cache[key] = value
        self._cache_ttl[key] = current_time + ttl
        self._cache_last_access[key] = current_time
        self._cache_access_count[key] = 1

    def _evict_lru_cache(self):
        """LRU缓存淘汰 - 移除最久未使用的10%"""
        if not hasattr(self, "_cache_last_access") or not self._cache_last_access:
            return

        # 按最后访问时间排序
        sorted_items = sorted(self._cache_last_access.items(), key=lambda x: x[1])

        # 淘汰最旧的20%
        evict_count = max(1, len(sorted_items) // 5)
        keys_to_evict = [item[0] for item in sorted_items[:evict_count]]

        for key in keys_to_evict:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
            self._cache_last_access.pop(key, None)
            if hasattr(self, "_cache_access_count"):
                self._cache_access_count.pop(key, None)

        logger.debug(f"LRU淘汰: 移除了 {evict_count} 个缓存项")

    async def preload_user_cache(self, chat_id: int, user_ids: List[int]):
        """预加载用户缓存 - 批量预热"""
        if not user_ids:
            return

        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT 
                        user_id, nickname, current_activity, activity_start_time,
                        total_accumulated_time, total_activity_count, total_fines,
                        overtime_count, total_overtime_time, last_updated,
                        checkin_message_id, shift
                    FROM users 
                    WHERE chat_id = $1 AND user_id = ANY($2::bigint[])
                """,
                    chat_id,
                    user_ids,
                )

                for row in rows:
                    cache_key = f"user:{chat_id}:{row['user_id']}"
                    result = dict(row)
                    self._set_cached(cache_key, result, 30)

                logger.debug(f"预加载了 {len(rows)} 个用户缓存")

        except Exception as e:
            logger.error(f"预加载用户缓存失败: {e}")

    async def cleanup_cache(self):
        """增强的缓存清理"""
        current_time = time.time()
        expired_keys = [
            key for key, expiry in self._cache_ttl.items() if current_time >= expiry
        ]

        for key in expired_keys:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
            if key in self._cache_access_order:
                self._cache_access_order.remove(key)

        if len(self._cache) > self._cache_max_size * 0.8:
            excess = len(self._cache) - int(self._cache_max_size * 0.7)
            if excess > 0 and self._cache_access_order:
                keys_to_remove = self._cache_access_order[:excess]
                for key in keys_to_remove:
                    self._cache.pop(key, None)
                    self._cache_ttl.pop(key, None)
                self._cache_access_order = self._cache_access_order[excess:]
                logger.info(f"LRU强制清理: 移除了 {len(keys_to_remove)} 个旧缓存")

        if expired_keys:
            logger.debug(
                f"缓存清理完成: {len(expired_keys)}个过期, 当前大小: {len(self._cache)}"
            )

    async def force_refresh_activity_cache(self):
        """强制刷新活动配置缓存"""
        cache_keys_to_remove = ["activity_limits", "push_settings", "fine_rates"]
        for key in cache_keys_to_remove:
            self._cache.pop(key, None)
            self._cache_ttl.pop(key, None)
        await self.get_activity_limits()
        await self.get_fine_rates()
        logger.info("活动配置缓存已强制刷新")

    # ========== 锁管理 ==========
    async def _get_lock(self, key: str) -> asyncio.Lock:
        """获取或创建命名锁，支持细粒度的并发控制"""
        async with self._locks_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()

            # 触发定期清理旧锁的任务
            current_time = time.time()
            if current_time - self._last_lock_cleanup > self._lock_cleanup_interval:
                # 异步执行，不阻塞当前请求
                asyncio.create_task(self._cleanup_old_locks())

            return self._locks[key]

    async def _cleanup_old_locks(self):
        """清理不再使用的锁，防止随着 key 的增加导致内存溢出"""
        # 更新清理时间，防止多个任务重叠执行
        self._last_lock_cleanup = time.time()

        try:
            async with self._locks_lock:
                keys_to_remove = [
                    k for k, lock in self._locks.items() if not lock.locked()
                ]

                for k in keys_to_remove:
                    del self._locks[k]

                if keys_to_remove:
                    logger.debug(
                        f"🧹 锁清理完成: 移除了 {len(keys_to_remove)} 个空闲锁"
                    )
        except Exception as e:
            logger.error(f"❌ 清理旧锁时出现异常: {e}")

    # ========== 群组相关操作 ==========
    async def init_group(self, chat_id: int):
        """初始化群组 - 默认开启双班模式"""
        await self.execute_with_retry(
            "初始化群组",
            "INSERT INTO groups (chat_id, dual_mode) VALUES ($1, TRUE) ON CONFLICT (chat_id) DO NOTHING",
            chat_id,
        )
        self._cache.pop(f"group:{chat_id}", None)

    async def get_group(self, chat_id: int) -> Optional[Dict]:
        """获取群组配置"""
        cache_key = f"group:{chat_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM groups WHERE chat_id = $1", chat_id
            )
            if row:
                result = dict(row)
                self._set_cached(cache_key, result, 300)
                return result
            return None

    async def get_group_cached(self, chat_id: int) -> Optional[Dict]:
        """带缓存的获取群组配置 - 加强版"""
        cache_key = f"group:{chat_id}"

        # 1. 一级缓存：内存缓存（LRU/TTL 机制）
        cached = self._get_cached(cache_key)

        # ✅ 正确修复：缓存的值可能是 None（表示之前查询不到）或字典
        # 只要 cached 不是 None，就说明缓存命中（即使缓存的是 None 本身）
        if cached is not None:
            return cached

        # 2. 二级缓存：检查是否有正在进行的相同查询（Singleflight 模式）
        pending_key = f"pending_group:{chat_id}"
        if hasattr(self, "_pending_queries") and pending_key in self._pending_queries:
            try:
                # 共享已有的异步查询任务，避免并发击穿数据库
                result = await self._pending_queries[pending_key]
                return result  # 直接返回，可能是 None 或字典
            except Exception as e:
                logger.warning(f"⚠️ 共享查询任务失败 {chat_id}: {e}")
                # 共享失败时，继续执行新的查询逻辑
                pass

        # 3. 定义实际的数据库获取逻辑
        async def _fetch_group():
            try:
                # 确保数据库连接池已准备就绪
                if not self.pool or not self._initialized:
                    await self.initialize()

                # 从连接池获取连接并执行查询
                async with self.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT * FROM groups WHERE chat_id = $1", chat_id
                    )

                    # 明确处理查询结果
                    if row:
                        result = dict(row)
                        logger.debug(f"✅ 获取群组 {chat_id} 配置成功")
                    else:
                        result = None
                        logger.debug(f"ℹ️ 群组 {chat_id} 在数据库中不存在")

                    # 4. 写入缓存：应用随机抖动 (Jitter)
                    import random

                    if result:
                        # 正常配置数据：缓存约 10 分钟
                        ttl = 600 + random.randint(-60, 60)
                    else:
                        # 负缓存：对不存在的群组缓存约 1 分钟，防止缓存穿透攻击
                        ttl = 60 + random.randint(-10, 10)

                    self._set_cached(cache_key, result, ttl)
                    return result

            except Exception as e:
                logger.error(f"❌ 获取群组配置失败 {chat_id}: {e}")
                # 异常熔断：出错时缓存 None 60秒，避免故障期间高频重试压垮数据库
                self._set_cached(cache_key, None, 60)
                return None

        # 5. 任务调度管理
        if not hasattr(self, "_pending_queries"):
            self._pending_queries = {}

        # 创建异步任务并注册到 pending 字典中
        task = asyncio.create_task(_fetch_group())
        self._pending_queries[pending_key] = task

        try:
            result = await task
            return result  # 这里的返回由 _fetch_group 保证类型安全性
        finally:
            # 无论执行成败，必须清理 pending 记录，允许后续请求再次查询
            self._pending_queries.pop(pending_key, None)

    async def update_group_channel(self, chat_id: int, channel_id: int):
        """更新群组频道ID"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET channel_id = $1, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $2",
                channel_id,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def update_group_notification(self, chat_id: int, group_id: int):
        """更新群组通知群组ID"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET notification_group_id = $1, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $2",
                group_id,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def update_group_reset_time(self, chat_id: int, hour: int, minute: int):
        """更新群组重置时间"""
        self._ensure_pool_initialized()
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
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE groups SET work_start_time = $1, work_end_time = $2, updated_at = CURRENT_TIMESTAMP WHERE chat_id = $3",
                work_start,
                work_end,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def update_group_extra_work_group(
        self, chat_id: int, extra_work_group_id: int
    ):
        """设置额外的上下班通知群组"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE groups 
                SET extra_work_notification_group = $1, 
                    updated_at = CURRENT_TIMESTAMP 
                WHERE chat_id = $2
                """,
                extra_work_group_id,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def get_extra_work_group(self, chat_id: int) -> Optional[int]:
        """获取额外的上下班通知群组ID"""
        group_data = await self.get_group_cached(chat_id)
        return group_data.get("extra_work_notification_group") if group_data else None

    async def clear_extra_work_group(self, chat_id: int):
        """清除额外的上下班通知群组"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE groups 
                SET extra_work_notification_group = NULL, 
                    updated_at = CURRENT_TIMESTAMP 
                WHERE chat_id = $1
                """,
                chat_id,
            )
            self._cache.pop(f"group:{chat_id}", None)

    async def get_group_work_time(self, chat_id: int) -> Dict[str, str]:
        """获取群组上下班时间 - 带缓存"""
        cache_key = f"work_time:{chat_id}"

        # 1. 一级缓存校验
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # 2. 执行带重试的数据库查询
        row = await self.execute_with_retry(
            "获取工作时间",
            "SELECT work_start_time, work_end_time FROM groups WHERE chat_id = $1",
            chat_id,
            fetchrow=True,
        )

        # 3. 结果解析与兜底策略
        result = {}
        if row and row["work_start_time"] and row["work_end_time"]:
            result = {
                "work_start": row["work_start_time"],
                "work_end": row["work_end_time"],
            }
        else:
            # 如果数据库未配置，使用 Config 中的全局默认值
            result = Config.DEFAULT_WORK_HOURS.copy()

        # 4. 写入缓存，TTL 设置为 300秒（5分钟）
        self._set_cached(cache_key, result, 300)
        return result

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
        await self.execute_with_retry(
            "初始化用户",
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

    async def update_user_last_updated(
        self, chat_id: int, user_id: int, update_date: date
    ):
        """更新用户最后更新时间"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET last_updated = $1 WHERE chat_id = $2 AND user_id = $3",
                update_date,
                chat_id,
                user_id,
            )

    async def get_user(self, chat_id: int, user_id: int) -> Optional[Dict]:
        """高性能获取用户数据 - 带二级缓存和查询优化"""

        # ===== 1. 一级缓存：内存缓存（最快） =====
        cache_key = f"user:{chat_id}:{user_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            # 缓存命中，记录统计
            if hasattr(self, "_cache_hits"):
                self._cache_hits += 1
            return cached

        # ===== 2. 二级缓存：检查是否有正在进行的查询（防止缓存击穿） =====
        pending_key = f"pending:{chat_id}:{user_id}"
        if hasattr(self, "_pending_queries") and pending_key in self._pending_queries:
            try:
                # 等待正在进行的查询结果
                return await self._pending_queries[pending_key]
            except Exception:
                pass

        # ===== 3. 创建查询任务（带超时和重试） =====
        async def _execute_query():
            try:
                # 使用更精确的字段选择（只选需要的）
                row = await self.execute_with_retry(
                    "获取用户数据",
                    """
                    SELECT 
                        user_id, 
                        nickname, 
                        current_activity, 
                        activity_start_time, 
                        total_accumulated_time, 
                        total_activity_count, 
                        total_fines,
                        overtime_count, 
                        total_overtime_time, 
                        last_updated,
                        checkin_message_id, 
                        shift
                    FROM users 
                    WHERE chat_id = $1 AND user_id = $2
                    LIMIT 1
                    """,
                    chat_id,
                    user_id,
                    fetchrow=True,
                    timeout=3,
                    slow_threshold=0.3,
                )

                if row:
                    result = dict(row)

                    # ===== 4. 数据验证和修复 =====
                    if "shift" not in result or result["shift"] is None:
                        active_shift = await self.get_user_active_shift(
                            chat_id, user_id
                        )
                        result["shift"] = (
                            active_shift.get("shift", "day") if active_shift else "day"
                        )
                        logger.debug(f"修复用户 {user_id} 的班次为: {result['shift']}")

                    if result.get("last_updated") and isinstance(
                        result["last_updated"], datetime
                    ):
                        result["last_updated"] = result["last_updated"].date()

                    # ===== 5. 写入缓存（带随机TTL防止缓存雪崩） =====
                    import random

                    cache_ttl = 30 + random.randint(-5, 5)
                    self._set_cached(cache_key, result, cache_ttl)

                    return result

                # 用户不存在，缓存空结果防止穿透
                self._set_cached(cache_key, None, 10)
                return None

            except asyncio.TimeoutError:
                logger.error(f"⏱️ 获取用户数据超时: {chat_id}-{user_id}")
                return {
                    "user_id": user_id,
                    "nickname": f"用户{user_id}",
                    "shift": "day",
                    "current_activity": None,
                    "total_accumulated_time": 0,
                    "total_activity_count": 0,
                    "total_fines": 0,
                    "overtime_count": 0,
                    "total_overtime_time": 0,
                    "checkin_message_id": None,
                }
            except Exception as e:
                logger.error(f"❌ 获取用户数据失败 {chat_id}-{user_id}: {e}")
                raise

        # ===== 6. 执行查询并缓存任务（防止并发重复查询） =====
        if not hasattr(self, "_pending_queries"):
            self._pending_queries = {}

        self._pending_queries[pending_key] = asyncio.create_task(_execute_query())

        try:
            result = await self._pending_queries[pending_key]
            return result
        finally:
            self._pending_queries.pop(pending_key, None)

    async def get_user_activity_count_by_shift(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        shift: Optional[str] = None,
        query_date: Optional[date] = None,
    ) -> int:
        """按班次获取用户活动次数"""
        if not isinstance(chat_id, int):
            raise TypeError(
                f"❌ chat_id 必须是 int，但收到了 {type(chat_id)}: {chat_id}"
            )

        if not isinstance(user_id, int):
            raise TypeError(
                f"❌ user_id 必须是 int，但收到了 {type(user_id)}: {user_id}"
            )

        if not isinstance(activity, str):
            raise TypeError(
                f"❌ activity 必须是 str，但收到了 {type(activity)}: {activity}"
            )

        if query_date is not None and not isinstance(query_date, date):
            raise TypeError(
                f"❌ query_date 必须是 date 类型，但收到了 {type(query_date)}: {query_date}"
            )

        if shift is not None and not isinstance(shift, str):
            if isinstance(shift, dict):
                error_msg = (
                    f"❌ shift 参数错误：传入了字典，但期望字符串或 None\n"
                    f"   收到的字典: {shift}\n"
                    f"   你应该从字典中提取 'shift' 字段，例如：shift_info.get('shift')\n"
                )
                import traceback

                error_msg += "".join(traceback.format_stack()[:-1])
                logger.error(error_msg)
                raise TypeError("shift 参数必须是字符串或 None，不能是字典")
            else:
                raise TypeError(
                    f"❌ shift 必须是 str 或 None，但收到了 {type(shift)}: {shift}"
                )

        if query_date:
            target_date = query_date
            logger.debug(f"📅 使用传入查询日期: {target_date}")
        else:
            target_date = await self.get_business_date(chat_id)
            if shift == "night":
                current_hour = self.get_beijing_time().hour
                if current_hour < 12:
                    target_date = target_date - timedelta(days=1)
                    logger.info(
                        f"🌙 [自动调整] 夜班凌晨查询: "
                        f"原始业务日期={target_date + timedelta(days=1)}, "
                        f"调整后={target_date}"
                    )
            logger.debug(f"📅 使用业务日期: {target_date}")

        query = """
            SELECT activity_count
            FROM user_activities
            WHERE chat_id = $1
              AND user_id = $2
              AND activity_date = $3
              AND activity_name = $4
        """
        params = [chat_id, user_id, target_date, activity]

        final_shift = None
        if shift is not None:
            shift = shift.strip()
            if shift in {"night_last", "night_tonight"}:
                logger.debug(f"🔄 自动转换班次值: {shift} -> night")
                final_shift = "night"
            elif shift in {"day", "night"}:
                final_shift = shift
            else:
                raise ValueError(
                    f"❌ 无效的班次值: '{shift}'，必须是 'day', 'night', 或 None"
                )

            query += " AND shift = $5"
            params.append(final_shift)

        logger.debug(
            f"🔎 查询活动次数: chat_id={chat_id}, "
            f"user_id={user_id}, date={target_date}, "
            f"activity={activity}, shift={final_shift or '所有班次'}"
        )

        try:
            count = await self.execute_with_retry(
                "按班次获取活动次数",
                query,
                *params,
                fetchval=True,
            )
        except Exception as e:
            logger.error(f"❌ 数据库查询失败: {e}")
            raise

        return count if count is not None else 0

    async def get_user_cached(self, chat_id: int, user_id: int) -> Optional[Dict]:
        """带缓存的获取用户数据"""
        cache_key = f"user:{chat_id}:{user_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        row = await self.execute_with_retry(
            "获取用户数据",
            """
            SELECT user_id, nickname, current_activity, activity_start_time, 
                   total_accumulated_time, total_activity_count, total_fines,
                   overtime_count, total_overtime_time, last_updated, 
                   checkin_message_id, shift
            FROM users 
            WHERE chat_id = $1 AND user_id = $2
            """,
            chat_id,
            user_id,
            fetchrow=True,
        )

        if row:
            result = dict(row)
            if "shift" not in result or result["shift"] is None:
                result["shift"] = "day"
                logger.warning(f"用户 {user_id} 的 shift 字段为 None，使用默认值 'day'")

            self._set_cached(cache_key, result, 30)
            logger.debug(f"获取用户缓存: {user_id}, shift={result['shift']}")
            return result
        return None

    async def update_user_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        start_time: str,
        nickname: str = None,
        shift: str = "day",
    ):
        """更新用户活动状态"""
        try:
            original_type = type(start_time).__name__

            if hasattr(start_time, "isoformat"):
                if start_time.tzinfo is None:
                    start_time = beijing_tz.localize(start_time)
                start_time_str = start_time.isoformat()

            elif isinstance(start_time, str):
                try:
                    clean_str = start_time.strip()

                    if clean_str.endswith("Z"):
                        clean_str = clean_str.replace("Z", "+00:00")

                    dt = datetime.fromisoformat(clean_str)

                    if dt.tzinfo is None:
                        dt = beijing_tz.localize(dt)

                    start_time_str = dt.isoformat()

                except ValueError:
                    start_time_str = start_time
                    logger.warning(f"⚠️ 时间字符串格式可能无效: {start_time_str}")

            else:
                start_time_str = str(start_time)
                logger.debug(
                    f"🔄 转换其他类型为字符串: {original_type} -> {start_time_str}"
                )

            logger.info(
                f"💾 保存活动时间: 用户{user_id}, 活动{activity}, 标准化时间: {start_time_str}, 班次: {shift}"
            )

            if nickname:
                await self.execute_with_retry(
                    "更新用户活动",
                    """
                    UPDATE users 
                    SET current_activity = $1, activity_start_time = $2, nickname = $3, shift = $4, updated_at = CURRENT_TIMESTAMP 
                    WHERE chat_id = $5 AND user_id = $6
                    """,
                    activity,
                    start_time_str,
                    nickname,
                    shift,
                    chat_id,
                    user_id,
                )
            else:
                await self.execute_with_retry(
                    "更新用户活动",
                    """
                    UPDATE users 
                    SET current_activity = $1, activity_start_time = $2, shift = $3, updated_at = CURRENT_TIMESTAMP 
                    WHERE chat_id = $4 AND user_id = $5
                    """,
                    activity,
                    start_time_str,
                    shift,
                    chat_id,
                    user_id,
                )

            self._cache.pop(f"user:{chat_id}:{user_id}", None)

            logger.debug(
                f"✅ 用户活动更新成功: {chat_id}-{user_id} -> {activity}（班次: {shift}）"
            )

        except Exception as e:
            logger.error(f"❌ 更新用户活动失败 {chat_id}-{user_id}: {e}")
            logger.error(
                f"❌ 失败时的参数 - activity: {activity}, start_time: {start_time}, nickname: {nickname}, shift: {shift}"
            )
            raise

    async def update_user_checkin_message(
        self, chat_id: int, user_id: int, message_id: int
    ):
        """更新用户的打卡消息ID"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE users 
                SET checkin_message_id = $1, updated_at = CURRENT_TIMESTAMP 
                WHERE chat_id = $2 AND user_id = $3
                """,
                message_id,
                chat_id,
                user_id,
            )
        cache_key = f"user:{chat_id}:{user_id}"
        self._cache.pop(cache_key, None)
        self._cache_ttl.pop(cache_key, None)
        logger.info(f"✅ 已更新用户 {user_id} 的打卡消息ID为 {message_id}，并清除缓存")

    async def get_user_checkin_message_id(
        self, chat_id: int, user_id: int
    ) -> Optional[int]:
        """获取用户的打卡消息ID"""
        user_data = await self.get_user_cached(chat_id, user_id)
        return user_data.get("checkin_message_id") if user_data else None

    async def clear_user_checkin_message(self, chat_id: int, user_id: int):
        """清除用户的打卡消息ID"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE users 
                SET checkin_message_id = NULL, updated_at = CURRENT_TIMESTAMP 
                WHERE chat_id = $1 AND user_id = $2
                """,
                chat_id,
                user_id,
            )
        self._cache.pop(f"user:{chat_id}:{user_id}", None)

    async def update_last_user_activity_message_id(
        self, chat_id: int, user_id: int, message_id: int
    ):
        """更新用户最后的活动消息ID（用于下次活动引用）"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE users 
                SET last_user_activity_message_id = $1, updated_at = CURRENT_TIMESTAMP 
                WHERE chat_id = $2 AND user_id = $3
                """,
                message_id,
                chat_id,
                user_id,
            )
        cache_key = f"user:{chat_id}:{user_id}"
        self._cache.pop(cache_key, None)
        self._cache_ttl.pop(cache_key, None)

    async def get_last_user_activity_message_id(
        self, chat_id: int, user_id: int
    ) -> Optional[int]:
        """获取用户最后的活动消息ID"""
        user_data = await self.get_user_cached(chat_id, user_id)
        if user_data:
            return user_data.get("last_user_activity_message_id")
        return None

    async def update_last_bot_reply_message_id(
        self, chat_id: int, user_id: int, message_id: int, is_checkin: bool = True
    ):
        """
        更新机器人最后的回复消息ID
        is_checkin=True: 打卡消息，用于下次回座引用
        is_checkin=False: 回座消息，用于下次活动引用
        """
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            if is_checkin:
                # 打卡消息：保存到 checkin_message_id
                await conn.execute(
                    """
                    UPDATE users 
                    SET checkin_message_id = $1, updated_at = CURRENT_TIMESTAMP 
                    WHERE chat_id = $2 AND user_id = $3
                    """,
                    message_id,
                    chat_id,
                    user_id,
                )
            else:
                # 回座消息：保存到 last_bot_reply_message_id
                await conn.execute(
                    """
                    UPDATE users 
                    SET last_bot_reply_message_id = $1, updated_at = CURRENT_TIMESTAMP 
                    WHERE chat_id = $2 AND user_id = $3
                    """,
                    message_id,
                    chat_id,
                    user_id,
                )
        cache_key = f"user:{chat_id}:{user_id}"
        self._cache.pop(cache_key, None)
        self._cache_ttl.pop(cache_key, None)

    async def get_last_bot_reply_message_id(
        self, chat_id: int, user_id: int
    ) -> Optional[int]:
        """获取机器人最后的回复消息ID（回座消息）"""
        user_data = await self.get_user_cached(chat_id, user_id)
        if user_data:
            return user_data.get("last_bot_reply_message_id")
        return None

    async def clear_user_message_ids(self, chat_id: int, user_id: int):
        """清除用户的所有消息ID（可选，用于重置）"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE users 
                SET checkin_message_id = NULL,
                    last_user_activity_message_id = NULL,
                    last_bot_reply_message_id = NULL,
                    updated_at = CURRENT_TIMESTAMP 
                WHERE chat_id = $1 AND user_id = $2
                """,
                chat_id,
                user_id,
            )
        cache_key = f"user:{chat_id}:{user_id}"
        self._cache.pop(cache_key, None)
        self._cache_ttl.pop(cache_key, None)

    # ====== 核心业务方法 ======
    async def complete_user_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        elapsed_time: int,
        fine_amount: int = 0,
        is_overtime: bool = False,
        shift: Optional[str] = None,
        forced_date: Optional[date] = None,
        max_retries: int = 3,
        transaction_timeout: float = 10.0,
    ) -> None:
        """最终企业级绝对安全版"""

        # 1. 确定班次
        if shift is None:
            user_shift_state = await self.get_user_active_shift(chat_id, user_id)
            if user_shift_state:
                shift = user_shift_state["shift"]
            else:
                now = self.get_beijing_time()
                shift_info = await self.determine_shift_for_time(chat_id, now)
                shift = shift_info.get("shift", "day") if shift_info else "day"

        # 2. 确定日期
        target_date = forced_date or await self.get_business_date(chat_id)
        statistic_date = target_date.replace(day=1)

        # 3. 计算超时
        time_limit = await self.get_activity_time_limit(activity)
        overtime_seconds = max(0, elapsed_time - time_limit * 60) if is_overtime else 0

        # 4. 确保连接池
        self._ensure_pool_initialized()

        # 5. 准备参数
        user_params = (
            chat_id,
            user_id,
            target_date,
            elapsed_time,
            fine_amount,
            1 if is_overtime else 0,
            overtime_seconds,
        )

        daily_params = (
            chat_id,
            user_id,
            target_date,
            shift,
            elapsed_time,
            fine_amount,
            1 if is_overtime else 0,
            overtime_seconds,
        )

        monthly_params = (
            chat_id,
            user_id,
            statistic_date,
            shift,
            elapsed_time,
            fine_amount,
            1 if is_overtime else 0,
            overtime_seconds,
        )

        activity_params = (
            chat_id,
            user_id,
            target_date,
            activity,
            1,  # activity_count
            elapsed_time,
            shift,
        )

        # 6. 重试事务
        attempt = 0
        while attempt < max_retries:
            try:
                async with asyncio.timeout(transaction_timeout):
                    async with self.pool.acquire() as conn:
                        async with conn.transaction():

                            # 6.1 确保用户存在
                            await conn.execute(
                                """
                                INSERT INTO users (chat_id, user_id, nickname, last_updated)
                                VALUES ($1, $2, $3, $4)
                                ON CONFLICT (chat_id, user_id) DO NOTHING
                                """,
                                chat_id,
                                user_id,
                                f"用户{user_id}",
                                target_date,
                            )

                            # 6.2 更新用户统计
                            await conn.execute(
                                """
                                UPDATE users SET
                                    total_accumulated_time = total_accumulated_time + $4,
                                    total_activity_count = total_activity_count + 1,
                                    total_fines = total_fines + $5,
                                    overtime_count = overtime_count + $6,
                                    total_overtime_time = total_overtime_time + $7,
                                    current_activity = NULL,
                                    activity_start_time = NULL,
                                    checkin_message_id = NULL,
                                    last_updated = $3,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE chat_id = $1 AND user_id = $2
                                """,
                                *user_params,
                            )

                            # 6.3 更新日统计
                            await conn.execute(
                                """
                                INSERT INTO daily_statistics (
                                    chat_id, user_id, record_date, shift,
                                    activity_count, accumulated_time, fine_amount,
                                    overtime_count, overtime_time
                                )
                                VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8)
                                ON CONFLICT (chat_id, user_id, record_date, shift)
                                DO UPDATE SET
                                    activity_count = daily_statistics.activity_count + 1,
                                    accumulated_time = daily_statistics.accumulated_time + $5,
                                    fine_amount = daily_statistics.fine_amount + $6,
                                    overtime_count = daily_statistics.overtime_count + $7,
                                    overtime_time = daily_statistics.overtime_time + $8,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                *daily_params,
                            )

                            # 6.4 更新月统计
                            await conn.execute(
                                """
                                INSERT INTO monthly_statistics (
                                    chat_id, user_id, statistic_date, shift,
                                    activity_count, accumulated_time, fine_amount,
                                    overtime_count, overtime_time
                                )
                                VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8)
                                ON CONFLICT (chat_id, user_id, statistic_date, shift)
                                DO UPDATE SET
                                    activity_count = monthly_statistics.activity_count + 1,
                                    accumulated_time = monthly_statistics.accumulated_time + $5,
                                    fine_amount = monthly_statistics.fine_amount + $6,
                                    overtime_count = monthly_statistics.overtime_count + $7,
                                    overtime_time = monthly_statistics.overtime_time + $8,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                *monthly_params,
                            )
                            # 6.5 更新user_activities 表
                            await conn.execute(
                                """
                                INSERT INTO user_activities (
                                    chat_id, user_id, activity_date, activity_name,
                                    activity_count, accumulated_time, shift
                                )
                                VALUES ($1, $2, $3, $4, $5, $6, $7)
                                ON CONFLICT (chat_id, user_id, activity_date, activity_name, shift)
                                DO UPDATE SET
                                    activity_count = user_activities.activity_count + EXCLUDED.activity_count,
                                    accumulated_time = user_activities.accumulated_time + EXCLUDED.accumulated_time,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                *activity_params,
                            )

                # 7. 清理缓存
                cache_key = f"user:{chat_id}:{user_id}"
                self._cache.pop(cache_key, None)
                self._cache_ttl.pop(cache_key, None)

                # 8. 日志记录
                logger.info(
                    f"✅ 活动完成 [群:{chat_id} 用户:{user_id}] {activity}\n"
                    f"   ├─ 日期: {target_date}{' [强制]' if forced_date else ''}\n"
                    f"   ├─ 班次: {shift}\n"
                    f"   ├─ 时长: {elapsed_time}秒\n"
                    f"   ├─ 罚款: {fine_amount}元\n"
                    f"   ├─ 超时: {'是' if is_overtime else '否'} ({overtime_seconds}秒)\n"
                    f"   └─ 统计月份: {statistic_date}"
                )

                break  # 成功

            except (SerializationError, PostgresError) as e:
                attempt += 1
                if attempt >= max_retries:
                    logger.error(f"❌ 活动最终失败，已重试 {max_retries} 次: {e}")
                    raise

                sleep_time = 0.1 * (2 ** (attempt - 1)) + random.uniform(0, 0.05)
                logger.warning(
                    f"⚠️ 数据库异常，第{attempt}次重试，延迟 {sleep_time:.2f}s\n"
                    f"   └─ 错误: {e}"
                )
                await asyncio.sleep(sleep_time)

            except Exception as e:
                logger.error(f"❌ 非数据库异常: {type(e).__name__} - {e}")
                raise

    # ========= 重置前批量完成所有未结束活动 =========
    async def complete_all_pending_activities_before_reset(
        self, chat_id: int, reset_time: datetime
    ) -> Dict[str, Any]:
        """在重置前批量完成所有未结束活动"""
        try:
            completed_count = 0
            total_fines = 0

            self._ensure_pool_initialized()
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # ✅ 修复：查询时包含 shift 字段
                    active_users = await conn.fetch(
                        """
                        SELECT user_id, nickname, current_activity, 
                               activity_start_time, shift
                        FROM users 
                        WHERE chat_id = $1 AND current_activity IS NOT NULL
                        """,
                        chat_id,
                    )

                    if not active_users:
                        return {"completed_count": 0, "total_fines": 0, "details": []}

                    completion_details = []
                    statistic_date = reset_time.date().replace(day=1)

                    for user in active_users:
                        user_id = user["user_id"]
                        nickname = user["nickname"]
                        activity = user["current_activity"]
                        start_time_str = user["activity_start_time"]
                        shift = user.get("shift", "day")  # ✅ 获取班次

                        try:
                            start_time = datetime.fromisoformat(start_time_str)
                            elapsed = int((reset_time - start_time).total_seconds())

                            # 获取活动时间限制
                            time_limit = await self.get_activity_time_limit(activity)
                            time_limit_seconds = time_limit * 60
                            is_overtime = elapsed > time_limit_seconds
                            overtime_seconds = max(0, elapsed - time_limit_seconds)
                            overtime_minutes = overtime_seconds / 60

                            # 计算罚款
                            fine_amount = 0
                            if is_overtime and overtime_seconds > 0:
                                fine_amount = await self.calculate_fine_for_activity(
                                    activity, overtime_minutes
                                )

                            # ✅ 修复：传入 activity_date 和 shift
                            await self._update_monthly_statistics_for_activity(
                                conn=conn,
                                chat_id=chat_id,
                                user_id=user_id,
                                statistic_date=statistic_date,
                                activity=activity,
                                elapsed=elapsed,
                                fine_amount=fine_amount,
                                is_overtime=is_overtime,
                                overtime_seconds=overtime_seconds,
                                shift=shift,  # ✅ 传入班次
                                activity_date=start_time.date(),  # ✅ 传入实际活动日期
                            )

                            completed_count += 1
                            total_fines += fine_amount

                            completion_details.append(
                                {
                                    "user_id": user_id,
                                    "nickname": nickname,
                                    "activity": activity,
                                    "elapsed_time": elapsed,
                                    "fine_amount": fine_amount,
                                    "is_overtime": is_overtime,
                                }
                            )

                            logger.info(
                                f"重置前结束活动: {chat_id}-{user_id} - {activity} "
                                f"(时长: {elapsed}秒, 罚款: {fine_amount}元, 班次: {shift})"
                            )

                        except Exception as e:
                            logger.error(f"结束用户活动失败 {chat_id}-{user_id}: {e}")

                    # 清理用户活动状态
                    await conn.execute(
                        """
                        UPDATE users 
                        SET current_activity = NULL, activity_start_time = NULL 
                        WHERE chat_id = $1 AND current_activity IS NOT NULL
                        """,
                        chat_id,
                    )

                    return {
                        "completed_count": completed_count,
                        "total_fines": total_fines,
                        "details": completion_details,
                    }

        except Exception as e:
            logger.error(f"批量结束活动失败 {chat_id}: {e}")
            return {"completed_count": 0, "total_fines": 0, "details": []}

    async def _update_monthly_statistics_for_activity(
        self,
        conn,
        chat_id: int,
        user_id: int,
        statistic_date: date,
        activity: str,
        elapsed: int,
        fine_amount: int,
        is_overtime: bool,
        overtime_seconds: int,
        shift: str = "day",
        activity_date: Optional[date] = None,
    ) -> None:
        """
        更新月度统计的辅助方法 - 完整版

        Args:
            conn: 数据库连接
            chat_id: 群组ID
            user_id: 用户ID
            statistic_date: 统计月份的第一天
            activity: 活动名称
            elapsed: 活动时长（秒）
            fine_amount: 罚款金额
            is_overtime: 是否超时
            overtime_seconds: 超时时长（秒）
            shift: 班次 (day/night)
            activity_date: 活动实际发生的日期（必须提供）

        Raises:
            ValueError: 如果 activity_date 为 None
        """

        # ===== 1. 参数验证 =====
        if activity_date is None:
            raise ValueError(
                f"activity_date is required for user_activities update. "
                f"chat_id={chat_id}, user_id={user_id}, activity={activity}"
            )

        # 确保日期类型正确
        if not isinstance(activity_date, date):
            try:
                activity_date = datetime.strptime(str(activity_date), "%Y-%m-%d").date()
            except (ValueError, TypeError):
                raise ValueError(
                    f"activity_date must be a date object, got {type(activity_date)}"
                )

        if not isinstance(statistic_date, date):
            try:
                statistic_date = datetime.strptime(
                    str(statistic_date), "%Y-%m-%d"
                ).date()
            except (ValueError, TypeError):
                raise ValueError(
                    f"statistic_date must be a date object, got {type(statistic_date)}"
                )

        # 确保统计日期是月份的第一天
        if statistic_date.day != 1:
            logger.warning(
                f"statistic_date is not the first day of month: {statistic_date}, "
                f"auto-correcting to {statistic_date.replace(day=1)}"
            )
            statistic_date = statistic_date.replace(day=1)

        # ===== 2. 验证参数有效性 =====
        if elapsed < 0:
            logger.warning(f"elapsed time is negative: {elapsed}, setting to 0")
            elapsed = 0

        if fine_amount < 0:
            logger.warning(f"fine_amount is negative: {fine_amount}, setting to 0")
            fine_amount = 0

        if overtime_seconds < 0:
            logger.warning(
                f"overtime_seconds is negative: {overtime_seconds}, setting to 0"
            )
            overtime_seconds = 0

        # ===== 3. 计算超时相关标志 =====
        has_overtime = 1 if (is_overtime and overtime_seconds > 0) else 0

        # ===== 4. 记录调试信息 =====
        logger.debug(
            f"📊 [_update_monthly_statistics_for_activity]\n"
            f"   ├─ chat_id: {chat_id}\n"
            f"   ├─ user_id: {user_id}\n"
            f"   ├─ activity: {activity}\n"
            f"   ├─ shift: {shift}\n"
            f"   ├─ statistic_date: {statistic_date}\n"
            f"   ├─ activity_date: {activity_date}\n"
            f"   ├─ elapsed: {elapsed}秒 ({elapsed//60}分钟)\n"
            f"   ├─ fine_amount: {fine_amount}\n"
            f"   ├─ is_overtime: {is_overtime}\n"
            f"   ├─ overtime_seconds: {overtime_seconds}秒 ({overtime_seconds//60}分钟)\n"
            f"   └─ has_overtime: {has_overtime}"
        )

        # ===== 5. 开始事务操作 =====
        try:
            # ===== 5.1 更新 monthly_statistics（宽表版本）=====
            await conn.execute(
                """
                INSERT INTO monthly_statistics (
                    chat_id, user_id, statistic_date, shift,
                    activity_count, accumulated_time, fine_amount,
                    overtime_count, overtime_time
                )
                VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8)
                ON CONFLICT (chat_id, user_id, statistic_date, shift)
                DO UPDATE SET
                    activity_count = monthly_statistics.activity_count + 1,
                    accumulated_time = monthly_statistics.accumulated_time + $5,
                    fine_amount = monthly_statistics.fine_amount + $6,
                    overtime_count = monthly_statistics.overtime_count + $7,
                    overtime_time = monthly_statistics.overtime_time + $8,
                    updated_at = CURRENT_TIMESTAMP
                """,
                chat_id,
                user_id,
                statistic_date,
                shift,
                elapsed,  # accumulated_time
                fine_amount,  # fine_amount
                has_overtime,  # overtime_count
                overtime_seconds,  # overtime_time
            )

            # ===== 5.2 更新 user_activities 表（详细记录）=====
            await conn.execute(
                """
                INSERT INTO user_activities (
                    chat_id, user_id, activity_date, activity_name,
                    activity_count, accumulated_time, shift
                )
                VALUES ($1, $2, $3, $4, 1, $5, $6)
                ON CONFLICT (chat_id, user_id, activity_date, activity_name, shift)
                DO UPDATE SET
                    activity_count = user_activities.activity_count + 1,
                    accumulated_time = user_activities.accumulated_time + $5,
                    updated_at = CURRENT_TIMESTAMP
                """,
                chat_id,
                user_id,
                activity_date,  # 使用实际的活动日期
                activity,
                elapsed,
                shift,
            )

            # ===== 5.3 如果有罚款，更新用户总罚款 =====
            if fine_amount > 0:
                await conn.execute(
                    """
                    UPDATE users
                    SET total_fines = total_fines + $1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE chat_id = $2 AND user_id = $3
                    """,
                    fine_amount,
                    chat_id,
                    user_id,
                )

            logger.debug(
                f"✅ [_update_monthly_statistics_for_activity] 成功\n"
                f"   ├─ monthly_statistics 更新: +1次, +{elapsed}秒\n"
                f"   ├─ user_activities 更新: {activity_date} +{elapsed}秒\n"
                f"   └─ users 更新: +{fine_amount}罚款"
            )

        except asyncpg.UniqueViolationError as e:
            logger.error(f"❌ 唯一约束冲突: {e}")
            logger.error(
                f"   参数: chat_id={chat_id}, user_id={user_id}, "
                f"statistic_date={statistic_date}, shift={shift}"
            )
            raise

        except asyncpg.ForeignKeyViolationError as e:
            logger.error(f"❌ 外键约束失败: {e}")
            logger.error(f"   可能 user_id {user_id} 在 users 表中不存在")
            await conn.execute(
                """
                INSERT INTO users (chat_id, user_id, nickname, last_updated)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (chat_id, user_id) DO NOTHING
                """,
                chat_id,
                user_id,
                f"用户{user_id}",
                activity_date,
            )
            await conn.execute(
                """
                INSERT INTO user_activities (
                    chat_id, user_id, activity_date, activity_name,
                    activity_count, accumulated_time, shift
                )
                VALUES ($1, $2, $3, $4, 1, $5, $6)
                ON CONFLICT (chat_id, user_id, activity_date, activity_name, shift)
                DO UPDATE SET
                    activity_count = user_activities.activity_count + 1,
                    accumulated_time = user_activities.accumulated_time + $5,
                    updated_at = CURRENT_TIMESTAMP
                """,
                chat_id,
                user_id,
                activity_date,
                activity,
                elapsed,
                shift,
            )
            logger.info(f"✅ 已创建用户 {user_id} 并重试成功")

        except Exception as e:
            logger.error(f"❌ 更新失败: {e}")
            logger.error(
                f"   参数: chat_id={chat_id}, user_id={user_id}, "
                f"activity={activity}, elapsed={elapsed}"
            )
            raise

    # ───────────────────────── 重置时间配置 ─────────────────────────
    async def reset_user_daily_data(
        self, chat_id: int, user_id: int, target_date: date | None = None
    ) -> bool:
        """企业级最终版：硬重置用户每日数据（宽表版本）"""

        try:
            # ===== 0. 确定业务日期 =====
            if target_date is None:
                target_date = await self.get_business_date(chat_id)
            elif not isinstance(target_date, date):
                raise ValueError(
                    f"target_date必须是date类型，得到: {type(target_date)}"
                )

            # ===== 1. 并行获取用户状态与业务日期 =====
            user_before, business_date = await asyncio.gather(
                self.get_user(chat_id, user_id), self.get_business_date(chat_id)
            )

            cross_day = {"activity": None, "duration": 0, "fine": 0}
            new_date = max(target_date, business_date)

            async with self.pool.acquire() as conn:
                async with conn.transaction():

                    # ===== 2. 获取当天 daily_statistics =====
                    stats_row = await conn.fetchrow(
                        """
                        SELECT * FROM daily_statistics
                        WHERE chat_id=$1 AND user_id=$2 AND record_date=$3
                        """,
                        chat_id,
                        user_id,
                        target_date,
                    )

                    # ===== 3. 日统计转入月统计（完整字段）=====
                    if stats_row:
                        statistic_date = target_date.replace(day=1)
                        shift = stats_row.get("shift", "day")

                        await conn.execute(
                            """
                            INSERT INTO monthly_statistics
                            (chat_id, user_id, statistic_date, shift,
                             activity_count, accumulated_time, fine_amount,
                             overtime_count, overtime_time,
                             work_days, work_hours,
                             work_start_count, work_end_count,
                             work_start_fines, work_end_fines,
                             late_count, early_count)
                            VALUES ($1,$2,$3,$4, $5,$6,$7,$8,$9, $10,$11, $12,$13, $14,$15, $16,$17)
                            ON CONFLICT (chat_id,user_id,statistic_date,shift)
                            DO UPDATE SET
                                activity_count = monthly_statistics.activity_count + EXCLUDED.activity_count,
                                accumulated_time = monthly_statistics.accumulated_time + EXCLUDED.accumulated_time,
                                fine_amount = monthly_statistics.fine_amount + EXCLUDED.fine_amount,
                                overtime_count = monthly_statistics.overtime_count + EXCLUDED.overtime_count,
                                overtime_time = monthly_statistics.overtime_time + EXCLUDED.overtime_time,
                                work_days = monthly_statistics.work_days + EXCLUDED.work_days,
                                work_hours = monthly_statistics.work_hours + EXCLUDED.work_hours,
                                work_start_count = monthly_statistics.work_start_count + EXCLUDED.work_start_count,
                                work_end_count = monthly_statistics.work_end_count + EXCLUDED.work_end_count,
                                work_start_fines = monthly_statistics.work_start_fines + EXCLUDED.work_start_fines,
                                work_end_fines = monthly_statistics.work_end_fines + EXCLUDED.work_end_fines,
                                late_count = monthly_statistics.late_count + EXCLUDED.late_count,
                                early_count = monthly_statistics.early_count + EXCLUDED.early_count,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            chat_id,
                            user_id,
                            statistic_date,
                            shift,
                            stats_row["activity_count"] or 0,
                            stats_row["accumulated_time"] or 0,
                            stats_row["fine_amount"] or 0,
                            stats_row["overtime_count"] or 0,
                            stats_row["overtime_time"] or 0,
                            stats_row["work_days"] or 0,
                            stats_row["work_hours"] or 0,
                            stats_row["work_start_count"] or 0,
                            stats_row["work_end_count"] or 0,
                            stats_row["work_start_fines"] or 0,
                            stats_row["work_end_fines"] or 0,
                            stats_row["late_count"] or 0,
                            stats_row["early_count"] or 0,
                        )

                    # ===== 4. 处理跨天活动 =====
                    if user_before and user_before.get("current_activity"):
                        act = user_before["current_activity"]
                        start_str = user_before.get("activity_start_time")

                        if start_str:
                            try:
                                start_dt = datetime.fromisoformat(start_str)
                                now_dt = self.get_beijing_time()
                                elapsed_sec = int((now_dt - start_dt).total_seconds())

                                # 并行获取活动时间限制 & 罚款规则
                                limit_min, fine_rates = await asyncio.gather(
                                    self.get_activity_time_limit(act),
                                    self.get_fine_rates_for_activity(act),
                                )

                                overtime_sec = max(0, elapsed_sec - limit_min * 60)
                                fine_amount = 0
                                if overtime_sec > 0 and fine_rates:
                                    overtime_min = overtime_sec / 60
                                    segments = sorted(
                                        [
                                            int(str(k).lower().replace("min", ""))
                                            for k in fine_rates.keys()
                                            if str(k)
                                            .lower()
                                            .replace("min", "")
                                            .isdigit()
                                        ]
                                    )
                                    for s in segments:
                                        if overtime_min <= s:
                                            fine_amount = fine_rates.get(
                                                str(s), fine_rates.get(f"{s}min", 0)
                                            )
                                            break
                                    if fine_amount == 0 and segments:
                                        s = segments[-1]
                                        fine_amount = fine_rates.get(
                                            str(s), fine_rates.get(f"{s}min", 0)
                                        )

                                activity_month = start_dt.date().replace(day=1)
                                shift = user_before.get("shift", "day")

                                # 使用 UPSERT 更新月度统计
                                await conn.execute(
                                    """
                                    INSERT INTO monthly_statistics
                                    (chat_id, user_id, statistic_date, shift,
                                     activity_count, accumulated_time, fine_amount,
                                     overtime_count, overtime_time)
                                    VALUES ($1,$2,$3,$4, 1, $5, $6, $7, $8)
                                    ON CONFLICT (chat_id, user_id, statistic_date, shift)
                                    DO UPDATE SET
                                        activity_count = monthly_statistics.activity_count + 1,
                                        accumulated_time = monthly_statistics.accumulated_time + $5,
                                        fine_amount = monthly_statistics.fine_amount + $6,
                                        overtime_count = monthly_statistics.overtime_count + $7,
                                        overtime_time = monthly_statistics.overtime_time + $8,
                                        updated_at = CURRENT_TIMESTAMP
                                    """,
                                    chat_id,
                                    user_id,
                                    activity_month,
                                    shift,
                                    elapsed_sec,
                                    fine_amount,
                                    1 if overtime_sec > 0 else 0,
                                    overtime_sec,
                                )

                                if fine_amount > 0:
                                    await conn.execute(
                                        """
                                        UPDATE users
                                        SET total_fines = total_fines + $1
                                        WHERE chat_id=$2 AND user_id=$3
                                        """,
                                        fine_amount,
                                        chat_id,
                                        user_id,
                                    )

                                cross_day.update(
                                    {
                                        "activity": act,
                                        "duration": elapsed_sec,
                                        "fine": fine_amount,
                                    }
                                )

                            except Exception as e:
                                logger.error(f"❌ 跨天活动结算失败: {e}")

                    # ===== 5. 删除当日数据（并行执行，捕获返回值）=====
                    daily_deleted, act_deleted, work_deleted = await asyncio.gather(
                        conn.execute(
                            "DELETE FROM daily_statistics WHERE chat_id=$1 AND user_id=$2 AND record_date=$3",
                            chat_id,
                            user_id,
                            target_date,
                        ),
                        conn.execute(
                            "DELETE FROM user_activities WHERE chat_id=$1 AND user_id=$2 AND activity_date=$3",
                            chat_id,
                            user_id,
                            target_date,
                        ),
                        conn.execute(
                            "DELETE FROM work_records WHERE chat_id=$1 AND user_id=$2 AND record_date=$3",
                            chat_id,
                            user_id,
                            target_date,
                        ),
                    )

                    # ===== 6. 重置用户表 =====
                    await conn.execute(
                        """
                        UPDATE users
                        SET
                            total_activity_count = 0,
                            total_accumulated_time = 0,
                            total_fines = 0,
                            total_overtime_time = 0,
                            overtime_count = 0,
                            current_activity = NULL,
                            activity_start_time = NULL,
                            checkin_message_id = NULL,
                            last_updated = $3,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE chat_id=$1 AND user_id=$2
                        """,
                        chat_id,
                        user_id,
                        new_date,
                    )

            # ===== 7. 缓存清理 =====
            cache_keys = [
                f"user:{chat_id}:{user_id}",
                f"group:{chat_id}",
                "activity_limits",
            ]
            for key in cache_keys:
                self._cache.pop(key, None)
                self._cache_ttl.pop(key, None)

            # ===== 8. 解析删除数量用于日志 =====
            def parse_deleted(result):
                try:
                    return (
                        int(result.split()[-1])
                        if result and result.startswith("DELETE")
                        else 0
                    )
                except:
                    return 0

            # ===== 9. 详细日志输出 =====
            log = (
                f"✅ [宽表重置完成] 用户:{user_id} 群:{chat_id}\n"
                f"📅 日期:{new_date}\n"
                f"🗑️ 删除: daily={parse_deleted(daily_deleted)}, "
                f"activities={parse_deleted(act_deleted)}, "
                f"work={parse_deleted(work_deleted)}\n"
            )
            if cross_day["activity"]:
                log += f"🌙 跨天结算: {cross_day['activity']} {self.format_seconds_to_hms(cross_day['duration'])}"
                if cross_day["fine"] > 0:
                    log += f" 💰罚款:{cross_day['fine']}元"
            logger.info(log)
            return True

        except Exception as e:
            logger.error(
                f"❌ 企业级宽表重置失败 {chat_id}-{user_id}: {e}", exc_info=True
            )
            return False

    async def get_user_all_activities(
        self, chat_id: int, user_id: int, target_date: date = None
    ) -> Dict[str, Dict]:
        """获取用户活动数据，可指定日期"""
        if target_date is None:
            target_date = await self.get_business_date(chat_id)

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT activity_name, activity_count, accumulated_time 
                FROM user_activities 
                WHERE chat_id = $1 AND user_id = $2 AND activity_date = $3
                """,
                chat_id,
                user_id,
                target_date,
            )

            activities = {}
            for row in rows:
                activities[row["activity_name"]] = {
                    "count": row["activity_count"],
                    "time": row["accumulated_time"],
                }
            return activities

    async def add_work_record(
        self,
        chat_id: int,
        user_id: int,
        record_date: date,
        checkin_type: str,
        checkin_time: str,
        status: str,
        time_diff_minutes: float,
        fine_amount: int = 0,
        shift: str = None,
        shift_detail: str = None,
        max_retries: int = 3,
        statement_timeout: int = 10000,
    ):

        # ===== 1. 使用传入的 record_date =====
        # business_date = await self.get_business_date(chat_id)  # ❌ 错误：应该用传入的
        business_date = record_date  # ✅ 修复：使用传入的日期
        statistic_date = business_date.replace(day=1)
        now = self.get_beijing_time()

        # ------------------- 2. 判定班次 -------------------
        if shift is None:
            try:
                checkin_dt = datetime.combine(
                    business_date, datetime.strptime(checkin_time, "%H:%M").time()
                )
                checkin_dt = beijing_tz.localize(checkin_dt)
                shift_info = await self.determine_shift_for_time(
                    chat_id, checkin_dt, checkin_type
                )
                shift = shift_info.get("shift", "day") if shift_info else "day"
            except Exception as e:
                logger.error(f"班次判定失败: {e}")
                shift = "day"

        self._ensure_pool_initialized()

        attempt = 0
        while attempt < max_retries:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        # 设置事务超时
                        await conn.execute(
                            f"SET LOCAL statement_timeout = '{statement_timeout}ms'"
                        )

                        # ===== 3. work_records UPSERT =====
                        await conn.execute(
                            """
                            INSERT INTO work_records
                            (chat_id, user_id, record_date, checkin_type,
                             checkin_time, status, time_diff_minutes,
                             fine_amount, shift, shift_detail)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                            ON CONFLICT (chat_id, user_id, record_date, checkin_type, shift)
                            DO UPDATE SET
                                checkin_time = EXCLUDED.checkin_time,
                                status = EXCLUDED.status,
                                time_diff_minutes = EXCLUDED.time_diff_minutes,
                                fine_amount = EXCLUDED.fine_amount,
                                shift_detail = EXCLUDED.shift_detail,
                                created_at = CURRENT_TIMESTAMP
                            """,
                            chat_id,
                            user_id,
                            business_date,  # 使用传入的 record_date
                            checkin_type,
                            checkin_time,
                            status,
                            time_diff_minutes,
                            fine_amount,
                            shift,
                            shift_detail,
                        )

                        # ===== 4. 重新计算当日统计 =====
                        daily_row = await conn.fetchrow(
                            """
                            SELECT 
                                COUNT(*) FILTER (WHERE checkin_type='work_start') AS work_start_count,
                                COUNT(*) FILTER (WHERE checkin_type='work_end') AS work_end_count,
                                COUNT(CASE WHEN checkin_type='work_start' AND time_diff_minutes>0 THEN 1 END) AS late_count,
                                COUNT(CASE WHEN checkin_type='work_end' AND time_diff_minutes<0 THEN 1 END) AS early_count,
                                COALESCE(SUM(CASE WHEN checkin_type='work_start' THEN fine_amount ELSE 0 END), 0) AS work_start_fines,
                                COALESCE(SUM(CASE WHEN checkin_type='work_end' THEN fine_amount ELSE 0 END), 0) AS work_end_fines,
                                COALESCE(
                                    SUM(
                                        CASE 
                                            WHEN checkin_type='work_start' THEN 0
                                            WHEN checkin_type='work_end' THEN
                                                EXTRACT(EPOCH FROM (
                                                    TO_TIMESTAMP(checkin_time, 'HH24:MI') - 
                                                    TO_TIMESTAMP((
                                                        SELECT COALESCE(checkin_time, '00:00')
                                                        FROM work_records ws
                                                        WHERE ws.chat_id = work_records.chat_id
                                                          AND ws.user_id = work_records.user_id
                                                          AND ws.record_date = work_records.record_date
                                                          AND ws.shift = work_records.shift
                                                          AND ws.checkin_type = 'work_start'
                                                        LIMIT 1
                                                    ), 'HH24:MI')
                                                ))
                                        END
                                    ), 0
                                ) AS work_seconds
                            FROM work_records
                            WHERE chat_id = $1 
                              AND user_id = $2 
                              AND record_date = $3 
                              AND shift = $4
                            """,
                            chat_id,
                            user_id,
                            business_date,
                            shift,
                        )

                        # ===== 5. 插入或更新 daily_statistics =====
                        await conn.execute(
                            """
                            INSERT INTO daily_statistics 
                            (chat_id, user_id, record_date, shift,
                             work_start_count, work_end_count, late_count, early_count,
                             work_start_fines, work_end_fines, work_hours, work_days, updated_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 
                                    CASE WHEN $5 > 0 OR $6 > 0 OR $11 > 0 THEN 1 ELSE 0 END,
                                    CURRENT_TIMESTAMP)
                            ON CONFLICT (chat_id, user_id, record_date, shift) 
                            DO UPDATE SET
                                work_start_count = EXCLUDED.work_start_count,
                                work_end_count = EXCLUDED.work_end_count,
                                late_count = EXCLUDED.late_count,
                                early_count = EXCLUDED.early_count,
                                work_start_fines = EXCLUDED.work_start_fines,
                                work_end_fines = EXCLUDED.work_end_fines,
                                work_hours = EXCLUDED.work_hours,
                                work_days = CASE 
                                    WHEN EXCLUDED.work_hours > 0 AND daily_statistics.work_hours = 0 
                                    THEN daily_statistics.work_days + 1
                                    WHEN EXCLUDED.work_hours = 0 AND daily_statistics.work_hours > 0
                                    THEN daily_statistics.work_days
                                    ELSE daily_statistics.work_days
                                END,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            chat_id,
                            user_id,
                            business_date,
                            shift,
                            daily_row["work_start_count"],
                            daily_row["work_end_count"],
                            daily_row["late_count"],
                            daily_row["early_count"],
                            daily_row["work_start_fines"],
                            daily_row["work_end_fines"],
                            int(daily_row["work_seconds"] or 0),
                        )

                        # ===== 6. 更新月度统计（按罚款类型分开）=====
                        if fine_amount > 0:
                            if checkin_type == "work_start":
                                # 上班罚款
                                await conn.execute(
                                    """
                                    INSERT INTO monthly_statistics 
                                    (chat_id, user_id, statistic_date, shift, work_start_fines, updated_at)
                                    VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                                    ON CONFLICT (chat_id, user_id, statistic_date, shift)
                                    DO UPDATE SET
                                        work_start_fines = monthly_statistics.work_start_fines + $5,
                                        updated_at = CURRENT_TIMESTAMP
                                    """,
                                    chat_id,
                                    user_id,
                                    statistic_date,
                                    shift,
                                    fine_amount,
                                )
                            else:  # work_end
                                # 下班罚款
                                await conn.execute(
                                    """
                                    INSERT INTO monthly_statistics 
                                    (chat_id, user_id, statistic_date, shift, work_end_fines, updated_at)
                                    VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                                    ON CONFLICT (chat_id, user_id, statistic_date, shift)
                                    DO UPDATE SET
                                        work_end_fines = monthly_statistics.work_end_fines + $5,
                                        updated_at = CURRENT_TIMESTAMP
                                    """,
                                    chat_id,
                                    user_id,
                                    statistic_date,
                                    shift,
                                    fine_amount,
                                )

                        # 更新月度统计的其他字段（次数等）
                        monthly_row = await conn.fetchrow(
                            """
                            SELECT 
                                COALESCE(SUM(work_start_count), 0) AS work_start_count,
                                COALESCE(SUM(work_end_count), 0) AS work_end_count,
                                COALESCE(SUM(late_count), 0) AS late_count,
                                COALESCE(SUM(early_count), 0) AS early_count,
                                COALESCE(SUM(work_hours), 0) AS work_hours,
                                COALESCE(SUM(work_days), 0) AS work_days
                            FROM daily_statistics
                            WHERE chat_id = $1 
                              AND user_id = $2 
                              AND DATE_TRUNC('month', record_date) = $3
                              AND shift = $4
                            """,
                            chat_id,
                            user_id,
                            statistic_date,
                            shift,
                        )

                        # 更新月度统计的总计字段
                        if monthly_row:
                            await conn.execute(
                                """
                                INSERT INTO monthly_statistics 
                                (chat_id, user_id, statistic_date, shift,
                                 work_start_count, work_end_count, late_count, early_count,
                                 work_hours, work_days, updated_at)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)
                                ON CONFLICT (chat_id, user_id, statistic_date, shift)
                                DO UPDATE SET
                                    work_start_count = EXCLUDED.work_start_count,
                                    work_end_count = EXCLUDED.work_end_count,
                                    late_count = EXCLUDED.late_count,
                                    early_count = EXCLUDED.early_count,
                                    work_hours = EXCLUDED.work_hours,
                                    work_days = EXCLUDED.work_days,
                                    updated_at = CURRENT_TIMESTAMP
                                """,
                                chat_id,
                                user_id,
                                statistic_date,
                                shift,
                                monthly_row["work_start_count"],
                                monthly_row["work_end_count"],
                                monthly_row["late_count"],
                                monthly_row["early_count"],
                                monthly_row["work_hours"],
                                monthly_row["work_days"],
                            )

                        # ===== 7. 更新用户总罚款 =====
                        await conn.execute(
                            """
                            UPDATE users
                            SET total_fines = (
                                SELECT COALESCE(SUM(fine_amount), 0)
                                FROM work_records
                                WHERE chat_id = $1 AND user_id = $2
                            ),
                            updated_at = CURRENT_TIMESTAMP
                            WHERE chat_id = $1 AND user_id = $2
                            """,
                            chat_id,
                            user_id,
                        )

                        # ===== 8. 清理缓存 =====
                        cache_key = f"user:{chat_id}:{user_id}"
                        self._cache.pop(cache_key, None)
                        self._cache_ttl.pop(cache_key, None)

                        logger.info(
                            f"✅ [工作记录完成] 用户:{user_id} | "
                            f"日期:{business_date} | 班次:{shift} | "
                            f"类型:{checkin_type} | 工时:{int(daily_row['work_seconds'] or 0)}s | "
                            f"罚款:{fine_amount} | "
                            f"上班次数:{daily_row['work_start_count']} | "
                            f"下班次数:{daily_row['work_end_count']}"
                        )

                    break  # 成功退出重试循环

            except (SerializationError, PostgresError) as e:
                attempt += 1
                if attempt >= max_retries:
                    logger.error(
                        f"❌ 工作记录最终失败\n"
                        f"   ├─ 群组: {chat_id}\n"
                        f"   ├─ 用户: {user_id}\n"
                        f"   ├─ 类型: {checkin_type}\n"
                        f"   └─ 错误: {e}"
                    )
                    raise

                jitter = random.uniform(0.05, 0.2)
                sleep_time = 0.1 * (2 ** (attempt - 1)) + jitter
                logger.warning(
                    f"⚠️ 数据库异常，重试 {attempt}/{max_retries}\n"
                    f"   ├─ 等待: {sleep_time:.2f}s\n"
                    f"   └─ 错误: {e}"
                )
                await asyncio.sleep(sleep_time)

            except Exception as e:
                logger.error(f"❌ 非数据库异常: {type(e).__name__} - {e}")
                raise

    async def get_work_records_by_shift(
        self,
        chat_id: int,
        user_id: int,
        shift: str = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """获取用户上下班记录（支持按班次过滤和日期范围）"""

        if start_date is None:
            start_date = await self.get_business_date(chat_id)
        if end_date is None:
            end_date = start_date

        query = """
            SELECT checkin_type, checkin_time, status, time_diff_minutes, 
                   fine_amount, shift, created_at, record_date
            FROM work_records 
            WHERE chat_id = $1 AND user_id = $2 
              AND record_date >= $3 AND record_date <= $4
        """
        params = [chat_id, user_id, start_date, end_date]

        if shift:
            if shift in ["day", "白班"]:
                shift_value = "day"
            elif shift in ["night", "夜班", "night_last", "night_tonight"]:
                shift_value = "night"
            else:
                raise ValueError(f"❌ 无效的班次值: {shift}")

            query += " AND shift = $5"
            params.append(shift_value)

        query += " ORDER BY created_at DESC"

        rows = await self.execute_with_retry(
            "按班次获取工作记录", query, *params, fetch=True
        )

        records: Dict[str, List[Dict[str, Any]]] = {}
        if rows:
            for row in rows:
                checkin_type = row["checkin_type"]
                if checkin_type not in records:
                    records[checkin_type] = []
                records[checkin_type].append(dict(row))

        return records

    async def get_today_work_records_fixed(
        self, chat_id: int, user_id: int
    ) -> Dict[str, Dict]:
        """获取用户今天的上下班记录"""
        try:
            group_data = await self.get_group_cached(chat_id)
            reset_hour = group_data.get("reset_hour", Config.DAILY_RESET_HOUR)
            reset_minute = group_data.get("reset_minute", Config.DAILY_RESET_MINUTE)

            now = self.get_beijing_time()

            reset_time_today = now.replace(
                hour=reset_hour, minute=reset_minute, second=0, microsecond=0
            )

            if now < reset_time_today:
                period_start = reset_time_today - timedelta(days=1)
            else:
                period_start = reset_time_today

            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT * FROM work_records 
                    WHERE chat_id = $1 
                    AND user_id = $2 
                    AND record_date >= $3
                    AND record_date <= $4
                    ORDER BY record_date DESC, checkin_type
                    """,
                    chat_id,
                    user_id,
                    period_start.date(),
                    now.date(),
                )

                records = {}
                for row in rows:
                    record_key = f"{row['record_date']}_{row['checkin_type']}"
                    if (
                        row["checkin_type"] not in records
                        or row["record_date"]
                        > records[row["checkin_type"]]["record_date"]
                    ):
                        records[row["checkin_type"]] = dict(row)

                logger.debug(
                    f"工作记录查询: {chat_id}-{user_id}, 重置周期: {period_start.date()}, 记录数: {len(records)}"
                )
                return records

        except Exception as e:
            logger.error(f"获取工作记录失败 {chat_id}-{user_id}: {e}")
            return {}

    # ========== 活动配置操作 ==========
    async def get_activity_limits(self) -> Dict:
        """获取所有活动限制"""
        cache_key = "activity_limits"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        if not await self._ensure_healthy_connection():
            logger.warning("数据库连接不健康，返回默认活动配置")
            return Config.DEFAULT_ACTIVITY_LIMITS.copy()

        try:
            rows = await self.execute_with_retry(
                "获取活动限制",
                "SELECT activity_name, max_times, time_limit FROM activity_configs",
                fetch=True,
            )
            limits = {
                row["activity_name"]: {
                    "max_times": row["max_times"],
                    "time_limit": row["time_limit"],
                }
                for row in rows
            }
            self._set_cached(cache_key, limits, 600)
            return limits
        except Exception as e:
            logger.error(f"获取活动配置失败: {e}，返回默认配置")
            return Config.DEFAULT_ACTIVITY_LIMITS.copy()

    async def get_activity_limits_cached(self) -> Dict:
        """带缓存的获取活动限制"""
        try:
            return await self.get_activity_limits()
        except Exception as e:
            logger.error(f"获取活动配置缓存失败: {e}，返回默认配置")
            return Config.DEFAULT_ACTIVITY_LIMITS.copy()

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
        cache_key = "activity_limits"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return activity in cached

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM activity_configs WHERE activity_name = $1", activity
            )
            return row is not None

    async def update_activity_config(
        self, activity: str, max_times: int, time_limit: int
    ):
        """更新活动配置"""
        self._ensure_pool_initialized()
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
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
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
        self._ensure_pool_initialized()
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
        self._ensure_pool_initialized()
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
        self._ensure_pool_initialized()
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

    async def calculate_fine_for_activity(
        self, activity: str, overtime_minutes: float
    ) -> int:
        """计算活动罚款金额"""
        fine_rates = await self.get_fine_rates_for_activity(activity)
        if not fine_rates:
            return 0

        segments = []
        for time_key in fine_rates.keys():
            try:
                if isinstance(time_key, str) and "min" in time_key.lower():
                    time_value = int(time_key.lower().replace("min", "").strip())
                else:
                    time_value = int(time_key)
                segments.append(time_value)
            except (ValueError, TypeError):
                continue

        if not segments:
            return 0

        segments.sort()

        applicable_fine = 0
        for segment in segments:
            if overtime_minutes <= segment:
                original_key = str(segment)
                if original_key not in fine_rates:
                    original_key = f"{segment}min"
                applicable_fine = fine_rates.get(original_key, 0)
                break

        if applicable_fine == 0 and segments:
            max_segment = segments[-1]
            original_key = str(max_segment)
            if original_key not in fine_rates:
                original_key = f"{max_segment}min"
            applicable_fine = fine_rates.get(original_key, 0)

        return applicable_fine

    async def get_work_fine_rates(self) -> Dict:
        """获取上下班罚款费率"""
        self._ensure_pool_initialized()
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
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT time_segment, fine_amount FROM work_fine_configs WHERE checkin_type = $1",
                checkin_type,
            )
            return {row["time_segment"]: row["fine_amount"] for row in rows}

    async def update_work_fine_rate(
        self, checkin_type: str, time_segment: str, fine_amount: int
    ):
        """更新上下班罚款费率"""
        self._ensure_pool_initialized()
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

    async def clear_work_fine_rates(self, checkin_type: str):
        """清空上下班罚款配置"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM work_fine_configs WHERE checkin_type = $1", checkin_type
            )

    # ========== 推送设置操作 ==========
    async def get_push_settings(self) -> Dict:
        """获取推送设置"""
        cache_key = "push_settings"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM push_settings")
            settings = {row["setting_key"]: bool(row["setting_value"]) for row in rows}
            self._set_cached(cache_key, settings, 300)
            return settings

    async def update_push_setting(self, key: str, value: bool):
        """更新推送设置"""
        self._ensure_pool_initialized()
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
        """获取群组统计信息 - 企业级最终优化版"""
        start_time = time.time()

        if target_date is None:
            target_date = await self.get_business_date(chat_id)

        cache_key = f"stats:{chat_id}:{target_date}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            logger.debug(f"✅ 命中统计缓存: {cache_key}")
            return cached

        # 获取群组配置（用于判断双班模式）
        group_data = await self.get_group_cached(chat_id)
        has_dual_mode = group_data.get("dual_mode", True) if group_data else True
        all_shifts = ["day", "night"] if has_dual_mode else ["day"]

        lock_key = f"stats_lock:{chat_id}:{target_date}"
        lock = await self._get_lock(lock_key)

        try:
            async with lock:
                # 二次检查缓存
                cached = self._get_cached(cache_key)
                if cached is not None:
                    return cached

                self._ensure_pool_initialized()

                async with self.pool.acquire() as conn:
                    t0 = time.time()

                    # ✅ 修复：修正 GROUP BY 子句
                    rows = await conn.fetch(
                        """
                        WITH stats AS (
                            SELECT 
                                u.user_id,
                                u.nickname,
                                COALESCE(ds.shift, 'day') AS shift,
                                COALESCE(ds.activity_count, 0) AS total_activity_count,
                                COALESCE(ds.accumulated_time, 0) AS total_accumulated_time,
                                COALESCE(ds.fine_amount, 0) AS total_fines,
                                COALESCE(ds.overtime_count, 0) AS overtime_count,
                                COALESCE(ds.overtime_time, 0) AS total_overtime_time,
                                COALESCE(ds.work_days, 0) AS work_days,
                                COALESCE(ds.work_hours, 0) AS work_hours,
                                COALESCE(ds.work_start_count, 0) AS work_start_count,
                                COALESCE(ds.work_end_count, 0) AS work_end_count,
                                COALESCE(ds.work_start_fines, 0) AS work_start_fines,
                                COALESCE(ds.work_end_fines, 0) AS work_end_fines,
                                COALESCE(ds.late_count, 0) AS late_count,
                                COALESCE(ds.early_count, 0) AS early_count,
                                COALESCE(
                                    jsonb_object_agg(
                                        ua.activity_name, 
                                        jsonb_build_object('count', ua.activity_count, 'time', ua.accumulated_time)
                                    ) FILTER (WHERE ua.activity_name IS NOT NULL),
                                    '{}'::jsonb
                                ) AS activities
                            FROM users u
                            LEFT JOIN daily_statistics ds
                                ON u.chat_id = ds.chat_id 
                                AND u.user_id = ds.user_id 
                                AND ds.record_date = $1
                            LEFT JOIN user_activities ua
                                ON u.chat_id = ua.chat_id 
                                AND u.user_id = ua.user_id 
                                AND ua.activity_date = $1 
                                AND ua.shift = ds.shift
                            WHERE u.chat_id = $2
                            GROUP BY u.user_id, u.nickname, ds.shift, 
                                     ds.activity_count, ds.accumulated_time, ds.fine_amount,
                                     ds.overtime_count, ds.overtime_time, ds.work_days, ds.work_hours,
                                     ds.work_start_count, ds.work_end_count, ds.work_start_fines,
                                     ds.work_end_fines, ds.late_count, ds.early_count
                        )
                        SELECT * FROM stats
                        ORDER BY user_id, shift
                        """,
                        target_date,
                        chat_id,
                    )

                    t1 = time.time()
                    logger.debug(f"📊 聚合查询完成，耗时 {(t1-t0)*1000:.1f}ms")

                    # 构建用户数据映射
                    user_data_map = {}
                    user_nicknames = {}

                    for row in rows:
                        key = (row["user_id"], row["shift"])
                        user_data_map[key] = dict(row)
                        user_nicknames[row["user_id"]] = (
                            row["nickname"] or f"用户{row['user_id']}"
                        )

                    # 填充所有可能的班次
                    result = []
                    for user_id in user_nicknames.keys():
                        for shift in all_shifts:
                            key = (user_id, shift)
                            row = user_data_map.get(key, {})

                            # 解析 activities JSON
                            activities = row.get("activities", {})
                            if isinstance(activities, str):
                                try:
                                    activities = json.loads(activities)
                                except:
                                    activities = {}

                            result.append(
                                {
                                    "user_id": user_id,
                                    "nickname": user_nicknames[user_id],
                                    "shift": shift,
                                    "total_activity_count": row.get(
                                        "total_activity_count", 0
                                    ),
                                    "total_accumulated_time": row.get(
                                        "total_accumulated_time", 0
                                    ),
                                    "total_fines": row.get("total_fines", 0),
                                    "overtime_count": row.get("overtime_count", 0),
                                    "total_overtime_time": row.get(
                                        "total_overtime_time", 0
                                    ),
                                    "work_days": row.get("work_days", 0),
                                    "work_hours": row.get("work_hours", 0),
                                    "work_start_count": row.get("work_start_count", 0),
                                    "work_end_count": row.get("work_end_count", 0),
                                    "work_start_fines": row.get("work_start_fines", 0),
                                    "work_end_fines": row.get("work_end_fines", 0),
                                    "late_count": row.get("late_count", 0),
                                    "early_count": row.get("early_count", 0),
                                    "activities": activities,
                                }
                            )

                    # 按用户ID排序
                    result.sort(key=lambda x: (x["user_id"], x["shift"]))

                    # 写入缓存
                    self._set_cached(cache_key, result, 300)

                    elapsed = (time.time() - start_time) * 1000
                    logger.info(
                        f"🚀 群统计完成\n"
                        f"   ├─ 群组: {chat_id}\n"
                        f"   ├─ 日期: {target_date}\n"
                        f"   ├─ 用户: {len(user_nicknames)} 人\n"
                        f"   ├─ 记录: {len(result)} 条\n"
                        f"   └─ 耗时: {elapsed:.1f}ms"
                    )

                    return result

        except asyncio.TimeoutError:
            logger.error(f"❌ 获取群统计锁超时: {chat_id}")
            return []
        except Exception as e:
            logger.error(f"❌ 获取群统计失败 chat={chat_id}: {e}", exc_info=True)
            return []

    async def get_all_groups(self) -> List[int]:
        """获取所有群组ID"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT chat_id FROM groups")
            return [row["chat_id"] for row in rows]

    async def get_group_members(self, chat_id: int) -> List[Dict]:
        """获取群组成员"""
        today = await self.get_business_date(chat_id)
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    user_id, 
                    nickname, 
                    current_activity, 
                    activity_start_time, 
                    total_accumulated_time, 
                    total_activity_count, 
                    total_fines, 
                    overtime_count, 
                    total_overtime_time 
                FROM users 
                WHERE chat_id = $1 AND last_updated = $2
                """,
                chat_id,
                today,
            )
            return [dict(row) for row in rows]

    # ========== 月度统计 ==========
    async def get_monthly_statistics(
        self, chat_id: int, year: int = None, month: int = None, timeout: int = 60
    ) -> list[dict]:
        """
        企业级终极版月度统计
        全量批量查询 + JSON 聚合 + 夜班跨天 + 迟到早退
        """

        from datetime import datetime, date, timedelta
        import json
        from config import beijing_tz
        import asyncio

        if year is None or month is None:
            today = self.get_beijing_time()
            year = today.year
            month = today.month

        month_start = date(year, month, 1)
        month_end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
        month_start_dt = beijing_tz.localize(
            datetime.combine(month_start, datetime.min.time())
        )
        month_end_dt = beijing_tz.localize(
            datetime.combine(month_end, datetime.min.time())
        )

        self._ensure_pool_initialized()

        try:
            async with asyncio.timeout(timeout):
                async with self.pool.acquire() as conn:

                    # ===== 1. 获取所有活跃用户 =====
                    users = await conn.fetch(
                        """
                        SELECT DISTINCT user_id
                        FROM (
                            SELECT user_id FROM user_activities 
                            WHERE chat_id = $1 AND activity_date >= $2 AND activity_date < $3
                            UNION
                            SELECT user_id FROM daily_statistics 
                            WHERE chat_id = $1 AND record_date >= $2 AND record_date < $3
                            UNION
                            SELECT user_id FROM work_records 
                            WHERE chat_id = $1 AND record_date >= $2 AND record_date < $3
                        ) t
                        """,
                        chat_id,
                        month_start,
                        month_end,
                    )

                    if not users:
                        return []

                    user_ids = [row["user_id"] for row in users]

                    # ===== 2. 批量获取用户昵称 =====
                    user_rows = await conn.fetch(
                        """
                        SELECT user_id, nickname 
                        FROM users 
                        WHERE chat_id = $1 AND user_id = ANY($2::bigint[])
                        """,
                        chat_id,
                        user_ids,
                    )
                    user_nicknames = {r["user_id"]: r["nickname"] for r in user_rows}

                    # ===== 3. 按班次聚合日统计 =====
                    stats_rows = await conn.fetch(
                        """
                        SELECT
                            ds.user_id,
                            ds.shift,
                            SUM(ds.activity_count) as total_activity_count,
                            SUM(ds.accumulated_time) as total_accumulated_time,
                            SUM(ds.fine_amount) as total_fines,
                            SUM(ds.overtime_count) as total_overtime_count,
                            SUM(ds.overtime_time) as total_overtime_time,
                            SUM(ds.work_days) as work_days,
                            SUM(ds.work_hours) as work_hours,
                            SUM(ds.work_start_count) as work_start_count,
                            SUM(ds.work_end_count) as work_end_count,
                            SUM(ds.work_start_fines) as work_start_fines,
                            SUM(ds.work_end_fines) as work_end_fines,
                            SUM(ds.late_count) as late_count,
                            SUM(ds.early_count) as early_count
                        FROM daily_statistics ds
                        WHERE ds.chat_id = $1 
                          AND ds.user_id = ANY($2::bigint[]) 
                          AND ds.record_date >= $3 
                          AND ds.record_date < $4
                        GROUP BY ds.user_id, ds.shift
                        """,
                        chat_id,
                        user_ids,
                        month_start,
                        month_end,
                    )

                    # 构建按用户+班次索引的统计数据
                    stats_by_user_shift = {}
                    for row in stats_rows:
                        key = (row["user_id"], row["shift"])
                        stats_by_user_shift[key] = dict(row)

                    # ===== 4. 用户活动明细 JSON 聚合 =====
                    activity_rows = await conn.fetch(
                        """
                        SELECT 
                            user_id, 
                            jsonb_object_agg(
                                activity_name, 
                                jsonb_build_object('count', SUM(activity_count), 'time', SUM(accumulated_time))
                            ) as activities
                        FROM user_activities
                        WHERE chat_id = $1 
                          AND user_id = ANY($2::bigint[]) 
                          AND activity_date >= $3 
                          AND activity_date < $4
                        GROUP BY user_id
                        """,
                        chat_id,
                        user_ids,
                        month_start,
                        month_end,
                    )

                    user_activities = {}
                    for row in activity_rows:
                        uid = row["user_id"]
                        activities = row["activities"]
                        if isinstance(activities, str):
                            try:
                                activities = json.loads(activities)
                            except:
                                activities = {}
                        user_activities[uid] = activities or {}

                    # ===== 5. 上下班计数 + 迟到早退 =====
                    work_counts_rows = await conn.fetch(
                        """
                        SELECT
                            user_id,
                            COUNT(CASE WHEN checkin_type='work_start' THEN 1 END) as work_start_count,
                            COUNT(CASE WHEN checkin_type='work_end' THEN 1 END) as work_end_count,
                            SUM(CASE WHEN checkin_type='work_start' THEN fine_amount ELSE 0 END) as work_start_fines,
                            SUM(CASE WHEN checkin_type='work_end' THEN fine_amount ELSE 0 END) as work_end_fines,
                            COUNT(CASE WHEN checkin_type='work_start' AND time_diff_minutes>0 THEN 1 END) as late_count,
                            COUNT(CASE WHEN checkin_type='work_end' AND time_diff_minutes<0 THEN 1 END) as early_count
                        FROM work_records
                        WHERE chat_id=$1 
                          AND user_id=ANY($2::bigint[]) 
                          AND record_date >= $3 
                          AND record_date < $4
                        GROUP BY user_id
                        """,
                        chat_id,
                        user_ids,
                        month_start,
                        month_end,
                    )
                    user_work_counts = {r["user_id"]: dict(r) for r in work_counts_rows}

                    # ===== 6. 夜班跨天处理 =====
                    night_rows = await conn.fetch(
                        """
                        SELECT 
                            wr_start.user_id,
                            wr_start.record_date as start_date,
                            wr_start.checkin_time as start_time,
                            wr_end.record_date as end_date,
                            wr_end.checkin_time as end_time
                        FROM work_records wr_start
                        LEFT JOIN work_records wr_end
                            ON wr_start.chat_id = wr_end.chat_id 
                            AND wr_start.user_id = wr_end.user_id
                            AND wr_start.shift = wr_end.shift
                            AND wr_start.record_date = wr_end.record_date
                            AND wr_start.checkin_type = 'work_start' 
                            AND wr_end.checkin_type = 'work_end'
                        WHERE wr_start.chat_id = $1 
                          AND wr_start.user_id = ANY($2::bigint[]) 
                          AND wr_start.shift = 'night' 
                          AND wr_start.checkin_type = 'work_start'
                        """,
                        chat_id,
                        user_ids,
                    )

                    user_night_work = {uid: {"days": 0, "hours": 0} for uid in user_ids}
                    for row in night_rows:
                        uid = row["user_id"]
                        if row["end_date"] and row["end_time"]:
                            try:
                                start_dt = beijing_tz.localize(
                                    datetime.combine(
                                        row["start_date"],
                                        datetime.strptime(
                                            row["start_time"], "%H:%M"
                                        ).time(),
                                    )
                                )
                                end_dt = beijing_tz.localize(
                                    datetime.combine(
                                        row["end_date"],
                                        datetime.strptime(
                                            row["end_time"], "%H:%M"
                                        ).time(),
                                    )
                                )
                                if end_dt < start_dt:
                                    end_dt += timedelta(days=1)

                                work_start = max(start_dt, month_start_dt)
                                work_end = min(end_dt, month_end_dt)
                                if work_end > work_start:
                                    seconds = int(
                                        (work_end - work_start).total_seconds()
                                    )
                                    user_night_work[uid]["hours"] += seconds
                                    user_night_work[uid]["days"] += 1
                            except Exception as e:
                                logger.error(f"夜班计算失败 user={uid}: {e}")

                    # ===== 7. 组装最终结果（按班次分离）=====
                    result = []
                    for uid in user_ids:
                        nickname = user_nicknames.get(uid, f"用户{uid}")
                        acts = user_activities.get(uid, {})
                        work = user_work_counts.get(uid, {})

                        # 白班数据
                        day_stats = stats_by_user_shift.get((uid, "day"), {})
                        result.append(
                            {
                                "user_id": uid,
                                "nickname": nickname,
                                "shift": "day",
                                "total_activity_count": day_stats.get(
                                    "total_activity_count", 0
                                ),
                                "total_accumulated_time": day_stats.get(
                                    "total_accumulated_time", 0
                                ),
                                "total_fines": day_stats.get("total_fines", 0),
                                "overtime_count": day_stats.get(
                                    "total_overtime_count", 0
                                ),
                                "total_overtime_time": day_stats.get(
                                    "total_overtime_time", 0
                                ),
                                "work_days": day_stats.get("work_days", 0),
                                "work_hours": day_stats.get("work_hours", 0),
                                "work_start_count": day_stats.get(
                                    "work_start_count", work.get("work_start_count", 0)
                                ),
                                "work_end_count": day_stats.get(
                                    "work_end_count", work.get("work_end_count", 0)
                                ),
                                "work_start_fines": day_stats.get(
                                    "work_start_fines", work.get("work_start_fines", 0)
                                ),
                                "work_end_fines": day_stats.get(
                                    "work_end_fines", work.get("work_end_fines", 0)
                                ),
                                "late_count": day_stats.get(
                                    "late_count", work.get("late_count", 0)
                                ),
                                "early_count": day_stats.get(
                                    "early_count", work.get("early_count", 0)
                                ),
                                "activities": acts,
                            }
                        )

                        # 夜班数据
                        night_stats = stats_by_user_shift.get((uid, "night"), {})
                        night_work = user_night_work.get(uid, {"days": 0, "hours": 0})
                        result.append(
                            {
                                "user_id": uid,
                                "nickname": nickname,
                                "shift": "night",
                                "total_activity_count": night_stats.get(
                                    "total_activity_count", 0
                                ),
                                "total_accumulated_time": night_stats.get(
                                    "total_accumulated_time", 0
                                ),
                                "total_fines": night_stats.get("total_fines", 0),
                                "overtime_count": night_stats.get(
                                    "total_overtime_count", 0
                                ),
                                "total_overtime_time": night_stats.get(
                                    "total_overtime_time", 0
                                ),
                                "work_days": night_work["days"],
                                "work_hours": night_work["hours"],
                                "work_start_count": night_stats.get(
                                    "work_start_count", 0
                                ),
                                "work_end_count": night_stats.get("work_end_count", 0),
                                "work_start_fines": night_stats.get(
                                    "work_start_fines", 0
                                ),
                                "work_end_fines": night_stats.get("work_end_fines", 0),
                                "late_count": night_stats.get("late_count", 0),
                                "early_count": night_stats.get("early_count", 0),
                                "activities": acts,
                            }
                        )

                    # 按用户ID和班次排序
                    result.sort(key=lambda x: (x["user_id"], x["shift"]))

                    logger.info(
                        f"✅ 月度统计完成\n"
                        f"   ├─ 年月: {year}年{month}月\n"
                        f"   ├─ 用户: {len(user_ids)} 人\n"
                        f"   └─ 记录: {len(result)} 条"
                    )
                    return result

        except asyncio.TimeoutError:
            logger.error(f"❌ 月度统计超时 ({timeout}s) chat={chat_id}")
            return []
        except Exception as e:
            logger.error(f"❌ 月度统计失败 chat={chat_id}: {e}", exc_info=True)
            return []

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

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT 
                    wr.user_id,
                    u.nickname,
                    COUNT(CASE WHEN wr.checkin_type = 'work_start' THEN 1 END) as work_start_count,
                    COUNT(CASE WHEN wr.checkin_type = 'work_end' THEN 1 END) as work_end_count,
                    SUM(CASE WHEN wr.checkin_type = 'work_start' THEN wr.fine_amount ELSE 0 END) as work_start_fines,
                    SUM(CASE WHEN wr.checkin_type = 'work_end' THEN wr.fine_amount ELSE 0 END) as work_end_fines
                FROM work_records wr
                JOIN users u ON wr.chat_id = u.chat_id AND wr.user_id = u.user_id
                WHERE wr.chat_id = $1 AND wr.record_date >= $2 AND wr.record_date < $3
                GROUP BY wr.user_id, u.nickname
                """,
                chat_id,
                start_date,
                end_date,
            )
            return [dict(row) for row in rows]

    async def get_monthly_activity_ranking(
        self, chat_id: int, year: int = None, month: int = None
    ) -> Dict[str, List]:
        """获取月度活动排行榜"""
        if year is None or month is None:
            today = self.get_beijing_time()
            year = today.year
            month = today.month

        statistic_date = date(year, month, 1)
        activity_limits = await self.get_activity_limits()

        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rankings = {}
            for activity in activity_limits.keys():
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
                rankings[activity] = [dict(row) for row in rows]
            return rankings

    async def get_user_late_early_counts(
        self, chat_id: int, user_id: int, year: int, month: int
    ) -> Dict[str, int]:
        """获取用户的迟到早退次数统计"""
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, month + 1, 1)

        async with self.pool.acquire() as conn:
            late_count = (
                await conn.fetchval(
                    """
                SELECT COUNT(*) FROM work_records 
                WHERE chat_id = $1 AND user_id = $2 
                AND record_date >= $3 AND record_date < $4
                AND checkin_type = 'work_start' AND time_diff_minutes > 0
                """,
                    chat_id,
                    user_id,
                    start_date,
                    end_date,
                )
                or 0
            )

            early_count = (
                await conn.fetchval(
                    """
                SELECT COUNT(*) FROM work_records 
                WHERE chat_id = $1 AND user_id = $2 
                AND record_date >= $3 AND record_date < $4
                AND checkin_type = 'work_end' AND time_diff_minutes < 0
                """,
                    chat_id,
                    user_id,
                    start_date,
                    end_date,
                )
                or 0
            )

            return {"late_count": late_count, "early_count": early_count}

    # ========== 设置用户班次状态==========

    async def set_user_shift_state(
        self,
        chat_id: int,
        user_id: int,
        shift: str,
        record_date: date,
    ) -> bool:
        """设置用户班次状态（上班打卡）"""
        try:
            now = self.get_beijing_time()
            await self.execute_with_retry(
                "设置用户班次状态",
                """
                INSERT INTO group_shift_state
                (chat_id, user_id, shift, record_date, shift_start_time)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (chat_id, user_id, shift)
                DO UPDATE SET
                    record_date = EXCLUDED.record_date,
                    shift_start_time = EXCLUDED.shift_start_time,
                    updated_at = CURRENT_TIMESTAMP
                """,
                chat_id,
                user_id,
                shift,
                record_date,
                now,
            )

            cache_key = f"shift_state:{chat_id}:{user_id}:{shift}"
            self._cache.pop(cache_key, None)
            self._cache_ttl.pop(cache_key, None)
            return True

        except Exception as e:
            logger.error(f"设置用户班次状态失败: {e}")
            return False

    async def clear_user_shift_state(
        self,
        chat_id: int,
        user_id: int,
        shift: str,
    ) -> bool:
        """清除用户班次状态（下班打卡）"""
        try:
            await self.execute_with_retry(
                "清除用户班次状态",
                """
                DELETE FROM group_shift_state
                WHERE chat_id = $1 AND user_id = $2 AND shift = $3
                """,
                chat_id,
                user_id,
                shift,
            )

            cache_key = f"shift_state:{chat_id}:{user_id}:{shift}"
            self._cache.pop(cache_key, None)
            self._cache_ttl.pop(cache_key, None)
            return True

        except Exception as e:
            logger.error(f"清除用户班次状态失败: {e}")
            return False

    async def get_user_shift_state(
        self,
        chat_id: int,
        user_id: int,
        shift: str,
    ) -> Optional[Dict]:
        """获取用户班次状态"""
        cache_key = f"shift_state:{chat_id}:{user_id}:{shift}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT * FROM group_shift_state
                    WHERE chat_id = $1 AND user_id = $2 AND shift = $3
                    """,
                    chat_id,
                    user_id,
                    shift,
                )

                if row:
                    result = dict(row)
                    self._set_cached(cache_key, result, 30)
                    return result
                return None

        except Exception as e:
            logger.error(f"获取用户班次状态失败: {e}")
            return None

    async def get_user_current_shift(
        self,
        chat_id: int,
        user_id: int,
    ) -> Optional[Dict]:
        """获取用户当前活跃的班次（基于 work_records）"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT shift, record_date, created_at as shift_start_time
                    FROM work_records 
                    WHERE chat_id = $1 
                      AND user_id = $2 
                      AND checkin_type = 'work_start'
                      AND NOT EXISTS (
                          SELECT 1 FROM work_records wr2
                          WHERE wr2.chat_id = work_records.chat_id
                            AND wr2.user_id = work_records.user_id
                            AND wr2.shift = work_records.shift
                            AND wr2.record_date = work_records.record_date
                            AND wr2.checkin_type = 'work_end'
                      )
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    chat_id,
                    user_id,
                )

                if row:
                    return {
                        "shift": row["shift"],
                        "record_date": row["record_date"],
                        "shift_start_time": row["shift_start_time"],
                    }
                return None

        except Exception as e:
            logger.error(f"获取用户当前班次失败: {e}")
            return None

    async def cleanup_expired_shift_states(self):
        """清理过期的用户班次状态（超过16小时）"""
        try:
            now = self.get_beijing_time()
            expired_time = now - timedelta(hours=16)

            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT chat_id, user_id, shift
                    FROM group_shift_state
                    WHERE shift_start_time < $1
                    """,
                    expired_time,
                )

                result = await conn.execute(
                    """
                    DELETE FROM group_shift_state
                    WHERE shift_start_time < $1
                    """,
                    expired_time,
                )

                deleted = 0
                if result and result.startswith("DELETE"):
                    deleted = int(result.split()[-1])

                if deleted > 0:
                    logger.info(f"🧹 清理了 {deleted} 个过期的用户班次状态")

                    for row in rows:
                        cache_key = f"shift_state:{row['chat_id']}:{row['user_id']}:{row['shift']}"
                        self._cache.pop(cache_key, None)
                        self._cache_ttl.pop(cache_key, None)

                return deleted

        except Exception as e:
            logger.error(f"清理过期班次状态失败: {e}")
            return 0

    # ========== 用户当前班次辅助方法 ==========
    async def get_user_active_shift(self, chat_id: int, user_id: int) -> Optional[Dict]:
        """获取用户当前活跃的班次（任意班次）"""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT shift, record_date, shift_start_time
                    FROM group_shift_state
                    WHERE chat_id = $1 AND user_id = $2
                    ORDER BY shift_start_time DESC
                    LIMIT 1
                    """,
                    chat_id,
                    user_id,
                )
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"获取用户活跃班次失败: {e}")
            return None

    async def count_active_users_in_shift(self, chat_id: int, shift: str) -> int:
        """统计指定班次中的活跃用户数"""
        try:
            async with self.pool.acquire() as conn:
                count = await conn.fetchval(
                    """
                    SELECT COUNT(*) FROM group_shift_state
                    WHERE chat_id = $1 AND shift = $2
                    """,
                    chat_id,
                    shift,
                )
                return count or 0
        except Exception as e:
            logger.error(f"统计班次活跃用户失败: {e}")
            return 0

    async def update_group_dual_mode(
        self, chat_id: int, enabled: bool, day_start: str = None, day_end: str = None
    ):
        """更新双班模式配置"""
        if enabled and (day_start is None or day_end is None):
            raise ValueError("开启双班模式必须提供白班开始和结束时间")

        await self.execute_with_retry(
            "更新双班模式",
            """
            UPDATE groups SET 
                dual_mode = $1,
                dual_day_start = $2,
                dual_day_end = $3,
                updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = $4
            """,
            enabled,
            day_start if enabled else None,
            day_end if enabled else None,
            chat_id,
        )
        self._cache.pop(f"group:{chat_id}", None)

    async def update_shift_grace_window(
        self, chat_id: int, grace_before: int, grace_after: int
    ):
        """更新时间宽容窗口"""
        await self.execute_with_retry(
            "更新时间宽容窗口",
            """
            UPDATE groups SET 
                shift_grace_before = $1,
                shift_grace_after = $2,
                updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = $3
            """,
            grace_before,
            grace_after,
            chat_id,
        )
        self._cache.pop(f"group:{chat_id}", None)

    async def update_workend_grace_window(
        self, chat_id: int, grace_before: int, grace_after: int
    ):
        """更新下班专用时间窗口"""
        await self.execute_with_retry(
            "更新下班时间窗口",
            """
            UPDATE groups SET 
                workend_grace_before = $1,
                workend_grace_after = $2,
                updated_at = CURRENT_TIMESTAMP
            WHERE chat_id = $3
            """,
            grace_before,
            grace_after,
            chat_id,
        )
        self._cache.pop(f"group:{chat_id}", None)

    async def get_shift_config(self, chat_id: int) -> Dict:
        """获取班次配置（默认双班模式）"""
        group_data = await self.get_group_cached(chat_id)
        if not group_data:
            return {
                "dual_mode": True,
                "day_start": "09:00",
                "day_end": "21:00",
                "grace_before": Config.DEFAULT_GRACE_BEFORE,
                "grace_after": Config.DEFAULT_GRACE_AFTER,
                "workend_grace_before": Config.DEFAULT_WORKEND_GRACE_BEFORE,
                "workend_grace_after": Config.DEFAULT_WORKEND_GRACE_AFTER,
            }

        work_hours = await self.get_group_work_time(chat_id)
        has_work_time = await self.has_work_hours_enabled(chat_id)

        if has_work_time:
            day_start = work_hours["work_start"]
            day_end = work_hours["work_end"]
        elif group_data.get("dual_mode"):
            day_start = group_data.get("dual_day_start", "09:00")
            day_end = group_data.get("dual_day_end", "21:00")
        else:
            day_start = "09:00"
            day_end = "21:00"

        return {
            "dual_mode": bool(group_data.get("dual_mode", True)),
            "day_start": day_start,
            "day_end": day_end,
            "grace_before": group_data.get(
                "shift_grace_before", Config.DEFAULT_GRACE_BEFORE
            ),
            "grace_after": group_data.get(
                "shift_grace_after", Config.DEFAULT_GRACE_AFTER
            ),
            "workend_grace_before": group_data.get(
                "workend_grace_before", Config.DEFAULT_WORKEND_GRACE_BEFORE
            ),
            "workend_grace_after": group_data.get(
                "workend_grace_after", Config.DEFAULT_WORKEND_GRACE_AFTER
            ),
        }

    # database.py 添加
    async def is_dual_mode_enabled(self, chat_id: int) -> bool:
        """检查双班模式是否启用"""
        group_data = await self.get_group_cached(chat_id)
        if not group_data:
            return True  # 默认开启
        return group_data.get("dual_mode", True)

    def calculate_shift_window(
        self,
        shift_config: Dict[str, Any],
        checkin_type: str = None,
        now: Optional[datetime] = None,
        active_shift: Optional[str] = None,
        active_record_date: Optional[date] = None,
    ) -> Dict[str, Any]:

        if now is None:
            now = self.get_beijing_time()

        tz = now.tzinfo

        default_return = {
            "day_window": {},
            "night_window": {},
            "current_shift": None,
        }

        if not shift_config:
            return default_return

        try:
            day_start_time = datetime.strptime(
                shift_config.get("day_start", "09:00"), "%H:%M"
            ).time()
            day_end_time = datetime.strptime(
                shift_config.get("day_end", "21:00"), "%H:%M"
            ).time()
        except Exception:
            return default_return

        if active_record_date:
            base_date = active_record_date
            logger.debug(f"使用状态日期计算窗口: {base_date}")
        else:
            base_date = now.date()
            logger.debug(f"使用当前日期计算窗口: {base_date}")

        day_start_dt = datetime.combine(base_date, day_start_time).replace(tzinfo=tz)
        day_end_dt = datetime.combine(base_date, day_end_time).replace(tzinfo=tz)

        if checkin_type == "activity":
            if active_shift:
                if active_shift == "day":
                    current_shift_detail = "day"
                    logger.debug(
                        f"📊 activity跟随白班: active_shift={active_shift}, "
                        f"now={now.strftime('%H:%M')}"
                    )
                else:
                    if now >= day_end_dt:
                        current_shift_detail = "night_tonight"
                        logger.debug(
                            f"📊 activity跟随夜班(今晚): active_shift={active_shift}, "
                            f"now={now.strftime('%H:%M')} >= {day_end_dt.strftime('%H:%M')}"
                        )
                    else:
                        current_shift_detail = "night_last"
                        logger.debug(
                            f"📊 activity跟随夜班(昨晚): active_shift={active_shift}, "
                            f"now={now.strftime('%H:%M')} < {day_end_dt.strftime('%H:%M')}"
                        )
            else:
                if day_start_dt <= now < day_end_dt:
                    current_shift_detail = "day"
                    logger.debug(
                        f"📊 activity无活跃班次，时间在白班区间: {now.strftime('%H:%M')}"
                    )
                elif now >= day_end_dt:
                    current_shift_detail = "night_tonight"
                    logger.debug(
                        f"📊 activity无活跃班次，时间在夜班区间(今晚): {now.strftime('%H:%M')}"
                    )
                else:
                    current_shift_detail = "night_last"
                    logger.debug(
                        f"📊 activity无活跃班次，时间在夜班区间(昨晚): {now.strftime('%H:%M')}"
                    )

            return {
                "day_window": {},
                "night_window": {},
                "current_shift": current_shift_detail,
            }

        grace_before = shift_config.get("grace_before", Config.DEFAULT_GRACE_BEFORE)
        grace_after = shift_config.get("grace_after", Config.DEFAULT_GRACE_AFTER)
        workend_grace_before = shift_config.get(
            "workend_grace_before", Config.DEFAULT_WORKEND_GRACE_BEFORE
        )
        workend_grace_after = shift_config.get(
            "workend_grace_after", Config.DEFAULT_WORKEND_GRACE_AFTER
        )

        day_window = {
            "work_start": {
                "start": (day_start_dt - timedelta(minutes=grace_before)).replace(
                    tzinfo=tz
                ),
                "end": (day_start_dt + timedelta(minutes=grace_after)).replace(
                    tzinfo=tz
                ),
            },
            "work_end": {
                "start": (day_end_dt - timedelta(minutes=workend_grace_before)).replace(
                    tzinfo=tz
                ),
                "end": (day_end_dt + timedelta(minutes=workend_grace_after)).replace(
                    tzinfo=tz
                ),
            },
        }

        last_night_window = {
            "work_start": {
                "start": (
                    day_end_dt
                    - timedelta(days=1)
                    - timedelta(minutes=workend_grace_before)
                ).replace(tzinfo=tz),
                "end": (
                    day_end_dt
                    - timedelta(days=1)
                    + timedelta(minutes=workend_grace_after)
                ).replace(tzinfo=tz),
            },
            "work_end": {
                "start": (day_start_dt - timedelta(minutes=grace_before)).replace(
                    tzinfo=tz
                ),
                "end": (day_start_dt + timedelta(minutes=grace_after)).replace(
                    tzinfo=tz
                ),
            },
        }

        tonight_window = {
            "work_start": {
                "start": (day_end_dt - timedelta(minutes=workend_grace_before)).replace(
                    tzinfo=tz
                ),
                "end": (day_end_dt + timedelta(minutes=workend_grace_after)).replace(
                    tzinfo=tz
                ),
            },
            "work_end": {
                "start": (
                    day_start_dt + timedelta(days=1) - timedelta(minutes=grace_before)
                ).replace(tzinfo=tz),
                "end": (
                    day_start_dt + timedelta(days=1) + timedelta(minutes=grace_after)
                ).replace(tzinfo=tz),
            },
        }

        current_shift = None

        if checkin_type in ("work_start", "work_end"):
            lookup = checkin_type

            if day_window[lookup]["start"] <= now <= day_window[lookup]["end"]:
                current_shift = "day"
            elif (
                last_night_window[lookup]["start"]
                <= now
                <= last_night_window[lookup]["end"]
            ):
                current_shift = "night_last"
            elif (
                tonight_window[lookup]["start"] <= now <= tonight_window[lookup]["end"]
            ):
                current_shift = "night_tonight"
            elif lookup == "work_start":
                afternoon_start = day_window["work_start"]["end"] + timedelta(minutes=1)
                afternoon_end = tonight_window["work_start"]["start"] - timedelta(
                    minutes=1
                )
                if afternoon_start <= now <= afternoon_end:
                    current_shift = "night_tonight"

        if current_shift is None and active_shift:
            if active_shift == "day":
                current_shift = "day"
            else:
                if now >= day_end_dt:
                    current_shift = "night_tonight"
                else:
                    current_shift = "night_last"

        return {
            "day_window": day_window,
            "night_window": {
                "last_night": last_night_window,
                "tonight": tonight_window,
            },
            "current_shift": current_shift,
        }

    async def get_business_date(
        self,
        chat_id: int,
        current_dt: datetime = None,
        shift: str = None,
        checkin_type: str = None,
        shift_detail: str = None,
        record_date: Optional[date] = None,
    ) -> date:
        """获取业务日期 - 纯双班模式"""
        if current_dt is None:
            current_dt = self.get_beijing_time()

        today = current_dt.date()

        if record_date is not None:
            if shift == "night" and checkin_type == "work_end":
                business_date = record_date + timedelta(days=1)
                logger.debug(
                    f"📅 [业务日期-状态模型-夜班下班] "
                    f"chat_id={chat_id}, "
                    f"record_date={record_date}, "
                    f"business_date={business_date}"
                )
            else:
                business_date = record_date
                logger.debug(
                    f"📅 [业务日期-状态模型] "
                    f"chat_id={chat_id}, "
                    f"record_date={record_date}, "
                    f"shift={shift}, "
                    f"checkin_type={checkin_type}"
                )
            return business_date

        if shift_detail in ("night_last", "night_tonight", "day"):
            if shift_detail == "night_last":
                business_date = today - timedelta(days=1)
            else:
                business_date = today

            logger.debug(
                f"📅 [业务日期-双班-detail] "
                f"chat_id={chat_id}, "
                f"time={current_dt.strftime('%H:%M:%S')}, "
                f"shift_detail={shift_detail}, "
                f"checkin_type={checkin_type}, "
                f"result={business_date}"
            )
            return business_date

        shift_config = await self.get_shift_config(chat_id)
        day_start = shift_config.get("day_start", "09:00")
        grace_before = shift_config.get("grace_before", 120)

        day_start_time = datetime.strptime(day_start, "%H:%M").time()
        day_start_dt = datetime.combine(today, day_start_time).replace(
            tzinfo=current_dt.tzinfo
        )

        earliest_day_time = day_start_dt - timedelta(minutes=grace_before)

        if current_dt >= earliest_day_time:
            logger.debug(
                f"📅 [提前上班判定] "
                f"chat={chat_id}, "
                f"time={current_dt.strftime('%H:%M')}, "
                f"earliest={earliest_day_time.strftime('%H:%M')}, "
                f"result={today}"
            )
            return today

        if shift and checkin_type:
            window_info = (
                self.calculate_shift_window(
                    shift_config=shift_config,
                    checkin_type=checkin_type,
                    now=current_dt,
                )
                or {}
            )

            current_shift_detail = window_info.get("current_shift")

            if current_shift_detail == "night_last":
                business_date = today - timedelta(days=1)
            elif current_shift_detail in ("night_tonight", "day"):
                business_date = today
            else:
                business_date = today

            logger.debug(
                f"📅 业务日期(双班-window): chat_id={chat_id}, "
                f"shift={shift}, checkin_type={checkin_type}, "
                f"判定={current_shift_detail}, 日期={business_date}"
            )
            return business_date

        logger.debug(f"📅 [双班-fallback] chat={chat_id}, 日期={today}")
        return today

    async def determine_shift_for_time(
        self,
        chat_id: int,
        current_time: Optional[datetime] = None,
        checkin_type: str = "work_start",
        active_shift: Optional[str] = None,
        active_record_date: Optional[date] = None,
    ) -> Dict[str, object]:
        """企业级终极班次判定函数"""

        now = current_time or self.get_beijing_time()

        shift_config = await self.get_shift_config(chat_id) or {}

        if active_shift and active_record_date:

            if active_shift not in ("day", "night"):
                raise ValueError(f"非法 shift: {active_shift}")

            if not isinstance(active_record_date, date):
                raise TypeError("active_record_date 必须是 date")

            shift = active_shift

            record_date = active_record_date

            if shift == "day":

                shift_detail = "day"

            else:

                day_end_str = shift_config.get("day_end", "21:00")

                day_end_time = datetime.strptime(day_end_str, "%H:%M").time()

                night_start = datetime.combine(
                    record_date,
                    day_end_time,
                ).replace(tzinfo=now.tzinfo)

                night_end = night_start + timedelta(days=1)

                if night_start <= now < night_end:

                    shift_detail = "night_tonight"

                else:

                    shift_detail = "night_last"

            window_info = (
                self.calculate_shift_window(
                    shift_config=shift_config,
                    checkin_type=checkin_type,
                    now=now,
                    active_shift=shift,
                    active_record_date=record_date,
                )
                or {}
            )

            if checkin_type == "activity":

                in_window = True

            else:

                in_window = self._is_time_in_window(
                    now,
                    shift,
                    shift_detail,
                    checkin_type,
                    window_info,
                )

            business_date = await self.get_business_date(
                chat_id=chat_id,
                current_dt=now,
                shift=shift,
                checkin_type=checkin_type,
                shift_detail=shift_detail,
                record_date=record_date,
            )

            return dict(
                shift=shift,
                shift_detail=shift_detail,
                business_date=business_date,
                record_date=record_date,
                is_dual=True,
                in_window=in_window,
                window_info=window_info,
                using_state=True,
            )

        window_info = (
            self.calculate_shift_window(
                shift_config=shift_config,
                checkin_type=checkin_type,
                now=now,
            )
            or {}
        )

        shift_detail = window_info.get("current_shift")

        if shift_detail is None:

            shift_detail = self._fallback_shift_detail(
                now,
                shift_config,
            )

        shift = "night" if shift_detail.startswith("night") else "day"

        if checkin_type == "activity":

            in_window = True

        else:

            in_window = self._is_time_in_window(
                now,
                shift,
                shift_detail,
                checkin_type,
                window_info,
            )

        record_date = await self.get_business_date(
            chat_id=chat_id,
            current_dt=now,
            shift=shift,
            checkin_type=checkin_type,
            shift_detail=shift_detail,
        )

        return dict(
            shift=shift,
            shift_detail=shift_detail,
            business_date=record_date,
            record_date=record_date,
            is_dual=True,
            in_window=in_window,
            window_info=window_info,
            using_state=False,
        )

    def _is_time_in_window(
        self,
        now: datetime,
        shift: str,
        shift_detail: str,
        checkin_type: str,
        window_info: dict,
    ) -> bool:
        """判断时间是否在窗口内"""
        try:
            if checkin_type == "work_start":
                if shift == "day":
                    day_window = window_info.get("day_window", {}).get("work_start", {})
                    return bool(
                        day_window.get("start")
                        and day_window.get("end")
                        and day_window["start"] <= now <= day_window["end"]
                    )
                else:
                    night_window = window_info.get("night_window", {})
                    if shift_detail == "night_last":
                        target = night_window.get("last_night", {}).get(
                            "work_start", {}
                        )
                    else:
                        target = night_window.get("tonight", {}).get("work_start", {})
                    return bool(
                        target.get("start")
                        and target.get("end")
                        and target["start"] <= now <= target["end"]
                    )
            else:
                if shift == "day":
                    day_window = window_info.get("day_window", {}).get("work_end", {})
                    return bool(
                        day_window.get("start")
                        and day_window.get("end")
                        and day_window["start"] <= now <= day_window["end"]
                    )
                else:
                    night_window = window_info.get("night_window", {})
                    if shift_detail == "night_last":
                        target = night_window.get("last_night", {}).get("work_end", {})
                    else:
                        target = night_window.get("tonight", {}).get("work_end", {})
                    return bool(
                        target.get("start")
                        and target.get("end")
                        and target["start"] <= now <= target["end"]
                    )
        except Exception as e:
            logger.error(f"窗口检查失败: {e}")
            return False

    def _fallback_shift_detail(
        self,
        now,
        shift_config,
    ):

        day_start = shift_config.get("day_start", "09:00")

        day_end = shift_config.get("day_end", "21:00")

        day_start_dt = datetime.combine(
            now.date(),
            datetime.strptime(day_start, "%H:%M").time(),
        ).replace(tzinfo=now.tzinfo)

        day_end_dt = datetime.combine(
            now.date(),
            datetime.strptime(day_end, "%H:%M").time(),
        ).replace(tzinfo=now.tzinfo)

        if day_start_dt <= now < day_end_dt:

            return "day"

        elif now >= day_end_dt:

            return "night_tonight"

        else:

            return "night_last"

    # ========== 数据清理 ==========
    def _parse_row_count(self, result_str: str) -> int:
        """解析 PostgreSQL DELETE 结果中的行数"""
        try:
            # asyncpg 返回的格式通常是 "DELETE 100"
            if result_str and result_str.startswith("DELETE"):
                parts = result_str.split()
                if len(parts) == 2:
                    return int(parts[1])
        except (ValueError, IndexError, AttributeError):
            # 捕获可能的解析异常，确保维护逻辑不会因为日志解析失败而中断
            pass
        return 0

    async def cleanup_old_data(self, days: int = 30) -> int:
        """清理旧数据 - 最终极致稳定版"""
        try:
            now = self.get_beijing_time()
            cutoff_date = (now - timedelta(days=days)).date()
            total_deleted = 0

            self._ensure_pool_initialized()

            async with self.pool.acquire() as conn:
                # 1. 设置语句级的安全超时（60s），防止在大表清理时因锁竞争卡死连接池
                await conn.execute("SET statement_timeout = '60s'")

                # 定义需要清理的表名及其对应的时间戳字段
                tables = [
                    ("user_activities", "activity_date"),
                    ("work_records", "record_date"),
                    ("daily_statistics", "record_date"),
                    ("reset_logs", "reset_date"),  # 补全日志清理
                ]

                # 2. 采用单条执行模式，而非大事务
                # 这样做的好处是：删掉一个表就即时生效一个，减少长事务对数据库 Undo Log 的压力
                for table, col in tables:
                    try:
                        # 执行删除操作
                        result = await conn.execute(
                            f"DELETE FROM {table} WHERE {col} < $1", cutoff_date
                        )

                        # 解析受影响的行数
                        count = self._parse_row_count(result)
                        total_deleted += count

                        if count > 0:
                            logger.info(f"🧹 已清理 {table}: {count} 条记录")
                    except Exception as table_e:
                        # 局部异常捕获：即使某个表清理失败（如字段改名或表锁），也不影响后续表的清理
                        logger.error(f"⚠️ 清理表 {table} 时出现局部异常: {table_e}")
                        continue

            if total_deleted > 0:
                logger.info(
                    f"✅ 定期数据清理任务完成，总计移除: {total_deleted} 条记录"
                )

            return total_deleted

        except Exception as e:
            # 全局异常捕获：记录完整堆栈，确保后台维护循环不被中断
            logger.error(f"❌ 数据清理全局任务失败: {e}", exc_info=True)
            return 0

    async def cleanup_monthly_data(self, days_or_date=None):
        """清理月度统计数据"""
        import traceback

        try:
            today = self.get_beijing_time()

            if days_or_date is None:
                cutoff_date = (
                    (today - timedelta(days=Config.MONTHLY_DATA_RETENTION_DAYS))
                    .date()
                    .replace(day=1)
                )
                logger.info(
                    f"📅 使用默认配置: {Config.MONTHLY_DATA_RETENTION_DAYS}天, "
                    f"截止日期={cutoff_date}"
                )

            elif isinstance(days_or_date, int):
                if days_or_date <= 0:
                    logger.warning(f"⚠️ 无效的天数: {days_or_date}，必须大于0")
                    return 0

                cutoff_date = (
                    (today - timedelta(days=days_or_date)).date().replace(day=1)
                )
                logger.info(
                    f"📅 按天数清理: {days_or_date}天前, 截止日期={cutoff_date}"
                )

            elif isinstance(days_or_date, date):
                cutoff_date = days_or_date
                logger.info(f"📅 按日期清理: 截止日期={cutoff_date}")

            else:
                logger.error(f"❌ 无效的参数类型: {type(days_or_date)}")
                return 0

            if cutoff_date > today.date():
                logger.warning(f"⚠️ 截止日期 {cutoff_date} 晚于今天，不会删除任何数据")
                return 0

            self._ensure_pool_initialized()
            async with self.pool.acquire() as conn:
                result = await conn.execute(
                    "DELETE FROM monthly_statistics WHERE statistic_date < $1",
                    cutoff_date,
                )

                deleted_count = 0
                if result and result.startswith("DELETE"):
                    try:
                        deleted_count = int(result.split()[-1])
                    except (ValueError, IndexError):
                        pass

                logger.info(
                    f"✅ 月度数据清理完成\n"
                    f"   ├─ 截止日期: {cutoff_date}\n"
                    f"   ├─ 删除记录: {deleted_count} 条\n"
                    f"   └─ 参数: {days_or_date or '默认'}"
                )

                return deleted_count

        except Exception as e:
            logger.error(f"❌ 月度数据清理失败: {e}")
            logger.error(traceback.format_exc())
            return 0

    async def cleanup_specific_month(self, year: int, month: int):
        """清理指定年月的月度统计数据"""
        target_date = date(year, month, 1)
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM monthly_statistics WHERE statistic_date = $1", target_date
            )
            return (
                int(result.split()[-1]) if result and result.startswith("DELETE") else 0
            )

    async def cleanup_inactive_users(self, days: int = 30) -> int:
        """清理长期未活动用户及其记录（安全版）"""

        # ===== 1. 计算截止日期 =====
        now = self.get_beijing_time()
        cutoff_date = (now - timedelta(days=days)).date()
        recent_threshold = (now - timedelta(days=30)).date()

        logger.info(f"🧹 开始清理 {days} 天未活动用户，截止日期: {cutoff_date}")

        try:
            async with self.pool.acquire() as conn:
                # 使用事务保证清理过程的原子性，防止出现“删了记录但没删用户”的情况
                async with conn.transaction():

                    # ===== 2. 找出要删除的用户 =====
                    # 判定标准：last_updated 过期，且在所有业务表中近期都没有活动记录
                    users_to_delete = await conn.fetch(
                        """
                        SELECT u.chat_id, u.user_id, u.nickname
                        FROM users u
                        WHERE u.last_updated < $1
                        AND NOT EXISTS (
                            SELECT 1 FROM daily_statistics ds 
                            WHERE ds.chat_id = u.chat_id 
                            AND ds.user_id = u.user_id
                            AND ds.record_date > $2
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM work_records wr 
                            WHERE wr.chat_id = u.chat_id 
                            AND wr.user_id = u.user_id
                            AND wr.record_date > $2
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM user_activities ua 
                            WHERE ua.chat_id = u.chat_id 
                            AND ua.user_id = u.user_id
                            AND ua.activity_date > $2
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM monthly_statistics ms 
                            WHERE ms.chat_id = u.chat_id 
                            AND ms.user_id = u.user_id
                        )
                        """,
                        cutoff_date,
                        recent_threshold,
                    )

                    if not users_to_delete:
                        logger.info("✅ 没有需要清理的用户")
                        return 0

                    user_ids = [u["user_id"] for u in users_to_delete]
                    chat_ids = list(set([u["chat_id"] for u in users_to_delete]))

                    logger.info(
                        f"📊 发现 {len(users_to_delete)} 个可清理的用户，"
                        f"分布在 {len(chat_ids)} 个群组"
                    )

                    # ===== 3. 批量删除相关记录 =====
                    # 使用 ANY($1::bigint[]) 配合 List 传入，比循环 DELETE 快得多
                    ua_result = await conn.execute(
                        "DELETE FROM user_activities WHERE user_id = ANY($1::bigint[])",
                        user_ids,
                    )
                    ua_count = self._parse_sql_result(ua_result)

                    wr_result = await conn.execute(
                        "DELETE FROM work_records WHERE user_id = ANY($1::bigint[])",
                        user_ids,
                    )
                    wr_count = self._parse_sql_result(wr_result)

                    ds_result = await conn.execute(
                        "DELETE FROM daily_statistics WHERE user_id = ANY($1::bigint[])",
                        user_ids,
                    )
                    ds_count = self._parse_sql_result(ds_result)

                    user_result = await conn.execute(
                        "DELETE FROM users WHERE user_id = ANY($1::bigint[])",
                        user_ids,
                    )
                    user_count = self._parse_sql_result(user_result)

                    # ===== 4. 清理内存缓存 =====
                    for user in users_to_delete:
                        cache_key = f"user:{user['chat_id']}:{user['user_id']}"
                        self._cache.pop(cache_key, None)
                        self._cache_ttl.pop(cache_key, None)

                    # ===== 5. 记录结果 =====
                    total_deleted = ua_count + wr_count + ds_count + user_count
                    logger.info(
                        f"✅ 清理完成:\n"
                        f"    ├─ 删除用户: {user_count} 个\n"
                        f"    ├─ 删除活动记录: {ua_count} 条\n"
                        f"    ├─ 删除工作记录: {wr_count} 条\n"
                        f"    ├─ 删除日统计: {ds_count} 条\n"
                        f"    └─ 总计删除: {total_deleted} 条记录"
                    )

                    return user_count

        except Exception as e:
            logger.error(f"❌ 清理用户失败: {e}", exc_info=True)
            return 0

    def _parse_sql_result(self, result: str) -> int:
        """解析 SQL 执行结果，返回影响的行数"""
        if not result or not isinstance(result, str):
            return 0
        try:
            parts = result.split()
            # 兼容 DELETE, UPDATE, INSERT 后的 Command Tag
            if len(parts) >= 2 and parts[0] in ("DELETE", "UPDATE", "INSERT"):
                return int(parts[-1])
        except (ValueError, IndexError):
            pass
        return 0

    # ========== 重置日志管理 ==========
    async def mark_reset_completed(self, chat_id: int, target_date: date) -> bool:
        """标记重置已完成（持久化到数据库）"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO reset_logs (chat_id, reset_date, completed_at)
                    VALUES ($1, $2, NOW())
                    ON CONFLICT (chat_id, reset_date) DO NOTHING
                """,
                    chat_id,
                    target_date,
                )
            logger.debug(f"✅ 已持久化重置标记: 群组 {chat_id}, 日期 {target_date}")
            return True
        except Exception as e:
            logger.error(f"❌ 持久化重置标记失败: {e}")
            return False

    async def is_reset_completed(self, chat_id: int, target_date: date) -> bool:
        """检查重置是否已完成"""
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval(
                    """
                    SELECT 1 FROM reset_logs
                    WHERE chat_id = $1 AND reset_date = $2
                """,
                    chat_id,
                    target_date,
                )
                return result is not None
        except Exception as e:
            logger.error(f"❌ 检查重置标记失败: {e}")
            return False

    async def cleanup_old_reset_logs(self, days: int = 90) -> int:
        """清理旧的 reset_logs（保留90天） - 优化版"""
        try:
            # 1. 统一北京时间计算：确保与业务逻辑中的 reset_date 统计口径一致
            now = self.get_beijing_time()
            cutoff_date = (now - timedelta(days=days)).date()

            # 确保连接池可用，避免连接池未初始化导致的空对象错误
            self._ensure_pool_initialized()

            async with self.pool.acquire() as conn:
                # 2. 安全防御：设置当前连接的语句超时时间为 30s
                # 防止在大表 DELETE 锁争用或 IO 繁忙时导致维护协程无限期挂起
                await conn.execute("SET statement_timeout = '30s'")

                # 执行清理动作
                result = await conn.execute(
                    "DELETE FROM reset_logs WHERE reset_date < $1",
                    cutoff_date,
                )

                # 3. 解析结果：从 asyncpg 返回的 'DELETE N' 字符串中提取删除数量
                deleted = 0
                if result and result.startswith("DELETE"):
                    try:
                        deleted = int(result.split()[-1])
                    except (ValueError, IndexError):
                        pass

                # 仅在有实际删除时记录日志，避免维护循环产生过多的冗余日志
                if deleted > 0:
                    logger.info(
                        f"🧹 清理了 {deleted} 条旧重置日志 (保留截止日期: {cutoff_date})"
                    )

                return deleted

        except Exception as e:
            # 记录详细的异常堆栈，便于排查 SQL 语法或连接超时问题
            logger.error(f"❌ 清理重置日志失败: {e}", exc_info=True)
            return 0

    # ========== 活动人数限制 ==========
    async def set_activity_user_limit(self, activity: str, max_users: int):
        """设置活动人数限制"""
        self._ensure_pool_initialized()
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
        self._cache.pop(f"activity_limit:{activity}", None)

    async def get_activity_user_limit(self, activity: str) -> int:
        """获取活动人数限制"""
        cache_key = f"activity_limit:{activity}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        self._ensure_pool_initialized()
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
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM users WHERE chat_id = $1 AND current_activity = $2",
                chat_id,
                activity,
            )
            return count or 0

    async def get_all_activity_limits(self) -> Dict[str, int]:
        """获取所有活动的人数限制"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT activity_name, max_users FROM activity_user_limits"
            )
            return {row["activity_name"]: row["max_users"] for row in rows}

    async def remove_activity_user_limit(self, activity: str):
        """移除活动人数限制"""
        self._ensure_pool_initialized()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM activity_user_limits WHERE activity_name = $1", activity
            )
        self._cache.pop(f"activity_limit:{activity}", None)

    async def force_reset_all_users_in_group(
        self, chat_id: int, target_date: date = None
    ):
        """强制重置该群组所有用户的每日统计数据"""
        if target_date is None:
            target_date = self.get_beijing_date()

        next_day = target_date + timedelta(days=1)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    DELETE FROM user_activities 
                    WHERE chat_id = $1 AND activity_date = $2
                """,
                    chat_id,
                    target_date,
                )

                await conn.execute(
                    """
                    DELETE FROM user_activities 
                    WHERE chat_id = $1 AND activity_date = $2
                """,
                    chat_id,
                    next_day,
                )

                await conn.execute(
                    """
                    UPDATE users 
                    SET total_accumulated_time = 0, 
                        total_activity_count = 0, 
                        total_fines = 0,
                        last_updated = $2
                    WHERE chat_id = $1
                """,
                    chat_id,
                    target_date,
                )

            keys_to_remove = [
                f"group:{chat_id}",
                f"rank:{chat_id}",
                f"group_config:{chat_id}",
            ]
            for key in keys_to_remove:
                self._cache.pop(key, None)
                if key in self._cache_ttl:
                    del self._cache_ttl[key]

            logger.info(
                f"✅ 已强制重置群组 {chat_id} (清理日期: {target_date} 及 {next_day})"
            )

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

    @staticmethod
    def format_time_for_csv(seconds: int) -> str:
        """为CSV导出格式化时间显示"""
        if not seconds:
            return "0分0秒"

        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours}时{minutes}分{secs}秒"
        else:
            return f"{minutes}分{secs}秒"

    async def connection_health_check(self) -> bool:
        """快速连接健康检查"""
        if not self.pool:
            return False

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
        except Exception as e:
            logger.debug(f"数据库连接健康检查失败: {e}")
            return False


# 全局数据库实例
db = PostgreSQLDatabase()
