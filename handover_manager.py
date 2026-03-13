# handover_manager.py

import logging
import asyncio
import time
from datetime import datetime, date, timedelta, time as dt_time
from typing import Dict, Optional, Tuple, Any, List

from database import db
from config import beijing_tz, Config

logger = logging.getLogger("GroupCheckInBot.HandoverManager")


class HandoverManager:
    """换班管理器 - 处理月末夜班跨月和月初白班跨日的18小时工作制"""

    def __init__(self):
        # ===== 1. 基础缓存（已有TTL）=====
        self._cache = {}
        self._cache_ttl = {}

        # ===== 2. 周期缓存（已有TTL）=====
        self._period_cache = {}
        self._period_cache_ttl = {}

        # ===== 3. 换班日缓存（已有TTL）=====
        self._handover_cache = {}
        self._handover_cache_ttl = {}

        # ===== 4. 用户周期缓存（需要修复）=====
        self._user_cycle_cache = {}  # 数据缓存
        self._user_cycle_cache_ttl = {}  # TTL缓存
        self._user_cycle_access = {}  # 最后访问时间（用于LRU）

        # ===== 5. 缓存配置 =====
        self._user_cycle_max_size = 2000  # 最大缓存条目
        self._user_cycle_default_ttl = 300  # 默认5分钟
        self._last_cleanup = time.time()
        self._cleanup_interval = 600  # 10分钟清理一次
        self._lock = asyncio.Lock()  # 并发控制

    # ========== 用户周期缓存管理（新增）==========

    async def _cleanup_user_cycle_cache(self):
        """清理过期的用户周期缓存（TTL + LRU）"""
        now = time.time()

        # 控制清理频率
        if now - self._last_cleanup < self._cleanup_interval:
            return

        async with self._lock:
            # 1. 清理过期缓存（基于TTL）
            expired = [
                key
                for key, expiry in self._user_cycle_cache_ttl.items()
                if now > expiry
            ]

            for key in expired:
                self._user_cycle_cache.pop(key, None)
                self._user_cycle_cache_ttl.pop(key, None)
                self._user_cycle_access.pop(key, None)

            # 2. LRU淘汰（如果仍然过大）
            if len(self._user_cycle_cache) > self._user_cycle_max_size:
                # 按最后访问时间排序
                sorted_keys = sorted(
                    self._user_cycle_access.items(), key=lambda x: x[1]
                )

                # 淘汰最旧的20%
                evict_count = max(1, len(sorted_keys) // 5)
                keys_to_evict = [k for k, _ in sorted_keys[:evict_count]]

                for key in keys_to_evict:
                    self._user_cycle_cache.pop(key, None)
                    self._user_cycle_cache_ttl.pop(key, None)
                    self._user_cycle_access.pop(key, None)

                logger.debug(f"🧹 LRU淘汰: 移除了 {evict_count} 个用户周期缓存")

            if expired:
                logger.debug(f"🧹 清理了 {len(expired)} 个过期用户周期缓存")

            self._last_cleanup = now

    async def _get_user_cycle_cached(self, key: str) -> Optional[Dict]:
        """安全获取用户周期缓存"""
        async with self._lock:
            now = time.time()

            # 检查TTL
            if key in self._user_cycle_cache_ttl:
                if now < self._user_cycle_cache_ttl[key]:
                    # 更新访问时间
                    self._user_cycle_access[key] = now
                    return self._user_cycle_cache.get(key)
                else:
                    # 过期删除
                    self._user_cycle_cache.pop(key, None)
                    self._user_cycle_cache_ttl.pop(key, None)
                    self._user_cycle_access.pop(key, None)

            return None

    async def _set_user_cycle_cached(self, key: str, value: Dict, ttl: int = None):
        """安全设置用户周期缓存"""
        if ttl is None:
            ttl = self._user_cycle_default_ttl

        async with self._lock:
            now = time.time()

            # 检查大小，必要时执行LRU
            if len(self._user_cycle_cache) >= self._user_cycle_max_size:
                await self._evict_lru()

            self._user_cycle_cache[key] = value
            self._user_cycle_cache_ttl[key] = now + ttl
            self._user_cycle_access[key] = now

    async def _evict_lru(self):
        """LRU淘汰（内部方法，调用时需持有锁）"""
        if len(self._user_cycle_access) <= 100:  # 太小就不淘汰
            return

        # 按最后访问时间排序
        sorted_keys = sorted(self._user_cycle_access.items(), key=lambda x: x[1])

        # 淘汰最旧的20%
        evict_count = max(1, len(sorted_keys) // 5)
        keys_to_evict = [k for k, _ in sorted_keys[:evict_count]]

        for key in keys_to_evict:
            self._user_cycle_cache.pop(key, None)
            self._user_cycle_cache_ttl.pop(key, None)
            self._user_cycle_access.pop(key, None)

    async def _invalidate_user_cycle_cache(self, chat_id: int, user_id: int):
        """使特定用户的周期缓存失效"""
        prefix = f"user_cycle:{chat_id}:{user_id}:"

        async with self._lock:
            keys_to_remove = [
                key for key in self._user_cycle_cache.keys() if key.startswith(prefix)
            ]

            for key in keys_to_remove:
                self._user_cycle_cache.pop(key, None)
                self._user_cycle_cache_ttl.pop(key, None)
                self._user_cycle_access.pop(key, None)

    # ========== 配置管理（原样保留）==========

    async def init_handover_config(self, chat_id: int) -> bool:
        """初始化换班配置"""
        try:
            await db.execute_with_retry(
                "初始化换班配置",
                """
                INSERT INTO shift_handover_configs 
                (chat_id, handover_enabled, night_start_time, day_start_time,
                 handover_night_hours, handover_day_hours, normal_night_hours, normal_day_hours)
                VALUES ($1, TRUE, '21:00', '09:00', 18, 18, 12, 12)
                ON CONFLICT (chat_id) DO NOTHING
                """,
                chat_id,
            )
            self._invalidate_cache(chat_id)
            return True
        except Exception as e:
            logger.error(f"初始化换班配置失败 {chat_id}: {e}")
            return False

    async def get_handover_config(self, chat_id: int) -> Dict[str, Any]:
        """获取换班配置"""
        cache_key = f"handover_config:{chat_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            row = await db.execute_with_retry(
                "获取换班配置",
                "SELECT * FROM shift_handover_configs WHERE chat_id = $1",
                chat_id,
                fetchrow=True,
            )

            if not row:
                await self.init_handover_config(chat_id)
                row = await db.execute_with_retry(
                    "获取换班配置",
                    "SELECT * FROM shift_handover_configs WHERE chat_id = $1",
                    chat_id,
                    fetchrow=True,
                )

            result = dict(row) if row else self._get_default_config()
            self._set_cached(cache_key, result, 300)
            return result

        except Exception as e:
            logger.error(f"获取换班配置失败 {chat_id}: {e}")
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "chat_id": 0,
            "handover_enabled": True,
            "night_start_time": "21:00",
            "day_start_time": "09:00",
            "handover_night_hours": 18,
            "handover_day_hours": 18,
            "normal_night_hours": 12,
            "normal_day_hours": 12,
        }

    # ========== 核心时间判定（原样保留）==========

    async def determine_current_period(
        self,
        chat_id: int,
        current_time: Optional[datetime] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """
        根据时间判定当前属于哪个时期（支持自定义换班日期）
        """
        if current_time is None:
            current_time = db.get_beijing_time()

        # ===== 1. 缓存检查 =====
        cache_key = f"period:{chat_id}:{current_time.strftime('%Y%m%d%H')}"
        if use_cache:
            cached = await self._get_period_cache(cache_key)
            if cached:
                logger.debug(f"📊 周期缓存命中: {cache_key}")
                return cached

        # ===== 2. 获取配置 =====
        config = await self.get_handover_config(chat_id)

        # ===== 3. 快速路径：非换班日直接返回 =====
        if not config.get("handover_enabled", True):
            result = await self._get_normal_period(chat_id, current_time, config)
            if use_cache:
                await self._set_period_cache(cache_key, result)
            return result

        # ===== 4. 预计算时间值（避免重复计算） =====
        night_start = config.get("night_start_time", "21:00")
        day_start = config.get("day_start_time", "09:00")

        night_h, night_m = map(int, night_start.split(":"))
        day_h, day_m = map(int, day_start.split(":"))

        current_date = current_time.date()
        current_decimal = current_time.hour + current_time.minute / 60
        night_decimal = night_h + night_m / 60
        day_decimal = day_h + day_m / 60

        # ===== 5. 判断是否是换班日（批量计算） =====
        is_handover, is_next_handover = await self._check_handover_days(
            chat_id, current_date, config
        )

        # ===== 6. 换班夜班判定 =====
        if is_handover and current_decimal >= night_decimal:
            result = await self._calculate_handover_night(
                current_time, current_date, night_h, night_m, config
            )
        elif is_next_handover and current_decimal < 15:
            result = await self._calculate_handover_night_cross(
                current_time, current_date, night_h, night_m, config
            )
        # ===== 7. 换班白班判定 =====
        elif is_next_handover and current_decimal >= 15:
            result = await self._calculate_handover_day(
                current_time, current_date, config
            )
        # ===== 8. 正常班次 =====
        else:
            result = await self._calculate_normal_period(
                current_time, current_date, night_decimal, day_decimal, config
            )

        # ===== 9. 缓存结果 =====
        if use_cache:
            await self._set_period_cache(cache_key, result)

        return result

    async def _check_handover_days(
        self, chat_id: int, current_date: date, config: dict
    ) -> tuple[bool, bool]:
        """批量判断换班日（使用缓存）"""
        cache_key = f"handover_check:{chat_id}:{current_date}"

        # 尝试从缓存获取
        cached = await self._get_handover_check_cache(cache_key)
        if cached:
            logger.debug(f"📊 换班日缓存命中: {cache_key}")
            return cached

        handover_day = config.get("handover_day", 31)
        handover_month = config.get("handover_month", 0)

        # 批量计算今天和明天
        is_handover = self._is_handover_date(current_date, handover_day, handover_month)
        is_next_handover = self._is_handover_date(
            current_date + timedelta(days=1), handover_day, handover_month
        )

        result = (is_handover, is_next_handover)

        # 缓存结果（24小时）
        await self._set_handover_check_cache(cache_key, result, ttl=86400)

        return result

    def _is_handover_date(
        self, check_date: date, handover_day: int, handover_month: int
    ) -> bool:
        """判断指定日期是否是换班日"""
        if handover_day == 0:  # 月末最后一天
            if check_date.month == 12:
                next_month = date(check_date.year + 1, 1, 1)
            else:
                next_month = date(check_date.year, check_date.month + 1, 1)
            last_day = (next_month - timedelta(days=1)).day
            return check_date.day == last_day
        else:
            if handover_month == 0:  # 每月
                return check_date.day == handover_day
            else:  # 指定月份
                return (
                    check_date.month == handover_month
                    and check_date.day == handover_day
                )

    async def _calculate_normal_period(
        self,
        current_time: datetime,
        current_date: date,
        night_decimal: float,
        day_decimal: float,
        config: dict,
    ) -> Dict[str, Any]:
        """计算正常班次"""
        from datetime import time as dt_time

        current_decimal = current_time.hour + current_time.minute / 60

        # 正常夜班
        if current_decimal >= night_decimal or current_decimal < day_decimal:
            if current_decimal >= night_decimal:
                business_date = current_date
                start_dt = datetime.combine(
                    current_date, self._time_from_decimal(night_decimal)
                ).replace(tzinfo=current_time.tzinfo)
            else:
                business_date = current_date - timedelta(days=1)
                start_dt = datetime.combine(
                    current_date - timedelta(days=1),
                    self._time_from_decimal(night_decimal),
                ).replace(tzinfo=current_time.tzinfo)

            elapsed_hours = (current_time - start_dt).total_seconds() / 3600
            next_reset = start_dt + timedelta(
                hours=config.get("normal_night_hours", 12)
            )

            return {
                "period_type": "normal_night",
                "business_date": business_date,
                "actual_date": current_date,
                "cycle": 1,
                "hours_elapsed": elapsed_hours,
                "total_hours": config.get("normal_night_hours", 12),
                "is_handover": False,
                "next_reset_time": next_reset,
            }

        # 正常白班
        start_dt = datetime.combine(
            current_date, self._time_from_decimal(day_decimal)
        ).replace(tzinfo=current_time.tzinfo)

        elapsed_hours = (current_time - start_dt).total_seconds() / 3600
        next_reset = start_dt + timedelta(hours=config.get("normal_day_hours", 12))

        return {
            "period_type": "normal_day",
            "business_date": current_date,
            "actual_date": current_date,
            "cycle": 1,
            "hours_elapsed": elapsed_hours,
            "total_hours": config.get("normal_day_hours", 12),
            "is_handover": False,
            "next_reset_time": next_reset,
        }

    async def _calculate_handover_night(
        self,
        current_time: datetime,
        current_date: date,
        night_h: int,
        night_m: int,
        config: dict,
    ) -> Dict[str, Any]:
        """计算换班夜班"""
        from datetime import time as dt_time

        start_dt = datetime.combine(current_date, dt_time(night_h, night_m)).replace(
            tzinfo=current_time.tzinfo
        )

        elapsed_hours = (current_time - start_dt).total_seconds() / 3600
        cycle = 1 if elapsed_hours < 12 else 2
        next_reset = (
            start_dt + timedelta(hours=12)
            if cycle == 1
            else start_dt + timedelta(hours=18)
        )

        return {
            "period_type": "handover_night",
            "business_date": current_date,
            "actual_date": current_date,
            "cycle": cycle,
            "hours_elapsed": elapsed_hours,
            "total_hours": config.get("handover_night_hours", 18),
            "is_handover": True,
            "next_reset_time": next_reset,
        }

    async def _calculate_handover_night_cross(
        self,
        current_time: datetime,
        current_date: date,
        night_h: int,
        night_m: int,
        config: dict,
    ) -> Dict[str, Any]:
        """计算跨天换班夜班"""
        from datetime import time as dt_time

        handover_date = current_date - timedelta(days=1)
        start_dt = datetime.combine(handover_date, dt_time(night_h, night_m)).replace(
            tzinfo=current_time.tzinfo
        )

        elapsed_hours = (current_time - start_dt).total_seconds() / 3600
        cycle = 1 if elapsed_hours < 12 else 2
        next_reset = (
            start_dt + timedelta(hours=12)
            if cycle == 1
            else start_dt + timedelta(hours=18)
        )

        return {
            "period_type": "handover_night",
            "business_date": handover_date,
            "actual_date": current_date,
            "cycle": cycle,
            "hours_elapsed": elapsed_hours,
            "total_hours": config.get("handover_night_hours", 18),
            "is_handover": True,
            "next_reset_time": next_reset,
        }

    async def _calculate_handover_day(
        self, current_time: datetime, current_date: date, config: dict
    ) -> Dict[str, Any]:
        """计算换班白班"""
        from datetime import time as dt_time

        start_dt = datetime.combine(current_date, dt_time(15, 0)).replace(
            tzinfo=current_time.tzinfo
        )

        elapsed_hours = (current_time - start_dt).total_seconds() / 3600
        cycle = 1 if elapsed_hours < 12 else 2
        next_reset = (
            start_dt + timedelta(hours=12)
            if cycle == 1
            else start_dt + timedelta(hours=18)
        )

        return {
            "period_type": "handover_day",
            "business_date": current_date,
            "actual_date": current_date,
            "cycle": cycle,
            "hours_elapsed": elapsed_hours,
            "total_hours": config.get("handover_day_hours", 18),
            "is_handover": True,
            "next_reset_time": next_reset,
        }

    def _time_from_decimal(self, decimal_hours: float):
        """将小数小时转换为时间对象"""
        from datetime import time as dt_time

        hours = int(decimal_hours)
        minutes = int((decimal_hours - hours) * 60)
        return dt_time(hours, minutes)

    async def _get_normal_period(
        self, chat_id: int, current_time: datetime, config: dict
    ) -> Dict[str, Any]:
        """获取正常班次时期（兼容原版逻辑）"""
        from datetime import time as dt_time

        night_start = config.get("night_start_time", "21:00")
        day_start = config.get("day_start_time", "09:00")

        night_h, night_m = map(int, night_start.split(":"))
        day_h, day_m = map(int, day_start.split(":"))

        current_date = current_time.date()
        current_decimal = current_time.hour + current_time.minute / 60
        night_decimal = night_h + night_m / 60
        day_decimal = day_h + day_m / 60

        # 正常夜班
        if (current_decimal >= night_decimal) or (current_decimal < day_decimal):
            if current_decimal >= night_decimal:
                business_date = current_date
                start_dt = datetime.combine(
                    current_date, dt_time(night_h, night_m)
                ).replace(tzinfo=current_time.tzinfo)
            else:
                business_date = current_date - timedelta(days=1)
                start_dt = datetime.combine(
                    current_date - timedelta(days=1), dt_time(night_h, night_m)
                ).replace(tzinfo=current_time.tzinfo)

            elapsed_hours = (current_time - start_dt).total_seconds() / 3600
            next_reset = start_dt + timedelta(
                hours=config.get("normal_night_hours", 12)
            )

            return {
                "period_type": "normal_night",
                "business_date": business_date,
                "actual_date": current_date,
                "cycle": 1,
                "hours_elapsed": elapsed_hours,
                "total_hours": config.get("normal_night_hours", 12),
                "is_handover": False,
                "next_reset_time": next_reset,
            }
        else:
            # 正常白班
            start_dt = datetime.combine(current_date, dt_time(day_h, day_m)).replace(
                tzinfo=current_time.tzinfo
            )
            elapsed_hours = (current_time - start_dt).total_seconds() / 3600
            next_reset = start_dt + timedelta(hours=config.get("normal_day_hours", 12))

            return {
                "period_type": "normal_day",
                "business_date": current_date,
                "actual_date": current_date,
                "cycle": 1,
                "hours_elapsed": elapsed_hours,
                "total_hours": config.get("normal_day_hours", 12),
                "is_handover": False,
                "next_reset_time": next_reset,
            }

    # ========== 缓存方法（原样保留）==========

    async def _get_period_cache(self, key: str):
        """获取周期缓存"""
        import time

        if hasattr(self, "_period_cache") and key in self._period_cache_ttl:
            if time.time() < self._period_cache_ttl[key]:
                return self._period_cache.get(key)
            else:
                # 清理过期缓存
                self._period_cache.pop(key, None)
                self._period_cache_ttl.pop(key, None)
        return None

    async def _set_period_cache(self, key: str, value: dict, ttl: int = 3600):
        """设置周期缓存（1小时）"""
        import time

        if not hasattr(self, "_period_cache"):
            self._period_cache = {}
            self._period_cache_ttl = {}

        self._period_cache[key] = value
        self._period_cache_ttl[key] = time.time() + ttl

    async def _get_handover_check_cache(self, key: str):
        """获取换班日检查缓存"""
        import time

        if hasattr(self, "_handover_cache") and key in self._handover_cache_ttl:
            if time.time() < self._handover_cache_ttl[key]:
                return self._handover_cache.get(key)
            else:
                # 清理过期缓存
                self._handover_cache.pop(key, None)
                self._handover_cache_ttl.pop(key, None)
        return None

    async def _set_handover_check_cache(self, key: str, value: tuple, ttl: int = 86400):
        """设置换班日检查缓存（24小时）"""
        import time

        if not hasattr(self, "_handover_cache"):
            self._handover_cache = {}
            self._handover_cache_ttl = {}

        self._handover_cache[key] = value
        self._handover_cache_ttl[key] = time.time() + ttl

    # ========== 用户周期管理（修改后的方法）==========

    async def get_user_cycle(
        self,
        chat_id: int,
        user_id: int,
        business_date: date,
        period_type: str,
        cycle: int,
    ) -> Optional[Dict[str, Any]]:
        """获取用户指定周期的数据（带缓存保护）"""

        # 定期清理过期缓存
        await self._cleanup_user_cycle_cache()

        cache_key = (
            f"user_cycle:{chat_id}:{user_id}:{business_date}:{period_type}:{cycle}"
        )

        # 尝试从缓存获取
        cached = await self._get_user_cycle_cached(cache_key)
        if cached:
            return cached

        try:
            shift_type = "night" if "night" in period_type else "day"

            row = await db.execute_with_retry(
                "获取用户周期",
                """
                SELECT * FROM user_handover_cycles 
                WHERE chat_id = $1 AND user_id = $2 
                  AND handover_date = $3 AND shift_type = $4 AND cycle_number = $5
                """,
                chat_id,
                user_id,
                business_date,
                shift_type,
                cycle,
                fetchrow=True,
            )

            if row:
                result = dict(row)
                await self._set_user_cycle_cached(cache_key, result)
                return result
            return None

        except Exception as e:
            logger.error(f"获取用户周期失败 {chat_id}-{user_id}: {e}")
            return None

    async def create_user_cycle(
        self,
        chat_id: int,
        user_id: int,
        business_date: date,
        period_type: str,
        cycle: int,
        start_time: Optional[datetime] = None,
    ) -> bool:
        """创建用户新周期（自动使缓存失效）"""
        if start_time is None:
            start_time = db.get_beijing_time()

        # ===== 修复：转换为无时区时间 =====
        # 确保插入数据库的时间不带 tzinfo，防止 PostgreSQL 驱动进行不必要的时区转换
        if start_time.tzinfo is not None:
            # 转换为无时区时间（去除时区信息）
            start_time = start_time.replace(tzinfo=None)

        # 根据 period_type 判定白班或晚班
        shift_type = "night" if "night" in period_type else "day"

        try:
            # 使用带重试机制的执行器
            await db.execute_with_retry(
                "创建用户周期",
                """
                INSERT INTO user_handover_cycles 
                (chat_id, user_id, handover_date, shift_type, cycle_number, 
                 cycle_start_time, total_work_seconds)
                VALUES ($1, $2, $3, $4, $5, $6, 0)
                ON CONFLICT (chat_id, user_id, handover_date, shift_type, cycle_number) 
                DO NOTHING
                """,
                chat_id,
                user_id,
                business_date,
                shift_type,
                cycle,
                start_time,  # 传入已处理的无时区时间
            )

            # 联动操作：使该用户的周期相关缓存失效，确保下次查询时从数据库拉取最新状态
            await self._invalidate_user_cycle_cache(chat_id, user_id)
            return True

        except Exception as e:
            logger.error(f"❌ 创建用户周期失败 {chat_id}-{user_id}: {e}")
            return False

    async def update_user_cycle_time(
        self,
        chat_id: int,
        user_id: int,
        business_date: date,
        period_type: str,
        cycle: int,
        elapsed_seconds: int,
    ) -> Tuple[int, bool]:
        """
        更新用户周期工作时间

        返回: (新累计时间, 是否达到周期切换点)
        """
        cycle_data = await self.get_user_cycle(
            chat_id, user_id, business_date, period_type, cycle
        )

        if not cycle_data and cycle == 1:
            # 周期1不存在，创建它
            await self.create_user_cycle(
                chat_id, user_id, business_date, period_type, 1
            )
            cycle_data = await self.get_user_cycle(
                chat_id, user_id, business_date, period_type, 1
            )

        if not cycle_data:
            logger.warning(f"用户 {user_id} 周期 {cycle} 不存在")
            return 0, False

        new_total = cycle_data["total_work_seconds"] + elapsed_seconds
        threshold_seconds = 12 * 3600  # 12小时

        try:
            await db.execute_with_retry(
                "更新用户周期工作时间",
                """
                UPDATE user_handover_cycles 
                SET total_work_seconds = $1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE chat_id = $2 AND user_id = $3 
                  AND handover_date = $4 AND shift_type = $5 AND cycle_number = $6
                """,
                new_total,
                chat_id,
                user_id,
                business_date,
                "night" if "night" in period_type else "day",
                cycle,
            )

            # 使缓存失效（而不是更新，保证一致性）
            await self._invalidate_user_cycle_cache(chat_id, user_id)

            # 判断是否达到12小时阈值
            reached_threshold = (
                cycle_data["total_work_seconds"] < threshold_seconds
                and new_total >= threshold_seconds
            )

            return new_total, reached_threshold

        except Exception as e:
            logger.error(f"更新用户周期工作时间失败 {chat_id}-{user_id}: {e}")
            return cycle_data["total_work_seconds"], False

    # ========== 对外核心接口（原样保留）==========

    async def get_activity_count(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        shift: str,
        current_time: Optional[datetime] = None,
    ) -> int:
        """
        获取用户在当前周期的活动计数

        关键：在换班日，12小时后计数重置
        """
        if current_time is None:
            current_time = db.get_beijing_time()

        # 获取当前时期信息
        period = await self.determine_current_period(chat_id, current_time)

        # 查询该业务日期的所有计数
        all_counts = await db.get_user_activity_count_by_shift(
            chat_id, user_id, activity, shift, query_date=period["business_date"]
        )

        if not period["is_handover"]:
            # 非换班日，直接返回
            logger.debug(f"📊 [计数] 正常日: {all_counts}")
            return all_counts

        # 换班日，获取当前周期
        cycle_data = await self.get_user_cycle(
            chat_id,
            user_id,
            period["business_date"],
            period["period_type"],
            period["cycle"],
        )

        if period["cycle"] == 1:
            # 周期1：返回所有计数（周期2还未开始）
            logger.debug(
                f"🔄 [计数-周期1] 业务日期 {period['business_date']}, 计数: {all_counts}"
            )
            return all_counts
        else:
            # 周期2：计数重置，从0开始
            logger.debug(
                f"🔄 [计数-周期2] 业务日期 {period['business_date']}, 计数重置"
            )
            return 0

    async def record_activity(
        self,
        chat_id: int,
        user_id: int,
        activity: str,
        elapsed_seconds: int,
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        记录一次活动完成

        返回: {
            'business_date': date,      # 应该使用的业务日期
            'cycle': int,                # 当前周期
            'should_reset_count': bool,  # 是否应该重置计数（达到12小时）
            'period_type': str           # 时期类型
        }
        """
        if current_time is None:
            current_time = db.get_beijing_time()

        # 获取当前时期信息
        period = await self.determine_current_period(chat_id, current_time)

        result = {
            "business_date": period["business_date"],
            "cycle": period["cycle"],
            "should_reset_count": False,
            "period_type": period["period_type"],
            "is_handover": period["is_handover"],
        }

        if not period["is_handover"]:
            # 非换班日，直接返回
            return result

        # 换班日，更新周期时间
        new_total, reached_threshold = await self.update_user_cycle_time(
            chat_id,
            user_id,
            period["business_date"],
            period["period_type"],
            period["cycle"],
            elapsed_seconds,
        )

        result["should_reset_count"] = reached_threshold

        logger.info(
            f"📝 [换班记录] 用户{user_id} {period['period_type']} "
            f"周期{period['cycle']} 累计 {new_total//60}分钟, "
            f"阈值达到: {reached_threshold}"
        )

        return result

    async def should_reset_data(
        self,
        chat_id: int,
        current_time: Optional[datetime] = None,
    ) -> Tuple[bool, date, str]:
        """
        判断是否应该执行数据重置导出

        返回: (是否重置, 目标日期, 原因)
        """
        if current_time is None:
            current_time = db.get_beijing_time()

        period = await self.determine_current_period(chat_id, current_time)

        # 检查是否达到总工作时长
        if period["hours_elapsed"] >= period["total_hours"] - 0.1:  # 加0.1容差
            if "handover" in period["period_type"]:
                reason = f"handover_complete_{period['period_type']}"
            else:
                reason = "normal_complete"

            logger.info(
                f"🔄 [重置触发] {period['period_type']} 已完成 {period['total_hours']}小时"
            )
            return True, period["business_date"], reason

        return False, period["business_date"], "not_complete"

    # ========== 缓存管理（原样保留）==========

    def _get_cached(self, key: str):
        import time

        if key in self._cache_ttl and time.time() < self._cache_ttl[key]:
            return self._cache.get(key)
        return None

    def _set_cached(self, key: str, value: Any, ttl: int = 300):
        import time

        self._cache[key] = value
        self._cache_ttl[key] = time.time() + ttl

    def _invalidate_cache(self, chat_id: int):
        cache_key = f"handover_config:{chat_id}"
        self._cache.pop(cache_key, None)
        self._cache_ttl.pop(cache_key, None)

    async def update_handover_config(self, chat_id: int, **kwargs) -> bool:
        """
        更新换班配置（带参数验证和事务支持）

        参数:
            chat_id: 群组ID
            **kwargs: 要更新的字段

        支持的字段:
            - handover_enabled: bool
            - night_start_time: str (HH:MM)
            - day_start_time: str (HH:MM)
            - handover_night_hours: int (1-24)
            - handover_day_hours: int (1-24)
            - normal_night_hours: int (1-24)
            - normal_day_hours: int (1-24)
            - handover_day: int (1-31, 0表示月末)
            - handover_month: int (1-12, 0表示每月)

        返回:
            bool: 是否更新成功
        """
        try:
            # ===== 1. 参数验证 =====
            if not kwargs:
                logger.warning(f"没有提供要更新的参数: {chat_id}")
                return False

            # 定义允许的字段及其验证规则
            allowed_fields = {
                "handover_enabled": lambda v: isinstance(v, bool),
                "night_start_time": lambda v: self._validate_time_format(v),
                "day_start_time": lambda v: self._validate_time_format(v),
                "handover_night_hours": lambda v: isinstance(v, int) and 1 <= v <= 24,
                "handover_day_hours": lambda v: isinstance(v, int) and 1 <= v <= 24,
                "normal_night_hours": lambda v: isinstance(v, int) and 1 <= v <= 24,
                "normal_day_hours": lambda v: isinstance(v, int) and 1 <= v <= 24,
                "handover_day": lambda v: isinstance(v, int) and 0 <= v <= 31,
                "handover_month": lambda v: isinstance(v, int) and 0 <= v <= 12,
            }

            # 验证并过滤参数
            valid_updates = {}
            for key, value in kwargs.items():
                if key not in allowed_fields:
                    logger.warning(f"忽略未知字段: {key}")
                    continue

                if not allowed_fields[key](value):
                    logger.error(f"字段 {key} 的值无效: {value}")
                    return False

                valid_updates[key] = value

            if not valid_updates:
                logger.warning(f"没有有效的更新参数: {chat_id}")
                return False

            # ===== 2. 构建更新语句 =====
            set_clauses = []
            values = []
            param_index = 2  # chat_id 是 $1

            for key, value in valid_updates.items():
                set_clauses.append(f"{key} = ${param_index}")
                values.append(value)
                param_index += 1

            # ===== 3. 执行更新（带事务）=====
            updated = 0
            async with db.pool.acquire() as conn:
                async with conn.transaction():
                    # 先检查记录是否存在
                    exists = await conn.fetchval(
                        """
                        SELECT 1 FROM shift_handover_configs 
                        WHERE chat_id = $1
                    """,
                        chat_id,
                    )

                    if not exists:
                        # 如果不存在，先插入默认配置
                        await conn.execute(
                            """
                            INSERT INTO shift_handover_configs (chat_id)
                            VALUES ($1)
                            ON CONFLICT (chat_id) DO NOTHING
                        """,
                            chat_id,
                        )

                    # 执行更新
                    query = f"""
                        UPDATE shift_handover_configs 
                        SET {', '.join(set_clauses)}, updated_at = CURRENT_TIMESTAMP
                        WHERE chat_id = $1
                    """

                    result = await conn.execute(query, chat_id, *values)

                    # 解析更新结果
                    if result and result.startswith("UPDATE"):
                        try:
                            updated = int(result.split()[-1])
                        except (ValueError, IndexError):
                            pass

            # ===== 4. 清除缓存 =====
            if updated > 0:
                self._invalidate_cache(chat_id)
                logger.info(
                    f"✅ 更新换班配置成功: {chat_id}, 字段: {list(valid_updates.keys())}"
                )
                return True
            else:
                logger.warning(f"⚠️ 没有更新任何配置: {chat_id}")
                return False

        except Exception as e:
            logger.error(f"❌ 更新换班配置失败 {chat_id}: {e}")
            return False

    # ========== 辅助方法 ==========
    def _validate_time_format(self, time_str: str) -> bool:
        """验证时间格式 HH:MM"""
        try:
            if not isinstance(time_str, str):
                return False
            datetime.strptime(time_str, "%H:%M")
            return True
        except ValueError:
            return False


# 全局实例
handover_manager = HandoverManager()

