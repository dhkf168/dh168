# heartbeat.py - 优化版心跳保持机制（Render 安全关闭 + session 自动恢复 + 无泄漏）

import asyncio
import aiohttp
import time
import logging
import atexit
from datetime import datetime
from typing import Dict, Any
from config import Config, beijing_tz

logger = logging.getLogger("GroupCheckInBot")


class HeartbeatManager:
    """心跳管理器 - 保持应用活跃 (Render 安全关闭版)"""

    def __init__(self):
        self.enabled = Config.HEARTBEAT_CONFIG["ENABLED"]
        self.interval = Config.HEARTBEAT_CONFIG["INTERVAL"] * 60  # 转换为秒
        self.ping_urls = Config.HEARTBEAT_CONFIG["PING_URLS"]
        self.self_ping_enabled = Config.HEARTBEAT_CONFIG["SELF_PING_ENABLED"]
        self.self_ping_interval = Config.HEARTBEAT_CONFIG["SELF_PING_INTERVAL"] * 60
        self.session = None
        self.last_heartbeat = None
        self.heartbeat_count = 0
        self.failed_count = 0
        self._stopping = False

        # 注册退出钩子，Render SIGTERM 时也会调用
        atexit.register(self._sync_close_session)

    # =====================================================
    # 🔹 初始化与关闭
    # =====================================================
    async def initialize(self):
        """初始化心跳管理器"""
        if not self.enabled:
            logger.info("❌ 心跳机制已禁用")
            return

        await self._create_session()
        logger.info("✅ 心跳管理器初始化完成")

    async def _create_session(self):
        """安全创建 aiohttp.ClientSession"""
        # 避免重复创建
        if self.session and not self.session.closed:
            return
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)
        logger.debug("🔄 新 aiohttp ClientSession 已创建")

    async def stop(self):
        """停止心跳管理器"""
        self._stopping = True
        if self.session and not self.session.closed:
            try:
                await self.session.close()
                logger.info("✅ 心跳管理器 session 已关闭")
            except Exception as e:
                logger.warning(f"⚠️ 关闭心跳 session 时出错: {e}")
        logger.info("🛑 心跳管理器已停止")

    def _sync_close_session(self):
        """同步关闭 aiohttp session（防止 Render SIGTERM 未 await stop）"""
        if self.session and not self.session.closed:
            try:
                import asyncio

                asyncio.run(self.session.close())
                print("✅ [atexit] 自动关闭心跳 session")
            except RuntimeError:
                pass

    # =====================================================
    # 🔹 核心心跳逻辑
    # =====================================================
    async def ping_url(self, url: str) -> Dict[str, Any]:
        """ping 一个URL"""
        await self._create_session()
        start_time = time.time()
        try:
            async with self.session.get(url) as response:
                response_time = time.time() - start_time
                return {
                    "url": url,
                    "status": "success",
                    "status_code": response.status,
                    "response_time": round(response_time * 1000, 2),  # 毫秒
                    "timestamp": datetime.now(beijing_tz),
                }
        except Exception as e:
            return {
                "url": url,
                "status": "failed",
                "error": str(e),
                "response_time": -1,
                "timestamp": datetime.now(beijing_tz),
            }

    async def ping_self(self) -> Dict[str, Any]:
        """自ping - 访问自己的健康检查接口"""
        await self._create_session()
        start_time = time.time()
        try:
            port = Config.WEB_SERVER_CONFIG["PORT"]
            url = f"http://localhost:{port}/health"

            async with self.session.get(url, timeout=10) as response:
                response_time = time.time() - start_time
                data = await response.json()

                return {
                    "url": "self",
                    "status": "success",
                    "status_code": response.status,
                    "response_time": round(response_time * 1000, 2),
                    "data": data,
                    "timestamp": datetime.now(beijing_tz),
                }
        except Exception as e:
            return {
                "url": "self",
                "status": "failed",
                "error": str(e),
                "response_time": -1,
                "timestamp": datetime.now(beijing_tz),
            }

    async def perform_heartbeat(self):
        """执行完整的心跳检查"""
        if not self.enabled:
            return

        # 自动恢复 session（Render reload 时可能丢失）
        if not self.session or self.session.closed:
            await self._create_session()

        logger.info("💓 执行心跳检查...")
        results = []

        # ping 外部URLs
        for url in self.ping_urls:
            result = await self.ping_url(url)
            results.append(result)

            if result["status"] == "success":
                logger.info(f"✅ Ping {url}: {result['response_time']}ms")
            else:
                logger.warning(f"❌ Ping {url} 失败: {result['error']}")
                self.failed_count += 1

        # 自ping
        if self.self_ping_enabled:
            self_ping_result = await self.ping_self()
            results.append(self_ping_result)

            if self_ping_result["status"] == "success":
                logger.info(f"✅ 自ping成功: {self_ping_result['response_time']}ms")
            else:
                logger.warning(f"❌ 自ping失败: {self_ping_result['error']}")
                self.failed_count += 1

        self.last_heartbeat = datetime.now(beijing_tz)
        self.heartbeat_count += 1

        # 记录统计
        success_count = sum(1 for r in results if r["status"] == "success")
        total_count = len(results)
        logger.info(f"📊 心跳完成: {success_count}/{total_count} 成功")

        return results

    # =====================================================
    # 🔹 主循环
    # =====================================================
    async def start_heartbeat_loop(self):
        """启动心跳循环"""
        if not self.enabled:
            return

        logger.info("🚀 启动心跳循环...")

        while not self._stopping:
            try:
                await self.perform_heartbeat()

                # 根据失败次数动态调整间隔
                current_interval = self.interval
                if self.failed_count > 5:
                    current_interval = max(60, self.interval // 2)
                    logger.warning(
                        f"⚠️ 心跳失败较多，调整间隔为 {current_interval // 60} 分钟"
                    )

                await asyncio.sleep(current_interval)

            except asyncio.CancelledError:
                logger.info("🛑 心跳循环已被取消")
                break
            except Exception as e:
                logger.error(f"❌ 心跳循环错误: {e}")
                await asyncio.sleep(60)  # 出错时等待1分钟

    # =====================================================
    # 🔹 状态查询
    # =====================================================
    def get_status(self) -> Dict[str, Any]:
        """获取心跳状态"""
        return {
            "enabled": self.enabled,
            "heartbeat_count": self.heartbeat_count,
            "failed_count": self.failed_count,
            "last_heartbeat": (
                self.last_heartbeat.isoformat() if self.last_heartbeat else None
            ),
            "interval_minutes": self.interval // 60,
            "ping_urls_count": len(self.ping_urls),
            "self_ping_enabled": self.self_ping_enabled,
        }


# =====================================================
# 全局心跳管理器实例
# =====================================================
heartbeat_manager = HeartbeatManager()
