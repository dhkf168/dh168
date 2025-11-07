# render_deploy.py - 更新导入部分
import os
import asyncio
import logging
import time
from aiohttp import web

# ✅ 导入所有需要的组件
from main import (
    dp,
    bot,
    db,
    heartbeat_manager,
    memory_cleanup_task,
    health_monitoring_task,
    daily_reset_task,
    auto_daily_export_task,
    efficient_monthly_export_task,
    monthly_report_task,
    performance_optimizer,
    task_manager,
)
from config import Config

# ===========================
# 日志配置
# ===========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RenderBot")


# ===========================
# Render 保活健康检查接口
# ===========================
async def health_check(request):
    """健康检查端点"""
    return web.json_response(
        {
            "status": "healthy",
            "service": "telegram-bot",
            "timestamp": time.time(),
        }
    )


# ===========================
# Render 必需 Web 服务（动态端口）
# ===========================
async def start_web_server():
    """启动 Web 服务器（Render FREE 必需）"""
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)

    # ✅ Render 提供动态端口，不可写死
    port = int(os.environ.get("PORT", 8080))

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    logger.info(f"✅ Web server started on Render dynamic port: {port}")
    return runner


# ===========================
# 初始化所有关键服务（数据库 / 心跳 / 配置）
# ===========================
async def initialize_services():
    logger.info("🔄 Initializing services...")

    # ✅ 初始化数据库
    await db.initialize()
    logger.info("✅ Database initialized")

    # ✅ 初始化心跳服务
    await heartbeat_manager.initialize()
    logger.info("✅ Heartbeat initialized")

    # ✅ 删除 webhook（Render 免费版无法用 Webhook）
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("✅ Webhook deleted → switching to polling mode")


# ===========================
# 启动后台任务（不会阻塞主线程）
# ===========================
async def start_background_tasks():
    """启动所有后台任务（不阻塞）"""

    # ✅ 所有后台任务都应该使用 create_task()
    asyncio.create_task(heartbeat_manager.start_heartbeat_loop())
    asyncio.create_task(memory_cleanup_task())
    asyncio.create_task(health_monitoring_task())
    asyncio.create_task(daily_reset_task())
    asyncio.create_task(auto_daily_export_task())
    asyncio.create_task(efficient_monthly_export_task())
    asyncio.create_task(monthly_report_task())

    logger.info("✅ All background tasks started")


# ===========================
# 主程序入口
# ===========================
async def main():
    try:
        # ✅ Render 必须先启动该 Web 服务，否则会 Deployment Timed Out
        await start_web_server()

        # ✅ 初始化所有服务
        await initialize_services()

        # ✅ 启动后台任务（不阻塞）
        await start_background_tasks()

        logger.info("🤖 Starting Telegram bot in POLLING mode...")

        # ✅ 轮询启动（skip_updates 重要！避免历史消息卡死）
        await dp.start_polling(bot, skip_updates=True)

    except Exception as e:
        logger.error(f"💥 Bot failed to start: {e}")
        raise

    finally:
        logger.info("🛑 Bot shutdown complete")


# ===========================
# 程序启动
# ===========================
if __name__ == "__main__":
    asyncio.run(main())
