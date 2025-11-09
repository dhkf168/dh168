# render_deploy.py - 完整修复版本（确保无遗漏）
import os
import asyncio
import logging
import time
import signal
import sys
from aiohttp import web

# ✅ 导入所有需要的组件
from main import (
    db,
    heartbeat_manager,
    memory_cleanup_task,
    health_monitoring_task,
    daily_reset_task,
    efficient_monthly_export_task,
    monthly_report_task,
    simple_on_startup,
    # ✅ 新增：导入清理函数
    cleanup_resources,
    # ✅ 新增：导入必要的工具函数
    get_beijing_time,
    performance_optimizer,
    timer_manager,
    user_lock_manager,
    global_cache,
)

from config import Config

# ===========================
# 日志配置
# ===========================
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("RenderBot")


# ===========================
# 全局状态管理
# ===========================
class AppState:
    def __init__(self):
        self.running = True
        self.web_server_started = False
        self.services_initialized = False
        self.background_tasks = []


app_state = AppState()


# ===========================
# 信号处理
# ===========================
def handle_sigterm(signum, frame):
    logger.info(f"📡 收到信号 {signum}，准备优雅关闭...")
    app_state.running = False


def handle_sigint(signum, frame):
    logger.info("👋 收到键盘中断信号")
    app_state.running = False


# 注册信号处理器
signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigint)


# ===========================
# 健康检查接口
# ===========================
async def health_check(request):
    """基础健康检查端点"""
    status = "healthy" if app_state.running else "shutting_down"

    return web.json_response(
        {
            "status": status,
            "service": "telegram-bot-web",
            "timestamp": time.time(),
            "beijing_time": get_beijing_time().isoformat(),
            "web_server_active": app_state.web_server_started,
            "services_initialized": app_state.services_initialized,
            "environment": "render",
        }
    )


async def detailed_health_check(request):
    """详细健康检查"""
    try:
        # 检查数据库连接
        db_healthy = await db.connection_health_check()

        # 检查心跳状态
        heartbeat_status = heartbeat_manager.get_status()

        # 获取性能统计
        perf_report = {
            "memory_ok": performance_optimizer.memory_usage_ok(),
            "active_timers": timer_manager.get_stats()["active_timers"],
            "user_locks": user_lock_manager.get_stats()["active_locks"],
            "cache_stats": global_cache.get_stats(),
        }

        return web.json_response(
            {
                "status": "healthy" if db_healthy else "degraded",
                "timestamp": time.time(),
                "beijing_time": get_beijing_time().isoformat(),
                "components": {
                    "database": db_healthy,
                    "heartbeat": heartbeat_status,
                    "web_server": app_state.web_server_started,
                    "services": app_state.services_initialized,
                    "performance": perf_report,
                },
                "background_tasks": len(app_state.background_tasks),
                "environment": "render",
            }
        )
    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        return web.json_response(
            {"status": "unhealthy", "error": str(e), "timestamp": time.time()},
            status=500,
        )


async def metrics_endpoint(request):
    """Prometheus 格式指标端点"""
    try:
        # 获取基本指标
        memory_bytes = 0
        try:
            import psutil

            memory_bytes = psutil.Process().memory_info().rss
        except:
            pass

        metrics = [
            "# HELP render_web_service_status Web 服务状态",
            "# TYPE render_web_service_status gauge",
            f"render_web_service_status {1 if app_state.running else 0}",
            "# HELP render_services_initialized 服务初始化状态",
            "# TYPE render_services_initialized gauge",
            f"render_services_initialized {1 if app_state.services_initialized else 0}",
            "# HELP render_background_tasks 后台任务数量",
            "# TYPE render_background_tasks gauge",
            f"render_background_tasks {len(app_state.background_tasks)}",
            "# HELP render_memory_usage_bytes 内存使用量",
            "# TYPE render_memory_usage_bytes gauge",
            f"render_memory_usage_bytes {memory_bytes}",
        ]

        return web.Response(text="\n".join(metrics), content_type="text/plain")
    except Exception as e:
        logger.error(f"指标端点错误: {e}")
        return web.Response(text=f"error: {e}", status=500)


# ===========================
# Render Web 服务器
# ===========================
async def start_render_web_server():
    """启动 Render 必需的 Web 服务器"""
    app = web.Application()

    # 注册路由
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    app.router.add_get("/status", detailed_health_check)
    app.router.add_get("/metrics", metrics_endpoint)
    app.router.add_get("/ping", lambda request: web.Response(text="pong"))

    # Render 提供动态端口
    port = int(os.environ.get("PORT", 8080))

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    app_state.web_server_started = True
    logger.info(f"🌐 Render Web 服务器已在端口 {port} 启动")

    return runner, site


# ===========================
# 服务初始化（不启动轮询）
# ===========================
async def initialize_services_without_polling():
    """初始化服务但不启动 Telegram 轮询"""
    logger.info("🔄 初始化服务（不启动轮询）...")

    try:
        # 数据库初始化
        await db.initialize()
        logger.info("✅ 数据库初始化完成")

        # 心跳服务初始化
        await heartbeat_manager.initialize()
        logger.info("✅ 心跳服务初始化完成")

        # ✅ 新增：确保删除 webhook，避免冲突
        try:
            from main import bot

            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook 已删除，为 Polling 模式做准备")
            await asyncio.sleep(2)  # 确保完全删除
        except Exception as e:
            logger.warning(f"⚠️ 删除 webhook 时出现警告: {e}")

        # 执行启动流程（恢复活动定时器等）
        await simple_on_startup()

        app_state.services_initialized = True
        logger.info("✅ 所有服务初始化完成（等待主程序启动轮询）")

    except Exception as e:
        logger.error(f"❌ 服务初始化失败: {e}")
        raise


# ===========================
# 启动后台任务
# ===========================
async def start_background_tasks():
    """启动所有必要的后台任务"""
    tasks = [
        asyncio.create_task(memory_cleanup_task(), name="memory_cleanup"),
        asyncio.create_task(health_monitoring_task(), name="health_monitoring"),
        asyncio.create_task(heartbeat_manager.start_heartbeat_loop(), name="heartbeat"),
        asyncio.create_task(daily_reset_task(), name="daily_reset"),
        asyncio.create_task(efficient_monthly_export_task(), name="monthly_export"),
        asyncio.create_task(monthly_report_task(), name="monthly_report"),
    ]

    # 保存任务引用
    app_state.background_tasks = tasks

    logger.info(f"✅ 后台任务已启动: {len(tasks)} 个任务")

    # 记录任务状态
    for task in tasks:
        logger.debug(f"   - {task.get_name()}: {task.get_coro().__name__}")

    return tasks


# ===========================
# 停止后台任务
# ===========================
async def stop_background_tasks():
    """安全停止所有后台任务"""
    if not app_state.background_tasks:
        return

    logger.info(f"🛑 停止 {len(app_state.background_tasks)} 个后台任务...")

    stopped_count = 0
    for task in app_state.background_tasks:
        if not task.done():
            task.cancel()
            try:
                await task
                stopped_count += 1
            except asyncio.CancelledError:
                stopped_count += 1
            except Exception as e:
                logger.warning(f"⚠️ 停止任务 {task.get_name()} 时出错: {e}")

    logger.info(f"✅ 已停止 {stopped_count} 个后台任务")
    app_state.background_tasks = []


# ===========================
# 环境检查
# ===========================
def check_render_environment():
    """检查 Render 环境配置"""
    required_vars = ["DATABASE_URL"]
    missing_vars = []

    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)

    if missing_vars:
        logger.error(f"❌ 缺少必要的环境变量: {', '.join(missing_vars)}")
        return False

    logger.info("✅ 环境变量检查通过")
    return True


# ===========================
# 主服务函数
# ===========================
async def render_web_service():
    """
    Render Web 服务主函数
    只启动 Web 服务器和后台服务，不启动 Telegram 轮询
    """
    web_runner = None

    try:
        logger.info("🚀 启动 Render Web 服务...")

        # 检查环境
        if not check_render_environment():
            sys.exit(1)

        # 1. 必须先启动 Web 服务器（Render 要求）
        web_runner, web_site = await start_render_web_server()

        # 2. 初始化业务服务（不启动轮询）
        await initialize_services_without_polling()

        # 3. 启动后台任务
        await start_background_tasks()

        logger.info("🎉 Render Web 服务启动完成！")
        logger.info("💡 Telegram 轮询将在主程序 (main.py) 中启动")
        logger.info("🌐 Web 服务保持运行中...")
        logger.info("📊 可通过 /health 和 /status 端点监控服务状态")

        # 4. 保持服务运行（不启动轮询）
        keepalive_count = 0
        while app_state.running:
            await asyncio.sleep(30)  # 每30秒检查一次
            keepalive_count += 1

            # 每10次记录一次保持活动状态
            if keepalive_count % 10 == 0:
                logger.debug("🌐 Web 服务保持运行中...")

                # 定期检查服务状态
                try:
                    db_ok = await db.connection_health_check()
                    if not db_ok:
                        logger.warning("⚠️ 数据库连接检查失败")
                except Exception as e:
                    logger.warning(f"⚠️ 服务状态检查失败: {e}")

    except Exception as e:
        logger.error(f"💥 Render Web 服务启动失败: {e}")
        # 在 Render 中，即使失败也要保持进程运行
        try:
            while app_state.running:
                await asyncio.sleep(30)
                logger.info("🔄 服务启动失败，但保持进程运行...")
        except:
            pass
        raise

    finally:
        logger.info("🛑 开始关闭 Render Web 服务...")

        # 停止后台任务
        await stop_background_tasks()

        # 关闭 Web 服务器
        if web_runner:
            try:
                await web_runner.cleanup()
                logger.info("✅ Web 服务器已关闭")
            except Exception as e:
                logger.warning(f"⚠️ 关闭 Web 服务器时出错: {e}")

        # 清理资源
        try:
            await cleanup_resources()
            logger.info("✅ 资源清理完成")
        except Exception as e:
            logger.warning(f"⚠️ 资源清理时出错: {e}")

        logger.info("🎉 Render Web 服务关闭完成")


# ===========================
# 快速启动函数（用于测试）
# ===========================
async def quick_start():
    """快速启动（用于测试）"""
    logger.info("⚡ 快速启动 Render Web 服务...")
    await render_web_service()


# ===========================
# 程序启动
# ===========================
if __name__ == "__main__":
    try:
        # 设置更详细的日志级别
        logging.getLogger().setLevel(logging.INFO)

        # 启动服务
        asyncio.run(render_web_service())

    except KeyboardInterrupt:
        logger.info("👋 收到键盘中断信号")
    except Exception as e:
        logger.error(f"💥 Render Web 服务异常退出: {e}")
        # 在 Render 中，即使异常也要确保进程不会立即退出
        try:
            # 等待一段时间让 Render 捕获错误
            import time as sync_time

            sync_time.sleep(10)
        except:
            pass
        sys.exit(1)
