# render_deploy.py - 修复版本
import os
import asyncio
import logging
import time
import signal
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
    efficient_monthly_export_task,
    monthly_report_task,
    simple_on_startup,
)

from config import Config


# ===========================
# 🆕 实例运行检查函数
# ===========================
async def is_another_instance_running() -> bool:
    """检查是否有其他实例在运行"""
    try:
        # 方法1: 检查特定端口是否被占用（Render 使用动态端口）
        import socket

        port = int(os.environ.get("PORT", 8080))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)  # 1秒超时
        result = sock.connect_ex(("localhost", port))
        sock.close()
        if result == 0:
            logger.warning(f"⚠️ 检测到端口 {port} 已被占用，可能已有实例在运行")
            return True
    except Exception as e:
        logger.warning(f"⚠️ 端口检查失败: {e}")

    # 方法2: 检查进程（在Render环境中可能不可用，但保留作为备选）
    try:
        import psutil

        current_pid = os.getpid()

        # 查找包含机器人相关关键词的进程
        bot_keywords = ["bot", "telegram", "render_deploy.py", "main.py", "python"]

        bot_process_count = 0
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if proc.info["pid"] == current_pid:
                    continue

                cmdline = proc.info["cmdline"]
                if cmdline:
                    cmd_str = " ".join(cmdline).lower()
                    # 检查是否包含机器人相关关键词且不是系统进程
                    if (
                        any(keyword in cmd_str for keyword in bot_keywords)
                        and "render_deploy.py" in cmd_str
                    ):
                        bot_process_count += 1
                        logger.warning(
                            f"⚠️ 检测到疑似机器人进程: PID {proc.info['pid']}"
                        )
            except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
                continue

        if bot_process_count > 0:
            logger.warning(f"⚠️ 检测到 {bot_process_count} 个疑似机器人进程")
            return True

    except ImportError:
        logger.info("📝 psutil 不可用，跳过进程检查")
    except Exception as e:
        logger.warning(f"⚠️ 进程检查失败: {e}")

    # 方法3: 检查文件锁（适用于大多数环境）
    try:
        lock_file = "bot_instance.lock"
        if os.path.exists(lock_file):
            # 检查锁文件是否过期（比如超过5分钟）
            file_age = time.time() - os.path.getmtime(lock_file)
            if file_age < 300:  # 5分钟内创建的锁文件认为有效
                logger.warning("⚠️ 检测到锁文件，可能已有实例在运行")
                return True
            else:
                logger.info("🗑️ 发现过期的锁文件，清理后继续")
                os.remove(lock_file)

        # 创建新的锁文件
        with open(lock_file, "w") as f:
            f.write(str(os.getpid()))
    except Exception as e:
        logger.warning(f"⚠️ 文件锁检查失败: {e}")

    return False


# ===========================
# 日志配置
# ===========================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RenderBot")


# ===========================
# 全局状态管理
# ===========================
class AppState:
    def __init__(self):
        self.running = True
        self.polling_started = False


app_state = AppState()


# ===========================
# 信号处理
# ===========================
def handle_sigterm(signum, frame):
    logger.info(f"📡 收到信号 {signum}，准备优雅关闭 polling...")
    app_state.running = False

    try:
        loop = asyncio.get_event_loop()
        loop.create_task(stop_polling_safely())
    except Exception as e:
        logger.warning(f"⚠️ 停止 polling 时出错: {e}")


async def stop_polling_safely():
    try:
        await dp.storage.close()
        await dp.storage.wait_closed()
        await dp.stop_polling()
        await bot.session.close()
        logger.info("✅ 已优雅停止 Telegram Polling")
    except Exception as e:
        logger.warning(f"⚠️ Polling 停止时出错: {e}")


# 注册信号处理器
signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


# ===========================
# Render 保活健康检查接口
# ===========================
async def health_check(request):
    """健康检查端点"""
    return web.json_response(
        {
            "status": "healthy" if app_state.running else "shutting_down",
            "service": "telegram-bot",
            "timestamp": time.time(),
            "polling_active": app_state.polling_started,
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
    app.router.add_get("/status", health_check)

    # ✅ Render 提供动态端口，不可写死
    port = int(os.environ.get("PORT", 8080))

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    logger.info(f"✅ Web server started on Render dynamic port: {port}")
    return runner, site


# ===========================
# 初始化所有关键服务（数据库 / 心跳 / 配置）
# ===========================
# 在 render_deploy.py 的 initialize_services 函数中添加
async def initialize_services():
    logger.info("🔄 Initializing services...")

    # 🆕 强制设置Polling模式
    Config.BOT_MODE = "polling"
    logger.info("✅ 强制设置为 Polling 模式")

    # ✅ 初始化数据库
    await db.initialize()
    logger.info("✅ Database initialized")

    # ✅ 初始化心跳服务
    await heartbeat_manager.initialize()
    logger.info("✅ Heartbeat initialized")

    # ✅ 确保删除所有 webhook，避免冲突
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook deleted → switching to polling mode")

        # 额外等待确保 webhook 完全删除
        await asyncio.sleep(2)

        # 🆕 双重确认
        webhook_info = await bot.get_webhook_info()
        if webhook_info.url:
            logger.warning(f"⚠️ Webhook 仍然存在: {webhook_info.url}")
            await bot.delete_webhook(drop_pending_updates=True)
            await asyncio.sleep(1)
    except Exception as e:
        logger.warning(f"⚠️ 删除 webhook 时出现警告: {e}")

    # 🆕 执行启动流程
    await simple_on_startup()
    logger.info("✅ All services initialized with activity recovery")


# ===========================
# 启动后台任务（不会阻塞主线程）
# ===========================
async def start_background_tasks():
    """启动所有后台任务（不阻塞）- Render专用保护"""

    # 🆕 防止在Render环境中重复启动
    if hasattr(start_background_tasks, "_executed"):
        logger.warning("⚠️ [Render保护] 后台任务已经启动，跳过重复启动")
        return

    # 🆕 标记为已执行
    start_background_tasks._executed = True

    logger.info("🚀 [Render] 启动所有后台任务...")

    try:
        # 启动所有后台任务
        asyncio.create_task(heartbeat_manager.start_heartbeat_loop())
        asyncio.create_task(memory_cleanup_task())
        asyncio.create_task(health_monitoring_task())
        asyncio.create_task(daily_reset_task())
        asyncio.create_task(efficient_monthly_export_task())
        asyncio.create_task(monthly_report_task())

        logger.info("✅ [Render] 所有后台任务已启动")

    except Exception as e:
        logger.error(f"❌ [Render] 启动后台任务失败: {e}")
        # 如果启动失败，清除标记以便重试
        if hasattr(start_background_tasks, "_executed"):
            delattr(start_background_tasks, "_executed")
        raise


# ===========================
# 安全启动轮询
# ===========================
async def safe_start_polling():
    """安全启动轮询，处理冲突"""
    max_retries = 3
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            logger.info(
                f"🤖 尝试启动 Telegram bot 轮询 (尝试 {attempt + 1}/{max_retries})..."
            )

            # 启动轮询
            await dp.start_polling(bot, skip_updates=True)
            app_state.polling_started = True
            logger.info("✅ Telegram bot 轮询启动成功")
            return True

        except Exception as e:
            logger.error(f"❌ 第 {attempt + 1} 次轮询启动失败: {e}")

            if "Conflict" in str(e) and attempt < max_retries - 1:
                logger.info(f"⏳ 检测到冲突，等待 {retry_delay} 秒后重试...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # 指数退避
            else:
                logger.error("💥 轮询启动彻底失败")
                return False

    return False


# ===========================
# 主程序入口
# ===========================
async def main():
    """Render部署的主函数 - 添加实例检查"""
    # 🆕 实例运行检查
    if await is_another_instance_running():
        logger.error("❌ 检测到另一个机器人实例正在运行，当前实例退出")
        return

    web_runner = None
    web_site = None

    try:
        # ✅ Render 必须先启动该 Web 服务，否则会 Deployment Timed Out
        web_runner, web_site = await start_web_server()

        # ✅ 初始化所有服务
        await initialize_services()

        # ✅ 启动后台任务（不阻塞）
        await start_background_tasks()

        logger.info("🤖 Starting Telegram bot in POLLING mode...")

        # ✅ 安全启动轮询
        polling_success = await safe_start_polling()

        if not polling_success:
            logger.error("❌ Telegram bot 启动失败，但 Web 服务仍在运行")

            # 即使轮询失败，也保持 Web 服务运行
            while app_state.running:
                await asyncio.sleep(10)
                logger.info("🌐 Web 服务保持运行中...")

    except Exception as e:
        logger.error(f"💥 Bot failed to start: {e}")
        raise

    finally:
        logger.info("🛑 Bot shutdown complete")

        # 清理资源
        try:
            if web_runner:
                await web_runner.cleanup()
                logger.info("✅ Web runner cleaned up")
        except Exception as e:
            logger.warning(f"⚠️ 清理 web runner 时出错: {e}")


# ===========================
# 程序启动
# ===========================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 收到键盘中断信号")
    except Exception as e:
        logger.error(f"💥 主程序异常: {e}")
        raise
