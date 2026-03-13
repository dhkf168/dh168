# 使用 Python 3.12 官方精简镜像
FROM python:3.12-slim

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai

WORKDIR /app

# 安装必要的系统依赖（libpq-dev 是连接 PostgreSQL 必须的）
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件（利用缓存）
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# Render 默认端口
EXPOSE 10000

# ✅ 改进的健康检查：使用原生 urllib 或 curl
# 注意：确保 main.py 里的 health_server 运行在 10000 端口
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:10000/health || exit 1

# 启动命令
CMD ["python", "main.py"]
