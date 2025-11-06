# rebuild 2025-11-06
FROM python:3.12-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 关键：输出安装日志，方便 Render 构建时看到
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt && pip show psutil

# 复制项目文件
COPY . .

# Render 默认会提供 PORT 环境变量
ENV PORT=10000
EXPOSE 10000

CMD ["python", "main.py"]