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

# 安装 Python 依赖
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# ✅ 修复：暴露标准端口，Render 会重定向
EXPOSE 8080

# ✅ 修复：使用正确的主启动文件
CMD ["python", "main.py"]