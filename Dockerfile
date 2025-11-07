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

# ✅ 重要：使用 Render 提供的动态 PORT 环境变量
# 不要设置固定 PORT，Render 会自动注入
EXPOSE 10000  

# ✅ 修改：使用 render_deploy.py 作为启动文件
CMD ["python", "render_deploy.py"]
