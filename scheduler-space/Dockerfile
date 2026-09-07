FROM python:3.10-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制调度器代码（复用 backend/scheduler）
COPY ../backend/scheduler ./scheduler
COPY ../backend/api ./api
COPY ../backend/core ./core

# 复制入口文件
COPY app.py .

# 环境变量
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:7860/health || exit 1

# 启动命令：单进程单事件循环
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860"]