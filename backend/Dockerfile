# 使用 Python 3.10作为基础镜像
FROM python:3.10-slim

# ---- 核心修改 1：创建非 root 用户 (UID 1000)，符合 Hugging Face 安全策略 ----
RUN useradd -m -u 1000 user

# 设置环境变量，强制 Python 不缓冲日志以防报错丢失
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/home/user/.local/bin:$PATH"

# ---- 核心修改 2：安装系统级依赖库，防止 psycopg2 等 C 库报错 ----
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 && \
    rm -rf /var/lib/apt/lists/*

# 设置工作目录为该用户的主目录
WORKDIR /home/user/app

# ---- 核心修改 3：强制切换为 user 用户进行后续操作 ----
USER user

# 拷贝并安装依赖 (使用 --user 参数)
COPY --chown=user:user requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# 复制后端代码并赋予权限
COPY --chown=user:user . .

# 创建数据缓存目录
RUN mkdir -p /home/user/app/data_cache && chmod 777 /home/user/app/data_cache

# 暴露端口 (HF Spaces 默认 7860)
EXPOSE 7860

# 启动命令
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7860"]
