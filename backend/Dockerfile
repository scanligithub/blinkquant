FROM python:3.10-slim

RUN useradd -m -u 1000 user

ENV POLARS_MAX_THREADS=2 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/home/user/.local/bin:$PATH"

# HF Persistent Storage 的 /data 可能由 root 持有；启动时先修正挂载点，
# 然后让实际应用继续以非 root 用户运行。
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 gosu && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user/app/data_cache /data && \
    chown -R user:user /home/user/app && \
    chmod 777 /home/user/app/data_cache

WORKDIR /home/user/app

COPY --chown=user:user requirements.txt .
USER user
RUN pip install --no-cache-dir --user -r requirements.txt

COPY --chown=user:user . .

USER root
COPY --chown=root:root docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 755 /usr/local/bin/docker-entrypoint.sh

EXPOSE 7860

# Deployment trigger: keep production Node Spaces restartable from a backend push.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7860"]
