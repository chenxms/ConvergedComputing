# 多阶段构建 - 优化镜像体积和安全性
FROM python:3.11-slim as builder

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 安装Poetry
RUN pip install --no-cache-dir poetry

# 复制依赖定义文件
COPY pyproject.toml poetry.lock* ./
COPY requirements.txt* ./

# 安装项目依赖
RUN poetry config virtualenvs.create false && \
    poetry install --no-root --only main || \
    pip install --no-cache-dir -r requirements.txt

# 生产环境镜像
FROM python:3.11-slim

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# 设置工作目录
WORKDIR /app

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 从builder阶段复制Python包
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# 复制项目代码（包含新的汇聚系统）
COPY ./app ./app

# 复制运行所需脚本与文档（仅必要）
COPY scripts/rewrite_subjects_v12.py ./scripts/
COPY scripts/acceptance_quick_check.py ./scripts/
COPY scripts/complete_questionnaire_labels.py ./scripts/
COPY docs/ ./docs/

# 创建非root用户
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# 切换到非root用户
USER appuser

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# 暴露端口
EXPOSE 8000

# 启动命令
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
