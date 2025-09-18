# 使用官方Python运行时作为基础镜像
FROM python:3.11-slim

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 设置工作目录
WORKDIR /app

# 安装系统依赖（最小化安装）
# 说明：项目使用的是 PyMySQL（见 requirements.txt: pymysql），不依赖 mysqlclient，
# 因此无需安装 libmysqlclient/mariadb 开发包，避免外网拉取失败（502/超时）。
# 同时增加 --fix-missing 与重试，提高 apt 稳定性。
RUN set -eux; \
    apt-get update; \
    apt-get -o Acquire::Retries=3 install -y --no-install-recommends --fix-missing \
      gcc \
      pkg-config \
      curl; \
    rm -rf /var/lib/apt/lists/*

# 设置pip镜像源（使用阿里云镜像加速）
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ && \
    pip config set global.trusted-host mirrors.aliyun.com

# 先复制依赖文件（利用Docker层缓存）
COPY requirements.txt .

# 安装Python依赖
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# 复制项目代码
COPY ./app ./app

# 复制测试文件（支持容器内运行测试）
COPY ./tests ./tests
COPY ./pytest.ini ./pytest.ini

# 复制必要的脚本和配置
COPY ./scripts ./scripts
# Alembic 迁移配置与脚本（用于容器内执行数据库迁移）
COPY ./alembic.ini ./alembic.ini
COPY ./alembic ./alembic
# 便于直接运行逐校重写脚本（无需路径前缀）
COPY ./scripts/rebuild_school_v12.py ./rebuild_school_v12.py
COPY ./config ./config

# 复制根目录下需要运行的Python脚本（单科清洗/汇聚等）
COPY ./data_cleaning_service.py ./data_cleaning_service.py
COPY ./run_single_subject_pipeline.py ./run_single_subject_pipeline.py
COPY ./run_full_batch_pipeline.py ./run_full_batch_pipeline.py
COPY ./fast_materialize_subjects_v12.py ./fast_materialize_subjects_v12.py
COPY ./fast_materialize_all_batches_v12.py ./fast_materialize_all_batches_v12.py
COPY ./enhanced_questionnaire_clean.py ./enhanced_questionnaire_clean.py
COPY ./fixed_questionnaire_clean.py ./fixed_questionnaire_clean.py

# 复制文档（只读）
COPY ./docs ./docs

# 创建非root用户
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# 创建必要的目录
RUN mkdir -p /app/logs /app/temp /app/reports && \
    chown -R appuser:appuser /app/logs /app/temp /app/reports

# 切换到非root用户
USER appuser

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# 暴露端口
EXPOSE 8000

# 启动命令（使用uvicorn）
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
