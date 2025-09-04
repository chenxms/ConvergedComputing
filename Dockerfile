# 使用官方Python基础镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 安装Poetry
RUN pip install poetry

# 复制依赖定义文件
COPY pyproject.toml ./

# 安装项目依赖
RUN poetry config virtualenvs.create false && poetry install --no-root --only main

# 复制项目代码
COPY ./app ./app

# 暴露端口
EXPOSE 8000

# 启动命令
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]