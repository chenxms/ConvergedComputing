# 教育统计分析服务 - Docker部署指南

## 项目概述

本项目是一个教育统计分析服务，用于处理学业发展质量监测数据，提供多维度统计分析和批次任务管理功能。

### 技术栈
- **后端框架**: FastAPI + SQLAlchemy 2.0
- **数据库**: MySQL 8.4.6
- **缓存**: Redis (可选)
- **容器化**: Docker + Docker Compose
- **Python版本**: 3.11

## 部署前准备

### 1. 服务器要求

#### 最低配置
- CPU: 2核
- 内存: 4GB
- 硬盘: 20GB
- 操作系统: Ubuntu 20.04+ / CentOS 7+ / Debian 10+

#### 推荐配置
- CPU: 4核+
- 内存: 8GB+
- 硬盘: 50GB+ (SSD)
- 操作系统: Ubuntu 22.04 LTS

### 2. 必要软件

运维同事需要在服务器上安装：

```bash
# Docker (版本 >= 20.10)
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER

# Docker Compose (版本 >= 2.0)
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# 验证安装
docker --version
docker-compose --version
```

### 3. 端口开放要求

需要开放以下端口：
- **8010**: 主API服务端口
- **8011**: 科目API服务端口 (subjects-v12-api)
- **6379**: Redis端口 (如果启用缓存)
- **80/443**: Nginx端口 (如果启用生产环境反向代理)

## 部署步骤

### 1. 获取项目代码

```bash
# 创建项目目录
mkdir -p /opt/converged-computing
cd /opt/converged-computing

# 拉取代码 (假设从Git仓库)
git clone [你的仓库地址] .

# 或者上传打包好的文件
# 将 converged-computing.tar.gz 上传到服务器
tar -xzvf converged-computing.tar.gz
```

### 2. 配置环境变量

创建 `.env` 文件配置环境变量：

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑环境变量
vim .env
```

**重要环境变量说明**：

```bash
# 数据库配置 (必须)
DATABASE_URL=mysql+pymysql://用户名:密码@数据库地址:端口/数据库名?charset=utf8mb4

# 应用环境
APP_ENV=production              # 环境: development/staging/production
LOG_LEVEL=INFO                  # 日志级别: DEBUG/INFO/WARNING/ERROR

# 性能配置
WORKERS=4                       # Worker进程数 (建议CPU核心数)
MAX_CONNECTIONS=100             # 最大数据库连接数
POOL_SIZE=20                    # 连接池大小
POOL_RECYCLE=3600              # 连接回收时间(秒)

# 批处理配置
BATCH_SIZE=10                   # 批处理大小
BATCH_TIMEOUT=300              # 批处理超时时间(秒)
BATCH_CODE=G7-2025             # 默认批次代码
```

### 3. 创建必要目录

```bash
# 创建日志目录
mkdir -p logs

# 创建临时文件目录
mkdir -p temp

# 创建报告输出目录
mkdir -p reports

# 设置权限
chmod -R 755 logs temp reports
```

### 4. 构建和启动服务

#### 基础部署 (仅主服务)

```bash
# 构建镜像
docker-compose build

# 启动主服务
docker-compose up -d app

# 查看日志
docker-compose logs -f app
```

#### 完整部署 (包含所有服务)

```bash
# 启动主服务 + 科目API服务
docker-compose up -d app subjects

# 启用Redis缓存
docker-compose --profile cache up -d

# 启用批处理服务
docker-compose --profile batch up -d

# 启用生产环境配置 (包含Nginx)
docker-compose --profile production up -d
```

### 5. 验证部署

```bash
# 检查服务状态
docker-compose ps

# 健康检查
curl http://localhost:8010/health

# API文档
# 访问: http://服务器IP:8010/docs

# 查看日志
docker-compose logs -f app

# 进入容器调试
docker-compose exec app bash
```

## 服务管理

### 启动/停止服务

```bash
# 启动所有服务
docker-compose up -d

# 停止所有服务
docker-compose down

# 重启单个服务
docker-compose restart app

# 查看服务状态
docker-compose ps
```

### 更新部署

```bash
# 拉取最新代码
git pull

# 重新构建镜像
docker-compose build --no-cache

# 滚动更新
docker-compose up -d --no-deps app

# 清理旧镜像
docker image prune -f
```

### 日志管理

```bash
# 查看实时日志
docker-compose logs -f app

# 查看最近100行日志
docker-compose logs --tail=100 app

# 导出日志
docker-compose logs app > app.log

# 日志文件位置
# 容器内: /app/logs/
# 宿主机: ./logs/
```

### 数据备份

```bash
# 备份数据库 (示例)
docker exec converged-computing-app mysqldump -h [数据库地址] -u [用户名] -p[密码] [数据库名] > backup_$(date +%Y%m%d).sql

# 备份报告文件
tar -czf reports_backup_$(date +%Y%m%d).tar.gz ./reports/
```

## 监控和维护

### 资源监控

```bash
# 查看容器资源使用
docker stats

# 查看磁盘使用
df -h
docker system df

# 清理未使用资源
docker system prune -a
```

### 常见问题处理

#### 1. 服务无法启动

```bash
# 检查端口占用
netstat -tlnp | grep 8010

# 检查容器日志
docker-compose logs app

# 重新构建
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

#### 2. 数据库连接失败

```bash
# 测试数据库连接
docker-compose exec app python -c "
from app.database.connection import engine
from sqlalchemy import text
with engine.connect() as conn:
    result = conn.execute(text('SELECT 1'))
    print('Database connected successfully')
"

# 检查环境变量
docker-compose exec app env | grep DATABASE
```

#### 3. 内存不足

```bash
# 调整Docker资源限制
# 编辑 docker-compose.yml 中的 deploy.resources

# 清理Docker缓存
docker system prune -a
```

## 性能优化建议

### 1. 数据库优化

```sql
-- 添加必要索引 (在MySQL中执行)
CREATE INDEX idx_batch_code ON student_score_detail(batch_code);
CREATE INDEX idx_school_id ON student_score_detail(school_id);
CREATE INDEX idx_grade ON student_score_detail(grade);
```

### 2. Docker优化

```bash
# 调整Docker daemon配置
sudo vim /etc/docker/daemon.json

{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "3"
  },
  "storage-driver": "overlay2"
}

sudo systemctl restart docker
```

### 3. 系统优化

```bash
# 调整系统参数
echo "vm.max_map_count=262144" >> /etc/sysctl.conf
sysctl -p
```

## 安全建议

1. **修改默认密码**: 部署后立即修改所有默认密码
2. **限制端口访问**: 使用防火墙限制端口访问
3. **启用HTTPS**: 生产环境必须配置SSL证书
4. **定期更新**: 定期更新Docker镜像和依赖包
5. **日志审计**: 定期检查日志文件，监控异常访问

## 联系支持

如遇到问题，请提供以下信息：

1. 错误日志: `docker-compose logs --tail=200 app`
2. 环境信息: `docker --version`, `docker-compose --version`
3. 系统信息: `uname -a`, `cat /etc/os-release`
4. 配置信息: `.env` 文件内容(隐去敏感信息)

## 附录：快速部署脚本

创建 `deploy.sh` 脚本：

```bash
#!/bin/bash

# 快速部署脚本
set -e

echo "开始部署教育统计分析服务..."

# 1. 检查Docker
if ! command -v docker &> /dev/null; then
    echo "错误: Docker未安装"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "错误: Docker Compose未安装"
    exit 1
fi

# 2. 创建必要目录
mkdir -p logs temp reports

# 3. 检查环境变量文件
if [ ! -f .env ]; then
    echo "错误: 请先创建.env文件"
    exit 1
fi

# 4. 构建镜像
echo "构建Docker镜像..."
docker-compose build

# 5. 启动服务
echo "启动服务..."
docker-compose up -d app subjects

# 6. 等待服务就绪
echo "等待服务启动..."
sleep 10

# 7. 健康检查
if curl -f http://localhost:8010/health; then
    echo "部署成功！"
    echo "API文档: http://localhost:8010/docs"
else
    echo "部署失败，请检查日志"
    docker-compose logs app
    exit 1
fi
```

使用脚本：

```bash
chmod +x deploy.sh
./deploy.sh
```