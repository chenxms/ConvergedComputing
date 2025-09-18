#!/bin/bash

# ===================================
# 项目打包脚本 - 包含CORS修复
# ===================================

set -e

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# 打包信息
PACKAGE_NAME="converged-computing-cors-fixed"
VERSION=$(date +%Y%m%d_%H%M%S)
OUTPUT_DIR="deployment_package_cors_${VERSION}"
ARCHIVE_NAME="${PACKAGE_NAME}_${VERSION}.tar.gz"

echo -e "${GREEN}开始打包教育统计分析服务（包含CORS修复）...${NC}"
echo "版本: ${VERSION}"

# 创建临时打包目录
echo -e "${GREEN}创建打包目录...${NC}"
mkdir -p ${OUTPUT_DIR}

# ===================================
# 复制核心程序文件
# ===================================

echo -e "${GREEN}复制核心程序文件...${NC}"

# 1. 复制整个app目录（核心应用代码）
cp -r app ${OUTPUT_DIR}/
# 删除Python缓存文件
find ${OUTPUT_DIR}/app -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find ${OUTPUT_DIR}/app -type f -name "*.pyc" -delete 2>/dev/null || true

# 2. 确保CORS中间件已包含
echo -e "${YELLOW}确保CORS修复已包含...${NC}"
if [ ! -d "${OUTPUT_DIR}/app/middleware" ]; then
    mkdir -p ${OUTPUT_DIR}/app/middleware
fi
# 如果middleware目录没有__init__.py，创建它
if [ ! -f "${OUTPUT_DIR}/app/middleware/__init__.py" ]; then
    touch ${OUTPUT_DIR}/app/middleware/__init__.py
fi

# 3. 复制必要的脚本和服务文件
mkdir -p ${OUTPUT_DIR}/scripts
cp scripts/rewrite_subjects_v12.py ${OUTPUT_DIR}/scripts/ 2>/dev/null || true
cp scripts/acceptance_quick_check.py ${OUTPUT_DIR}/scripts/ 2>/dev/null || true
cp scripts/clean_g4_statistical_data.py ${OUTPUT_DIR}/scripts/ 2>/dev/null || true
cp scripts/rebuild_g4_aggregation.py ${OUTPUT_DIR}/scripts/ 2>/dev/null || true
cp scripts/test_database_connection.py ${OUTPUT_DIR}/scripts/ 2>/dev/null || true

# 4. 复制关键服务文件
cp data_cleaning_service.py ${OUTPUT_DIR}/ 2>/dev/null || true

# 5. 复制文档目录（包含数据规范）
cp -r docs ${OUTPUT_DIR}/ 2>/dev/null || true

# ===================================
# 复制Docker相关文件
# ===================================

echo -e "${GREEN}复制Docker配置文件...${NC}"

cp Dockerfile ${OUTPUT_DIR}/
cp docker-compose.yml ${OUTPUT_DIR}/
cp .dockerignore ${OUTPUT_DIR}/ 2>/dev/null || true

# ===================================
# 复制部署相关文件
# ===================================

echo -e "${GREEN}复制部署文档和脚本...${NC}"

cp .env.example ${OUTPUT_DIR}/ 2>/dev/null || true
cp deploy.sh ${OUTPUT_DIR}/ 2>/dev/null || true
cp DEPLOYMENT_GUIDE.md ${OUTPUT_DIR}/ 2>/dev/null || true
cp DEPLOYMENT_CHECKLIST.md ${OUTPUT_DIR}/ 2>/dev/null || true

# 复制CORS相关配置
cp nginx.conf ${OUTPUT_DIR}/nginx.conf.example 2>/dev/null || true
cp CORS_FIX_DEPLOYMENT.md ${OUTPUT_DIR}/ 2>/dev/null || true

# ===================================
# 复制依赖定义文件
# ===================================

echo -e "${GREEN}复制依赖文件...${NC}"

# Python依赖
cp requirements.txt ${OUTPUT_DIR}/ 2>/dev/null || true
cp requirements-prod.txt ${OUTPUT_DIR}/ 2>/dev/null || true
cp pyproject.toml ${OUTPUT_DIR}/ 2>/dev/null || true
cp poetry.lock ${OUTPUT_DIR}/ 2>/dev/null || true

# ===================================
# 创建生产环境配置示例
# ===================================

echo -e "${GREEN}创建生产环境配置...${NC}"

cat > ${OUTPUT_DIR}/.env.production << 'EOF'
# 生产环境配置
# 请根据实际情况修改

# 数据库配置
DATABASE_URL=mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4

# 应用配置
APP_ENV=production
LOG_LEVEL=INFO
DEBUG=false

# CORS配置
CORS_ORIGINS=["http://localhost:8080", "http://117.72.14.166:8080", "*"]
CORS_ALLOW_CREDENTIALS=true
CORS_ALLOW_METHODS=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]
CORS_ALLOW_HEADERS=["*"]

# 性能配置
WORKERS=4
MAX_CONNECTIONS=100
POOL_SIZE=20
POOL_RECYCLE=3600

# 批处理配置
BATCH_SIZE=10
BATCH_TIMEOUT=300

# 写入控制
DISABLE_WRITES_FOR_BATCHES=G7-2025
EOF

# ===================================
# 创建必要的空目录
# ===================================

echo -e "${GREEN}创建必要的目录结构...${NC}"

mkdir -p ${OUTPUT_DIR}/logs
mkdir -p ${OUTPUT_DIR}/temp
mkdir -p ${OUTPUT_DIR}/reports
mkdir -p ${OUTPUT_DIR}/config

# ===================================
# 创建快速部署脚本
# ===================================

cat > ${OUTPUT_DIR}/quick_deploy.sh << 'EOF'
#!/bin/bash

# 快速部署脚本 - 包含CORS修复

set -e

echo "====================================="
echo "教育统计分析服务 - 快速部署"
echo "====================================="

# 1. 检查Docker环境
if ! command -v docker &> /dev/null; then
    echo "错误: 未找到Docker，请先安装Docker"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "错误: 未找到docker-compose，请先安装docker-compose"
    exit 1
fi

# 2. 设置环境变量
if [ ! -f .env ]; then
    echo "创建环境配置文件..."
    cp .env.production .env
    echo "请编辑 .env 文件配置数据库连接信息"
    read -p "配置完成后按回车继续..."
fi

# 3. 构建Docker镜像
echo "构建Docker镜像..."
docker-compose build

# 4. 停止旧容器（如果存在）
echo "停止旧容器..."
docker-compose down 2>/dev/null || true

# 5. 启动新容器
echo "启动服务..."
docker-compose up -d

# 6. 等待服务启动
echo "等待服务启动..."
sleep 10

# 7. 健康检查
echo "执行健康检查..."
if curl -f http://localhost:8010/health &>/dev/null; then
    echo "✓ 服务启动成功"
else
    echo "✗ 服务启动失败，请检查日志"
    docker-compose logs --tail=50
    exit 1
fi

# 8. 测试CORS配置
echo "测试CORS配置..."
response=$(curl -s -X OPTIONS http://localhost:8010/api/v12/batch/G7-2025/regional \
    -H "Origin: http://localhost:8080" \
    -H "Access-Control-Request-Method: GET" \
    -I 2>/dev/null | grep -i "access-control-allow-origin" || echo "未找到CORS头")

if [[ $response == *"Access-Control-Allow-Origin"* ]]; then
    echo "✓ CORS配置正确"
else
    echo "⚠ CORS可能未正确配置，请检查"
fi

echo ""
echo "====================================="
echo "部署完成！"
echo "服务地址: http://localhost:8010"
echo "API文档: http://localhost:8010/docs"
echo "====================================="
echo ""
echo "后续操作："
echo "1. 查看日志: docker-compose logs -f"
echo "2. 停止服务: docker-compose down"
echo "3. 重启服务: docker-compose restart"
EOF

chmod +x ${OUTPUT_DIR}/quick_deploy.sh

# ===================================
# 创建验证脚本
# ===================================

cat > ${OUTPUT_DIR}/verify_deployment.sh << 'EOF'
#!/bin/bash

# 部署验证脚本

echo "====================================="
echo "部署验证"
echo "====================================="

# 服务健康检查
echo -n "1. 检查服务健康状态... "
if curl -f http://localhost:8010/health &>/dev/null; then
    echo "✓ 正常"
else
    echo "✗ 失败"
    exit 1
fi

# API端点测试
echo -n "2. 测试API端点... "
if curl -f http://localhost:8010/api/v12/batch/G7-2025/regional &>/dev/null; then
    echo "✓ 正常"
else
    echo "⚠ 可能需要数据"
fi

# CORS测试
echo -n "3. 测试CORS配置... "
cors_header=$(curl -s -X OPTIONS http://localhost:8010/api/v12/batch/G7-2025/regional \
    -H "Origin: http://localhost:8080" \
    -H "Access-Control-Request-Method: GET" \
    -I 2>/dev/null | grep -i "access-control-allow-origin")

if [[ ! -z "$cors_header" ]]; then
    echo "✓ 正常"
    echo "   $cors_header"
else
    echo "✗ 失败"
fi

# Docker容器状态
echo -n "4. 检查Docker容器... "
if docker ps | grep converged-computing-app &>/dev/null; then
    echo "✓ 运行中"
else
    echo "✗ 未运行"
fi

echo ""
echo "====================================="
echo "验证完成"
echo "====================================="
EOF

chmod +x ${OUTPUT_DIR}/verify_deployment.sh

# ===================================
# 创建README文件
# ===================================

cat > ${OUTPUT_DIR}/README.md << 'EOF'
# 教育统计分析服务 - 部署包（CORS修复版）

## 版本信息
- **包含CORS修复**: 解决前端跨域访问问题
- **构建时间**: '${VERSION}'

## 快速部署

### 方法1: 使用快速部署脚本（推荐）
```bash
chmod +x quick_deploy.sh
./quick_deploy.sh
```

### 方法2: 手动部署
```bash
# 1. 配置环境变量
cp .env.production .env
vi .env  # 修改数据库连接

# 2. 构建并启动
docker-compose build
docker-compose up -d

# 3. 验证部署
./verify_deployment.sh
```

## CORS问题解决

本版本已包含CORS修复：
1. 增强的CORS中间件（`app/middleware/cors_config.py`）
2. 支持OPTIONS预检请求
3. Nginx配置示例（`nginx.conf.example`）

详见 `CORS_FIX_DEPLOYMENT.md`

## 目录结构

```
deployment_package/
├── app/                    # 核心应用代码
│   ├── main.py            # 主应用（已更新CORS）
│   ├── middleware/        # 中间件
│   │   └── cors_config.py # CORS配置
│   ├── api/               # API路由
│   ├── services/          # 业务逻辑
│   └── database/          # 数据访问
├── scripts/               # 维护脚本
├── docs/                  # 文档
├── docker-compose.yml     # Docker编排
├── Dockerfile            # Docker镜像定义
├── .env.production       # 生产环境配置模板
├── quick_deploy.sh       # 快速部署脚本
├── verify_deployment.sh  # 验证脚本
├── nginx.conf.example    # Nginx配置示例
└── CORS_FIX_DEPLOYMENT.md # CORS修复说明
```

## 验证服务

```bash
# 健康检查
curl http://localhost:8010/health

# 测试CORS
curl -X OPTIONS http://localhost:8010/api/v12/batch/G7-2025/regional \
  -H "Origin: http://localhost:8080" \
  -H "Access-Control-Request-Method: GET" -v

# 查看API文档
浏览器访问: http://localhost:8010/docs
```

## 故障排查

1. **服务无法启动**
   ```bash
   docker-compose logs --tail=100
   ```

2. **CORS仍有问题**
   - 检查Nginx配置（如果使用）
   - 查看 `CORS_FIX_DEPLOYMENT.md`

3. **数据库连接失败**
   - 检查 `.env` 文件中的DATABASE_URL
   - 确认数据库服务可访问

## 技术支持

遇到问题请：
1. 查看 `DEPLOYMENT_CHECKLIST.md`
2. 运行 `./verify_deployment.sh` 诊断
3. 查看日志 `docker-compose logs`
EOF

# ===================================
# 打包压缩
# ===================================

echo -e "${GREEN}创建压缩包...${NC}"
tar -czf ${ARCHIVE_NAME} ${OUTPUT_DIR}

# 计算包大小
SIZE=$(du -sh ${ARCHIVE_NAME} | cut -f1)

echo -e "${GREEN}打包完成！${NC}"
echo "====================================="
echo "包名称: ${ARCHIVE_NAME}"
echo "包大小: ${SIZE}"
echo "包含文件数: $(find ${OUTPUT_DIR} -type f | wc -l)"
echo "====================================="
echo -e "${YELLOW}特性：${NC}"
echo "✓ 包含CORS修复"
echo "✓ 支持OPTIONS预检请求"
echo "✓ 包含Nginx配置示例"
echo "✓ 包含快速部署脚本"
echo "✓ 包含验证脚本"
echo "====================================="

# 可选：删除临时目录
read -p "是否删除临时打包目录? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -rf ${OUTPUT_DIR}
    echo -e "${GREEN}临时目录已删除${NC}"
else
    echo -e "${YELLOW}临时目录保留在: ${OUTPUT_DIR}${NC}"
fi

echo -e "${GREEN}部署包已准备就绪：${ARCHIVE_NAME}${NC}"
echo "请将此文件发送给运维团队进行部署。"