#!/bin/bash
# 构建离线部署包脚本
# 包含所有Docker镜像和依赖

set -e

echo "================================"
echo "构建离线部署包"
echo "================================"

# 定义变量
PACKAGE_NAME="converged-computing-offline-v1.2.0"
PACKAGE_DIR="deployment_package_production"
OUTPUT_DIR="offline_deployment"
IMAGE_NAME="converged-computing-app"
IMAGE_TAG="v1.2.0"

# 创建输出目录
echo "1. 创建输出目录..."
rm -rf $OUTPUT_DIR
mkdir -p $OUTPUT_DIR/images
mkdir -p $OUTPUT_DIR/package

# 复制部署包内容
echo "2. 复制部署包内容..."
cp -r $PACKAGE_DIR/* $OUTPUT_DIR/package/

# 使用优化的依赖文件
echo "3. 准备优化构建..."
cp $PACKAGE_DIR/requirements-prod.txt requirements.txt

# 构建Docker镜像（使用优化版本）
echo "4. 构建优化的Docker镜像..."
cd $PACKAGE_DIR/docker
docker build -f Dockerfile.optimized -t ${IMAGE_NAME}:${IMAGE_TAG} ../..
cd ../..

# 导出Docker镜像
echo "5. 导出Docker镜像..."
echo "   - 导出应用镜像..."
docker save -o $OUTPUT_DIR/images/app.tar ${IMAGE_NAME}:${IMAGE_TAG}

echo "   - 导出Redis镜像..."
docker pull redis:7-alpine
docker save -o $OUTPUT_DIR/images/redis.tar redis:7-alpine

echo "   - 导出Nginx镜像..."
docker pull nginx:alpine
docker save -o $OUTPUT_DIR/images/nginx.tar nginx:alpine

# 创建离线部署脚本
echo "6. 创建离线部署脚本..."
cat > $OUTPUT_DIR/offline_deploy.sh << 'EOF'
#!/bin/bash
# 离线部署脚本

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo "================================"
echo "ConvergedComputing 离线部署"
echo "================================"

# 检查Docker
print_info "检查Docker环境..."
if ! command -v docker &> /dev/null; then
    print_error "Docker未安装，请先安装Docker"
    exit 1
fi

# 加载镜像
print_info "加载Docker镜像..."
print_info "  - 加载应用镜像..."
docker load -i images/app.tar

print_info "  - 加载Redis镜像..."
docker load -i images/redis.tar

print_info "  - 加载Nginx镜像..."
docker load -i images/nginx.tar

# 进入部署包目录
cd package

# 配置检查
print_info "检查配置文件..."
if [ ! -f "config/.env.production" ]; then
    if [ -f "config/.env.production.example" ]; then
        print_warn "配置文件不存在，复制模板..."
        cp config/.env.production.example config/.env.production
        print_error "请编辑 config/.env.production 配置文件后重新运行"
        exit 1
    fi
fi

# 创建必要目录
print_info "创建必要目录..."
mkdir -p logs reports temp

# 启动服务
print_info "启动服务..."
cd docker
docker-compose -f docker-compose.production.yml up -d
cd ..

# 等待服务启动
print_info "等待服务启动..."
sleep 10

# 健康检查
print_info "执行健康检查..."
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -f http://localhost:8000/health >/dev/null 2>&1; then
        print_info "✓ 服务健康检查通过"
        break
    fi
    
    RETRY_COUNT=$((RETRY_COUNT+1))
    print_warn "健康检查失败，重试 $RETRY_COUNT/$MAX_RETRIES..."
    sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    print_error "服务健康检查失败"
    docker-compose -f docker/docker-compose.production.yml logs --tail=50
    exit 1
fi

echo ""
print_info "================================"
print_info "部署成功!"
print_info "================================"
print_info "API地址: http://localhost:8000"
print_info "API文档: http://localhost:8000/docs"
print_info "健康检查: http://localhost:8000/health"
echo ""
print_info "查看日志: docker-compose -f docker/docker-compose.production.yml logs -f"
print_info "停止服务: docker-compose -f docker/docker-compose.production.yml down"
echo ""
EOF

chmod +x $OUTPUT_DIR/offline_deploy.sh

# 创建README
echo "7. 创建README..."
cat > $OUTPUT_DIR/README.md << 'EOF'
# ConvergedComputing 离线部署包

## 部署包内容

- `images/`: Docker镜像文件
  - `app.tar`: 应用主镜像
  - `redis.tar`: Redis缓存镜像
  - `nginx.tar`: Nginx反向代理镜像
- `package/`: 应用部署文件
- `offline_deploy.sh`: 离线部署脚本

## 部署要求

- Docker 20.10.0+
- Docker Compose 1.29.0+
- 无需互联网连接

## 部署步骤

1. **配置环境变量**
   ```bash
   cd package
   cp config/.env.production.example config/.env.production
   vim config/.env.production  # 修改数据库连接等配置
   cd ..
   ```

2. **执行离线部署**
   ```bash
   bash offline_deploy.sh
   ```

## 镜像信息

- converged-computing-app:v1.2.0 (~300MB)
- redis:7-alpine (~30MB)
- nginx:alpine (~40MB)

总计约 370MB 镜像文件
EOF

# 计算大小
echo "8. 计算包大小..."
APP_SIZE=$(du -h $OUTPUT_DIR/images/app.tar | cut -f1)
REDIS_SIZE=$(du -h $OUTPUT_DIR/images/redis.tar | cut -f1)
NGINX_SIZE=$(du -h $OUTPUT_DIR/images/nginx.tar | cut -f1)
TOTAL_SIZE=$(du -sh $OUTPUT_DIR | cut -f1)

echo ""
echo "================================"
echo "离线包构建完成!"
echo "================================"
echo "镜像大小:"
echo "  - 应用镜像: $APP_SIZE"
echo "  - Redis镜像: $REDIS_SIZE"
echo "  - Nginx镜像: $NGINX_SIZE"
echo "总包大小: $TOTAL_SIZE"
echo ""
echo "正在打包..."

# 创建最终压缩包
tar -czf ${PACKAGE_NAME}.tar.gz -C $OUTPUT_DIR .

FINAL_SIZE=$(du -h ${PACKAGE_NAME}.tar.gz | cut -f1)
echo ""
echo "最终部署包: ${PACKAGE_NAME}.tar.gz ($FINAL_SIZE)"
echo "部署步骤:"
echo "1. 传输到服务器: scp ${PACKAGE_NAME}.tar.gz user@server:/opt/"
echo "2. 解压: tar -xzf ${PACKAGE_NAME}.tar.gz"
echo "3. 配置: vim package/config/.env.production"
echo "4. 部署: bash offline_deploy.sh"
echo "================================"