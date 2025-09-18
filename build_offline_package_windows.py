#!/usr/bin/env python3
"""
Windows环境下构建离线部署包
包含所有Docker镜像，无需在线下载依赖
"""
import os
import shutil
import subprocess
import tarfile
from pathlib import Path

# 配置
PACKAGE_NAME = "converged-computing-offline-v1.2.0"
PACKAGE_DIR = "deployment_package_production"
OUTPUT_DIR = "offline_deployment"
IMAGE_NAME = "converged-computing-app"
IMAGE_TAG = "v1.2.0"

def run_command(cmd, shell=True):
    """执行命令并返回结果"""
    print(f"执行: {cmd}")
    result = subprocess.run(cmd, shell=shell, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"错误: {result.stderr}")
        raise Exception(f"命令执行失败: {cmd}")
    return result.stdout

def build_offline_package():
    """构建离线部署包主函数"""
    print("=" * 50)
    print("构建离线部署包")
    print("=" * 50)
    
    # 1. 创建输出目录
    print("\n1. 创建输出目录...")
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(f"{OUTPUT_DIR}/images")
    os.makedirs(f"{OUTPUT_DIR}/package")
    
    # 2. 复制部署包内容
    print("\n2. 复制部署包内容...")
    shutil.copytree(PACKAGE_DIR, f"{OUTPUT_DIR}/package", dirs_exist_ok=True)
    
    # 3. 准备优化构建
    print("\n3. 准备优化构建...")
    shutil.copy(f"{PACKAGE_DIR}/requirements-prod.txt", "requirements.txt")
    
    # 4. 构建Docker镜像
    print("\n4. 构建优化的Docker镜像...")
    docker_context = os.path.abspath(".")
    dockerfile_path = os.path.abspath(f"{PACKAGE_DIR}/docker/Dockerfile.optimized")
    
    build_cmd = f'docker build -f "{dockerfile_path}" -t {IMAGE_NAME}:{IMAGE_TAG} "{docker_context}"'
    run_command(build_cmd)
    
    # 5. 导出Docker镜像
    print("\n5. 导出Docker镜像...")
    
    print("   - 导出应用镜像...")
    run_command(f"docker save -o {OUTPUT_DIR}/images/app.tar {IMAGE_NAME}:{IMAGE_TAG}")
    
    print("   - 拉取并导出Redis镜像...")
    run_command("docker pull redis:7-alpine")
    run_command(f"docker save -o {OUTPUT_DIR}/images/redis.tar redis:7-alpine")
    
    print("   - 拉取并导出Nginx镜像...")
    run_command("docker pull nginx:alpine")
    run_command(f"docker save -o {OUTPUT_DIR}/images/nginx.tar nginx:alpine")
    
    # 6. 创建离线部署脚本
    print("\n6. 创建离线部署脚本...")
    with open(f"{OUTPUT_DIR}/offline_deploy.sh", "w", encoding="utf-8") as f:
        f.write(OFFLINE_DEPLOY_SCRIPT)
    
    # 7. 创建README
    print("\n7. 创建README...")
    with open(f"{OUTPUT_DIR}/README.md", "w", encoding="utf-8") as f:
        f.write(README_CONTENT)
    
    # 8. 计算大小
    print("\n8. 计算包大小...")
    sizes = {}
    for img in ["app.tar", "redis.tar", "nginx.tar"]:
        size = os.path.getsize(f"{OUTPUT_DIR}/images/{img}") / (1024 * 1024)  # MB
        sizes[img] = f"{size:.1f}MB"
    
    total_size = sum(os.path.getsize(os.path.join(dp, f)) 
                    for dp, dn, filenames in os.walk(OUTPUT_DIR) 
                    for f in filenames) / (1024 * 1024)
    
    print("\n" + "=" * 50)
    print("离线包构建完成!")
    print("=" * 50)
    print("镜像大小:")
    print(f"  - 应用镜像: {sizes['app.tar']}")
    print(f"  - Redis镜像: {sizes['redis.tar']}")
    print(f"  - Nginx镜像: {sizes['nginx.tar']}")
    print(f"总包大小: {total_size:.1f}MB")
    
    # 9. 创建最终压缩包
    print(f"\n正在打包 {PACKAGE_NAME}.tar.gz...")
    with tarfile.open(f"{PACKAGE_NAME}.tar.gz", "w:gz") as tar:
        tar.add(OUTPUT_DIR, arcname=PACKAGE_NAME)
    
    final_size = os.path.getsize(f"{PACKAGE_NAME}.tar.gz") / (1024 * 1024)
    
    print(f"\n最终部署包: {PACKAGE_NAME}.tar.gz ({final_size:.1f}MB)")
    print("\n部署步骤:")
    print(f"1. 传输到服务器: scp {PACKAGE_NAME}.tar.gz user@server:/opt/")
    print(f"2. 解压: tar -xzf {PACKAGE_NAME}.tar.gz")
    print("3. 配置: vim package/config/.env.production")
    print("4. 部署: bash offline_deploy.sh")
    print("=" * 50)

# 离线部署脚本内容
OFFLINE_DEPLOY_SCRIPT = '''#!/bin/bash
# 离线部署脚本

set -e

# 颜色定义
RED='\\033[0;31m'
GREEN='\\033[0;32m'
YELLOW='\\033[1;33m'
NC='\\033[0m'

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
'''

# README内容
README_CONTENT = '''# ConvergedComputing 离线部署包

## 部署包内容

- `images/`: Docker镜像文件
  - `app.tar`: 应用主镜像（优化版，约200MB）
  - `redis.tar`: Redis缓存镜像（约30MB）
  - `nginx.tar`: Nginx反向代理镜像（约40MB）
- `package/`: 应用部署文件
- `offline_deploy.sh`: 离线部署脚本

## 部署要求

- Docker 20.10.0+
- Docker Compose 1.29.0+
- **无需互联网连接**
- 8GB+ 内存
- 4+ CPU核心

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

## 优化说明

- 使用精简的生产环境依赖（requirements-prod.txt）
- 多阶段Docker构建，减少镜像体积
- 基础镜像使用python:3.11-slim
- 移除开发工具和测试依赖

## 常见问题

1. **镜像加载失败**: 确保有足够的磁盘空间
2. **端口冲突**: 检查8000端口是否被占用
3. **数据库连接失败**: 验证配置文件中的数据库连接信息

## 镜像信息

总计约 270MB 镜像文件，压缩后约 150MB
'''

if __name__ == "__main__":
    try:
        build_offline_package()
    except Exception as e:
        print(f"\n错误: {e}")
        exit(1)