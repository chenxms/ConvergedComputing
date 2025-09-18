#!/bin/bash
# 生产环境打包脚本

set -e

echo "开始打包生产环境部署包..."

# 定义变量
PACKAGE_NAME="converged-computing-production-v1.2.0"
PACKAGE_DIR="deployment_package_production"
OUTPUT_FILE="${PACKAGE_NAME}.tar.gz"

# 检查部署包目录
if [ ! -d "$PACKAGE_DIR" ]; then
    echo "错误: 部署包目录不存在"
    exit 1
fi

# 清理旧的打包文件
if [ -f "$OUTPUT_FILE" ]; then
    rm -f "$OUTPUT_FILE"
    echo "已删除旧的打包文件"
fi

# 设置可执行权限
chmod +x ${PACKAGE_DIR}/deploy.sh
chmod +x ${PACKAGE_DIR}/docker/healthcheck.sh

# 创建压缩包
echo "正在创建压缩包..."
tar -czf "$OUTPUT_FILE" \
    --transform "s|^${PACKAGE_DIR}|${PACKAGE_NAME}|" \
    --exclude="__pycache__" \
    --exclude="*.pyc" \
    --exclude=".git" \
    --exclude=".env" \
    --exclude="logs/*" \
    --exclude="temp/*" \
    --exclude="reports/*" \
    "$PACKAGE_DIR"

# 计算文件大小
SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)

echo ""
echo "================================"
echo "打包完成!"
echo "================================"
echo "文件名: $OUTPUT_FILE"
echo "文件大小: $SIZE"
echo ""
echo "部署步骤:"
echo "1. 将 $OUTPUT_FILE 传输到服务器"
echo "2. 解压: tar -xzf $OUTPUT_FILE"
echo "3. 进入目录: cd $PACKAGE_NAME"
echo "4. 配置环境: vim config/.env.production"
echo "5. 执行部署: bash deploy.sh"
echo "================================"