#!/usr/bin/env bash
set -euo pipefail

# 一键发布与验证 G7-2025 区域级重复修复
# 先决条件：已安装 docker 与 docker compose，且能访问配置的 DATABASE_URL

echo "[1/5] 构建镜像 app/subjects ..."
docker compose build app subjects

echo "[2/5] 重启服务 app/subjects ..."
docker compose up -d app subjects

echo "[3/5] 执行区域级去重与规范化 ..."
docker compose exec app python scripts/fix_regional_duplicates.py --batch G7-2025 || true

echo "[4/5] 验证 60 秒内是否无增长 ..."
docker compose exec app python scripts/verify_no_growth.py --batch G7-2025

echo "[5/5] 完成。若需恢复写入，请在 docker-compose.yml 调整 DISABLE_WRITES_FOR_BATCHES 后："
echo "        docker compose up -d app subjects"

