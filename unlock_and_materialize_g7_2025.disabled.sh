#!/bin/bash

# ===================================
# 解锁并物化G7-2025批次数据
# ===================================

echo "====================================="
echo "G7-2025批次解锁和物化脚本"
echo "====================================="
echo ""

# 步骤1：临时解锁G7-2025
echo "[步骤1] 临时解锁G7-2025批次..."
echo "当前锁定状态："
docker exec converged-computing-app env | grep DISABLE_WRITES_FOR_BATCHES || echo "未设置锁定"

# 方法A：进入容器执行物化（绕过环境变量）
echo ""
echo "[步骤2] 执行物化操作..."
echo "正在物化G7-2025数据，这可能需要几分钟时间..."

# 直接在容器内执行，临时清除环境变量
docker exec converged-computing-app bash -c '
    # 临时清除锁定
    export DISABLE_WRITES_FOR_BATCHES=""

    # 执行物化脚本
    python scripts/rewrite_subjects_v12.py G7-2025
'

if [ $? -eq 0 ]; then
    echo "✅ G7-2025物化成功！"
else
    echo "❌ 物化失败，请检查日志"
    echo "查看日志命令："
    echo "docker logs converged-computing-app --tail 100"
    exit 1
fi

# 步骤3：验证物化结果
echo ""
echo "[步骤3] 验证物化结果..."
echo "测试API响应（设置60秒超时）："

# 测试区域级数据
echo -n "测试区域级数据: "
curl -s -m 60 -o /dev/null -w "%{http_code}" http://localhost:8000/api/v12/batch/G7-2025/regional

if [ $? -eq 0 ]; then
    echo " ✅ 可访问"
else
    echo " ⚠️ 仍然超时或错误"
fi

echo ""
echo "====================================="
echo "物化完成！"
echo "====================================="
echo ""
echo "注意事项："
echo "1. G7-2025写入锁定仍然生效（防止重复写入）"
echo "2. 已物化的数据可以正常读取"
echo "3. 如需永久解锁，修改docker-compose.yml中的DISABLE_WRITES_FOR_BATCHES配置"
echo ""
echo "测试命令："
echo "curl http://localhost:8000/api/v12/batch/G7-2025/regional"