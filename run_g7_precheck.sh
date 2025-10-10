#!/bin/bash

# G7-2025 汇聚重启预检查脚本 (Linux/macOS Shell版本)
#
# 用法：
#   ./run_g7_precheck.sh                    # 完整预检查
#   ./run_g7_precheck.sh quick              # 快速检查
#   ./run_g7_precheck.sh backup-only        # 仅备份
#   ./run_g7_precheck.sh validation-only    # 仅验证

set -e  # 遇到错误时退出

echo "============================================="
echo "G7-2025 汇聚重启预检查套件"
echo "============================================="
echo "开始时间: $(date)"
echo

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    if ! command -v python &> /dev/null; then
        echo "[ERROR] Python未安装或不在PATH中"
        exit 1
    else
        PYTHON_CMD="python"
    fi
else
    PYTHON_CMD="python3"
fi

echo "使用Python: $($PYTHON_CMD --version)"

# 设置参数
ARGS=""
case "$1" in
    "quick")
        ARGS="--quick"
        echo "模式: 快速检查"
        ;;
    "backup-only")
        ARGS="--backup-only"
        echo "模式: 仅备份"
        ;;
    "validation-only")
        ARGS="--validation-only"
        echo "模式: 仅验证"
        ;;
    "")
        echo "模式: 完整预检查"
        ;;
    *)
        echo "错误: 未知参数 '$1'"
        echo "用法: $0 [quick|backup-only|validation-only]"
        exit 1
        ;;
esac

echo

# 进入脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 执行预检查套件
echo "执行预检查套件..."
if $PYTHON_CMD g7_precheck_suite.py $ARGS; then
    echo
    echo "============================================="
    echo "[SUCCESS] 预检查完成"
    echo "============================================="
    echo "结束时间: $(date)"
    exit 0
else
    echo
    echo "============================================="
    echo "[ERROR] 预检查失败，请查看上述错误信息"
    echo "============================================="
    echo "结束时间: $(date)"
    exit 1
fi