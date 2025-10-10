#!/usr/bin/env bash
set -euo pipefail

# G7-2025 批次汇聚重启一键脚本（白名单-only，安全）
# 说明：
# - 不开启维护模式，仅放行专用用户 chenlei，旧流程写入将被阻断
# - 需预先在当前 Shell 导出 G7_DB_PASSWORD 或在此脚本前交互赋值
#
# 使用示例：
#   export G7_DB_HOST=117.72.14.166
#   export G7_DB_PORT=23506
#   export G7_DB_NAME=appraisal_test
#   export G7_DB_PASSWORD='你的密码'
#   bash scripts/run_g7_relaunch.sh

echo "[G7 Relaunch] Start"

: "${G7_DB_HOST:=117.72.14.166}"
: "${G7_DB_PORT:=23506}"
: "${G7_DB_NAME:=appraisal_test}"
if [[ -z "${G7_DB_PASSWORD:-}" ]]; then
  echo "[ERROR] 请先设置环境变量 G7_DB_PASSWORD（chenlei 的密码）" >&2
  exit 1
fi

export DATABASE_URL="mysql+pymysql://chenlei:${G7_DB_PASSWORD}@${G7_DB_HOST}:${G7_DB_PORT}/${G7_DB_NAME}?charset=utf8mb4"
export PRODUCTION_DATABASE_URL="${DATABASE_URL}"

echo "[1/6] 安装增强守卫并添加白名单(chenlei%)，关闭维护模式"
python scripts/enhanced_g7_guard.py install
python scripts/enhanced_g7_guard.py add-whitelist "chenlei%" || true
python scripts/enhanced_g7_guard.py disable-maintenance || true

echo "[2/6] 触发器快速校验"
python scripts/validate_g7_triggers.py --quick

echo "[3/6] 预检查与备份（含干净版SQL）"
python g7_precheck_suite.py --with-validation || {
  echo "[WARN] 预检查存在告警/失败，请根据日志处理后重试" >&2
}

echo "[4/6] 启动新流水线（包装器）"
python run_g7_pipeline_wrapper.py --batch G7-2025 --env production

echo "[5/6] 数据与API验证"
python validate_g7_data.py --compare-backup || {
  echo "[WARN] 验证存在告警/失败，请根据报告处理" >&2
}

echo "[6/6] 触发器最终校验（应保持保护开启，白名单放行）"
python scripts/validate_g7_triggers.py || true

echo "[DONE] G7-2025 汇聚重启流程完成。请查看验证报告与守卫日志确认。"

