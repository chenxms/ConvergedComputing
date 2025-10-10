# G7-2025 批次汇聚重启一键脚本（PowerShell）
# 模式：白名单-only（不开维护模式），放行 chenlei，阻断非白名单

Param(
  [string]$DbHost = $env:G7_DB_HOST
    ? $env:G7_DB_HOST : '117.72.14.166',
  [int]$DbPort = $env:G7_DB_PORT
    ? [int]$env:G7_DB_PORT : 23506,
  [string]$DbName = $env:G7_DB_NAME
    ? $env:G7_DB_NAME : 'appraisal_test',
  [Parameter(Mandatory=$false)][string]$Password
)

if (-not $Password) {
  if ($env:G7_DB_PASSWORD) { $Password = $env:G7_DB_PASSWORD }
}

if (-not $Password) {
  Write-Error "请通过参数 -Password 或环境变量 G7_DB_PASSWORD 提供 chenlei 密码"
  exit 1
}

$env:DATABASE_URL = "mysql+pymysql://chenlei:$Password@$DbHost:$DbPort/$DbName?charset=utf8mb4"
$env:PRODUCTION_DATABASE_URL = $env:DATABASE_URL

Write-Host "[1/6] 安装增强守卫并添加白名单(chenlei%)，关闭维护模式"
python scripts/enhanced_g7_guard.py install
python scripts/enhanced_g7_guard.py add-whitelist "chenlei%"
python scripts/enhanced_g7_guard.py disable-maintenance

Write-Host "[2/6] 触发器快速校验"
python scripts/validate_g7_triggers.py --quick

Write-Host "[3/6] 预检查与备份（含干净版SQL）"
python g7_precheck_suite.py --with-validation

Write-Host "[4/6] 启动新流水线（包装器）"
python run_g7_pipeline_wrapper.py --batch G7-2025 --env production

Write-Host "[5/6] 数据与API验证"
python validate_g7_data.py --compare-backup

Write-Host "[6/6] 触发器最终校验（应保持保护开启，白名单放行）"
python scripts/validate_g7_triggers.py

Write-Host "[DONE] G7-2025 汇聚重启流程完成。请查看验证报告与守卫日志确认。"

