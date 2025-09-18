Param(
  [string]$Batch = "G7-2025"
)

$ErrorActionPreference = "Stop"

Write-Host "[1/5] 构建镜像 app/subjects ..."
docker compose build app subjects

Write-Host "[2/5] 重启服务 app/subjects ..."
docker compose up -d app subjects

Write-Host "[3/5] 执行区域级去重与规范化 ..."
try {
  docker compose exec app python scripts/fix_regional_duplicates.py --batch $Batch | Write-Host
} catch {
  Write-Warning "去重脚本执行出现警告：$($_.Exception.Message)；将继续"
}

Write-Host "[4/5] 验证 60 秒内是否无增长 ..." -ForegroundColor Green
docker compose exec app python scripts/verify_no_growth.py --batch $Batch

Write-Host "[5/5] 完成。若需恢复写入，请在 docker-compose.yml 调整 DISABLE_WRITES_FOR_BATCHES 后执行：" -ForegroundColor Green
Write-Host "        docker compose up -d app subjects"

