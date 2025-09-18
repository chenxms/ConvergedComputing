param(
  [Parameter(Mandatory = $true)] [string[]]$Batches,
  [string]$BaseUrl = "http://localhost:8000"
)

# 用法示例：
#   pwsh scripts/materialize_v12.ps1 -Batches G4-2025,G7-2025 -BaseUrl http://<host>:8000

function Invoke-Materialize {
  param(
    [string]$Batch
  )
  $url = "$BaseUrl/api/v12/batch/$Batch/materialize"
  Write-Host "[INFO] Materializing v1.2 subjects for batch $Batch ..." -ForegroundColor Cyan
  try {
    $resp = Invoke-RestMethod -Uri $url -Method Post -TimeoutSec 1800
    if ($resp.success -ne $true) {
      Write-Host "[ERROR] $Batch failed: $($resp.message)" -ForegroundColor Red
    } else {
      Write-Host ("[OK]  {0} => schools: {1}" -f $Batch, $resp.data.schools_materialized) -ForegroundColor Green
    }
  } catch {
    Write-Host "[ERROR] $Batch exception: $($_.Exception.Message)" -ForegroundColor Red
  }
}

if ($Batches.Count -eq 1 -and $Batches[0] -match ',') {
  # 兼容逗号分隔的单字符串输入
  $Batches = $Batches[0].Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }
}

foreach ($b in $Batches) {
  Invoke-Materialize -Batch $b
}
