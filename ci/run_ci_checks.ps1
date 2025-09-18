Param(
  [string]$Schema = "docs/qa/aggregation_v12_schema.json"
)

$ErrorActionPreference = 'Stop'
Write-Host "[qa-v12] Installing requirements..."
python -m pip install --upgrade pip | Out-Null
python -m pip install -r ci/requirements.txt | Out-Null

$snapDir = "docs/qa/snapshots"
if (!(Test-Path -LiteralPath $snapDir)) {
  Write-Host "[qa-v12] No snapshots directory. Skipping (non-blocking)."
  exit 0
}

$snaps = Get-ChildItem -LiteralPath $snapDir -Filter *.json -File -ErrorAction SilentlyContinue
if (-not $snaps -or $snaps.Count -eq 0) {
  Write-Host "[qa-v12] No snapshots found in $snapDir. Skipping (non-blocking)."
  exit 0
}

Write-Host "[qa-v12] Validating $($snaps.Count) snapshot(s)..."
python ci/validate_aggregation_v12.py --schema $Schema --files $($snaps | ForEach-Object { $_.FullName })
Write-Host "[qa-v12] Validation complete."

