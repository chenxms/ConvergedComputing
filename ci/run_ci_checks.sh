#!/usr/bin/env bash
set -euo pipefail

echo "[qa-v12] Installing requirements..."
python -m pip install --upgrade pip >/dev/null
python -m pip install -r ci/requirements.txt >/dev/null

shopt -s nullglob
SNAPS=(docs/qa/snapshots/*.json)
if (( ${#SNAPS[@]} == 0 )); then
  echo "[qa-v12] No snapshots found in docs/qa/snapshots. Skipping (non-blocking)."
  exit 0
fi

echo "[qa-v12] Validating ${#SNAPS[@]} snapshot(s)..."
python ci/validate_aggregation_v12.py --schema docs/qa/aggregation_v12_schema.json --files "${SNAPS[@]}"
echo "[qa-v12] Validation complete."

