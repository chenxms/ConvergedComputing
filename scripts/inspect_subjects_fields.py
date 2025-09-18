#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
快速检查指定学校subjects是否包含所需字段：
- 科目层：avg、stddev、difficulty、discrimination、p10、p50、p90、region_rank
- 维度层：avg、score_rate、rank

用法：
  python scripts/inspect_subjects_fields.py G4-2025 5074
"""

from __future__ import annotations
import sys
import json
from typing import Any, Dict, List
from sqlalchemy import text
import os

# 确保可以导入应用模块
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db


def main(argv: List[str]) -> int:
    if len(argv) < 3:
        print("Usage: python scripts/inspect_subjects_fields.py <BATCH> <SCHOOL_ID>")
        return 1

    batch, school = argv[1], argv[2]
    db = next(get_db())
    row = db.execute(
        text("SELECT statistics_data FROM statistical_aggregations WHERE batch_code=:b AND aggregation_level='SCHOOL' AND school_id=:s"),
        {"b": batch, "s": school},
    ).fetchone()
    if not row:
        print("NOT_FOUND")
        return 2
    data = row[0]
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            pass
    subjects = data.get("subjects", []) if isinstance(data, dict) else []

    checks: List[Dict[str, Any]] = []
    for s in subjects:
        metrics = s.get("metrics") or {}
        dims = s.get("dimensions", []) or []
        subj_ok = all(k in metrics for k in ("avg","stddev","difficulty")) and \
                  ("region_rank" in s) and \
                  all(k in s for k in ("p10","p50","p90")) and \
                  ("discrimination" in s)
        dims_ok = True
        for d in dims:
            if not all(k in d for k in ("avg","score_rate","rank")):
                dims_ok = False
                break
        checks.append({
            "subject_name": s.get("subject_name"),
            "type": s.get("type"),
            "subject_fields_ok": subj_ok,
            "has_percentiles": all(k in s for k in ("p10","p50","p90")),
            "has_discrimination": ("discrimination" in s),
            "dimensions_fields_ok": dims_ok,
        })

    print(json.dumps({
        "batch": batch,
        "school": school,
        "subjects_count": len(subjects),
        "subjects": checks,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
