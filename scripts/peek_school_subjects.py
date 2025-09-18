#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
打印指定学校的 subjects 的 grade_distribution 形态检查：
  - 确认 metrics 内无 grade_distribution
  - 确认顶层 grade_distribution 仅包含 counts 与 percentages

用法：
  python scripts/peek_school_subjects.py G4-2025 5074
"""
from __future__ import annotations
import sys, os, json
from typing import Any, Dict
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db


def main(argv):
    if len(argv) < 3:
        print("Usage: python scripts/peek_school_subjects.py <BATCH> <SCHOOL_ID>")
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
        data = json.loads(data)
    for s in data.get('subjects', []):
        name = s.get('subject_name')
        metrics = s.get('metrics') or {}
        top_gd = s.get('grade_distribution')
        print(f"\nSubject: {name}")
        print(f"  metrics_has_grade_distribution: {'grade_distribution' in metrics}")
        if isinstance(top_gd, dict):
            print(f"  top_grade_distribution_keys: {sorted(list(top_gd.keys()))}")
            if 'counts' in top_gd:
                print(f"  counts_keys: {sorted(list((top_gd.get('counts') or {}).keys()))}")
            if 'percentages' in top_gd:
                print(f"  percentages_keys: {sorted(list((top_gd.get('percentages') or {}).keys()))}")
        else:
            print(f"  top_grade_distribution_type: {type(top_gd).__name__}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

