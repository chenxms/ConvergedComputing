#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查指定批次的物化结果（statistical_aggregations），对比区域与学校级科目结构是否符合文档约定。

用法：
  python scripts/inspect_materialized_diff.py G7-2025 G4-2025
"""
from __future__ import annotations
import sys
from typing import Any, Dict, List
from sqlalchemy import text
import os, os.path
import sys as _sys
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _ROOT not in _sys.path:
    _sys.path.insert(0, _ROOT)
from app.database.connection import get_db_context
import json as _json


def summarize_subject_struct(subject: Dict[str, Any]) -> Dict[str, Any]:
    metrics = subject.get('metrics') or {}
    out = {
        'subject_name': subject.get('subject_name'),
        'type': subject.get('type'),
        'top_level': {
            'p10': 'p10' in subject,
            'p50': 'p50' in subject,
            'p90': 'p90' in subject,
            'discrimination': 'discrimination' in subject,
            'rank': 'rank' in subject,
            'grade_distribution': 'grade_distribution' in subject,
        },
        'metrics': {
            'avg': 'avg' in metrics,
            'stddev': 'stddev' in metrics,
            'difficulty': 'difficulty' in metrics,
            'score_rate': 'score_rate' in metrics,
            'percentiles': ('percentiles' in metrics and isinstance(metrics.get('percentiles'), dict)),
            'discrimination': 'discrimination' in metrics,
            'rank': 'rank' in metrics,
            'rate_excellent': 'rate_excellent' in metrics,
            'rate_good': 'rate_good' in metrics,
            'rate_pass': 'rate_pass' in metrics,
            'rate_fail': 'rate_fail' in metrics,
        },
        'dimensions_has_rank': any(isinstance(d, dict) and 'rank' in d for d in subject.get('dimensions') or []),
        'dimensions_has_questions': any(isinstance(d, dict) and 'questions' in d for d in subject.get('dimensions') or []),
    }
    return out


def fetch_one_school_row(batch: str) -> Dict[str, Any] | None:
    with get_db_context() as db:
        row = db.execute(text(
            """
            SELECT school_id, school_name, JSON_EXTRACT(statistics_data, '$')
            FROM statistical_aggregations
            WHERE batch_code=:b AND aggregation_level='SCHOOL'
            ORDER BY school_id
            LIMIT 1
            """
        ), {'b': batch}).fetchone()
        if not row:
            return None
        school_id, school_name, data = row[0], row[1], row[2]
        payload = data if isinstance(data, dict) else None
        if payload is None:
            # 回退：再取原字段（部分驱动不支持 JSON_EXTRACT('$') 返回 dict）
            row2 = db.execute(text(
                """
                SELECT school_id, school_name, statistics_data
                FROM statistical_aggregations
                WHERE batch_code=:b AND aggregation_level='SCHOOL' AND school_id=:s
                LIMIT 1
                """
            ), {'b': batch, 's': school_id}).fetchone()
            raw = row2[2] if row2 else None
            try:
                payload = _json.loads(raw) if isinstance(raw, (str, bytes)) else raw
            except Exception:
                payload = None
        return {'school_id': school_id, 'school_name': school_name, 'data': payload}


def fetch_regional_row(batch: str) -> Dict[str, Any] | None:
    with get_db_context() as db:
        row = db.execute(text(
            """
            SELECT JSON_EXTRACT(statistics_data, '$')
            FROM statistical_aggregations
            WHERE batch_code=:b AND aggregation_level='REGIONAL'
            LIMIT 1
            """
        ), {'b': batch}).fetchone()
        if not row:
            return None
        data = row[0]
        payload = data if isinstance(data, dict) else None
        if payload is None:
            row2 = db.execute(text(
                """
                SELECT statistics_data
                FROM statistical_aggregations
                WHERE batch_code=:b AND aggregation_level='REGIONAL'
                LIMIT 1
                """
            ), {'b': batch}).fetchone()
            raw = row2[0] if row2 else None
            try:
                payload = _json.loads(raw) if isinstance(raw, (str, bytes)) else raw
            except Exception:
                payload = None
        return {'data': payload}


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("用法: python scripts/inspect_materialized_diff.py <BATCH> [<BATCH> ...]")
        return 1
    for batch in argv[1:]:
        print(f"\n=== 批次 {batch} ===")
        reg = fetch_regional_row(batch)
        if not reg or not isinstance(reg.get('data'), dict):
            print("[REGIONAL] 未找到或结构非JSON")
        else:
            subjects = reg['data'].get('subjects') or []
            print(f"[REGIONAL] subjects: {len(subjects)}")
            if subjects:
                s0 = summarize_subject_struct(subjects[0])
                print("[REGIONAL] 示例科目结构:", s0)
        sch = fetch_one_school_row(batch)
        if not sch or not isinstance(sch.get('data'), dict):
            print("[SCHOOL] 未找到或结构非JSON")
        else:
            subjects = sch['data'].get('subjects') or []
            print(f"[SCHOOL] 示例学校 {sch['school_id']} {sch['school_name']} -> subjects: {len(subjects)}")
            if subjects:
                s0 = summarize_subject_struct(subjects[0])
                print("[SCHOOL] 示例科目结构:", s0)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
