#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
快速全批次汇聚脚本：对 school_master_data 中的所有批次执行 subjects v1.2 物化

逻辑要点：
- 每个批次在重跑前先清除该批次历史记录（statistical_aggregations）
- 区域级 school_id='REGION'、school_name='区域汇总'，避免 NULL 引发重复
- 学校级仅使用主数据 ACTIVE 学校，丢弃孤儿学校
- total_schools / total_students 字段完整填充

用法：
  python fast_materialize_all_batches_v12.py    # 自动发现批次
  python fast_materialize_all_batches_v12.py G4-2025 G7-2025 G8-2025   # 指定批次
"""

from typing import List, Tuple
from sqlalchemy import text

from app.database.connection import get_db
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus
from app.services.subjects_builder import SubjectsBuilder
from app.utils.precision import round2_json


def _materialize_one(batch_code: str) -> Tuple[int, int]:
    db = next(get_db())
    try:
        repo = StatisticalAggregationRepository(db)
        builder = SubjectsBuilder()

        # 清理历史
        db.execute(text("DELETE FROM statistical_aggregations WHERE batch_code=:b"), {"b": batch_code})
        db.commit()

        # 主数据学校数
        total_schools_active = db.execute(
            text("SELECT COUNT(*) FROM school_master_data WHERE batch_code=:b AND status='ACTIVE'"),
            {"b": batch_code},
        ).scalar() or 0

        # 区域级
        regional_subjects = builder.build_regional_subjects(batch_code)
        regional_payload = {
            'schema_version': 'v1.2',
            'batch_code': batch_code,
            'aggregation_level': 'REGIONAL',
            'subjects': regional_subjects,
        }
        repo.upsert_statistics({
            'batch_code': batch_code,
            'aggregation_level': DBAggregationLevel.REGIONAL,
            'school_id': 'REGION',
            'school_name': '区域汇总',
            'statistics_data': round2_json(regional_payload),
            'calculation_status': CalculationStatus.COMPLETED,
            'total_schools': total_schools_active,
        })

        # 学校级（按主数据）
        rows = db.execute(
            text("SELECT school_id FROM school_master_data WHERE batch_code=:b AND status='ACTIVE' ORDER BY school_id"),
            {"b": batch_code},
        ).fetchall()
        total = len(rows)
        ok = 0
        for (school_code,) in rows:
            school_subjects = builder.build_school_subjects(batch_code, school_code)
            school_payload = {
                'schema_version': 'v1.2',
                'batch_code': batch_code,
                'aggregation_level': 'SCHOOL',
                'school_code': school_code,
                'subjects': school_subjects,
            }
            total_students = db.execute(text(
                """
                SELECT COUNT(DISTINCT scs.student_id)
                  FROM student_cleaned_scores scs
                  JOIN school_master_data smd
                    ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
                   AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
                   AND smd.status='ACTIVE'
                 WHERE scs.batch_code=:b AND scs.school_code=:s
                   AND scs.subject_type IN ('exam','questionnaire')
                """
            ), {"b": batch_code, "s": school_code}).scalar() or 0
            repo.upsert_statistics({
                'batch_code': batch_code,
                'aggregation_level': DBAggregationLevel.SCHOOL,
                'school_id': school_code,
                'school_name': None,
                'statistics_data': round2_json(school_payload),
                'calculation_status': CalculationStatus.COMPLETED,
                'total_students': total_students,
                'total_schools': total_schools_active,
            })
            ok += 1
            if ok % 20 == 0:
                print(f"[{batch_code}] 学校级已生成 {ok}/{total} ...")

        return ok, total
    finally:
        db.close()


def fast_materialize_all(batches: List[str] = None) -> dict:
    db = next(get_db())
    try:
        if not batches:
            rows = db.execute(text("SELECT DISTINCT batch_code FROM school_master_data WHERE status='ACTIVE' ORDER BY batch_code")).fetchall()
            batches = [r[0] for r in rows]
    finally:
        db.close()

    summary = {"batches": [], "total_schools": 0}
    for b in batches:
        print(f"开始汇聚批次: {b}")
        ok, total = _materialize_one(b)
        summary["batches"].append({"batch_code": b, "schools_generated": ok, "schools_expected": total, "regional": 1})
        summary["total_schools"] += ok
    summary["regional_total"] = len(batches)
    return summary


if __name__ == '__main__':
    import sys, json
    bs = sys.argv[1:] if len(sys.argv) > 1 else None
    out = fast_materialize_all(bs)
    print(json.dumps(out, ensure_ascii=False, indent=2))

