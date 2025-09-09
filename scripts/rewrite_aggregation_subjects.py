#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
重写 statistical_aggregations.statistics_data �?subjects 结构并补齐排�?分布（方�?v1.2�?
用途：
- 针对已经入库但结�?字段不符合契约的批次（例如仍�?academic/non_academic），
  生成并写回统一�?subjects 结构，包含：
  - 区域�?school_rankings（考试+问卷�?  - 学校�?region_rank/total_schools
  - 学校层维度排�?dimensions[].rank（按学校维度均分�?  - 问卷维度与题�?option_distribution
- 同时写入 schema_version = v1.2

仅覆�?statistics_data 字段，保留其他列（total_students/total_schools 等）�?"""

from __future__ import annotations
import sys
import os
import json
from datetime import datetime
from typing import List

from sqlalchemy import text

# 确保可导�?app �?CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.database.connection import get_db
from app.services.subjects_builder import SubjectsBuilder


def rewrite_batch(batch_code: str) -> None:
    sb = SubjectsBuilder()
    with next(get_db()) as db:
        # 区域层：若存在记录则更新，否则创�?        regional = db.execute(
            text(
                """
                SELECT id, statistics_data FROM statistical_aggregations
                WHERE batch_code=:batch AND aggregation_level='REGIONAL'
                LIMIT 1
                """
            ),
            {"batch": batch_code},
        ).fetchone()

        regional_subjects = sb.build_regional_subjects(batch_code)
        regional_data = {
            "schema_version": "v1.2",
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "subjects": regional_subjects,
            "updated_at": datetime.utcnow().isoformat(),
        }
        if regional:
            db.execute(
                text(
                    """
                    UPDATE statistical_aggregations
                    SET statistics_data=:data, updated_at=NOW()
                    WHERE id=:id
                    """
                ),
                {"data": json.dumps(regional_data, ensure_ascii=False), "id": regional[0]},
            )
        else:
            db.execute(
                text(
                    """
                    INSERT INTO statistical_aggregations
                    (batch_code, aggregation_level, statistics_data, data_version, calculation_status, created_at, updated_at)
                    VALUES (:batch, 'REGIONAL', :data, 'v1.2', 'COMPLETED', NOW(), NOW())
                    """
                ),
                {"batch": batch_code, "data": json.dumps(regional_data, ensure_ascii=False)},
            )

        # 学校层：遍历批次内学校，逐一重写 subjects
        schools = db.execute(
            text(
                """
                SELECT DISTINCT school_code
                FROM student_cleaned_scores
                WHERE batch_code=:batch
                ORDER BY school_code
                """
            ),
            {"batch": batch_code},
        ).fetchall()
        for (school_code,) in schools:
            subjects = sb.build_school_subjects(batch_code, school_code)
            school_json = {
                "schema_version": "v1.2",
                "batch_code": batch_code,
                "aggregation_level": "SCHOOL",
                "school_code": school_code,
                "subjects": subjects,
                "updated_at": datetime.utcnow().isoformat(),
            }
            db.execute(
                text(
                    """
                    INSERT INTO statistical_aggregations
                    (batch_code, aggregation_level, school_id, statistics_data, data_version, calculation_status, created_at, updated_at)
                    VALUES (:batch, 'SCHOOL', :school, :data, 'COMPLETED', NOW(), NOW())
                    ON DUPLICATE KEY UPDATE statistics_data=VALUES(statistics_data), updated_at=NOW()
                    """
                ),
                {"batch": batch_code, "school": school_code, "data": json.dumps(school_json, ensure_ascii=False)},
            )

        db.commit()


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("用法: python scripts/rewrite_aggregation_subjects.py <BATCH_CODE> [<BATCH_CODE2> ...]")
        return 1
    for batch in argv[1:]:
        print(f"重写批次 {batch} �?subjects ...")
        rewrite_batch(batch)
        print(f"完成: {batch}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
