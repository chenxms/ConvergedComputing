#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rewrite statistical_aggregations.statistics_data to v1.2 subjects schema

- Adds regional subjects with school_rankings (exam + questionnaire)
- Adds school subjects with region_rank/total_schools
- Adds school dimension ranks per subject
- Adds questionnaire option distributions (dimension/question)
"""

from __future__ import annotations
import sys
import os
import json
from datetime import datetime, timezone
from typing import List

from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db
from app.services.subjects_builder import SubjectsBuilder


def rewrite_batch(batch_code: str) -> None:
    sb = SubjectsBuilder()
    with next(get_db()) as db:
        # 尽量提高单连接的超时容忍度，避免大JSON写入超时
        try:
            db.execute(text("SET SESSION net_write_timeout=600"))
            db.execute(text("SET SESSION net_read_timeout=600"))
            db.execute(text("SET SESSION wait_timeout=600"))
        except Exception:
            pass
        # Regional record
        reg_row = db.execute(
            text("SELECT id FROM statistical_aggregations WHERE batch_code=:b AND aggregation_level='REGIONAL' LIMIT 1"),
            {"b": batch_code},
        ).fetchone()

        regional_subjects = sb.build_regional_subjects(batch_code)
        regional_json = {
            "schema_version": "v1.2",
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "subjects": regional_subjects,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        # 使用插入+冲突更新，避免依赖现有id，并尽量缩短事务
        db.execute(
            text(
                "INSERT INTO statistical_aggregations (batch_code, aggregation_level, school_id, school_name, statistics_data, data_version, calculation_status, created_at, updated_at)"
                " VALUES (:b, 'REGIONAL', NULL, NULL, :d, 'v1.2', 'COMPLETED', NOW(), NOW())"
                " ON DUPLICATE KEY UPDATE statistics_data=VALUES(statistics_data), calculation_status='COMPLETED', updated_at=NOW()"
            ),
            {"b": batch_code, "d": json.dumps(regional_json, ensure_ascii=False)},
        )

        # School records
        schools = db.execute(
            text("SELECT DISTINCT school_code FROM student_cleaned_scores WHERE batch_code=:b ORDER BY school_code"),
            {"b": batch_code},
        ).fetchall()

        for (school_code,) in schools:
            school_subjects = sb.build_school_subjects(batch_code, school_code)
            school_json = {
                "schema_version": "v1.2",
                "batch_code": batch_code,
                "aggregation_level": "SCHOOL",
                "school_code": school_code,
                "subjects": school_subjects,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            db.execute(
                text(
                    "INSERT INTO statistical_aggregations (batch_code, aggregation_level, school_id, statistics_data, data_version, calculation_status, created_at, updated_at)"
                    " VALUES (:b, 'SCHOOL', :s, :d, 'v1.2', 'COMPLETED', NOW(), NOW())"
                    " ON DUPLICATE KEY UPDATE statistics_data=VALUES(statistics_data), updated_at=NOW()"
                ),
                {"b": batch_code, "s": school_code, "d": json.dumps(school_json, ensure_ascii=False)},
            )
        db.commit()


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: python scripts/rewrite_subjects_v12.py <BATCH> [<BATCH> ...]")
        return 1
    for b in argv[1:]:
        print(f"Rewriting subjects for {b} ...")
        rewrite_batch(b)
        print(f"Done: {b}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
