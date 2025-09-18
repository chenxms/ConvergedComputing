#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
为汇聚相关表创建高价值索引（可重复执行，已存在则跳过）。

用法：
  python scripts/create_aggregation_indexes.py
"""

import sys
import os
from typing import List, Tuple
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db


def _index_exists(db, table: str, index: str) -> bool:
    sql = text(
        """
        SELECT 1
          FROM information_schema.STATISTICS
         WHERE TABLE_SCHEMA = DATABASE()
           AND TABLE_NAME = :t
           AND INDEX_NAME = :i
        LIMIT 1
        """
    )
    return db.execute(sql, {"t": table, "i": index}).fetchone() is not None


def _create_index(db, ddl: str, table: str, index: str) -> Tuple[str, bool, str]:
    try:
        if _index_exists(db, table, index):
            return index, False, "exists"
        db.execute(text(ddl))
        db.commit()
        return index, True, "created"
    except Exception as e:
        return index, False, f"error: {e}"


def main() -> int:
    with next(get_db()) as db:
        tasks: List[Tuple[str, str, str]] = [
            (
                "student_cleaned_scores",
                "idx_scs_batch_school_subject",
                "CREATE INDEX idx_scs_batch_school_subject ON student_cleaned_scores(batch_code, school_code, subject_name)",
            ),
            (
                "student_cleaned_scores",
                "idx_scs_batch_subject",
                "CREATE INDEX idx_scs_batch_subject ON student_cleaned_scores(batch_code, subject_name)",
            ),
            (
                "school_master_data",
                "idx_smd_batch_status",
                "CREATE INDEX idx_smd_batch_status ON school_master_data(batch_code, status)",
            ),
            (
                "question_dimension_mapping",
                "idx_qdm_batch_question",
                "CREATE INDEX idx_qdm_batch_question ON question_dimension_mapping(batch_code, question_id)",
            ),
            (
                "questionnaire_question_scores",
                "idx_qqs_batch_subject_school",
                "CREATE INDEX idx_qqs_batch_subject_school ON questionnaire_question_scores(batch_code, subject_name, school_id)",
            ),
            (
                "statistical_aggregations",
                "uk_stat_agg_batch_level_school",
                "CREATE UNIQUE INDEX uk_stat_agg_batch_level_school ON statistical_aggregations(batch_code, aggregation_level, school_id)",
            ),
        ]

        print("=== Creating indexes (idempotent) ===")
        created = 0
        skipped = 0
        failed = 0
        for table, idx, ddl in tasks:
            name, ok, msg = _create_index(db, ddl, table, idx)
            if ok:
                created += 1
                print(f"[OK] {table}.{name}: {msg}")
            else:
                if msg == "exists":
                    skipped += 1
                    print(f"[SKIP] {table}.{name}: exists")
                else:
                    failed += 1
                    print(f"[ERR] {table}.{name}: {msg}")
        print(f"Summary: created={created}, skipped={skipped}, failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

