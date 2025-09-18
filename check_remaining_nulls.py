#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
检查剩余的空值记录
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db_context

def check_remaining_nulls():
    """检查剩余的空值记录"""
    print("检查剩余的空值记录...")

    with get_db_context() as db:
        # 查看仍为空的记录
        null_records = db.execute(text("""
            SELECT batch_code, subject_name, dimension_code, dimension_name
            FROM batch_dimension_definition
            WHERE dimension_max_score IS NULL
            ORDER BY batch_code, subject_name, dimension_code
        """)).fetchall()

        print(f"仍为空的记录数: {len(null_records)}")
        for rec in null_records:
            print(f"  {rec.batch_code} | {rec.subject_name} | {rec.dimension_code} | {rec.dimension_name}")

            # 检查是否有对应的映射关系
            mapping_count = db.execute(text("""
                SELECT COUNT(*) as cnt
                FROM question_dimension_mapping
                WHERE batch_code = :batch
                  AND subject_name = :subject
                  AND dimension_code = :dimension
            """), {
                "batch": rec.batch_code,
                "subject": rec.subject_name,
                "dimension": rec.dimension_code
            }).scalar()

            print(f"    映射关系数: {mapping_count}")

            if mapping_count == 0:
                print(f"    原因: 没有对应的题目映射关系")

if __name__ == "__main__":
    check_remaining_nulls()