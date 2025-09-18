#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
验证维度最大分数修复结果
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db_context

def validate_dimension_max_score_fix():
    """验证维度最大分数修复结果"""
    print("=== 维度最大分数修复验证报告 ===\n")

    with get_db_context() as db:
        # 总体统计
        result = db.execute(text("""
            SELECT
                COUNT(*) as total_records,
                SUM(CASE WHEN dimension_max_score IS NOT NULL THEN 1 ELSE 0 END) as filled_records,
                SUM(CASE WHEN dimension_max_score IS NULL THEN 1 ELSE 0 END) as empty_records,
                ROUND(SUM(CASE WHEN dimension_max_score IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fill_rate_percent
            FROM batch_dimension_definition
            WHERE batch_code = 'G7-2025'
        """)).fetchone()

        print(f"1. 总体统计:")
        print(f"   - 总记录数: {result.total_records}")
        print(f"   - 已填充记录: {result.filled_records}")
        print(f"   - 空记录数: {result.empty_records}")
        print(f"   - 填充率: {result.fill_rate_percent}%\n")

        # 按科目统计
        subjects = db.execute(text("""
            SELECT
                subject_name,
                COUNT(*) as total_dims,
                SUM(CASE WHEN dimension_max_score IS NOT NULL THEN 1 ELSE 0 END) as filled_dims,
                ROUND(SUM(CASE WHEN dimension_max_score IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as fill_rate
            FROM batch_dimension_definition
            WHERE batch_code = 'G7-2025'
            GROUP BY subject_name
            ORDER BY fill_rate DESC, subject_name
        """)).fetchall()

        print("2. 按科目统计:")
        for subj in subjects:
            print(f"   - {subj.subject_name}: {subj.filled_dims}/{subj.total_dims} ({subj.fill_rate}%)")

        # 查看未填充的记录
        empty_records = db.execute(text("""
            SELECT subject_name, dimension_code, dimension_name
            FROM batch_dimension_definition
            WHERE batch_code = 'G7-2025' AND dimension_max_score IS NULL
            ORDER BY subject_name, dimension_code
        """)).fetchall()

        if empty_records:
            print(f"\n3. 未填充记录详情 ({len(empty_records)}条):")
            for rec in empty_records:
                print(f"   - {rec.subject_name} | {rec.dimension_code} | {rec.dimension_name}")

                # 检查是否有对应的映射关系
                mapping_count = db.execute(text("""
                    SELECT COUNT(*) as cnt
                    FROM question_dimension_mapping
                    WHERE batch_code = 'G7-2025'
                      AND subject_name = :subj
                      AND dimension_code = :dim
                """), {"subj": rec.subject_name, "dim": rec.dimension_code}).scalar()

                print(f"     (映射关系数: {mapping_count})")

        # 数据验证 - 随机检查几个维度的计算准确性
        print(f"\n4. 数据验证 (随机抽样):")
        sample_records = db.execute(text("""
            SELECT subject_name, dimension_code, dimension_max_score
            FROM batch_dimension_definition
            WHERE batch_code = 'G7-2025' AND dimension_max_score IS NOT NULL
            ORDER BY RAND()
            LIMIT 3
        """)).fetchall()

        for rec in sample_records:
            # 手工计算该维度最大分数
            manual_calc = db.execute(text("""
                SELECT SUM(sqc.max_score * COALESCE(qdm.weight, 1.0)) AS manual_max_score
                FROM question_dimension_mapping qdm
                INNER JOIN subject_question_config sqc
                  ON sqc.batch_code = qdm.batch_code
                 AND sqc.subject_name = qdm.subject_name
                 AND sqc.question_id = qdm.question_id
                WHERE qdm.batch_code = 'G7-2025'
                  AND qdm.subject_name = :subj
                  AND qdm.dimension_code = :dim
            """), {"subj": rec.subject_name, "dim": rec.dimension_code}).scalar()

            match_status = "✓" if abs(float(rec.dimension_max_score) - float(manual_calc or 0)) < 0.01 else "✗"
            print(f"   {match_status} {rec.subject_name}.{rec.dimension_code}: 存储={rec.dimension_max_score}, 计算={manual_calc}")

        print(f"\n=== 验证完成 ===")

if __name__ == "__main__":
    validate_dimension_max_score_fix()