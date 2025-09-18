#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
修复权重归一化问题
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db_context

def fix_weight_normalization():
    """修复权重归一化问题"""
    print("=== 修复权重归一化问题 ===\n")

    with get_db_context() as db:
        print("正在重新计算维度满分（使用权重归一化）...")

        # 使用权重归一化的SQL
        # 对于每个题目，如果权重总和>1，则按比例缩放所有维度的权重
        fix_sql = text("""
            UPDATE batch_dimension_definition bdd
            INNER JOIN (
                SELECT
                    qdm.batch_code,
                    qdm.subject_name,
                    qdm.dimension_code,
                    SUM(
                        sqc.max_score *
                        (COALESCE(qdm.weight, 1.0) / GREATEST(question_weight_totals.total_weight, 1.0))
                    ) AS normalized_max_score
                FROM question_dimension_mapping qdm
                INNER JOIN subject_question_config sqc
                  ON sqc.batch_code = qdm.batch_code
                 AND sqc.subject_name = qdm.subject_name
                 AND sqc.question_id = qdm.question_id
                INNER JOIN (
                    SELECT
                        batch_code,
                        subject_name,
                        question_id,
                        SUM(COALESCE(weight, 1.0)) as total_weight
                    FROM question_dimension_mapping
                    WHERE batch_code = 'G7-2025'
                    GROUP BY batch_code, subject_name, question_id
                ) question_weight_totals
                  ON question_weight_totals.batch_code = qdm.batch_code
                 AND question_weight_totals.subject_name = qdm.subject_name
                 AND question_weight_totals.question_id = qdm.question_id
                WHERE qdm.batch_code = 'G7-2025'
                GROUP BY qdm.batch_code, qdm.subject_name, qdm.dimension_code
            ) normalized_calc
              ON normalized_calc.batch_code = bdd.batch_code
             AND normalized_calc.subject_name = bdd.subject_name
             AND normalized_calc.dimension_code = bdd.dimension_code
            SET bdd.dimension_max_score = normalized_calc.normalized_max_score
            WHERE bdd.batch_code = 'G7-2025'
        """)

        result = db.execute(fix_sql)
        affected_rows = result.rowcount
        db.commit()

        print(f"已更新 {affected_rows} 条维度记录")

def verify_english_fix():
    """验证英语科目修复结果"""
    print("\n=== 验证英语科目修复结果 ===\n")

    with get_db_context() as db:
        # 检查英语科目维度满分
        english_dims = db.execute(text("""
            SELECT dimension_code, dimension_name, dimension_max_score
            FROM batch_dimension_definition
            WHERE batch_code = 'G7-2025' AND subject_name = '英语'
            ORDER BY dimension_max_score DESC
        """)).fetchall()

        print("修复后的英语科目维度满分:")
        total_dimension_score = 0
        for dim in english_dims:
            total_dimension_score += dim.dimension_max_score or 0
            status = "正常" if (dim.dimension_max_score or 0) <= 150 else "异常"
            print(f"   {status}: {dim.dimension_code} | {dim.dimension_name} | {dim.dimension_max_score:.2f}")

        print(f"\n维度满分总和: {total_dimension_score:.2f}")

        # 验证英语科目总分
        english_total = db.execute(text("""
            SELECT SUM(max_score) as total_score
            FROM subject_question_config
            WHERE batch_code = 'G7-2025' AND subject_name = '英语'
        """)).scalar()

        print(f"英语科目实际总分: {english_total}")

        # 检查是否还有超过科目总分的维度
        over_limit = [dim for dim in english_dims if (dim.dimension_max_score or 0) > english_total]
        if over_limit:
            print(f"仍有超出科目总分的维度: {len(over_limit)}个")
            for dim in over_limit:
                print(f"   异常: {dim.dimension_code}: {dim.dimension_max_score}")
        else:
            print("所有维度满分都在合理范围内")

def check_other_subjects():
    """检查其他科目是否也有类似问题"""
    print("\n=== 检查其他科目维度满分 ===\n")

    with get_db_context() as db:
        # 查看所有科目的维度满分情况
        subjects_check = db.execute(text("""
            SELECT
                bdd.subject_name,
                SUM(sqc.max_score) as subject_total_score,
                MAX(bdd.dimension_max_score) as max_dimension_score,
                COUNT(bdd.dimension_code) as dimension_count
            FROM batch_dimension_definition bdd
            LEFT JOIN subject_question_config sqc
              ON sqc.batch_code = bdd.batch_code
             AND sqc.subject_name = bdd.subject_name
            WHERE bdd.batch_code = 'G7-2025'
            GROUP BY bdd.subject_name
            ORDER BY (MAX(bdd.dimension_max_score) / SUM(sqc.max_score)) DESC
        """)).fetchall()

        print("各科目维度满分检查:")
        for subj in subjects_check:
            max_dim_score = subj.max_dimension_score or 0
            total_score = subj.subject_total_score or 1
            ratio = max_dim_score / total_score
            status = "异常" if ratio > 1.0 else "正常"
            print(f"   {status}: {subj.subject_name} | 最大维度分: {max_dim_score:.2f} | 科目总分: {total_score} | 比率: {ratio:.2f}")

if __name__ == "__main__":
    fix_weight_normalization()
    verify_english_fix()
    check_other_subjects()