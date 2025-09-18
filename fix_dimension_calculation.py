#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
修复维度满分计算错误
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db_context

def analyze_english_dimension_error():
    """分析英语科目维度计算错误"""
    print("=== 英语科目维度满分计算错误分析 ===\n")

    with get_db_context() as db:
        # 查看英语科目异常维度
        print("1. 英语科目维度计算结果:")
        english_dims = db.execute(text("""
            SELECT dimension_code, dimension_name, dimension_max_score
            FROM batch_dimension_definition
            WHERE batch_code = 'G7-2025' AND subject_name = '英语'
            ORDER BY dimension_max_score DESC
        """)).fetchall()

        for dim in english_dims:
            status = "异常" if dim.dimension_max_score and dim.dimension_max_score > 150 else "正常"
            print(f"   {status}: {dim.dimension_code} | {dim.dimension_name} | {dim.dimension_max_score}")

        # 分析权重分配问题
        print(f"\n2. 权重分配分析 - 语言能力维度(YY-yynl):")
        yynl_details = db.execute(text("""
            SELECT
                qdm.question_id,
                sqc.max_score,
                COALESCE(qdm.weight, 1.0) as weight,
                sqc.max_score * COALESCE(qdm.weight, 1.0) as weighted_score
            FROM question_dimension_mapping qdm
            INNER JOIN subject_question_config sqc
              ON sqc.batch_code = qdm.batch_code
             AND sqc.subject_name = qdm.subject_name
             AND sqc.question_id = qdm.question_id
            WHERE qdm.batch_code = 'G7-2025'
              AND qdm.subject_name = '英语'
              AND qdm.dimension_code = 'YY-yynl'
            ORDER BY weighted_score DESC
        """)).fetchall()

        total_weighted = 0
        for detail in yynl_details:
            total_weighted += detail.weighted_score
            print(f"   题目{detail.question_id}: {detail.max_score}分 × {detail.weight} = {detail.weighted_score}分")

        print(f"   总计: {total_weighted}分 (应该≤150分)")

        # 检查权重设计是否合理
        print(f"\n3. 检查权重设计合理性:")
        weight_check = db.execute(text("""
            SELECT
                question_id,
                COUNT(*) as dimension_count,
                SUM(COALESCE(weight, 1.0)) as total_weight_for_question
            FROM question_dimension_mapping
            WHERE batch_code = 'G7-2025' AND subject_name = '英语'
            GROUP BY question_id
            HAVING SUM(COALESCE(weight, 1.0)) > 1.1  -- 权重总和应该≤1
            ORDER BY total_weight_for_question DESC
            LIMIT 10
        """)).fetchall()

        if weight_check:
            print("   发现权重分配问题的题目:")
            for q in weight_check:
                print(f"   题目{q.question_id}: 被分配给{q.dimension_count}个维度，总权重{q.total_weight_for_question}")

                # 显示具体分配
                allocations = db.execute(text("""
                    SELECT dimension_code, COALESCE(weight, 1.0) as weight
                    FROM question_dimension_mapping
                    WHERE batch_code = 'G7-2025' AND subject_name = '英语' AND question_id = :qid
                    ORDER BY weight DESC
                """), {"qid": q.question_id}).fetchall()

                for alloc in allocations:
                    print(f"     -> {alloc.dimension_code}: {alloc.weight}")

def fix_dimension_calculation():
    """修复维度满分计算"""
    print("\n=== 修复维度满分计算 ===\n")

    with get_db_context() as db:
        print("修复方案：确保题目权重在维度间合理分配")

        # 方案1：如果一个题目分配给多个维度，权重总和应该等于1
        # 方案2：如果权重设计有问题，需要重新设计权重分配逻辑

        # 首先检查当前的权重分配逻辑是否正确
        print("1. 分析当前权重分配逻辑...")

        # 查看一些具体例子
        sample_questions = db.execute(text("""
            SELECT DISTINCT question_id
            FROM question_dimension_mapping
            WHERE batch_code = 'G7-2025' AND subject_name = '英语'
            LIMIT 5
        """)).fetchall()

        for sq in sample_questions:
            qid = sq.question_id
            print(f"\n   题目{qid}的维度分配:")

            mappings = db.execute(text("""
                SELECT
                    qdm.dimension_code,
                    COALESCE(qdm.weight, 1.0) as weight,
                    sqc.max_score
                FROM question_dimension_mapping qdm
                INNER JOIN subject_question_config sqc
                  ON sqc.batch_code = qdm.batch_code
                 AND sqc.subject_name = qdm.subject_name
                 AND sqc.question_id = qdm.question_id
                WHERE qdm.batch_code = 'G7-2025'
                  AND qdm.subject_name = '英语'
                  AND qdm.question_id = :qid
            """), {"qid": qid}).fetchall()

            total_weight = sum(m.weight for m in mappings)
            max_score = mappings[0].max_score if mappings else 0

            print(f"     题目满分: {max_score}")
            print(f"     权重分配:")
            for m in mappings:
                contribution = max_score * m.weight
                print(f"       {m.dimension_code}: {m.weight} (贡献{contribution}分)")
            print(f"     权重总和: {total_weight}")

        # 基于分析结果，提供修复建议
        print(f"\n2. 修复建议:")
        print("   问题诊断：题目权重分配给多个维度时，权重可能超过1.0")
        print("   解决方案：重新计算维度满分时，考虑权重归一化")

def apply_fix():
    """应用修复"""
    print("\n=== 应用修复 ===\n")

    with get_db_context() as db:
        print("正在重新计算维度满分（使用归一化权重）...")

        # 修复后的SQL：对权重进行归一化处理
        # 对于每个题目，如果权重总和>1，则按比例缩放权重
        fix_sql = text("""
            UPDATE batch_dimension_definition bdd
            INNER JOIN (
                SELECT
                    qdm.batch_code,
                    qdm.subject_name,
                    qdm.dimension_code,
                    SUM(
                        sqc.max_score *
                        (COALESCE(qdm.weight, 1.0) / question_weight_totals.total_weight)
                    ) AS normalized_max_score
                FROM question_dimension_mapping qdm
                INNER JOIN subject_question_config sqc
                  ON sqc.batch_code = qdm.batch_code
                 AND sqc.subject_name = qdm.subject_name
                 AND sqc.question_id = qdm.question_id
                INNER JOIN (
                    -- 计算每个题目的权重总和
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

        print(f"✓ 已更新 {affected_rows} 条维度记录")

def verify_fix():
    """验证修复结果"""
    print("\n=== 验证修复结果 ===\n")

    with get_db_context() as db:
        # 检查英语科目是否还有超过150分的维度
        over_limit = db.execute(text("""
            SELECT dimension_code, dimension_name, dimension_max_score
            FROM batch_dimension_definition
            WHERE batch_code = 'G7-2025'
              AND subject_name = '英语'
              AND dimension_max_score > 150
        """)).fetchall()

        if over_limit:
            print("❌ 仍有超出限制的维度:")
            for dim in over_limit:
                print(f"   {dim.dimension_code}: {dim.dimension_max_score}")
        else:
            print("✓ 英语科目所有维度满分都在合理范围内")

        # 显示修复后的英语科目维度满分
        english_dims = db.execute(text("""
            SELECT dimension_code, dimension_name, dimension_max_score
            FROM batch_dimension_definition
            WHERE batch_code = 'G7-2025' AND subject_name = '英语'
            ORDER BY dimension_max_score DESC
        """)).fetchall()

        print("\n修复后的英语科目维度满分:")
        for dim in english_dims:
            print(f"   {dim.dimension_code} | {dim.dimension_name} | {dim.dimension_max_score}")

if __name__ == "__main__":
    analyze_english_dimension_error()
    fix_dimension_calculation()
    apply_fix()
    verify_fix()