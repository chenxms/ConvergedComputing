#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
调试英语科目维度满分计算错误
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db_context

def debug_english_dimension_error():
    """调试英语科目维度满分计算错误"""
    print("=== 英语科目维度满分计算错误调试 ===\n")

    with get_db_context() as db:
        # 1. 查看英语科目的维度定义和计算结果
        print("1. 英语科目维度定义和计算结果:")
        english_dims = db.execute(text("""
            SELECT
                dimension_code,
                dimension_name,
                dimension_max_score
            FROM batch_dimension_definition
            WHERE batch_code = 'G7-2025' AND subject_name = '英语'
            ORDER BY dimension_max_score DESC
        """)).fetchall()

        for dim in english_dims:
            print(f"   - {dim.dimension_code} | {dim.dimension_name} | 满分: {dim.dimension_max_score}")

        # 2. 找出超过150分的维度
        over_150_dims = db.execute(text("""
            SELECT dimension_code, dimension_name, dimension_max_score
            FROM batch_dimension_definition
            WHERE batch_code = 'G7-2025'
              AND subject_name = '英语'
              AND dimension_max_score > 150
        """)).fetchall()

        if over_150_dims:
            print(f"\n2. 超过150分的异常维度 ({len(over_150_dims)}个):")
            for dim in over_150_dims:
                print(f"   ❌ {dim.dimension_code} | {dim.dimension_name} | 满分: {dim.dimension_max_score}")

                # 详细分析这个维度的构成
                print(f"   详细构成分析:")
                details = db.execute(text("""
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
                      AND qdm.dimension_code = :dim_code
                    ORDER BY qdm.question_id
                """), {"dim_code": dim.dimension_code}).fetchall()

                total_weighted = 0
                for detail in details:
                    total_weighted += detail.weighted_score
                    print(f"     题目{detail.question_id}: {detail.max_score}分 × {detail.weight}权重 = {detail.weighted_score}分")

                print(f"     合计: {total_weighted}分")
                print()

        # 3. 检查英语科目的题目配置
        print(f"3. 英语科目题目配置总览:")
        english_questions = db.execute(text("""
            SELECT
                COUNT(*) as question_count,
                SUM(max_score) as total_max_score,
                MIN(max_score) as min_score,
                MAX(max_score) as max_score,
                AVG(max_score) as avg_score
            FROM subject_question_config
            WHERE batch_code = 'G7-2025' AND subject_name = '英语'
        """)).fetchone()

        print(f"   - 题目数量: {english_questions.question_count}")
        print(f"   - 总满分: {english_questions.total_max_score}")
        print(f"   - 单题分值范围: {english_questions.min_score} ~ {english_questions.max_score}")
        print(f"   - 平均分值: {english_questions.avg_score:.2f}")

        # 4. 检查题目维度映射的权重情况
        print(f"\n4. 英语科目维度映射权重分析:")
        weight_analysis = db.execute(text("""
            SELECT
                dimension_code,
                COUNT(*) as question_count,
                SUM(COALESCE(weight, 1.0)) as total_weight,
                MIN(COALESCE(weight, 1.0)) as min_weight,
                MAX(COALESCE(weight, 1.0)) as max_weight,
                AVG(COALESCE(weight, 1.0)) as avg_weight
            FROM question_dimension_mapping
            WHERE batch_code = 'G7-2025' AND subject_name = '英语'
            GROUP BY dimension_code
            ORDER BY total_weight DESC
        """)).fetchall()

        for weight in weight_analysis:
            print(f"   - {weight.dimension_code}: {weight.question_count}题, 权重合计{weight.total_weight}, 范围{weight.min_weight}~{weight.max_weight}")

        # 5. 检查是否存在重复映射
        print(f"\n5. 检查重复映射:")
        duplicates = db.execute(text("""
            SELECT
                question_id,
                dimension_code,
                COUNT(*) as mapping_count
            FROM question_dimension_mapping
            WHERE batch_code = 'G7-2025' AND subject_name = '英语'
            GROUP BY question_id, dimension_code
            HAVING COUNT(*) > 1
        """)).fetchall()

        if duplicates:
            print(f"   发现 {len(duplicates)} 个重复映射:")
            for dup in duplicates:
                print(f"   ❌ 题目{dup.question_id} -> {dup.dimension_code}: {dup.mapping_count}次映射")
        else:
            print("   ✓ 未发现重复映射")

        # 6. 验证数据一致性：一个题目的所有维度权重分配
        print(f"\n6. 题目权重分配验证:")
        question_weight_check = db.execute(text("""
            SELECT
                question_id,
                SUM(COALESCE(weight, 1.0)) as total_weight,
                COUNT(*) as dimension_count
            FROM question_dimension_mapping
            WHERE batch_code = 'G7-2025' AND subject_name = '英语'
            GROUP BY question_id
            HAVING SUM(COALESCE(weight, 1.0)) > 1.5  -- 允许一些浮点误差
            ORDER BY total_weight DESC
            LIMIT 10
        """)).fetchall()

        if question_weight_check:
            print(f"   发现权重分配异常的题目:")
            for q in question_weight_check:
                print(f"   ❌ 题目{q.question_id}: 总权重{q.total_weight} (分配给{q.dimension_count}个维度)")

                # 显示这个题目的具体权重分配
                q_details = db.execute(text("""
                    SELECT dimension_code, COALESCE(weight, 1.0) as weight
                    FROM question_dimension_mapping
                    WHERE batch_code = 'G7-2025' AND subject_name = '英语' AND question_id = :qid
                    ORDER BY weight DESC
                """), {"qid": q.question_id}).fetchall()

                for qd in q_details:
                    print(f"     {qd.dimension_code}: {qd.weight}")
        else:
            print("   ✓ 题目权重分配正常")

        print(f"\n=== 调试完成 ===")

if __name__ == "__main__":
    debug_english_dimension_error()