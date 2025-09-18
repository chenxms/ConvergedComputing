#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
简单的维度满分计算脚本 - 只执行计算和验证
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db_context

def execute_calculation():
    """执行维度满分计算"""
    print("正在执行维度满分计算...")

    with get_db_context() as db:
        # 查看计算前的状态
        before_stats = db.execute(text("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN dimension_max_score IS NULL THEN 1 ELSE 0 END) as null_count
            FROM batch_dimension_definition
        """)).fetchone()

        print(f"计算前: 总记录{before_stats.total}, 待计算{before_stats.null_count}")

        # 执行批量计算
        update_sql = text("""
            UPDATE batch_dimension_definition bdd
            INNER JOIN (
                SELECT
                    qdm.batch_code,
                    qdm.subject_name,
                    qdm.dimension_code,
                    SUM(sqc.max_score * COALESCE(qdm.weight, 1.0)) AS calculated_max_score
                FROM question_dimension_mapping qdm
                INNER JOIN subject_question_config sqc
                  ON sqc.batch_code = qdm.batch_code
                 AND sqc.subject_name = qdm.subject_name
                 AND sqc.question_id = qdm.question_id
                GROUP BY qdm.batch_code, qdm.subject_name, qdm.dimension_code
            ) calc
              ON calc.batch_code = bdd.batch_code
             AND calc.subject_name = bdd.subject_name
             AND calc.dimension_code = bdd.dimension_code
            SET bdd.dimension_max_score = calc.calculated_max_score
        """)

        result = db.execute(update_sql)
        affected_rows = result.rowcount
        db.commit()

        print(f"已更新 {affected_rows} 条维度记录")

        # 查看计算后的状态
        after_stats = db.execute(text("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN dimension_max_score IS NULL THEN 1 ELSE 0 END) as null_count,
                SUM(CASE WHEN dimension_max_score IS NOT NULL THEN 1 ELSE 0 END) as filled_count
            FROM batch_dimension_definition
        """)).fetchone()

        print(f"计算后: 总记录{after_stats.total}, 已填充{after_stats.filled_count}, 仍为空{after_stats.null_count}")
        print(f"完成率: {after_stats.filled_count/after_stats.total*100:.1f}%")

        return affected_rows

def verify_results():
    """验证计算结果"""
    print("\n开始验证计算结果...")

    with get_db_context() as db:
        # 抽样验证3个维度
        samples = db.execute(text("""
            SELECT batch_code, subject_name, dimension_code, dimension_max_score
            FROM batch_dimension_definition
            WHERE dimension_max_score IS NOT NULL
            ORDER BY RAND()
            LIMIT 3
        """)).fetchall()

        print("抽样验证:")
        for sample in samples:
            # 手工验证计算
            manual_calc = db.execute(text("""
                SELECT SUM(sqc.max_score * COALESCE(qdm.weight, 1.0)) AS manual_score
                FROM question_dimension_mapping qdm
                INNER JOIN subject_question_config sqc
                  ON sqc.batch_code = qdm.batch_code
                 AND sqc.subject_name = qdm.subject_name
                 AND sqc.question_id = qdm.question_id
                WHERE qdm.batch_code = :batch
                  AND qdm.subject_name = :subject
                  AND qdm.dimension_code = :dimension
            """), {
                "batch": sample.batch_code,
                "subject": sample.subject_name,
                "dimension": sample.dimension_code
            }).scalar()

            match = abs(float(sample.dimension_max_score) - float(manual_calc or 0)) < 0.01
            status = "OK" if match else "ERROR"
            print(f"  {status}: {sample.batch_code}.{sample.subject_name}.{sample.dimension_code}")
            print(f"    存储值: {sample.dimension_max_score}, 计算值: {manual_calc}")

def main():
    """主函数"""
    try:
        affected_rows = execute_calculation()
        verify_results()
        print(f"\n维度满分计算完成! 更新了 {affected_rows} 条记录")
    except Exception as e:
        print(f"执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main()