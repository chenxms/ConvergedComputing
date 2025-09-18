#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
计算所有批次所有维度的满分并回填到batch_dimension_definition.dimension_max_score字段
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db_context

def step1_analyze_data_scope():
    """第一步：统计待处理数据量"""
    print("=== 第一步：统计待处理数据量 ===\n")

    with get_db_context() as db:
        # 1. 查看所有批次列表
        print("1. 所有批次列表:")
        batches = db.execute(text("""
            SELECT DISTINCT batch_code
            FROM batch_dimension_definition
            ORDER BY batch_code
        """)).fetchall()

        batch_codes = [b.batch_code for b in batches]
        print(f"   总批次数: {len(batch_codes)}")
        for i, batch_code in enumerate(batch_codes, 1):
            print(f"   {i:2d}. {batch_code}")

        # 2. 统计batch_dimension_definition表记录
        print(f"\n2. batch_dimension_definition表统计:")
        total_records = db.execute(text("""
            SELECT COUNT(*) as total
            FROM batch_dimension_definition
        """)).scalar()

        null_records = db.execute(text("""
            SELECT COUNT(*) as nulls
            FROM batch_dimension_definition
            WHERE dimension_max_score IS NULL
        """)).scalar()

        filled_records = total_records - null_records

        print(f"   总记录数: {total_records}")
        print(f"   已填充记录: {filled_records}")
        print(f"   待计算记录: {null_records}")
        print(f"   完成率: {filled_records/total_records*100:.1f}%")

        # 3. 按批次统计待处理记录
        print(f"\n3. 按批次统计待处理记录:")
        batch_stats = db.execute(text("""
            SELECT
                batch_code,
                COUNT(*) as total_dims,
                SUM(CASE WHEN dimension_max_score IS NULL THEN 1 ELSE 0 END) as null_dims,
                SUM(CASE WHEN dimension_max_score IS NOT NULL THEN 1 ELSE 0 END) as filled_dims
            FROM batch_dimension_definition
            GROUP BY batch_code
            ORDER BY null_dims DESC, batch_code
        """)).fetchall()

        for stat in batch_stats:
            if stat.null_dims > 0:
                print(f"   {stat.batch_code}: {stat.null_dims}/{stat.total_dims} 待计算")

        return batch_codes, null_records

def step2_build_calculation_logic():
    """第二步：建立正确的计算逻辑"""
    print("\n=== 第二步：建立正确的计算逻辑 ===\n")

    with get_db_context() as db:
        # 1. 分析题目-维度映射关系表结构
        print("1. 分析题目-维度映射关系:")
        mapping_sample = db.execute(text("""
            SELECT batch_code, subject_name, question_id, dimension_code, weight
            FROM question_dimension_mapping
            LIMIT 5
        """)).fetchall()

        print("   question_dimension_mapping表样本:")
        for m in mapping_sample:
            print(f"   {m.batch_code} | {m.subject_name} | {m.question_id} | {m.dimension_code} | {m.weight}")

        # 2. 分析题目配置表结构
        print(f"\n2. 分析题目配置关系:")
        config_sample = db.execute(text("""
            SELECT batch_code, subject_name, question_id, max_score
            FROM subject_question_config
            LIMIT 5
        """)).fetchall()

        print("   subject_question_config表样本:")
        for c in config_sample:
            print(f"   {c.batch_code} | {c.subject_name} | {c.question_id} | {c.max_score}")

        # 3. 检查权重字段的数据分布
        print(f"\n3. 权重字段数据分布:")
        weight_stats = db.execute(text("""
            SELECT
                COUNT(*) as total_mappings,
                SUM(CASE WHEN weight IS NULL THEN 1 ELSE 0 END) as null_weights,
                MIN(weight) as min_weight,
                MAX(weight) as max_weight,
                AVG(weight) as avg_weight
            FROM question_dimension_mapping
        """)).fetchone()

        print(f"   总映射记录: {weight_stats.total_mappings}")
        print(f"   权重为NULL的记录: {weight_stats.null_weights}")
        print(f"   权重范围: {weight_stats.min_weight} ~ {weight_stats.max_weight}")
        print(f"   平均权重: {weight_stats.avg_weight:.3f}")

        # 4. 制定计算公式
        print(f"\n4. 计算公式:")
        print("   每个维度满分 = SUM(题目满分 × COALESCE(权重, 1.0))")
        print("   GROUP BY: batch_code, subject_name, dimension_code")

def step3_execute_calculation():
    """第三步：执行批量计算"""
    print("\n=== 第三步：执行批量计算 ===\n")

    with get_db_context() as db:
        print("正在执行维度满分计算...")

        # 执行批量更新SQL
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

        print(f"✓ 已更新 {affected_rows} 条维度记录")

        return affected_rows

def step4_simple_verification():
    """第四步：简单验证"""
    print("\n=== 第四步：简单验证 ===\n")

    with get_db_context() as db:
        # 1. 统计更新后的情况
        print("1. 更新后统计:")
        after_stats = db.execute(text("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN dimension_max_score IS NULL THEN 1 ELSE 0 END) as still_null,
                SUM(CASE WHEN dimension_max_score IS NOT NULL THEN 1 ELSE 0 END) as now_filled
            FROM batch_dimension_definition
        """)).fetchone()

        print(f"   总记录数: {after_stats.total}")
        print(f"   已填充记录: {after_stats.now_filled}")
        print(f"   仍为空记录: {after_stats.still_null}")
        print(f"   完成率: {after_stats.now_filled/after_stats.total*100:.1f}%")

        # 2. 抽样验证几个维度
        print(f"\n2. 抽样验证 (随机选择5个维度):")
        samples = db.execute(text("""
            SELECT batch_code, subject_name, dimension_code, dimension_max_score
            FROM batch_dimension_definition
            WHERE dimension_max_score IS NOT NULL
            ORDER BY RAND()
            LIMIT 5
        """)).fetchall()

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
            status = "✓" if match else "✗"
            print(f"   {status} {sample.batch_code}.{sample.subject_name}.{sample.dimension_code}: 存储={sample.dimension_max_score}, 计算={manual_calc}")

        # 3. 检查异常值
        print(f"\n3. 异常值检查:")
        outliers = db.execute(text("""
            SELECT batch_code, subject_name, dimension_code, dimension_max_score
            FROM batch_dimension_definition
            WHERE dimension_max_score > 2000  -- 假设超过2000分的维度是异常的
            ORDER BY dimension_max_score DESC
            LIMIT 10
        """)).fetchall()

        if outliers:
            print("   发现异常高分维度:")
            for outlier in outliers:
                print(f"   ⚠ {outlier.batch_code}.{outlier.subject_name}.{outlier.dimension_code}: {outlier.dimension_max_score}")
        else:
            print("   ✓ 未发现异常高分维度")

def main():
    """主函数"""
    print("开始执行全批次维度满分计算")
    print("=" * 50)

    try:
        # 第一步：统计待处理数据量
        batch_codes, null_records = step1_analyze_data_scope()

        if null_records == 0:
            print("\n所有维度满分已经计算完成，无需处理。")
            return

        # 第二步：建立正确的计算逻辑
        step2_build_calculation_logic()

        # 第三步：执行批量计算
        affected_rows = step3_execute_calculation()

        # 第四步：简单验证
        step4_simple_verification()

        print(f"\n✅ 维度满分计算完成！")
        print(f"   处理批次数: {len(batch_codes)}")
        print(f"   更新记录数: {affected_rows}")

    except Exception as e:
        print(f"\n❌ 执行过程中发生错误: {str(e)}")
        raise

if __name__ == "__main__":
    main()