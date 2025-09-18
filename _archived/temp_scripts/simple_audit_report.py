#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
简化数据审查报告 - 避免编码问题
"""

import sys
import os
import json
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db_context

def audit_regional_subject_full_score():
    """审查REGIONAL记录的subject_full_score字段"""
    print("=== 审查1: REGIONAL记录的subject_full_score字段 ===")

    with get_db_context() as db:
        regional_records = db.execute(text("""
            SELECT batch_code, statistics_data, updated_at
            FROM statistical_aggregations
            WHERE school_id = 'REGIONAL'
            ORDER BY batch_code, updated_at DESC
        """)).fetchall()

        print(f"发现 {len(regional_records)} 个REGIONAL记录")

        total_subjects = 0
        subjects_with_full_score = 0

        for record in regional_records:
            try:
                data = json.loads(record.statistics_data)
                batch_code = record.batch_code
                subjects = data.get('subjects', [])

                print(f"\n批次 {batch_code}: {len(subjects)} 个科目")

                for subject in subjects:
                    total_subjects += 1
                    subject_name = subject.get('subject_name', 'Unknown')
                    metrics = subject.get('metrics', {})

                    if 'subject_full_score' in metrics and metrics['subject_full_score'] is not None:
                        subjects_with_full_score += 1
                        print(f"  OK: {subject_name}: subject_full_score = {metrics['subject_full_score']}")
                    else:
                        print(f"  MISSING: {subject_name}: subject_full_score 缺失")

            except Exception as e:
                print(f"  ERROR: 解析批次 {record.batch_code} 失败: {str(e)}")

        completion_rate = subjects_with_full_score / total_subjects * 100 if total_subjects > 0 else 0
        print(f"\nsubject_full_score完整率: {subjects_with_full_score}/{total_subjects} ({completion_rate:.1f}%)")

        return subjects_with_full_score, total_subjects

def audit_questionnaire_dimensions():
    """审查问卷维度的avg与option_distribution"""
    print("\n=== 审查2: 问卷维度数据一致性 ===")

    with get_db_context() as db:
        questionnaire_records = db.execute(text("""
            SELECT batch_code, statistics_data
            FROM statistical_aggregations
            WHERE school_id = 'REGIONAL'
              AND statistics_data LIKE '%问卷%'
            LIMIT 2
        """)).fetchall()

        checked_dimensions = 0

        for record in questionnaire_records:
            try:
                data = json.loads(record.statistics_data)
                batch_code = record.batch_code

                print(f"\n批次 {batch_code}:")

                subjects = data.get('subjects', [])
                for subject in subjects:
                    subject_name = subject.get('subject_name', '')
                    if '问卷' in subject_name:
                        print(f"  问卷科目: {subject_name}")

                        dimensions = subject.get('dimensions', [])
                        for dim in dimensions[:2]:  # 检查前2个维度
                            checked_dimensions += 1
                            dim_code = dim.get('dimension_code', 'Unknown')
                            dim_avg = dim.get('avg')
                            option_dist = dim.get('option_distribution', {})

                            print(f"    维度 {dim_code}:")
                            print(f"      平均分: {dim_avg}")
                            print(f"      选项分布: {len(option_dist)} 个题目")

                            if len(option_dist) > 0:
                                sample_question = list(option_dist.keys())[0]
                                sample_dist = option_dist[sample_question]
                                print(f"      样本题目 {sample_question}: {sample_dist}")

            except Exception as e:
                print(f"  ERROR: 处理批次 {record.batch_code} 失败: {str(e)}")

        print(f"\n检查了 {checked_dimensions} 个问卷维度")
        return checked_dimensions

def audit_school_score_rates():
    """审查学校级记录的score_rate"""
    print("\n=== 审查3: 学校级记录的score_rate ===")

    with get_db_context() as db:
        school_records = db.execute(text("""
            SELECT batch_code, school_id, statistics_data
            FROM statistical_aggregations
            WHERE school_id != 'REGIONAL'
              AND school_id IS NOT NULL
            ORDER BY RAND()
            LIMIT 2
        """)).fetchall()

        total_dimensions = 0
        dimensions_with_score_rate = 0

        for record in school_records:
            try:
                data = json.loads(record.statistics_data)
                batch_code = record.batch_code
                school_id = record.school_id

                print(f"\n学校 {school_id} (批次 {batch_code}):")

                subjects = data.get('subjects', [])
                for subject in subjects:
                    subject_name = subject.get('subject_name', '')
                    dimensions = subject.get('dimensions', [])

                    for dim in dimensions[:2]:  # 检查前2个维度
                        total_dimensions += 1
                        dim_code = dim.get('dimension_code', 'Unknown')
                        score_rate = dim.get('score_rate')
                        avg = dim.get('avg')

                        if score_rate is not None:
                            dimensions_with_score_rate += 1
                            print(f"  OK: {subject_name}.{dim_code}: score_rate = {score_rate}")
                        else:
                            print(f"  MISSING: {subject_name}.{dim_code}: score_rate 缺失")

            except Exception as e:
                print(f"  ERROR: 处理学校 {record.school_id} 失败: {str(e)}")

        score_rate_completion = dimensions_with_score_rate / total_dimensions * 100 if total_dimensions > 0 else 0
        print(f"\nscore_rate完整率: {dimensions_with_score_rate}/{total_dimensions} ({score_rate_completion:.1f}%)")

        return dimensions_with_score_rate, total_dimensions

def audit_dimension_max_score():
    """审查维度满分回填"""
    print("\n=== 审查4: 维度满分回填质量 ===")

    with get_db_context() as db:
        # 统计回填情况
        stats = db.execute(text("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN dimension_max_score IS NOT NULL THEN 1 ELSE 0 END) as filled,
                SUM(CASE WHEN dimension_max_score IS NULL THEN 1 ELSE 0 END) as empty
            FROM batch_dimension_definition
        """)).fetchone()

        completion_rate = stats.filled / stats.total * 100 if stats.total > 0 else 0

        print(f"维度满分回填统计:")
        print(f"  总记录数: {stats.total}")
        print(f"  已回填数: {stats.filled}")
        print(f"  仍为空数: {stats.empty}")
        print(f"  完成率: {completion_rate:.1f}%")

        # 抽样检查合理性
        print(f"\n合理性抽样检查:")
        samples = db.execute(text("""
            SELECT
                bdd.batch_code,
                bdd.subject_name,
                bdd.dimension_code,
                bdd.dimension_max_score,
                sqc_total.total_score
            FROM batch_dimension_definition bdd
            LEFT JOIN (
                SELECT batch_code, subject_name, SUM(max_score) as total_score
                FROM subject_question_config
                GROUP BY batch_code, subject_name
            ) sqc_total
              ON sqc_total.batch_code = bdd.batch_code
             AND sqc_total.subject_name = bdd.subject_name
            WHERE bdd.dimension_max_score IS NOT NULL
            ORDER BY RAND()
            LIMIT 5
        """)).fetchall()

        anomalies = 0
        for sample in samples:
            dim_score = sample.dimension_max_score
            subject_total = sample.total_score or 0
            ratio = dim_score / subject_total if subject_total > 0 else 0

            status = "正常"
            if ratio > 1.2:
                status = "异常"
                anomalies += 1

            print(f"  {status}: {sample.batch_code}.{sample.subject_name}.{sample.dimension_code}")
            print(f"    维度满分: {dim_score}, 科目总分: {subject_total}, 比率: {ratio:.2f}")

        return stats.filled, stats.total, anomalies

def generate_final_report(subject_stats, questionnaire_count, score_rate_stats, dimension_stats):
    """生成最终审查报告"""
    print("\n" + "=" * 60)
    print("数据审查总结报告")
    print("=" * 60)

    subjects_filled, total_subjects = subject_stats
    score_rate_filled, total_score_rate = score_rate_stats
    dimension_filled, total_dimensions, anomalies = dimension_stats

    print(f"\n【审查结果】")
    print(f"1. REGIONAL记录subject_full_score完整率: {subjects_filled}/{total_subjects} ({subjects_filled/total_subjects*100:.1f}%)")
    print(f"2. 问卷维度数据检查: 检查了 {questionnaire_count} 个维度")
    print(f"3. 学校级score_rate完整率: {score_rate_filled}/{total_score_rate} ({score_rate_filled/total_score_rate*100:.1f}%)")
    print(f"4. 维度满分回填完整率: {dimension_filled}/{total_dimensions} ({dimension_filled/total_dimensions*100:.1f}%)")

    print(f"\n【关键发现】")
    if subjects_filled == total_subjects:
        print("- subject_full_score字段已完全填充")
    else:
        print(f"- subject_full_score字段缺失 {total_subjects - subjects_filled} 个")

    if score_rate_filled == total_score_rate:
        print("- 学校级维度score_rate字段完整")
    else:
        print(f"- 学校级维度score_rate字段缺失 {total_score_rate - score_rate_filled} 个")

    if dimension_filled / total_dimensions > 0.95:
        print("- 维度满分回填基本完成")
    else:
        print(f"- 维度满分回填仍有 {total_dimensions - dimension_filled} 个未完成")

    if anomalies > 0:
        print(f"- 发现 {anomalies} 个维度满分异常值")
    else:
        print("- 未发现维度满分异常值")

    print(f"\n【总体评价】")
    if (subjects_filled/total_subjects > 0.9 and
        score_rate_filled/total_score_rate > 0.9 and
        dimension_filled/total_dimensions > 0.95 and
        anomalies == 0):
        print("数据质量良好，可以进行后续处理")
    else:
        print("数据存在一些问题，建议进一步检查")

def main():
    """主函数"""
    print("数据质量审查报告")
    print("注意: 仅做审查，不修改数据")

    try:
        subject_stats = audit_regional_subject_full_score()
        questionnaire_count = audit_questionnaire_dimensions()
        score_rate_stats = audit_school_score_rates()
        dimension_stats = audit_dimension_max_score()

        generate_final_report(subject_stats, questionnaire_count, score_rate_stats, dimension_stats)

    except Exception as e:
        print(f"\n审查过程出错: {str(e)}")
        raise

if __name__ == "__main__":
    main()