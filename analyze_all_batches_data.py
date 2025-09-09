#!/usr/bin/env python3
"""
分析数据库中所有批次的数据结构和问卷量表类型
"""

from app.database.connection import get_db_context
from sqlalchemy import text
import json

def analyze_all_batches():
    """分析所有批次的数据"""
    
    try:
        with get_db_context() as session:
            print("=== 数据库批次分析报告 ===\n")
            
            # 1. 获取所有批次
            print("1. 所有批次列表:")
            result = session.execute(text("""
                SELECT DISTINCT batch_code, COUNT(DISTINCT student_id) as student_count,
                       COUNT(DISTINCT subject_id) as subject_count,
                       COUNT(*) as record_count
                FROM student_score_detail
                GROUP BY batch_code
                ORDER BY batch_code
            """))
            batches = result.fetchall()
            
            all_batches = []
            for batch in batches:
                print(f"  批次 {batch.batch_code}: {batch.student_count}名学生, "
                      f"{batch.subject_count}个科目, {batch.record_count}条记录")
                all_batches.append(batch.batch_code)
            
            # 2. 分析清洗表数据
            print("\n2. 清洗表数据分析:")
            result = session.execute(text("""
                SELECT batch_code, COUNT(*) as cleaned_count,
                       COUNT(DISTINCT student_id) as student_count,
                       COUNT(DISTINCT subject_id) as subject_count
                FROM student_cleaned_scores
                GROUP BY batch_code
                ORDER BY batch_code
            """))
            cleaned_data = result.fetchall()
            
            for data in cleaned_data:
                print(f"  批次 {data.batch_code}: {data.cleaned_count}条清洗记录, "
                      f"{data.student_count}名学生, {data.subject_count}个科目")
            
            # 3. 分析问卷量表类型
            print("\n3. 问卷量表类型分析:")
            result = session.execute(text("""
                SELECT batch_code, 
                       instrument_type,
                       scale_level,
                       COUNT(DISTINCT dimension_code) as dimension_count,
                       COUNT(DISTINCT question_id) as question_count,
                       COUNT(DISTINCT student_id) as student_count,
                       GROUP_CONCAT(DISTINCT option_label) as option_labels
                FROM questionnaire_question_scores
                GROUP BY batch_code, instrument_type, scale_level
                ORDER BY batch_code, scale_level
            """))
            questionnaire_data = result.fetchall()
            
            current_batch = None
            for data in questionnaire_data:
                if current_batch != data.batch_code:
                    current_batch = data.batch_code
                    print(f"\n  批次 {data.batch_code}:")
                
                print(f"    量表类型: {data.instrument_type}")
                print(f"    量表等级: {data.scale_level}级")
                print(f"    维度数: {data.dimension_count}")
                print(f"    题目数: {data.question_count}")
                print(f"    学生数: {data.student_count}")
                if data.option_labels:
                    print(f"    选项标签: {data.option_labels}")
            
            # 4. 分析维度结构
            print("\n4. 批次维度分布:")
            for batch_code in all_batches[:3]:  # 只分析前3个批次作为示例
                result = session.execute(text("""
                    SELECT dimension_code, dimension_name,
                           COUNT(DISTINCT question_id) as question_count,
                           MIN(option_level) as min_level,
                           MAX(option_level) as max_level
                    FROM questionnaire_question_scores
                    WHERE batch_code = :batch_code
                    GROUP BY dimension_code, dimension_name
                    ORDER BY dimension_code
                    LIMIT 5
                """), {"batch_code": batch_code})
                dimensions = result.fetchall()
                
                if dimensions:
                    print(f"\n  批次 {batch_code} 维度示例:")
                    for dim in dimensions:
                        print(f"    {dim.dimension_code} ({dim.dimension_name}): "
                              f"{dim.question_count}题, 选项等级{dim.min_level}-{dim.max_level}")
            
            # 5. 检查题目选项分布
            print("\n5. 选项分布示例（批次G7-2025）:")
            result = session.execute(text("""
                SELECT dimension_code, question_id, option_label, 
                       COUNT(*) as response_count,
                       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY dimension_code, question_id), 2) as percentage
                FROM questionnaire_question_scores
                WHERE batch_code = 'G7-2025'
                  AND dimension_code IN (SELECT DISTINCT dimension_code FROM questionnaire_question_scores WHERE batch_code = 'G7-2025' LIMIT 1)
                GROUP BY dimension_code, question_id, option_label
                ORDER BY dimension_code, question_id, option_label
                LIMIT 20
            """))
            option_dist = result.fetchall()
            
            current_question = None
            for opt in option_dist:
                if current_question != f"{opt.dimension_code}-{opt.question_id}":
                    current_question = f"{opt.dimension_code}-{opt.question_id}"
                    print(f"\n  维度{opt.dimension_code} 题目{opt.question_id}:")
                print(f"    {opt.option_label}: {opt.response_count}人 ({opt.percentage}%)")
            
            # 6. 统计汇总
            print("\n6. 数据统计汇总:")
            result = session.execute(text("""
                SELECT 
                    (SELECT COUNT(DISTINCT batch_code) FROM student_score_detail) as total_batches,
                    (SELECT COUNT(DISTINCT batch_code) FROM student_cleaned_scores) as cleaned_batches,
                    (SELECT COUNT(DISTINCT batch_code) FROM questionnaire_question_scores) as questionnaire_batches,
                    (SELECT COUNT(DISTINCT batch_code) FROM statistical_aggregations) as aggregated_batches
            """))
            summary = result.fetchone()
            
            print(f"  总批次数: {summary.total_batches}")
            print(f"  已清洗批次数: {summary.cleaned_batches}")
            print(f"  含问卷批次数: {summary.questionnaire_batches}")
            print(f"  已汇聚批次数: {summary.aggregated_batches}")
            
            print("\n=== 分析完成 ===")
            
    except Exception as e:
        print(f"分析过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_all_batches()