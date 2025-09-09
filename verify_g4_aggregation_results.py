#!/usr/bin/env python3
"""
验证G4-2025批次汇聚结果的准确性
"""

import json
from app.database.connection import get_db_context
from sqlalchemy import text

def verify_aggregation_results():
    """验证汇聚结果"""
    
    try:
        with get_db_context() as session:
            print("=== 批次G4-2025汇聚结果验证 ===")
            
            # 1. 检查区域级汇聚数据
            print("\n1. 区域级汇聚数据检查:")
            result = session.execute(text("""
                SELECT aggregation_level, COUNT(*) as record_count, 
                       SUM(total_students) as total_students,
                       SUM(calculation_duration) as total_duration
                FROM statistical_aggregations 
                WHERE batch_code = 'G4-2025' AND aggregation_level = 'REGIONAL'
                GROUP BY aggregation_level
            """))
            regional_stats = result.fetchone()
            
            if regional_stats:
                print(f"  区域级记录数: {regional_stats.record_count}")
                print(f"  汇聚学生总数: {regional_stats.total_students}")
                print(f"  计算总用时: {regional_stats.total_duration:.2f}秒")
            else:
                print("  未找到区域级汇聚数据")
            
            # 2. 检查学校级汇聚数据
            print("\n2. 学校级汇聚数据检查:")
            result = session.execute(text("""
                SELECT COUNT(*) as school_count,
                       COUNT(DISTINCT school_id) as unique_schools,
                       SUM(total_students) as total_students,
                       AVG(calculation_duration) as avg_duration,
                       MIN(calculation_duration) as min_duration,
                       MAX(calculation_duration) as max_duration
                FROM statistical_aggregations 
                WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            """))
            school_stats = result.fetchone()
            
            if school_stats:
                print(f"  学校级记录数: {school_stats.school_count}")
                print(f"  唯一学校数: {school_stats.unique_schools}")
                print(f"  汇聚学生总数: {school_stats.total_students}")
                print(f"  平均计算用时: {school_stats.avg_duration:.3f}秒")
                print(f"  最短计算用时: {school_stats.min_duration:.3f}秒")
                print(f"  最长计算用时: {school_stats.max_duration:.3f}秒")
            
            # 3. 检查计算状态分布
            print("\n3. 计算状态分布:")
            result = session.execute(text("""
                SELECT calculation_status, COUNT(*) as count
                FROM statistical_aggregations 
                WHERE batch_code = 'G4-2025'
                GROUP BY calculation_status
                ORDER BY count DESC
            """))
            status_dist = result.fetchall()
            
            for status in status_dist:
                print(f"  {status.calculation_status}: {status.count} 条记录")
            
            # 4. 抽样检查统计数据内容
            print("\n4. 统计数据内容抽样检查:")
            result = session.execute(text("""
                SELECT school_id, school_name, total_students, 
                       JSON_EXTRACT(statistics_data, '$.basic_stats.mean') as avg_score,
                       JSON_EXTRACT(statistics_data, '$.basic_stats.count') as data_count
                FROM statistical_aggregations 
                WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
                AND total_students > 200
                ORDER BY total_students DESC
                LIMIT 5
            """))
            sample_schools = result.fetchall()
            
            print("  大规模学校样本:")
            for school in sample_schools:
                print(f"    学校{school.school_id} ({school.school_name})")
                print(f"      学生数: {school.total_students}")
                print(f"      平均分: {school.avg_score}")
                print(f"      数据记录数: {school.data_count}")
            
            # 5. 检查科目维度统计
            print("\n5. 科目维度统计检查:")
            result = session.execute(text("""
                SELECT 
                    JSON_EXTRACT(statistics_data, '$.subjects') as subjects_data
                FROM statistical_aggregations 
                WHERE batch_code = 'G4-2025' AND aggregation_level = 'REGIONAL'
                LIMIT 1
            """))
            subjects_result = result.fetchone()
            
            if subjects_result and subjects_result.subjects_data:
                try:
                    subjects_data = json.loads(subjects_result.subjects_data)
                    print(f"  区域级包含科目数: {len(subjects_data)}")
                    for subject_id, subject_data in subjects_data.items():
                        subject_name = subject_data.get('subject_name', '未知')
                        student_count = subject_data.get('basic_stats', {}).get('count', 0)
                        avg_score = subject_data.get('basic_stats', {}).get('mean', 0)
                        print(f"    {subject_id} ({subject_name}): {student_count}名学生, 平均分{avg_score:.2f}")
                except json.JSONDecodeError as e:
                    print(f"  解析科目数据失败: {e}")
            
            # 6. 数据一致性验证
            print("\n6. 数据一致性验证:")
            result = session.execute(text("""
                SELECT COUNT(DISTINCT student_id) as unique_students
                FROM student_score_detail 
                WHERE batch_code = 'G4-2025'
            """))
            original_student_count = result.fetchone().unique_students
            
            print(f"  原始数据学生总数: {original_student_count}")
            if regional_stats:
                print(f"  区域级汇聚学生数: {regional_stats.total_students}")
                if regional_stats.total_students == original_student_count:
                    print("  [OK] 数据一致性检查通过")
                else:
                    print("  [ERROR] 数据一致性检查失败 - 汇聚数据与原始数据不一致")
            
            print("\n=== 汇聚结果验证完成 ===")
            
    except Exception as e:
        print(f"验证过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_aggregation_results()