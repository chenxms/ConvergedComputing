#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查学校ID映射问题 - 对比statistical_aggregations和school_statistics_summary
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import json

def check_school_id_mapping():
    """检查学校ID映射问题"""
    print("=== 检查学校ID映射问题 ===")
    print()
    
    db = next(get_db())
    
    try:
        # 1. 检查G4-2025批次在statistical_aggregations中的学校ID
        print("1. statistical_aggregations表中G4-2025的学校ID:")
        result = db.execute(text("""
            SELECT school_id, school_name, 
                   JSON_UNQUOTE(JSON_EXTRACT(statistics_data, '$.student_count')) as student_count
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' 
                AND aggregation_level = 'SCHOOL'
            ORDER BY school_id
            LIMIT 10
        """))
        
        stat_agg_schools = []
        for row in result.fetchall():
            stat_agg_schools.append({
                'school_id': row.school_id,
                'school_name': row.school_name,
                'student_count': row.student_count
            })
            print(f"  {row.school_id} - {row.school_name} ({row.student_count}学生)")
        
        print(f"  ... 总共{len(stat_agg_schools)}所学校（仅显示前10个）")
        print()
        
        # 2. 检查school_statistics_summary表中的学校ID（历史数据）
        print("2. school_statistics_summary表中G4-2025的学校ID:")
        result = db.execute(text("""
            SELECT school_id, school_name, student_count
            FROM school_statistics_summary 
            WHERE batch_code = 'G4-2025'
            ORDER BY school_id
            LIMIT 10
        """))
        
        school_summary_schools = []
        for row in result.fetchall():
            school_summary_schools.append({
                'school_id': row.school_id,
                'school_name': row.school_name,
                'student_count': row.student_count
            })
            print(f"  {row.school_id} - {row.school_name} ({row.student_count}学生)")
        
        print(f"  ... 总共{len(school_summary_schools)}所学校（仅显示前10个）")
        print()
        
        # 3. 检查student_score_detail中的实际school_id
        print("3. student_score_detail表中G4-2025的学校ID:")
        result = db.execute(text("""
            SELECT DISTINCT school_id, school_name, COUNT(DISTINCT student_id) as student_count
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025'
            GROUP BY school_id, school_name
            ORDER BY school_id
            LIMIT 10
        """))
        
        source_schools = []
        for row in result.fetchall():
            source_schools.append({
                'school_id': row.school_id,
                'school_name': row.school_name,
                'student_count': row.student_count
            })
            print(f"  {row.school_id} - {row.school_name} ({row.student_count}学生)")
        
        print()
        
        # 4. 对比分析
        print("4. ID格式对比分析:")
        if stat_agg_schools:
            stat_sample = stat_agg_schools[0]['entity_id']
            print(f"  statistical_aggregations使用的ID格式: {stat_sample}")
        
        if school_summary_schools:
            summary_sample = school_summary_schools[0]['school_id']
            print(f"  school_statistics_summary使用的ID格式: {summary_sample}")
        
        if source_schools:
            source_sample = source_schools[0]['school_id']
            print(f"  student_score_detail中的ID格式: {source_sample}")
        
        print()
        
        # 5. 检查是否存在映射关系问题
        print("5. 映射关系分析:")
        if stat_agg_schools and source_schools:
            # 检查第一个学校的ID是否匹配
            stat_id = stat_agg_schools[0]['school_id']
            source_id = source_schools[0]['school_id']
            
            if stat_id != source_id:
                print(f"  发现ID不匹配:")
                print(f"    汇聚表使用: {stat_id}")
                print(f"    源表使用: {source_id}")
                print(f"  这可能导致前端查询时找不到对应的学校数据")
            else:
                print(f"  ID格式匹配: {stat_id}")
        
        # 6. 检查计算服务中使用的school_id字段
        print(f"\n6. 建议解决方案:")
        print(f"  1. 检查calculation_service.py中学校级计算时使用的school_id字段")
        print(f"  2. 确保entity_id字段使用student_score_detail表中的原始school_id")
        print(f"  3. 可能需要修改学校级汇聚逻辑，使用正确的school_id作为entity_id")
        
        return {
            'stat_agg_schools': stat_agg_schools[:5],
            'source_schools': source_schools[:5],
            'school_summary_schools': school_summary_schools[:5]
        }
        
    except Exception as e:
        print(f"检查失败: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.close()

if __name__ == "__main__":
    check_school_id_mapping()