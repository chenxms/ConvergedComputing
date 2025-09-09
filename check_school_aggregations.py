#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查G7-2025学校级聚合数据
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text

def check_school_aggregations():
    """检查学校级聚合数据"""
    print("=== 检查G7-2025学校级聚合数据 ===")
    
    db = next(get_db())
    batch_code = "G7-2025"
    
    try:
        # 1. 检查statistical_aggregations表中的数据
        print("1. statistical_aggregations表:")
        result = db.execute(text("""
            SELECT aggregation_level, COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code = :batch_code
            GROUP BY aggregation_level
        """), {'batch_code': batch_code})
        
        for row in result.fetchall():
            print(f"  {row.aggregation_level}: {row.count}条")
        
        # 2. 检查所有可能的学校数据表
        print("\n2. 检查数据库中的所有表:")
        tables_to_check = [
            'school_statistics_summary',
            'school_aggregation_main', 
            'school_aggregation_data',
            'grade_aggregation_main',
            'statistical_aggregations'
        ]
        
        for table_name in tables_to_check:
            try:
                result = db.execute(text(f"""
                    SELECT COUNT(*) as count 
                    FROM {table_name} 
                    WHERE batch_code = :batch_code
                """), {'batch_code': batch_code})
                count = result.fetchone().count
                if count > 0:
                    print(f"  {table_name}: {count}条记录")
            except Exception as e:
                print(f"  {table_name}: 表不存在或查询失败")
        
        # 3. 如果有school-level数据，显示一些样本
        print("\n3. 学校级样本数据:")
        result = db.execute(text("""
            SELECT school_id, school_name, statistics_data
            FROM statistical_aggregations 
            WHERE batch_code = :batch_code AND aggregation_level = 'SCHOOL'
            LIMIT 3
        """), {'batch_code': batch_code})
        
        schools = result.fetchall()
        if schools:
            for school in schools:
                print(f"  学校: {school.school_name} (ID: {school.school_id})")
                # 不显示完整的JSON数据，太长了
        else:
            print("  没有找到学校级数据")
            
        # 4. 检查数据总体情况
        print("\n4. 总结:")
        result = db.execute(text("""
            SELECT 
                COUNT(DISTINCT student_id) as students,
                COUNT(DISTINCT school_id) as schools,
                COUNT(DISTINCT subject_name) as subjects
            FROM student_score_detail 
            WHERE batch_code = :batch_code
        """), {'batch_code': batch_code})
        
        stats = result.fetchone()
        print(f"  基础数据: {stats.students}学生, {stats.schools}学校, {stats.subjects}科目")
        
        result = db.execute(text("""
            SELECT COUNT(*) as stats_count
            FROM statistical_aggregations 
            WHERE batch_code = :batch_code
        """), {'batch_code': batch_code})
        
        stats_count = result.fetchone().stats_count
        expected_count = stats.schools + 1  # 43学校 + 1区域
        print(f"  统计数据: {stats_count}条 (期望: {expected_count}条)")
        
        if stats_count == expected_count:
            print("  状态: 完成!")
        elif stats_count > 0:
            print("  状态: 部分完成")
        else:
            print("  状态: 未开始")
            
    except Exception as e:
        print(f"检查失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    check_school_aggregations()