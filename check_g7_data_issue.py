#!/usr/bin/env python3
"""
检查G7-2025批次数据问题诊断脚本
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database.connection import get_db
from sqlalchemy import text
import time

def check_data_status():
    """检查G7-2025批次数据状态"""
    print("="*60)
    print("G7-2025批次数据状态检查")
    print("="*60)
    
    try:
        db = next(get_db())
        
        # 1. 检查数据总量
        print("\n1. 数据总量检查:")
        query = text("""
            SELECT 
                COUNT(DISTINCT student_id) as students,
                COUNT(DISTINCT school_code) as schools,
                COUNT(DISTINCT subject_name) as subjects,
                COUNT(*) as total_records
            FROM student_cleaned_scores
            WHERE batch_code = 'G7-2025'
        """)
        result = db.execute(query).fetchone()
        print(f"   学生数: {result[0]}")
        print(f"   学校数: {result[1]}")
        print(f"   科目数: {result[2]}")
        print(f"   总记录数: {result[3]}")
        
        # 2. 检查每个学校的数据量
        print("\n2. 每个学校的数据量（前5个）:")
        query = text("""
            SELECT 
                school_code,
                COUNT(DISTINCT student_id) as students,
                COUNT(DISTINCT subject_name) as subjects,
                COUNT(*) as records
            FROM student_cleaned_scores
            WHERE batch_code = 'G7-2025'
            GROUP BY school_code
            ORDER BY records DESC
            LIMIT 5
        """)
        results = db.execute(query).fetchall()
        for row in results:
            print(f"   {row[0]}: {row[1]}个学生, {row[2]}个科目, {row[3]}条记录")
        
        # 3. 测试GROUP_CONCAT长度
        print("\n3. 测试GROUP_CONCAT问题:")
        query = text("""
            SELECT 
                school_code,
                subject_name,
                LENGTH(GROUP_CONCAT(total_score)) as concat_length,
                COUNT(*) as count
            FROM student_cleaned_scores
            WHERE batch_code = 'G7-2025'
            GROUP BY school_code, subject_name
            ORDER BY concat_length DESC
            LIMIT 3
        """)
        results = db.execute(query).fetchall()
        for row in results:
            print(f"   {row[0]}-{row[1]}: 拼接长度={row[2]}字节, 记录数={row[3]}")
        
        # 4. 检查dimension_scores字段
        print("\n4. 检查dimension_scores字段:")
        query = text("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN dimension_scores IS NULL THEN 1 ELSE 0 END) as null_count,
                SUM(CASE WHEN dimension_scores = '{}' THEN 1 ELSE 0 END) as empty_count,
                AVG(LENGTH(dimension_scores)) as avg_length
            FROM student_cleaned_scores
            WHERE batch_code = 'G7-2025'
        """)
        result = db.execute(query).fetchone()
        print(f"   总记录: {result[0]}")
        print(f"   NULL记录: {result[1]}")
        print(f"   空JSON记录: {result[2]}")
        print(f"   平均长度: {result[3]:.0f}字节")
        
        # 5. 测试单个学校聚合性能
        print("\n5. 测试单个学校聚合性能:")
        query = text("""
            SELECT school_code
            FROM student_cleaned_scores
            WHERE batch_code = 'G7-2025'
            GROUP BY school_code
            LIMIT 1
        """)
        school_code = db.execute(query).fetchone()[0]
        print(f"   测试学校: {school_code}")
        
        # 测试统计计算性能
        start_time = time.time()
        query = text("""
            SELECT 
                COUNT(*) as count,
                AVG(total_score) as avg,
                STDDEV(total_score) as std,
                MIN(total_score) as min,
                MAX(total_score) as max
            FROM student_cleaned_scores
            WHERE batch_code = 'G7-2025' 
            AND school_code = :school_code
            AND subject_name = '数学'
        """)
        result = db.execute(query, {'school_code': school_code}).fetchone()
        elapsed = time.time() - start_time
        print(f"   基础统计查询耗时: {elapsed:.3f}秒")
        
        # 测试GROUP_CONCAT性能
        start_time = time.time()
        query = text("""
            SELECT 
                GROUP_CONCAT(total_score) as scores
            FROM student_cleaned_scores
            WHERE batch_code = 'G7-2025' 
            AND school_code = :school_code
            AND subject_name = '数学'
        """)
        result = db.execute(query, {'school_code': school_code}).fetchone()
        elapsed = time.time() - start_time
        concat_len = len(str(result[0])) if result[0] else 0
        print(f"   GROUP_CONCAT查询耗时: {elapsed:.3f}秒 (长度: {concat_len}字节)")
        
        # 6. 检查已有汇聚记录
        print("\n6. 检查已有汇聚记录:")
        query = text("""
            SELECT 
                aggregation_level,
                COUNT(*) as count
            FROM statistical_aggregations
            WHERE batch_code = 'G7-2025'
            GROUP BY aggregation_level
        """)
        results = db.execute(query).fetchall()
        if results:
            for row in results:
                print(f"   {row[0]}: {row[1]}条")
        else:
            print("   无汇聚记录")
        
        db.close()
        
    except Exception as e:
        print(f"错误: {str(e)}")

if __name__ == "__main__":
    check_data_status()