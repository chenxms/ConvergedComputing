#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查G4-2025批次的学校ID数据质量
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import asyncio

async def check_school_id_quality():
    """检查G4-2025批次的学校ID数据质量"""
    print("=== 检查G4-2025批次的学校ID数据质量 ===")
    print()
    
    db = next(get_db())
    
    try:
        # 1. 检查G4-2025批次的所有学校ID
        result = db.execute(text("""
            SELECT DISTINCT school_id, school_name, COUNT(*) as student_count
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025'
            GROUP BY school_id, school_name
            ORDER BY CAST(school_id AS UNSIGNED)
        """))
        
        print("1. G4-2025批次所有学校ID:")
        school_ids = []
        for row in result:
            school_ids.append(row.school_id)
            print(f"   {row.school_id}: {row.school_name} ({row.student_count}学生)")
        
        print(f"\n   总计: {len(school_ids)}所学校")
        print()
        
        # 2. 检查是否有NULL或空的学校ID
        result = db.execute(text("""
            SELECT COUNT(*) as null_count
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025' 
            AND (school_id IS NULL OR school_id = '' OR TRIM(school_id) = '')
        """))
        
        null_count = result.fetchone().null_count
        print(f"2. 无效学校ID数量: {null_count}")
        print()
        
        # 3. 检查学校ID长度分布
        result = db.execute(text("""
            SELECT LENGTH(school_id) as id_length, COUNT(DISTINCT school_id) as school_count
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025'
            GROUP BY LENGTH(school_id)
            ORDER BY id_length
        """))
        
        print("3. 学校ID长度分布:")
        for row in result:
            print(f"   长度{row.id_length}: {row.school_count}所学校")
        print()
        
        # 4. 检查statistical_aggregations表中已存在的约束冲突记录
        result = db.execute(text("""
            SELECT batch_code, aggregation_level, school_id, COUNT(*) as duplicates
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025'
            GROUP BY batch_code, aggregation_level, school_id
            HAVING COUNT(*) > 1
            ORDER BY duplicates DESC
        """))
        
        print("4. 当前数据库中的重复记录:")
        duplicates_found = False
        for row in result:
            duplicates_found = True
            print(f"   {row.batch_code}-{row.aggregation_level}-{row.school_id}: {row.duplicates}条重复")
        
        if not duplicates_found:
            print("   没有发现重复记录")
        print()
        
        # 5. 模拟批量插入以找出约束冲突
        print("5. 模拟约束冲突测试:")
        for i, school_id in enumerate(school_ids[:5]):  # 只测试前5个学校
            try:
                result = db.execute(text("""
                    SELECT COUNT(*) as existing_count
                    FROM statistical_aggregations 
                    WHERE batch_code = 'G4-2025' 
                    AND aggregation_level = 'SCHOOL' 
                    AND school_id = :school_id
                """), {'school_id': school_id})
                
                existing = result.fetchone().existing_count
                print(f"   学校{school_id}: 现有记录{existing}条")
                
            except Exception as e:
                print(f"   学校{school_id}: 查询错误 - {e}")
        
        # 6. 检查最近的插入尝试日志 - 通过查看日志表
        result = db.execute(text("""
            SELECT school_id, school_name, created_at, calculation_status
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            ORDER BY created_at DESC
            LIMIT 10
        """))
        
        print("\n6. 最近保存的学校记录:")
        for row in result:
            print(f"   {row.school_id}: {row.school_name} - {row.created_at} - {row.calculation_status}")
        
    except Exception as e:
        print(f"检查失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(check_school_id_quality())