#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查数据库中的实际数据情况
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import asyncio

async def check_database_data():
    """检查数据库中的实际数据情况"""
    print("=== 检查数据库中的实际数据情况 ===")
    print()
    
    db = next(get_db())
    
    try:
        # 1. 检查所有批次的汇聚数据统计
        result = db.execute(text("""
            SELECT batch_code, aggregation_level, COUNT(*) as count
            FROM statistical_aggregations 
            GROUP BY batch_code, aggregation_level
            ORDER BY batch_code, aggregation_level
        """))
        
        print("1. 所有批次汇聚数据统计:")
        for row in result:
            print(f"   {row.batch_code} - {row.aggregation_level}: {row.count}条记录")
        print()
        
        # 2. 重点检查G4-2025的学校数据
        result = db.execute(text("""
            SELECT COUNT(*) as school_count,
                   MIN(school_id) as min_school_id,
                   MAX(school_id) as max_school_id,
                   GROUP_CONCAT(DISTINCT school_id ORDER BY CAST(school_id AS UNSIGNED) LIMIT 10) as sample_ids
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
        """))
        
        row = result.fetchone()
        print(f"2. G4-2025学校级数据详情:")
        print(f"   学校总数: {row.school_count}")
        print(f"   学校ID范围: {row.min_school_id} - {row.max_school_id}")
        print(f"   样本ID: {row.sample_ids}")
        print()
        
        # 3. 检查最近的插入时间
        result = db.execute(text("""
            SELECT batch_code, aggregation_level, 
                   COUNT(*) as count,
                   MIN(created_at) as earliest,
                   MAX(created_at) as latest
            FROM statistical_aggregations 
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
            GROUP BY batch_code, aggregation_level
            ORDER BY latest DESC
        """))
        
        print("3. 最近1小时内的数据插入:")
        for row in result:
            print(f"   {row.batch_code} - {row.aggregation_level}: {row.count}条")
            print(f"      时间范围: {row.earliest} - {row.latest}")
        print()
        
        # 4. 检查G4-2025具体学校数据样例
        result = db.execute(text("""
            SELECT school_id, school_name, total_students, created_at
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            ORDER BY CAST(school_id AS UNSIGNED)
            LIMIT 10
        """))
        
        print("4. G4-2025学校数据样例（前10个）:")
        for row in result:
            print(f"   {row.school_id}: {row.school_name} ({row.total_students}学生) - {row.created_at}")
        print()
        
        # 5. 检查是否有重复数据
        result = db.execute(text("""
            SELECT batch_code, school_id, COUNT(*) as duplicates
            FROM statistical_aggregations 
            WHERE aggregation_level = 'SCHOOL'
            GROUP BY batch_code, school_id
            HAVING COUNT(*) > 1
            ORDER BY duplicates DESC
        """))
        
        print("5. 重复数据检查:")
        duplicate_found = False
        for row in result:
            duplicate_found = True
            print(f"   {row.batch_code} - 学校{row.school_id}: {row.duplicates}条重复记录")
        
        if not duplicate_found:
            print("   无重复数据")
        print()
        
        # 6. 检查计算状态
        result = db.execute(text("""
            SELECT batch_code, aggregation_level, calculation_status, COUNT(*) as count
            FROM statistical_aggregations 
            GROUP BY batch_code, aggregation_level, calculation_status
            ORDER BY batch_code, aggregation_level, calculation_status
        """))
        
        print("6. 计算状态统计:")
        for row in result:
            print(f"   {row.batch_code} - {row.aggregation_level} - {row.calculation_status}: {row.count}条")
            
    except Exception as e:
        print(f"检查失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(check_database_data())