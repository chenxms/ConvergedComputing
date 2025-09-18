#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单检查数据库数据
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import asyncio

async def simple_check():
    """简单检查数据库数据"""
    print("=== 简单检查数据库数据 ===")
    print()
    
    db = next(get_db())
    
    try:
        # 1. 检查所有批次数据统计
        result = db.execute(text("""
            SELECT batch_code, aggregation_level, COUNT(*) as count
            FROM statistical_aggregations 
            GROUP BY batch_code, aggregation_level
            ORDER BY batch_code, aggregation_level
        """))
        
        print("1. 所有批次数据统计:")
        for row in result:
            print(f"   {row.batch_code} - {row.aggregation_level}: {row.count}条记录")
        print()
        
        # 2. 检查G4-2025的学校数据详情
        result = db.execute(text("""
            SELECT school_id, school_name, total_students, created_at
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            ORDER BY created_at DESC
        """))
        
        print("2. G4-2025学校数据详情:")
        school_count = 0
        for row in result:
            school_count += 1
            print(f"   {row.school_id}: {row.school_name} ({row.total_students}学生) - {row.created_at}")
        print(f"   总计: {school_count}所学校")
        print()
        
        # 3. 检查最新的计算记录
        result = db.execute(text("""
            SELECT batch_code, aggregation_level, school_id, school_name, created_at
            FROM statistical_aggregations 
            ORDER BY created_at DESC
            LIMIT 10
        """))
        
        print("3. 最新的10条计算记录:")
        for row in result:
            print(f"   {row.created_at}: {row.batch_code} - {row.aggregation_level} - {row.school_id or 'REGION'} - {row.school_name or 'N/A'}")
        print()
        
        # 4. 检查是否有唯一约束冲突的迹象
        result = db.execute(text("""
            SELECT batch_code, aggregation_level, school_id, COUNT(*) as duplicates
            FROM statistical_aggregations 
            GROUP BY batch_code, aggregation_level, school_id
            HAVING COUNT(*) > 1
            ORDER BY duplicates DESC
        """))
        
        print("4. 重复数据检查:")
        has_duplicates = False
        for row in result:
            has_duplicates = True
            print(f"   {row.batch_code} - {row.aggregation_level} - {row.school_id}: {row.duplicates}条重复")
        
        if not has_duplicates:
            print("   没有发现重复数据")
            
    except Exception as e:
        print(f"检查失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(simple_check())