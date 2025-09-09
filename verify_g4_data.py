#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证G4-2025批次的数据汇聚结果
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import asyncio

async def verify_g4_data():
    """验证G4-2025数据汇聚结果"""
    print("=== 验证G4-2025批次数据汇聚结果 ===")
    print()
    
    db = next(get_db())
    
    try:
        # 1. 检查汇聚数据统计
        result = db.execute(text("""
            SELECT aggregation_level, COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025'
            GROUP BY aggregation_level
            ORDER BY aggregation_level
        """))
        
        print("1. G4-2025汇聚数据统计:")
        for row in result:
            print(f"   {row.aggregation_level}: {row.count}条记录")
        print()
        
        # 2. 检查学校数据样例
        result = db.execute(text("""
            SELECT school_id, school_name, total_students,
                   JSON_EXTRACT(statistics_data, '$.subjects') as subjects_count
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            ORDER BY CAST(school_id AS UNSIGNED)
            LIMIT 5
        """))
        
        print("2. G4-2025学校数据样例:")
        for row in result:
            print(f"   学校ID: {row.school_id}")
            print(f"   学校名: {row.school_name}")
            print(f"   学生数: {row.total_students}")
            print()
        
        # 3. 验证学校ID格式是否正确
        result = db.execute(text("""
            SELECT COUNT(*) as total_schools,
                   COUNT(CASE WHEN school_id REGEXP '^[0-9]+$' THEN 1 END) as numeric_ids,
                   COUNT(CASE WHEN school_id LIKE 'SCH_%' THEN 1 END) as sch_format_ids
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
        """))
        
        row = result.fetchone()
        print("3. 学校ID格式验证:")
        print(f"   总学校数: {row.total_schools}")
        print(f"   数字格式ID: {row.numeric_ids}")
        print(f"   SCH_格式ID: {row.sch_format_ids}")
        print()
        
        # 4. 对比原始数据中的学校ID
        result = db.execute(text("""
            SELECT school_id, school_name
            FROM school_statistics_summary 
            WHERE batch_code = 'G4-2025'
            ORDER BY CAST(school_id AS UNSIGNED)
            LIMIT 3
        """))
        
        print("4. 原始数据中的学校ID样例(school_statistics_summary):")
        for row in result:
            print(f"   {row.school_id} - {row.school_name}")
        print()
        
        # 5. 检查是否匹配
        result = db.execute(text("""
            SELECT sa.school_id as agg_school_id, sa.school_name as agg_school_name,
                   sss.school_id as orig_school_id, sss.school_name as orig_school_name
            FROM statistical_aggregations sa
            LEFT JOIN school_statistics_summary sss 
                ON sa.school_id = sss.school_id AND sa.batch_code = sss.batch_code
            WHERE sa.batch_code = 'G4-2025' AND sa.aggregation_level = 'SCHOOL'
            ORDER BY CAST(sa.school_id AS UNSIGNED)
            LIMIT 3
        """))
        
        print("5. 学校ID匹配验证:")
        for row in result:
            match_status = "匹配" if row.orig_school_id else "不匹配"
            print(f"   汇聚: {row.agg_school_id} - {row.agg_school_name}")
            print(f"   原始: {row.orig_school_id} - {row.orig_school_name}")
            print(f"   状态: {match_status}")
            print()
            
    except Exception as e:
        print(f"验证失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(verify_g4_data())