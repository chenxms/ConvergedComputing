#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证G4-2025汇聚结果
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import asyncio

async def verify_g4_results():
    """验证G4-2025汇聚结果"""
    print("=== 验证G4-2025汇聚结果 ===")
    print()
    
    db = next(get_db())
    
    try:
        # 1. 验证汇聚数据统计
        print("1. G4-2025汇聚数据总览:")
        result = db.execute(text("""
            SELECT aggregation_level, COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025'
            GROUP BY aggregation_level
            ORDER BY aggregation_level
        """))
        
        for row in result:
            print(f"   {row.aggregation_level}: {row.count}条记录")
        print()
        
        # 2. 学校数据样例验证
        print("2. 学校数据样例（前10个）:")
        school_check = db.execute(text("""
            SELECT school_id, school_name, total_students, created_at
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            ORDER BY CAST(school_id AS UNSIGNED)
            LIMIT 10
        """))
        
        for row in school_check:
            print(f"   ID:{row.school_id} 名称:{row.school_name} 学生:{row.total_students}人 时间:{row.created_at}")
        print()
        
        # 3. 验证学校ID匹配
        print("3. 验证学校ID匹配情况:")
        id_check = db.execute(text("""
            SELECT sa.school_id, sa.school_name, sss.school_name as original_name
            FROM statistical_aggregations sa
            LEFT JOIN school_statistics_summary sss 
                ON sa.school_id = sss.school_id 
                AND sa.batch_code = sss.batch_code
            WHERE sa.batch_code = 'G4-2025' 
                AND sa.aggregation_level = 'SCHOOL'
            ORDER BY CAST(sa.school_id AS UNSIGNED)
            LIMIT 10
        """))
        
        for row in id_check:
            match_status = "匹配" if row.original_name else "不匹配"
            print(f"   ID:{row.school_id} 汇聚名称:{row.school_name} 原名称:{row.original_name} [{match_status}]")
        print()
        
        # 4. 数据完整性检查
        print("4. 数据完整性检查:")
        total_check = db.execute(text("""
            SELECT COUNT(*) as total_schools,
                   SUM(total_students) as total_students,
                   MIN(created_at) as earliest_time,
                   MAX(created_at) as latest_time
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
        """))
        
        stats = total_check.fetchone()
        print(f"   学校总数: {stats.total_schools}")
        print(f"   学生总数: {stats.total_students}")
        print(f"   最早时间: {stats.earliest_time}")
        print(f"   最晚时间: {stats.latest_time}")
        print()
        
        # 5. 区域数据检查
        print("5. 区域数据检查:")
        region_check = db.execute(text("""
            SELECT total_students, total_schools, created_at
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'REGIONAL'
        """))
        
        region = region_check.fetchone()
        if region:
            print(f"   区域学生总数: {region.total_students}")
            print(f"   区域学校总数: {region.total_schools}")
            print(f"   区域创建时间: {region.created_at}")
        else:
            print("   未找到区域数据!")
        print()
        
        # 6. 科目数据验证
        print("6. 科目数据验证（以第一个学校为例）:")
        subject_check = db.execute(text("""
            SELECT statistics_data
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' 
                AND aggregation_level = 'SCHOOL'
                AND school_id != ''
            ORDER BY CAST(school_id AS UNSIGNED)
            LIMIT 1
        """))
        
        school_data = subject_check.fetchone()
        if school_data and school_data.statistics_data:
            import json
            stats_json = json.loads(school_data.statistics_data)
            
            if 'subjects' in stats_json:
                print(f"   发现科目数: {len(stats_json['subjects'])}")
                for subject_name, subject_data in stats_json['subjects'].items():
                    student_count = subject_data.get('basic_statistics', {}).get('count', 0)
                    avg_score = subject_data.get('educational_metrics', {}).get('average_score', 0)
                    print(f"   科目: {subject_name}, 学生数: {student_count}, 平均分: {avg_score:.1f}")
            else:
                print("   数据结构中未找到科目信息")
        else:
            print("   无法获取学校统计数据")
        print()
        
        print("✅ G4-2025汇聚结果验证完成!")
            
    except Exception as e:
        print(f"验证失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(verify_g4_results())