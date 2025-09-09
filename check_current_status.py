#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查当前数据库中所有批次的汇聚统计状态
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import time

def check_current_database_status():
    """检查数据库当前状态"""
    print("=== 检查数据库汇聚统计状态 ===")
    
    db = next(get_db())
    
    try:
        # 1. 检查所有批次的基础数据
        print("1. 基础数据情况:")
        result = db.execute(text("""
            SELECT DISTINCT batch_code, 
                   COUNT(DISTINCT student_id) as student_count,
                   COUNT(DISTINCT school_id) as school_count,
                   COUNT(DISTINCT subject_name) as subject_count,
                   MIN(created_at) as first_record,
                   MAX(updated_at) as last_update
            FROM student_score_detail 
            WHERE batch_code IN ('G4-2025', 'G7-2025', 'G8-2025')
            GROUP BY batch_code
            ORDER BY batch_code
        """))
        
        batches = result.fetchall()
        for batch in batches:
            print(f"  批次 {batch.batch_code}:")
            print(f"    学生数: {batch.student_count}, 学校数: {batch.school_count}, 科目数: {batch.subject_count}")
            print(f"    数据时间: {batch.first_record} ~ {batch.last_update}")
            print()
        
        # 2. 检查statistical_aggregations汇聚数据
        print("2. 汇聚统计数据情况:")
        result = db.execute(text("""
            SELECT batch_code, aggregation_level, 
                   COUNT(*) as record_count,
                   MIN(created_at) as first_agg,
                   MAX(updated_at) as last_agg
            FROM statistical_aggregations 
            WHERE batch_code IN ('G4-2025', 'G7-2025', 'G8-2025')
            GROUP BY batch_code, aggregation_level
            ORDER BY batch_code, aggregation_level
        """))
        
        agg_results = result.fetchall()
        if agg_results:
            current_batch = None
            for row in agg_results:
                if current_batch != row.batch_code:
                    if current_batch is not None:
                        print()
                    current_batch = row.batch_code
                    print(f"  批次 {row.batch_code}:")
                print(f"    {row.aggregation_level}: {row.record_count}条记录")
                print(f"      统计时间: {row.first_agg} ~ {row.last_agg}")
        else:
            print("  没有找到任何汇聚统计记录")
        
        # 3. 总计统计
        print("\n3. 总计:")
        result = db.execute(text("SELECT COUNT(*) as total FROM statistical_aggregations"))
        total_records = result.fetchone().total
        print(f"  statistical_aggregations表总记录数: {total_records}")
        
        # 4. 检查school_statistics_summary表 (用于对比)
        print("\n4. 历史school_statistics_summary表对比:")
        result = db.execute(text("""
            SELECT batch_code, COUNT(DISTINCT school_id) as school_count,
                   MIN(created_at) as first_record,
                   MAX(updated_at) as last_update
            FROM school_statistics_summary
            WHERE batch_code IN ('G4-2025', 'G7-2025', 'G8-2025')
            GROUP BY batch_code
            ORDER BY batch_code
        """))
        
        school_stats = result.fetchall()
        for row in school_stats:
            print(f"  批次 {row.batch_code}: {row.school_count}个学校")
            print(f"    时间: {row.first_record} ~ {row.last_update}")
        
        # 5. 最近的计算时间
        print("\n5. 最近汇聚计算时间:")
        result = db.execute(text("""
            SELECT batch_code, aggregation_level, MAX(updated_at) as latest_update
            FROM statistical_aggregations
            WHERE batch_code IN ('G4-2025', 'G7-2025', 'G8-2025')
            GROUP BY batch_code, aggregation_level
            ORDER BY latest_update DESC
            LIMIT 10
        """))
        
        recent_updates = result.fetchall()
        for row in recent_updates:
            print(f"  {row.batch_code} {row.aggregation_level}: {row.latest_update}")
        
    except Exception as e:
        print(f"检查状态失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    check_current_database_status()