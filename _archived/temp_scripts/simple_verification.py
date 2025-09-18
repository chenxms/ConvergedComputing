#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单验证所有批次计算结果 - 避免Unicode问题
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import time

def simple_verification():
    """简单验证所有批次的计算结果"""
    print("=== 简单验证所有批次计算结果 ===")
    print("验证时间:", time.strftime('%Y-%m-%d %H:%M:%S'))
    print()
    
    db = next(get_db())
    
    try:
        # 1. 检查statistical_aggregations汇聚结果
        print("1. 汇聚统计结果:")
        result = db.execute(text("""
            SELECT batch_code, aggregation_level, COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code IN ('G4-2025', 'G7-2025', 'G8-2025')
            GROUP BY batch_code, aggregation_level
            ORDER BY batch_code, aggregation_level
        """))
        
        batch_results = {}
        for row in result.fetchall():
            if row.batch_code not in batch_results:
                batch_results[row.batch_code] = {}
            batch_results[row.batch_code][row.aggregation_level] = row.count
        
        # 显示每个批次的结果
        for batch_code in ['G4-2025', 'G7-2025', 'G8-2025']:
            print(f"\n  批次 {batch_code}:")
            if batch_code in batch_results:
                regional_count = batch_results[batch_code].get('REGIONAL', 0)
                school_count = batch_results[batch_code].get('SCHOOL', 0)
                
                print(f"    区域级: {regional_count}条记录")
                print(f"    学校级: {school_count}条记录")
                
                if regional_count >= 1 and school_count > 0:
                    print(f"    状态: 完成")
                elif regional_count >= 1:
                    print(f"    状态: 区域级完成")
                else:
                    print(f"    状态: 未完成")
            else:
                print(f"    状态: 无数据")
        
        # 2. 总计统计
        print(f"\n2. 总计:")
        result = db.execute(text("SELECT COUNT(*) as total FROM statistical_aggregations"))
        total_records = result.fetchone().total
        print(f"  statistical_aggregations表总记录数: {total_records}")
        
        # 3. 最新更新时间
        print(f"\n3. 最新更新时间:")
        result = db.execute(text("""
            SELECT batch_code, MAX(updated_at) as latest_update
            FROM statistical_aggregations
            WHERE batch_code IN ('G4-2025', 'G7-2025', 'G8-2025')
            GROUP BY batch_code
            ORDER BY latest_update DESC
        """))
        
        for row in result.fetchall():
            print(f"  {row.batch_code}: {row.latest_update}")
        
        # 4. 完成情况统计
        completed_count = 0
        partial_count = 0
        failed_count = 0
        
        for batch_code in ['G4-2025', 'G7-2025', 'G8-2025']:
            if batch_code in batch_results:
                regional = batch_results[batch_code].get('REGIONAL', 0)
                school = batch_results[batch_code].get('SCHOOL', 0)
                
                if regional >= 1 and school > 0:
                    completed_count += 1
                elif regional >= 1:
                    partial_count += 1
                else:
                    failed_count += 1
            else:
                failed_count += 1
        
        print(f"\n4. 完成情况:")
        print(f"  完全完成: {completed_count}个批次")
        print(f"  部分完成: {partial_count}个批次")
        print(f"  未完成: {failed_count}个批次")
        
        # 5. 最终结论
        print(f"\n5. 最终结论:")
        if completed_count == 3:
            print("所有主要批次汇聚计算完成！数据可查询使用。")
        elif completed_count + partial_count > 0:
            print(f"{completed_count}个批次完成，{partial_count}个批次部分完成。")
        else:
            print("所有批次计算都未完成，需要重新运行计算。")
        
        return total_records
        
    except Exception as e:
        print(f"验证失败: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.close()

if __name__ == "__main__":
    simple_verification()