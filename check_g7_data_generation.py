#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
检查G7-2025批次数据生成模式
"""

import sys
import os
from sqlalchemy import text
from datetime import datetime

# 添加项目路径
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db


def check_g7_data_generation():
    """检查G7-2025批次最近的数据生成模式"""
    
    with next(get_db()) as db:
        print("=== G7-2025批次数据生成检查 ===")
        print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # 第一步：查看最近2小时的数据生成模式
        print("1. 最近2小时数据生成模式:")
        result = db.execute(text("""
            SELECT 
                DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i') as update_time,
                COUNT(*) as record_count,
                GROUP_CONCAT(DISTINCT calculation_status) as statuses
            FROM statistical_aggregations 
            WHERE batch_code = 'G7-2025' 
              AND updated_at > DATE_SUB(NOW(), INTERVAL 2 HOUR)
            GROUP BY DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i')
            ORDER BY update_time DESC
            LIMIT 20
        """)).fetchall()
        
        if result:
            print("时间              | 记录数 | 状态")
            print("-" * 40)
            for row in result:
                print(f"{row[0]:<16} | {row[1]:<6} | {row[2]}")
        else:
            print("✅ 最近2小时没有新的数据生成")
        
        print()
        
        # 检查当前总体状态
        print("2. 当前G7-2025批次总体状态:")
        total_result = db.execute(text("""
            SELECT 
                calculation_status,
                COUNT(*) as count,
                MAX(updated_at) as last_updated
            FROM statistical_aggregations 
            WHERE batch_code = 'G7-2025'
            GROUP BY calculation_status
        """)).fetchall()
        
        if total_result:
            print("状态          | 数量   | 最后更新时间")
            print("-" * 45)
            for row in total_result:
                print(f"{row[0]:<12} | {row[1]:<6} | {row[2]}")
        
        print()
        
        # 检查最近5分钟的数据
        print("3. 最近5分钟新产生的记录:")
        recent_result = db.execute(text("""
            SELECT COUNT(*) as recent_records
            FROM statistical_aggregations 
            WHERE batch_code = 'G7-2025' 
              AND updated_at > DATE_SUB(NOW(), INTERVAL 5 MINUTE)
        """)).scalar()
        
        print(f"最近5分钟新记录数: {recent_result}")
        
        if recent_result > 0:
            print("WARNING: 数据仍在持续生成！需要执行停止操作")
        else:
            print("OK: 最近5分钟没有新数据产生")
        
        print()
        
        # 检查是否有processing状态的记录
        processing_count = db.execute(text("""
            SELECT COUNT(*) 
            FROM statistical_aggregations 
            WHERE batch_code = 'G7-2025' 
              AND calculation_status = 'processing'
        """)).scalar()
        
        print(f"4. 当前processing状态的记录数: {processing_count}")
        if processing_count > 0:
            print("WARNING: 发现processing状态的记录，需要执行停止操作")
        else:
            print("OK: 没有processing状态的记录")


def stop_g7_data_generation():
    """停止G7-2025批次数据生成"""
    
    with next(get_db()) as db:
        print("\n=== 执行停止操作 ===")
        
        # 停止processing状态的记录
        result1 = db.execute(text("""
            UPDATE statistical_aggregations 
            SET 
                calculation_status = 'failed',
                updated_at = NOW()
            WHERE batch_code = 'G7-2025' 
              AND calculation_status = 'processing'
        """))
        
        affected_rows1 = result1.rowcount
        print(f"已停止 {affected_rows1} 条processing记录")
        
        # 取消相关任务
        result2 = db.execute(text("""
            UPDATE tasks 
            SET 
                status = 'cancelled',
                completed_at = NOW(),
                error_message = '手动取消：停止批处理数据生成',
                updated_at = NOW()
            WHERE status IN ('running', 'pending')
              AND started_at > DATE_SUB(NOW(), INTERVAL 4 HOUR)
        """))
        
        affected_rows2 = result2.rowcount
        print(f"已取消 {affected_rows2} 个相关任务")
        
        db.commit()
        print("OK: 停止操作执行完成")


if __name__ == '__main__':
    try:
        # 检查数据生成状态
        check_g7_data_generation()
        
        # 询问是否执行停止操作
        if len(sys.argv) > 1 and sys.argv[1] == '--stop':
            stop_g7_data_generation()
            
            # 再次检查效果
            print("\n=== 停止操作后再次检查 ===")
            check_g7_data_generation()
        else:
            print("\n如需执行停止操作，请运行：")
            print("python check_g7_data_generation.py --stop")
            
    except Exception as e:
        print(f"❌ 执行出错: {str(e)}")
        import traceback
        traceback.print_exc()