#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
深度分析G7-2025数据持续生成的原因
"""

import sys
import os
import time
from sqlalchemy import text
from datetime import datetime

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db


def deep_analysis():
    """深度分析数据生成原因"""
    
    with next(get_db()) as db:
        print("=== G7-2025数据生成深度分析 ===")
        print(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # 1. 检查所有可能的批次变体
        print("1. 检查所有可能的G7批次变体:")
        batch_variants = db.execute(text("""
            SELECT DISTINCT batch_code, COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
            GROUP BY batch_code
            ORDER BY count DESC
        """)).fetchall()
        
        for row in batch_variants:
            print(f"  - {row[0]}: {row[1]} 条记录")
        
        print()
        
        # 2. 检查数据库触发器
        print("2. 检查数据库触发器:")
        triggers = db.execute(text("""
            SELECT TRIGGER_NAME, EVENT_MANIPULATION, EVENT_OBJECT_TABLE 
            FROM information_schema.TRIGGERS 
            WHERE TRIGGER_SCHEMA = DATABASE()
        """)).fetchall()
        
        if triggers:
            for row in triggers:
                print(f"  - {row[0]}: {row[1]} on {row[2]}")
        else:
            print("  - 没有发现数据库触发器")
        
        print()
        
        # 3. 检查最新数据的具体内容
        print("3. 最近生成数据的详细信息:")
        recent_data = db.execute(text("""
            SELECT id, batch_code, aggregation_level, school_id, 
                   calculation_status, created_at, updated_at,
                   JSON_EXTRACT(statistics_data, '$.batch_code') as json_batch_code
            FROM statistical_aggregations 
            WHERE updated_at > DATE_SUB(NOW(), INTERVAL 10 MINUTE)
            ORDER BY updated_at DESC
            LIMIT 10
        """)).fetchall()
        
        if recent_data:
            print("ID       | 批次           | 级别     | 学校ID | 状态      | 创建时间            | 更新时间            | JSON批次")
            print("-" * 120)
            for row in recent_data:
                print(f"{row[0]:<8} | {row[1]:<13} | {row[2]:<8} | {row[3] or 'NULL':<6} | {row[4]:<9} | {row[5]} | {row[6]} | {row[7]}")
        else:
            print("  - 最近10分钟没有新数据")
        
        print()
        
        # 4. 检查是否有外键约束或级联操作
        print("4. 检查外键约束:")
        foreign_keys = db.execute(text("""
            SELECT 
                TABLE_NAME,
                COLUMN_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME,
                UPDATE_RULE,
                DELETE_RULE
            FROM information_schema.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = DATABASE() 
                AND REFERENCED_TABLE_NAME IS NOT NULL
        """)).fetchall()
        
        if foreign_keys:
            for row in foreign_keys:
                print(f"  - {row[0]}.{row[1]} -> {row[2]}.{row[3]} (UPDATE: {row[4]}, DELETE: {row[5]})")
        else:
            print("  - 没有发现外键约束")
        
        print()
        
        # 5. 检查数据源表
        print("5. 检查数据源表状态:")
        
        # 检查school_master_data
        school_data = db.execute(text("""
            SELECT batch_code, status, COUNT(*) as count
            FROM school_master_data 
            WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
            GROUP BY batch_code, status
        """)).fetchall()
        
        print("  school_master_data:")
        for row in school_data:
            print(f"    - {row[0]} ({row[1]}): {row[2]} 条记录")
        
        # 检查student_cleaned_scores  
        student_data = db.execute(text("""
            SELECT batch_code, COUNT(*) as count, MAX(updated_at) as last_update
            FROM student_cleaned_scores 
            WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
            GROUP BY batch_code
            ORDER BY last_update DESC
        """)).fetchall()
        
        print("  student_cleaned_scores:")
        for row in student_data:
            print(f"    - {row[0]}: {row[1]} 条记录, 最后更新: {row[2]}")
        
        print()
        
        # 6. 实时监控数据生成
        print("6. 实时监控数据生成 (监控30秒):")
        initial_count = db.execute(text("""
            SELECT COUNT(*) FROM statistical_aggregations 
            WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
        """)).scalar()
        
        print(f"  - 初始记录数: {initial_count}")
        
        for i in range(6):  # 监控30秒，每5秒检查一次
            time.sleep(5)
            current_count = db.execute(text("""
                SELECT COUNT(*) FROM statistical_aggregations 
                WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
            """)).scalar()
            
            new_records = current_count - initial_count
            print(f"  - {(i+1)*5}秒后: {current_count} 条记录 (新增: {new_records})")
            
            if new_records > 0:
                # 显示新增记录的详情
                new_data = db.execute(text("""
                    SELECT batch_code, aggregation_level, school_id, updated_at
                    FROM statistical_aggregations 
                    WHERE (batch_code LIKE '%G7%' OR batch_code LIKE '%2025%')
                        AND updated_at > DATE_SUB(NOW(), INTERVAL 6 SECOND)
                    ORDER BY updated_at DESC
                    LIMIT 5
                """)).fetchall()
                
                for row in new_data:
                    print(f"    新增: {row[0]} | {row[1]} | {row[2]} | {row[3]}")
        
        print()
        print("=== 分析完成 ===")


def nuclear_stop():
    """核弹级停止 - 阻断所有可能的生成源"""
    
    with next(get_db()) as db:
        print("=== 核弹级阻断操作 ===")
        
        try:
            # 1. 删除所有G7/2025相关的汇聚数据
            result1 = db.execute(text("""
                DELETE FROM statistical_aggregations 
                WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
            """))
            print(f"已删除所有G7/2025汇聚记录: {result1.rowcount}")
            
            # 2. 重命名所有相关的基础数据
            result2 = db.execute(text("""
                UPDATE school_master_data 
                SET batch_code = CONCAT(batch_code, '-NUCLEAR-STOPPED')
                WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
            """))
            print(f"已重命名school_master_data: {result2.rowcount}")
            
            result3 = db.execute(text("""
                UPDATE student_cleaned_scores 
                SET batch_code = CONCAT(batch_code, '-NUCLEAR-STOPPED')
                WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
            """))
            print(f"已重命名student_cleaned_scores: {result3.rowcount}")
            
            # 3. 禁用所有可能触发的任务
            result4 = db.execute(text("""
                UPDATE tasks 
                SET status = 'nuclear_cancelled',
                    error_message = '核弹级停止 - 所有G7/2025批次已禁用'
                WHERE status IN ('pending', 'running')
            """))
            print(f"已核弹级取消任务: {result4.rowcount}")
            
            # 4. 如果有触发器，临时禁用
            db.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
            db.execute(text("SET SQL_SAFE_UPDATES = 0"))
            
            db.commit()
            print("核弹级阻断完成!")
            
        except Exception as e:
            db.rollback()
            print(f"核弹级操作失败: {str(e)}")
            raise


if __name__ == '__main__':
    try:
        if len(sys.argv) > 1 and sys.argv[1] == '--nuclear':
            nuclear_stop()
        else:
            deep_analysis()
            print("\n如需执行核弹级阻断，请运行：")
            print("python deep_analysis_g7.py --nuclear")
            
    except Exception as e:
        print(f"执行出错: {str(e)}")
        import traceback
        traceback.print_exc()