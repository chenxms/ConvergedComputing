#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
紧急阻断G7-2025批次数据生成的脚本
通过修改批次状态或删除触发条件来阻止持续生成
"""

import sys
import os
from sqlalchemy import text
from datetime import datetime

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db


def emergency_stop():
    """紧急停止G7-2025数据生成"""
    
    with next(get_db()) as db:
        print("=== 紧急阻断G7-2025数据生成 ===")
        print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        try:
            # 方法1: 临时重命名批次，阻断脚本识别
            print("1. 临时重命名批次防止脚本识别...")
            result1 = db.execute(text("""
                UPDATE school_master_data 
                SET batch_code = 'G7-2025-STOPPED' 
                WHERE batch_code = 'G7-2025' 
                  AND status = 'ACTIVE'
            """))
            print(f"  - 已重命名 {result1.rowcount} 条school_master_data记录")
            
            result1b = db.execute(text("""
                UPDATE student_cleaned_scores 
                SET batch_code = 'G7-2025-STOPPED' 
                WHERE batch_code = 'G7-2025'
            """))
            print(f"  - 已重命名 {result1b.rowcount} 条student_cleaned_scores记录")
            
            # 方法2: 将所有ACTIVE状态改为INACTIVE
            print("\n2. 临时停用ACTIVE状态...")
            result2 = db.execute(text("""
                UPDATE school_master_data 
                SET status = 'INACTIVE_TEMP' 
                WHERE batch_code = 'G7-2025-STOPPED' 
                  AND status = 'ACTIVE'
            """))
            print(f"  - 已停用 {result2.rowcount} 条学校记录")
            
            # 方法3: 删除最近的汇聚记录，减少重复处理
            print("\n3. 清理最近生成的重复记录...")
            result3 = db.execute(text("""
                DELETE FROM statistical_aggregations 
                WHERE batch_code = 'G7-2025' 
                  AND updated_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)
            """))
            print(f"  - 已删除最近1小时的 {result3.rowcount} 条汇聚记录")
            
            # 提交所有更改
            db.commit()
            print("\n✅ 紧急阻断操作执行完成!")
            print("\n⚠️  重要提醒:")
            print("   - 批次已临时重命名为 G7-2025-STOPPED")
            print("   - 学校状态已临时改为 INACTIVE_TEMP")
            print("   - 需要恢复时请运行恢复脚本")
            
        except Exception as e:
            db.rollback()
            print(f"❌ 操作失败: {str(e)}")
            raise


def restore_g7():
    """恢复G7-2025批次正常状态"""
    
    with next(get_db()) as db:
        print("=== 恢复G7-2025批次正常状态 ===")
        
        try:
            # 恢复批次名称
            result1 = db.execute(text("""
                UPDATE school_master_data 
                SET batch_code = 'G7-2025' 
                WHERE batch_code = 'G7-2025-STOPPED'
            """))
            print(f"已恢复 {result1.rowcount} 条school_master_data记录的批次名称")
            
            result1b = db.execute(text("""
                UPDATE student_cleaned_scores 
                SET batch_code = 'G7-2025' 
                WHERE batch_code = 'G7-2025-STOPPED'
            """))
            print(f"已恢复 {result1b.rowcount} 条student_cleaned_scores记录的批次名称")
            
            # 恢复ACTIVE状态
            result2 = db.execute(text("""
                UPDATE school_master_data 
                SET status = 'ACTIVE' 
                WHERE batch_code = 'G7-2025' 
                  AND status = 'INACTIVE_TEMP'
            """))
            print(f"已恢复 {result2.rowcount} 条学校记录的ACTIVE状态")
            
            db.commit()
            print("✅ 恢复操作完成!")
            
        except Exception as e:
            db.rollback()
            print(f"❌ 恢复操作失败: {str(e)}")
            raise


if __name__ == '__main__':
    try:
        if len(sys.argv) > 1 and sys.argv[1] == '--restore':
            restore_g7()
        else:
            emergency_stop()
            print("\n如需恢复正常状态，请运行：")
            print("python emergency_stop_g7.py --restore")
            
    except Exception as e:
        print(f"执行出错: {str(e)}")
        import traceback
        traceback.print_exc()