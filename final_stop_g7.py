#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
最终阻断G7-2025数据生成 - 更彻底的方法
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db


def final_stop():
    """最终阻断方法"""
    
    with next(get_db()) as db:
        print("=== 最终阻断G7-2025数据生成 ===")
        
        try:
            # 清除所有G7-2025相关的汇聚数据
            result1 = db.execute(text("""
                DELETE FROM statistical_aggregations 
                WHERE batch_code = 'G7-2025'
            """))
            print(f"已删除所有G7-2025汇聚记录: {result1.rowcount}")
            
            # 确保批次数据已重命名
            result2 = db.execute(text("""
                UPDATE school_master_data 
                SET batch_code = 'G7-2025-DISABLED'
                WHERE batch_code IN ('G7-2025', 'G7-2025-STOPPED')
            """))
            print(f"已重命名批次为DISABLED: {result2.rowcount}")
            
            result3 = db.execute(text("""
                UPDATE student_cleaned_scores 
                SET batch_code = 'G7-2025-DISABLED'
                WHERE batch_code IN ('G7-2025', 'G7-2025-STOPPED')  
            """))
            print(f"已重命名学生数据批次: {result3.rowcount}")
            
            # 禁用所有相关任务
            result4 = db.execute(text("""
                UPDATE tasks 
                SET status = 'cancelled', 
                    error_message = 'G7-2025批次已禁用'
                WHERE status IN ('pending', 'running')
            """))
            print(f"已取消相关任务: {result4.rowcount}")
            
            db.commit()
            print("OK: 最终阻断完成!")
            print("批次已完全禁用，数据生成应该已停止")
            
        except Exception as e:
            db.rollback()
            print(f"ERROR: {str(e)}")
            raise


if __name__ == '__main__':
    final_stop()