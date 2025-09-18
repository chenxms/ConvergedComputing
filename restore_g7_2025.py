#\!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025阻断恢复脚本
创建时间: 2025-09-12 00:56:33
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db

def restore_g7_2025():
    """恢复G7-2025数据生成能力"""
    
    with next(get_db()) as db:
        print("=== G7-2025阻断恢复 ===")
        
        try:
            # 删除触发器
            db.execute(text("DROP TRIGGER IF EXISTS prevent_g7_2025_insert"))
            db.execute(text("DROP TRIGGER IF EXISTS prevent_g7_2025_update"))
            
            # 测试恢复
            db.execute(text("""
                INSERT INTO statistical_aggregations 
                (batch_code, aggregation_level, school_id, statistics_data, 
                 data_version, calculation_status, created_at, updated_at)
                VALUES 
                ("G7-2025", "REGIONAL", "RESTORE_TEST", "{}", "TEST", "COMPLETED", NOW(), NOW())
            """))
            
            db.execute(text("DELETE FROM statistical_aggregations WHERE school_id = "RESTORE_TEST""))
            
            db.commit()
            print("恢复完成\! G7-2025数据现在可以正常插入")
            
        except Exception as e:
            db.rollback()
            print(f"恢复失败: {str(e)}")

if __name__ == "__main__":
    restore_g7_2025()
