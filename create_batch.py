#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text

def create_batch():
    db = next(get_db())
    
    try:
        # 检查是否已存在
        result = db.execute(text('SELECT COUNT(*) FROM batches WHERE name="G7-2025"'))
        count = result.fetchone()[0]
        
        if count > 0:
            print("G7-2025批次已存在")
            return True
            
        # 插入G7-2025批次记录
        result = db.execute(text('''
            INSERT INTO batches (name, description, status, created_at) 
            VALUES ('G7-2025', '七年级2025学年统计分析批次', 'active', NOW())
        '''))
        db.commit()
        print("SUCCESS: 成功创建G7-2025批次记录")
        
        # 验证插入
        result = db.execute(text('SELECT id, name, description, status FROM batches WHERE name="G7-2025"'))
        batch = result.fetchone()
        if batch:
            print(f"批次信息: ID={batch[0]}, Name={batch[1]}, Status={batch[3]}")
            return True
        
    except Exception as e:
        print(f"创建批次记录失败: {e}")
        db.rollback()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    create_batch()