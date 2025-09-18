#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
紧急检查G7-2025数据生成情况
"""

import time
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 直连数据库
DATABASE_URL = "mysql+pymysql://root:123456@127.0.0.1:3306/appraisal_test"

def check_g7_status():
    try:
        engine = create_engine(DATABASE_URL)
        SessionLocal = sessionmaker(bind=engine)
        
        with SessionLocal() as db:
            print(f"[{datetime.now()}] 紧急检查G7-2025数据生成...")
            
            # 检查最新记录
            latest_records = db.execute(text("""
                SELECT id, created_at, updated_at 
                FROM statistical_aggregations 
                WHERE batch_code='G7-2025' 
                ORDER BY id DESC 
                LIMIT 5
            """)).fetchall()
            
            print("最新5条G7-2025记录:")
            for i, record in enumerate(latest_records):
                print(f"  {i+1}. ID={record[0]}, created={record[1]}")
            
            # 检查最近1分钟
            recent_1min = db.execute(text("""
                SELECT COUNT(*) FROM statistical_aggregations 
                WHERE batch_code='G7-2025' AND created_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
            """)).fetchone()
            
            print(f"最近1分钟新增: {recent_1min[0]}")
            
            # 检查最近5分钟
            recent_5min = db.execute(text("""
                SELECT COUNT(*) FROM statistical_aggregations 
                WHERE batch_code='G7-2025' AND created_at >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)
            """)).fetchone()
            
            print(f"最近5分钟新增: {recent_5min[0]}")
            
            # 检查当前总数
            total = db.execute(text("""
                SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code='G7-2025'
            """)).fetchone()
            
            print(f"G7-2025总记录数: {total[0]}")
            
            # 获取当前最大ID等待检查
            max_id = latest_records[0][0] if latest_records else 0
            print(f"当前最大ID: {max_id}")
            
            print("等待60秒检查是否还在生成...")
            time.sleep(60)
            
            # 再次检查
            new_max = db.execute(text("""
                SELECT MAX(id) FROM statistical_aggregations WHERE batch_code='G7-2025'
            """)).fetchone()
            
            new_max_id = new_max[0] if new_max else 0
            
            if new_max_id > max_id:
                print(f"❌ 警告: G7-2025仍在生成! 新增到ID {new_max_id}")
                return False
            else:
                print(f"✅ G7-2025已停止生成")
                return True
                
    except Exception as e:
        print(f"检查失败: {e}")
        return False

if __name__ == "__main__":
    check_g7_status()