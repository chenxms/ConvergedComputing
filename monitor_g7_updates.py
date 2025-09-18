#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025数据更新监控脚本
实时监控G7-2025批次的regional_statistics表更新情况
"""

import time
import traceback
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 数据库连接配置
DATABASE_URL = "mysql+pymysql://root:123456@127.0.0.1:3306/appraisal_test"

def monitor_g7_updates():
    """监控G7-2025数据更新"""
    try:
        engine = create_engine(DATABASE_URL)
        SessionLocal = sessionmaker(bind=engine)
        
        print(f"[{datetime.now()}] 开始监控 G7-2025 数据更新...")
        
        # 获取当前记录数和最新更新时间
        with SessionLocal() as db:
            result = db.execute(text("""
                SELECT 
                    COUNT(*) as total_count,
                    MAX(updated_at) as latest_update,
                    COUNT(CASE WHEN subjects = '[]' THEN 1 END) as empty_subjects_count
                FROM regional_statistics 
                WHERE batch_code = 'G7-2025'
            """)).fetchone()
            
            if result:
                initial_count = result[0]
                latest_update = result[1]
                empty_count = result[2]
                
                print(f"初始状态: 总记录数={initial_count}, 空subjects记录={empty_count}, 最新更新={latest_update}")
            else:
                initial_count = 0
                latest_update = None
                empty_count = 0
                print("初始状态: 无G7-2025记录")
        
        # 持续监控
        check_interval = 10  # 10秒检查一次
        iteration = 0
        
        while True:
            iteration += 1
            time.sleep(check_interval)
            
            try:
                with SessionLocal() as db:
                    # 检查是否有新的更新
                    current_result = db.execute(text("""
                        SELECT 
                            COUNT(*) as total_count,
                            MAX(updated_at) as latest_update,
                            COUNT(CASE WHEN subjects = '[]' THEN 1 END) as empty_subjects_count
                        FROM regional_statistics 
                        WHERE batch_code = 'G7-2025'
                    """)).fetchone()
                    
                    if current_result:
                        current_count = current_result[0]
                        current_latest = current_result[1]
                        current_empty = current_result[2]
                        
                        # 检查是否有更新
                        if current_latest != latest_update or current_count != initial_count:
                            print(f"\n[{datetime.now()}] 检测到G7-2025数据更新！")
                            print(f"记录数变化: {initial_count} -> {current_count}")
                            print(f"空subjects记录: {empty_count} -> {current_empty}")
                            print(f"最新更新时间: {latest_update} -> {current_latest}")
                            
                            # 获取最近更新的记录详情
                            recent_updates = db.execute(text("""
                                SELECT id, aggregation_level, subjects, created_at, updated_at
                                FROM regional_statistics 
                                WHERE batch_code = 'G7-2025' 
                                AND updated_at >= DATE_SUB(NOW(), INTERVAL 2 MINUTE)
                                ORDER BY updated_at DESC
                                LIMIT 5
                            """)).fetchall()
                            
                            print("\n最近更新的记录:")
                            for record in recent_updates:
                                subjects_preview = record[2][:100] if record[2] else "NULL"
                                print(f"  ID={record[0]}, level={record[1]}, subjects_preview={subjects_preview}")
                                print(f"    created={record[3]}, updated={record[4]}")
                            
                            # 更新基准值
                            initial_count = current_count
                            latest_update = current_latest
                            empty_count = current_empty
                            
                            # 检查是否有正在运行的进程可能导致这个更新
                            print("\n检查可能的触发源...")
                            processes = db.execute(text("SHOW PROCESSLIST")).fetchall()
                            active_processes = [p for p in processes if p[4] and 'INSERT' in str(p[7]).upper() or 'UPDATE' in str(p[7]).upper()]
                            
                            if active_processes:
                                print("发现活跃的数据库写入进程:")
                                for proc in active_processes:
                                    print(f"  Process ID: {proc[0]}, User: {proc[1]}, Command: {proc[4]}")
                                    print(f"  Info: {proc[7]}")
                            
                        else:
                            # 每10次检查打印一次状态
                            if iteration % 10 == 0:
                                print(f"[{datetime.now()}] 监控中... (第{iteration}次检查, 无新更新)")
                    
            except Exception as e:
                print(f"[{datetime.now()}] 监控过程中出错: {e}")
                traceback.print_exc()
                
    except KeyboardInterrupt:
        print(f"\n[{datetime.now()}] 监控已停止")
    except Exception as e:
        print(f"监控脚本启动失败: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    monitor_g7_updates()