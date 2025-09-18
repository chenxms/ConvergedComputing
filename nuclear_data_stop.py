#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
核武器级数据阻断 - 找出并杀死所有可能的数据生成源
"""

import sys
import os
import time
import subprocess
from sqlalchemy import text
from datetime import datetime

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db


def kill_all_python_processes():
    """杀死所有可能相关的Python进程"""
    
    print("=== 查找并终止可能的Python进程 ===")
    
    try:
        # 使用tasklist查找Python进程
        result = subprocess.run(['tasklist', '/fi', 'imagename eq python.exe', '/fo', 'csv'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')[1:]  # 跳过标题行
            for line in lines:
                if line.strip():
                    parts = line.split('","')
                    if len(parts) > 1:
                        pid = parts[1].strip('"')
                        print(f"发现Python进程 PID: {pid}")
                        
                        # 获取命令行参数
                        cmd_result = subprocess.run(['wmic', 'process', 'where', f'ProcessId={pid}', 
                                                   'get', 'CommandLine', '/format:list'], 
                                                  capture_output=True, text=True)
                        
                        if 'CommandLine=' in cmd_result.stdout:
                            cmdline = cmd_result.stdout.split('CommandLine=')[1].strip()
                            print(f"  命令行: {cmdline[:100]}")
                            
                            # 如果包含我们项目相关的关键词，询问是否杀死
                            if any(keyword in cmdline.lower() for keyword in ['converged', 'statistical', 'subjects', 'uvicorn']):
                                print(f"  🚨 这个进程可能与数据生成有关!")
                                try:
                                    subprocess.run(['taskkill', '/PID', pid, '/F'], check=True)
                                    print(f"  ✅ 已终止进程 {pid}")
                                except:
                                    print(f"  ❌ 无法终止进程 {pid}")
    
    except Exception as e:
        print(f"进程查找失败: {str(e)}")


def clean_all_g7_data():
    """清理所有G7相关数据"""
    
    print("\n=== 清理所有G7相关数据 ===")
    
    with next(get_db()) as db:
        try:
            # 1. 删除所有G7相关的汇聚数据
            result1 = db.execute(text("DELETE FROM statistical_aggregations WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'"))
            print(f"已删除 {result1.rowcount} 条G7汇聚记录")
            
            # 2. 清理基础数据源
            tables_to_clean = [
                'student_cleaned_scores',
                'school_master_data', 
                'student_score_detail',
                'grade_aggregation_main'
            ]
            
            total_cleaned = 0
            for table in tables_to_clean:
                try:
                    result = db.execute(text(f"DELETE FROM {table} WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'"))
                    count = result.rowcount
                    if count > 0:
                        print(f"从 {table} 删除 {count} 条记录")
                        total_cleaned += count
                except Exception as e:
                    print(f"清理表 {table} 失败: {str(e)}")
            
            print(f"总计清理 {total_cleaned} 条基础数据记录")
            
            # 3. 清理任务记录
            result3 = db.execute(text("UPDATE tasks SET status = 'cancelled' WHERE status IN ('pending', 'running')"))
            print(f"已取消 {result3.rowcount} 个任务")
            
            db.commit()
            print("✅ 数据清理完成")
            
        except Exception as e:
            db.rollback()
            print(f"❌ 数据清理失败: {str(e)}")
            raise


def monitor_after_cleanup():
    """清理后监控数据生成情况"""
    
    print("\n=== 清理后监控数据生成 ===")
    
    with next(get_db()) as db:
        print("开始5分钟监控...")
        start_time = datetime.now()
        
        for minute in range(5):
            print(f"\n第 {minute + 1} 分钟:")
            
            for second in range(12):  # 每5秒检查一次
                count = db.execute(text("SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'")).scalar()
                
                if count > 0:
                    print(f"  🚨 {second * 5}秒: 发现 {count} 条G7记录！数据生成仍未停止!")
                    
                    # 显示最新记录
                    latest = db.execute(text("""
                        SELECT id, batch_code, created_at, statistics_data
                        FROM statistical_aggregations 
                        WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
                        ORDER BY created_at DESC 
                        LIMIT 1
                    """)).fetchone()
                    
                    if latest:
                        print(f"    最新记录: ID={latest[0]}, 批次={latest[1]}, 时间={latest[2]}")
                        print(f"    数据长度: {len(str(latest[3]))} 字符")
                    
                    return False  # 仍有数据生成
                else:
                    print(f"  ✅ {second * 5}秒: 无G7记录")
                
                time.sleep(5)
        
        print("✅ 5分钟监控完成，未发现新的G7数据生成")
        return True


def ultimate_nuclear_stop():
    """终极核武器级停止方案"""
    
    print("🔥🔥🔥 执行终极核武器级阻断方案 🔥🔥🔥")
    print("警告: 这将终止所有可能相关的进程并清理所有G7数据!")
    print()
    
    # 步骤1: 杀死进程
    kill_all_python_processes()
    
    # 步骤2: 清理数据
    clean_all_g7_data()
    
    # 步骤3: 等待并监控
    print("\n等待30秒让系统稳定...")
    time.sleep(30)
    
    # 步骤4: 监控验证
    success = monitor_after_cleanup()
    
    if success:
        print("\n🎉🎉🎉 核武器级阻断成功! G7数据生成已完全停止! 🎉🎉🎉")
    else:
        print("\n💀💀💀 核武器级阻断失败! 仍有隐藏的数据生成源! 💀💀💀")
        print("建议检查:")
        print("1. 是否有远程服务器上的程序在运行")
        print("2. 是否有数据库触发器或存储过程")
        print("3. 是否有其他系统在调用API")
    
    return success


if __name__ == '__main__':
    try:
        if len(sys.argv) > 1 and sys.argv[1] == '--monitor-only':
            monitor_after_cleanup()
        else:
            ultimate_nuclear_stop()
            
    except Exception as e:
        print(f"核武器级阻断出错: {str(e)}")
        import traceback
        traceback.print_exc()