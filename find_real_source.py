#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
找到真正的数据生成源头
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


def find_all_services():
    """查找所有可能的服务"""
    
    print("=== 查找所有可能的服务和进程 ===")
    
    # 1. 检查Python进程
    print("1. 查找Python相关进程:")
    try:
        result = subprocess.run(['powershell', 'Get-Process | Where-Object {$_.ProcessName -like "*python*" -or $_.ProcessName -like "*uvicorn*"} | Select-Object Id,ProcessName,Path'], 
                              capture_output=True, text=True, timeout=10, shell=True)
        if result.stdout:
            print(result.stdout)
        else:
            print("   未找到Python进程")
    except Exception as e:
        print(f"   Python进程检查失败: {str(e)}")
    
    # 2. 检查端口占用
    print("\n2. 查找端口占用:")
    for port in [8000, 8001, 8010, 8011]:
        try:
            result = subprocess.run(['powershell', f'Get-NetTCPConnection -LocalPort {port} -ErrorAction SilentlyContinue | Select-Object LocalAddress,LocalPort,State'], 
                                  capture_output=True, text=True, timeout=5, shell=True)
            if result.stdout and "LocalAddress" in result.stdout:
                print(f"   端口 {port}: 有服务在监听")
                print(f"   {result.stdout}")
            else:
                print(f"   端口 {port}: 无服务")
        except Exception as e:
            print(f"   端口 {port} 检查失败: {str(e)}")
    
    # 3. 检查定时任务
    print("\n3. 查找Windows定时任务:")
    try:
        result = subprocess.run(['powershell', 'Get-ScheduledTask | Where-Object {$_.TaskName -like "*python*" -or $_.TaskName -like "*converged*"} | Select-Object TaskName,State'], 
                              capture_output=True, text=True, timeout=10, shell=True)
        if result.stdout and "TaskName" in result.stdout:
            print(result.stdout)
        else:
            print("   未找到相关定时任务")
    except Exception as e:
        print(f"   定时任务检查失败: {str(e)}")
    
    # 4. 检查Docker容器（如果可用）
    print("\n4. 查找Docker容器:")
    try:
        result = subprocess.run(['docker', 'ps', '-a'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print(result.stdout)
        else:
            print("   Docker不可用或无权限")
    except Exception as e:
        print("   Docker检查失败: 未安装或无权限")
    
    # 5. 检查所有可能的Python脚本进程
    print("\n5. 查找运行中的Python脚本:")
    try:
        result = subprocess.run(['powershell', '''
        $processes = Get-WmiObject Win32_Process | Where-Object {$_.CommandLine -like "*python*"}
        foreach ($proc in $processes) {
            Write-Output "PID: $($proc.ProcessId), CMD: $($proc.CommandLine)"
        }
        '''], capture_output=True, text=True, timeout=15, shell=True)
        if result.stdout:
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if line.strip() and 'python' in line.lower():
                    print(f"   {line}")
        else:
            print("   未找到Python脚本进程")
    except Exception as e:
        print(f"   Python脚本进程检查失败: {str(e)}")


def monitor_database_writes():
    """监控数据库写入操作"""
    
    print("\n=== 监控数据库写入操作 ===")
    
    with next(get_db()) as db:
        # 开启SQL日志（如果可能）
        try:
            db.execute(text("SET GLOBAL general_log = 'ON'"))
            db.execute(text("SET GLOBAL log_output = 'TABLE'"))
            print("已开启MySQL查询日志")
        except Exception as e:
            print(f"无法开启MySQL查询日志: {str(e)}")
        
        print("开始监控数据库写入...")
        initial_count = db.execute(text("SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = 'G7-2025'")).scalar()
        print(f"初始记录数: {initial_count}")
        
        for i in range(24):  # 监控2分钟，每5秒检查一次
            time.sleep(5)
            
            current_count = db.execute(text("SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = 'G7-2025'")).scalar()
            
            if current_count > initial_count:
                print(f"\n🚨 检测到新记录！时间: {datetime.now().strftime('%H:%M:%S')}")
                
                # 获取最新记录详情
                latest = db.execute(text("""
                    SELECT id, created_at, updated_at, statistics_data
                    FROM statistical_aggregations 
                    WHERE batch_code = 'G7-2025'
                    ORDER BY updated_at DESC 
                    LIMIT 1
                """)).fetchone()
                
                if latest:
                    print(f"最新记录ID: {latest[0]}")
                    print(f"创建时间: {latest[1]}")
                    print(f"更新时间: {latest[2]}")
                    print(f"数据大小: {len(str(latest[3]))} 字符")
                
                # 尝试查看MySQL进程列表
                try:
                    processes = db.execute(text("""
                        SELECT ID, USER, HOST, COMMAND, STATE, INFO 
                        FROM information_schema.PROCESSLIST 
                        WHERE COMMAND != 'Sleep' 
                        ORDER BY TIME DESC 
                        LIMIT 5
                    """)).fetchall()
                    
                    print("\n当前数据库连接:")
                    for proc in processes:
                        print(f"  ID:{proc[0]} 用户:{proc[1]} 主机:{proc[2]} 命令:{proc[3]} 状态:{proc[4]}")
                        if proc[5]:
                            print(f"    SQL: {str(proc[5])[:100]}")
                
                except Exception as e:
                    print(f"无法获取数据库进程信息: {str(e)}")
                
                initial_count = current_count
                break
            else:
                print(f"{(i+1)*5}秒: 无新记录")
        
        print("监控完成")


def check_all_possible_sources():
    """检查所有可能的数据源"""
    
    print("\n=== 检查所有可能的数据源 ===")
    
    with next(get_db()) as db:
        # 检查是否有其他批次也在生成
        all_recent = db.execute(text("""
            SELECT batch_code, COUNT(*) as count, MAX(updated_at) as latest
            FROM statistical_aggregations 
            WHERE updated_at > DATE_SUB(NOW(), INTERVAL 30 MINUTE)
            GROUP BY batch_code
            ORDER BY latest DESC
        """)).fetchall()
        
        print("最近30分钟所有批次的数据生成:")
        for row in all_recent:
            print(f"  {row[0]}: {row[1]} 条记录, 最新: {row[2]}")
        
        # 检查是否有应用连接
        try:
            connections = db.execute(text("""
                SELECT USER, HOST, DB, COMMAND, TIME, STATE
                FROM information_schema.PROCESSLIST 
                WHERE USER != 'root' 
                  AND COMMAND != 'Sleep'
                ORDER BY TIME DESC
            """)).fetchall()
            
            print("\n活跃的应用连接:")
            for conn in connections:
                print(f"  用户: {conn[0]}, 主机: {conn[1]}, 数据库: {conn[2]}")
                print(f"  命令: {conn[3]}, 时长: {conn[4]}秒, 状态: {conn[5]}")
        
        except Exception as e:
            print(f"连接信息获取失败: {str(e)}")


if __name__ == '__main__':
    try:
        find_all_services()
        check_all_possible_sources()
        monitor_database_writes()
        
    except Exception as e:
        print(f"检查出错: {str(e)}")
        import traceback
        traceback.print_exc()