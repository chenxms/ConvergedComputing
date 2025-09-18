#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
深度追踪数据生成源 - API关闭后仍在生成数据的原因分析
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


def trace_data_source():
    """追踪数据生成的真正来源"""
    
    with next(get_db()) as db:
        print("=== 深度追踪G7-2025数据生成源 ===")
        print(f"分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # 1. 检查所有可能的批处理脚本或任务
        print("1. 检查系统中可能运行的脚本或任务:")
        
        # 检查是否有其他服务在处理数据
        print("  - 检查Docker容器状态...")
        try:
            import subprocess
            result = subprocess.run(['docker', 'ps'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print(f"    Docker容器: {result.stdout}")
            else:
                print("    无法访问Docker")
        except Exception as e:
            print(f"    Docker检查失败: {str(e)}")
        
        print()
        
        # 2. 分析最新生成数据的特征
        print("2. 分析最新生成数据的详细特征:")
        latest_data = db.execute(text("""
            SELECT id, batch_code, aggregation_level, school_id, school_name,
                   calculation_status, created_at, updated_at,
                   JSON_EXTRACT(statistics_data, '$.schema_version') as schema_version,
                   JSON_EXTRACT(statistics_data, '$.subjects[0].type') as first_subject_type,
                   JSON_LENGTH(statistics_data, '$.subjects') as subject_count,
                   CHAR_LENGTH(statistics_data) as data_size
            FROM statistical_aggregations 
            WHERE batch_code = 'G7-2025'
              AND updated_at > DATE_SUB(NOW(), INTERVAL 30 MINUTE)
            ORDER BY updated_at DESC
            LIMIT 10
        """)).fetchall()
        
        if latest_data:
            print("ID     | 批次    | 级别     | 学校ID   | 状态      | 创建时间        | 更新时间        | 版本 | 科目数 | 数据大小")
            print("-" * 120)
            for row in latest_data:
                print(f"{row[0]:<6} | {row[1]:<7} | {row[2]:<8} | {row[3] or 'NULL':<8} | {row[5]:<9} | {row[6]} | {row[7]} | {row[8] or 'N/A':<4} | {row[10] or 0:<6} | {row[11]}")
        
        print()
        
        # 3. 检查数据生成的时间规律
        print("3. 分析数据生成时间规律:")
        time_pattern = db.execute(text("""
            SELECT 
                MINUTE(updated_at) as minute_of_hour,
                SECOND(updated_at) as second_of_minute,
                COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code = 'G7-2025'
              AND updated_at > DATE_SUB(NOW(), INTERVAL 2 HOUR)
            GROUP BY MINUTE(updated_at), SECOND(updated_at)
            ORDER BY count DESC, minute_of_hour, second_of_minute
            LIMIT 10
        """)).fetchall()
        
        if time_pattern:
            print("分钟:秒  | 次数")
            print("-" * 15)
            for row in time_pattern:
                print(f"{row[0]:02d}:{row[1]:02d}    | {row[2]}")
        
        print()
        
        # 4. 检查是否有定时任务或调度器
        print("4. 检查可能的定时任务或调度器:")
        
        # 检查Windows任务计划程序
        try:
            result = subprocess.run(['schtasks', '/query', '/fo', 'csv'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                lines = result.stdout.split('\n')
                relevant_tasks = [line for line in lines if 'python' in line.lower() or 'converged' in line.lower()]
                if relevant_tasks:
                    print("    发现相关的Windows任务:")
                    for task in relevant_tasks[:5]:  # 显示前5个
                        print(f"      {task}")
                else:
                    print("    未发现相关的Windows任务")
            else:
                print("    无法查询Windows任务计划程序")
        except Exception as e:
            print(f"    Windows任务查询失败: {str(e)}")
        
        print()
        
        # 5. 检查数据库连接和会话
        print("5. 检查活跃的数据库连接:")
        try:
            connections = db.execute(text("""
                SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO
                FROM information_schema.PROCESSLIST 
                WHERE DB = DATABASE()
                  AND COMMAND != 'Sleep'
                  AND ID != CONNECTION_ID()
                ORDER BY TIME DESC
                LIMIT 10
            """)).fetchall()
            
            if connections:
                print("连接ID | 用户     | 主机            | 数据库 | 命令     | 时间 | 状态     | 信息")
                print("-" * 100)
                for row in connections:
                    info = str(row[7])[:50] if row[7] else 'NULL'
                    print(f"{row[0]:<6} | {row[1]:<8} | {row[2]:<15} | {row[3]:<6} | {row[4]:<8} | {row[5]:<4} | {row[6]:<8} | {info}")
            else:
                print("    没有发现活跃的数据库连接")
        except Exception as e:
            print(f"    数据库连接查询失败: {str(e)}")
        
        print()
        
        # 6. 实时监控数据生成
        print("6. 实时监控数据生成 (监控60秒):")
        initial_count = db.execute(text("""
            SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = 'G7-2025'
        """)).scalar()
        
        print(f"    初始记录数: {initial_count}")
        
        for i in range(12):  # 监控60秒，每5秒检查一次
            time.sleep(5)
            current_count = db.execute(text("""
                SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = 'G7-2025'
            """)).scalar()
            
            new_records = current_count - initial_count
            print(f"    {(i+1)*5}秒后: {current_count} 条记录 (新增: {new_records})")
            
            if new_records > 0:
                # 显示最新记录的详情
                newest = db.execute(text("""
                    SELECT id, aggregation_level, school_id, updated_at,
                           JSON_EXTRACT(statistics_data, '$.updated_at') as json_updated
                    FROM statistical_aggregations 
                    WHERE batch_code = 'G7-2025'
                    ORDER BY updated_at DESC
                    LIMIT 1
                """)).fetchone()
                
                if newest:
                    print(f"      最新记录: ID={newest[0]}, 级别={newest[1]}, 学校={newest[2]}, 时间={newest[3]}")
                    print(f"      JSON时间: {newest[4]}")
        
        print()
        
        # 7. 分析数据内容，寻找生成源线索
        print("7. 分析数据内容寻找生成源线索:")
        content_analysis = db.execute(text("""
            SELECT 
                JSON_EXTRACT(statistics_data, '$.subjects[0].subject_name') as first_subject,
                JSON_EXTRACT(statistics_data, '$.subjects[0].metrics.avg') as first_avg,
                COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code = 'G7-2025'
              AND updated_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)
            GROUP BY first_subject, first_avg
            ORDER BY count DESC
        """)).fetchall()
        
        if content_analysis:
            print("第一个科目 | 平均分 | 出现次数")
            print("-" * 30)
            for row in content_analysis:
                print(f"{row[0] or 'NULL':<10} | {row[1] or 'NULL':<6} | {row[2]}")
        
        print("\n=== 分析完成 ===")


if __name__ == '__main__':
    try:
        trace_data_source()
    except Exception as e:
        print(f"分析出错: {str(e)}")
        import traceback
        traceback.print_exc()