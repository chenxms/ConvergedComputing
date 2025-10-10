#!/usr/bin/env python3
"""
终止阻塞连接工具
用于清理数据库中的长时间运行连接和事务

使用方法:
python kill_blocking_connections.py

功能:
1. 终止长时间运行的连接 (超过5分钟)
2. 强制提交或回滚长时间事务
3. 清理Sleep状态的连接
"""

import sys
import os
from datetime import datetime
import mysql.connector
from mysql.connector import Error

def create_connection():
    """创建数据库连接"""
    try:
        config = {
            'host': os.getenv("DATABASE_HOST", "117.72.14.166"),
            'port': int(os.getenv("DATABASE_PORT", "23506")),
            'user': os.getenv("DATABASE_USER", "root"),
            'password': os.getenv("DATABASE_PASSWORD", "mysql_Lujing2022"),
            'database': os.getenv("DATABASE_NAME", "appraisal_test"),
            'charset': 'utf8mb4',
            'autocommit': False,
            'connection_timeout': 10
        }
        connection = mysql.connector.connect(**config)
        return connection
    except Error as e:
        print(f"错误: 数据库连接失败: {e}")
        return None

def kill_long_running_connections(cursor, min_time=300):
    """终止长时间运行的连接"""
    print(f"\n=== 终止运行超过{min_time}秒的连接 ===")

    try:
        # 查找长时间运行的连接
        cursor.execute("SHOW PROCESSLIST")
        processes = cursor.fetchall()

        killed_count = 0
        for process in processes:
            pid, user, host, db, command, time_val, state, info = process

            # 跳过系统进程和当前连接
            if user == 'event_scheduler' or command == 'Query':
                continue

            # 终止长时间运行的连接
            if time_val and time_val > min_time:
                print(f"终止连接 ID {pid}: {user}@{host} (运行{time_val}秒)")
                try:
                    cursor.execute(f"KILL {pid}")
                    killed_count += 1
                    print(f"  OK: 已终止连接 {pid}")
                except Error as e:
                    print(f"  错误: 终止连接 {pid} 失败: {e}")

        if killed_count == 0:
            print("没有找到需要终止的长时间连接")
        else:
            print(f"总共终止了 {killed_count} 个连接")

    except Error as e:
        print(f"错误: 查询或终止连接失败: {e}")

def clear_blocking_transactions(cursor):
    """清理阻塞事务"""
    print("\n=== 清理阻塞事务 ===")

    try:
        # 查找活跃事务
        cursor.execute("""
            SELECT trx_id, trx_state, trx_started, trx_mysql_thread_id
            FROM information_schema.INNODB_TRX
            WHERE trx_state = 'RUNNING'
        """)
        transactions = cursor.fetchall()

        if not transactions:
            print("没有发现活跃事务")
            return

        print(f"发现 {len(transactions)} 个活跃事务:")
        for trx in transactions:
            trx_id, state, started, thread_id = trx
            print(f"\n事务ID: {trx_id}")
            print(f"  状态: {state}")
            print(f"  开始时间: {started}")
            print(f"  线程ID: {thread_id}")

            # 计算事务运行时间
            if started:
                now = datetime.now()
                duration = (now - started).total_seconds()
                print(f"  运行时间: {duration:.0f}秒")

                # 如果事务运行超过10分钟，建议终止
                if duration > 600:
                    print(f"  建议终止线程: KILL {thread_id}")
                    try:
                        cursor.execute(f"KILL {thread_id}")
                        print(f"  OK: 已终止事务线程 {thread_id}")
                    except Error as e:
                        print(f"  错误: 终止事务线程 {thread_id} 失败: {e}")

    except Error as e:
        # MySQL 8.0可能没有INNODB_TRX表或字段名不同
        print(f"提示: 无法查询事务信息: {e}")

def optimize_mysql_settings(cursor):
    """优化MySQL锁等待设置"""
    print("\n=== 优化MySQL设置 ===")

    settings = [
        ("innodb_lock_wait_timeout", "120"),
        ("innodb_rollback_on_timeout", "ON")
    ]

    for setting, value in settings:
        try:
            cursor.execute(f"SET GLOBAL {setting} = {value}")
            print(f"OK: 设置 {setting} = {value}")
        except Error as e:
            print(f"错误: 设置 {setting} 失败: {e}")

def verify_cleanup(cursor):
    """验证清理结果"""
    print("\n=== 验证清理结果 ===")

    try:
        # 检查当前连接数
        cursor.execute("SHOW PROCESSLIST")
        processes = cursor.fetchall()
        active_connections = len([p for p in processes if p[1] != 'event_scheduler'])

        print(f"当前活跃连接数: {active_connections}")

        # 检查长时间运行的连接
        long_running = [p for p in processes if p[5] and p[5] > 300 and p[1] != 'event_scheduler']
        if long_running:
            print(f"仍有 {len(long_running)} 个长时间运行的连接")
            for process in long_running:
                pid, user, host, db, command, time_val, state, info = process
                print(f"  - ID {pid}: {user}@{host}, 运行时间: {time_val}秒")
        else:
            print("OK: 没有长时间运行的连接")

    except Error as e:
        print(f"错误: 验证失败: {e}")

def main():
    """主函数"""
    print("数据库阻塞连接清理工具")
    print("=" * 50)
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    connection = create_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor()

        # 执行清理步骤
        kill_long_running_connections(cursor, min_time=300)  # 5分钟
        clear_blocking_transactions(cursor)
        optimize_mysql_settings(cursor)

        # 提交更改
        connection.commit()

        # 验证清理结果
        verify_cleanup(cursor)

        print("\nOK: 清理完成! 现在可以重新运行汇聚操作")

    except Error as e:
        print(f"错误: 执行清理时出错: {e}")
        connection.rollback()

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("\nOK: 数据库连接已关闭")

if __name__ == "__main__":
    # 确认执行
    response = input("\n警告: 此操作将终止长时间运行的数据库连接。确认执行? (y/N): ")
    if response.lower() in ['y', 'yes']:
        main()
    else:
        print("操作已取消")