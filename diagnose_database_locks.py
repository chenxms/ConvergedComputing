#!/usr/bin/env python3
"""
数据库锁诊断工具
用于检查和解决G7汇聚操作失败问题

使用方法:
python diagnose_database_locks.py

功能:
1. 检查当前数据库连接状态
2. 查找长时间运行的事务
3. 检查锁等待情况
4. 提供解决建议
"""

import sys
import os
from datetime import datetime
import mysql.connector
from mysql.connector import Error

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def create_connection():
    """创建数据库连接"""
    try:
        # 使用环境变量或默认配置
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


def check_current_connections(cursor):
    """检查当前连接状态"""
    print("\n=== 当前数据库连接状态 ===")

    try:
        # 查看所有活跃连接
        cursor.execute("SHOW PROCESSLIST")
        processes = cursor.fetchall()

        print(f"总连接数: {len(processes)}")
        print("\n活跃连接详情:")
        print(f"{'ID':<8} {'用户':<15} {'主机':<20} {'数据库':<15} {'命令':<10} {'时间':<8} {'状态':<20} {'信息'}")
        print("-" * 120)

        long_running = []
        navicat_connections = []

        for process in processes:
            pid, user, host, db, command, time_val, state, info = process
            print(f"{pid:<8} {user:<15} {host:<20} {db or 'NULL':<15} {command:<10} {time_val:<8} {state or '':<20} {(info or '')[:50]}")

            # 标记长时间运行的连接 (>60秒)
            if time_val and time_val > 60:
                long_running.append(process)

            # 检查可能的Navicat连接
            if host and ('navicat' in host.lower() or '127.0.0.1' in host):
                navicat_connections.append(process)

        if long_running:
            print(f"\n警告: 发现 {len(long_running)} 个长时间运行的连接:")
            for process in long_running:
                pid, user, host, db, command, time_val, state, info = process
                print(f"  - ID {pid}: {user}@{host}, 运行时间: {time_val}秒, 状态: {state}")

        if navicat_connections:
            print(f"\n发现 {len(navicat_connections)} 个可能的Navicat连接:")
            for process in navicat_connections:
                pid, user, host, db, command, time_val, state, info = process
                print(f"  - ID {pid}: {user}@{host}, 运行时间: {time_val}秒")

        return long_running, navicat_connections

    except Error as e:
        print(f"错误: 查询连接状态失败: {e}")
        return [], []


def check_innodb_status(cursor):
    """检查InnoDB状态和锁信息"""
    print("\n=== InnoDB 状态检查 ===")

    try:
        # 检查当前事务
        cursor.execute("""
            SELECT trx_id, trx_state, trx_started, trx_requested_lock_id,
                   trx_wait_started, trx_mysql_thread_id, trx_query
            FROM information_schema.INNODB_TRX
        """)
        transactions = cursor.fetchall()

        if transactions:
            print(f"发现 {len(transactions)} 个活跃事务:")
            for trx in transactions:
                trx_id, state, started, lock_id, wait_started, thread_id, query = trx
                print(f"\n事务ID: {trx_id}")
                print(f"  状态: {state}")
                print(f"  开始时间: {started}")
                print(f"  线程ID: {thread_id}")
                if wait_started:
                    print(f"  等待开始: {wait_started}")
                if query:
                    print(f"  查询: {query[:100]}...")
        else:
            print("OK: 没有发现活跃事务")

        # 检查锁等待
        cursor.execute("""
            SELECT requesting_trx_id, requested_lock_id, blocking_trx_id, blocking_lock_id
            FROM information_schema.INNODB_LOCK_WAITS
        """)
        lock_waits = cursor.fetchall()

        if lock_waits:
            print(f"\n警告: 发现 {len(lock_waits)} 个锁等待:")
            for wait in lock_waits:
                req_trx, req_lock, block_trx, block_lock = wait
                print(f"  请求事务 {req_trx} 被事务 {block_trx} 阻塞")
        else:
            print("OK: 没有发现锁等待")

    except Error as e:
        print(f"错误: 查询InnoDB状态失败: {e}")


def check_table_locks(cursor):
    """检查表锁状态"""
    print("\n=== 表锁检查 ===")

    try:
        # 检查statistical_aggregations表的锁状态
        cursor.execute("SHOW OPEN TABLES WHERE In_use > 0")
        locked_tables = cursor.fetchall()

        if locked_tables:
            print("发现被锁定的表:")
            for table_info in locked_tables:
                database, table, in_use, name_locked = table_info
                if 'statistical_aggregations' in table:
                    print(f"警告: 关键表被锁: {database}.{table}, 使用中: {in_use}")
                else:
                    print(f"  {database}.{table}, 使用中: {in_use}")
        else:
            print("OK: 没有发现被锁定的表")

    except Error as e:
        print(f"错误: 查询表锁状态失败: {e}")


def provide_solutions(long_running, navicat_connections):
    """提供解决方案"""
    print("\n=== 解决方案建议 ===")

    if long_running or navicat_connections:
        print("\n立即行动:")

        if navicat_connections:
            print("\n1. 检查Navicat连接:")
            print("   - 关闭所有Navicat窗口中未提交的事务")
            print("   - 执行 COMMIT; 或 ROLLBACK; 来结束事务")
            print("   - 考虑暂时断开Navicat连接")

        if long_running:
            print("\n2. 终止长时间运行的连接:")
            for process in long_running:
                pid, user, host, db, command, time_val, state, info = process
                if time_val > 300:  # 超过5分钟
                    print(f"   建议终止: KILL {pid}; -- {user}@{host} (运行{time_val}秒)")

        print("\n3. 重新运行汇聚操作:")
        print("   python run_g7_pipeline_wrapper.py")

    else:
        print("OK: 数据库状态良好，可以直接重新运行汇聚操作")

    print("\nMySQL配置优化建议:")
    print("   SET GLOBAL innodb_lock_wait_timeout = 120;")
    print("   SET GLOBAL innodb_rollback_on_timeout = ON;")


def main():
    """主函数"""
    print("数据库锁诊断工具")
    print("=" * 50)
    print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    connection = create_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor()

        # 执行各项检查
        long_running, navicat_connections = check_current_connections(cursor)
        check_innodb_status(cursor)
        check_table_locks(cursor)

        # 提供解决方案
        provide_solutions(long_running, navicat_connections)

    except Error as e:
        print(f"错误: 执行检查时出错: {e}")

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("\nOK: 数据库连接已关闭")


if __name__ == "__main__":
    main()