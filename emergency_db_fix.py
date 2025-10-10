#!/usr/bin/env python3
"""紧急数据库连接修复脚本"""

import os
import sys
import time
from contextlib import contextmanager
from typing import List, Dict, Any

import pymysql
from sqlalchemy import create_engine, text


def get_db_connection():
    """获取直接的数据库连接"""
    host = os.getenv("DATABASE_HOST", "117.72.14.166")
    port = int(os.getenv("DATABASE_PORT", "23506"))
    user = os.getenv("DATABASE_USER", "root")
    password = os.getenv("DATABASE_PASSWORD", "mysql_Lujing2022")
    database = os.getenv("DATABASE_NAME", "appraisal_test")

    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset='utf8mb4',
        autocommit=True
    )


@contextmanager
def emergency_db_session():
    """紧急数据库会话上下文"""
    conn = None
    try:
        conn = get_db_connection()
        yield conn
    except Exception as e:
        print(f"数据库连接错误: {e}")
        raise
    finally:
        if conn:
            conn.close()


def check_processlist() -> List[Dict[str, Any]]:
    """检查当前数据库进程列表"""
    with emergency_db_session() as conn:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SHOW PROCESSLIST")
        processes = cursor.fetchall()
        return processes


def check_innodb_locks() -> List[Dict[str, Any]]:
    """检查InnoDB锁状态"""
    with emergency_db_session() as conn:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        try:
            # MySQL 8.x 使用 performance_schema
            cursor.execute("""
                SELECT
                    waiting_thread_id,
                    waiting_pid,
                    blocking_thread_id,
                    blocking_pid,
                    object_name
                FROM performance_schema.data_lock_waits
                LIMIT 50
            """)
            lock_waits = cursor.fetchall()

            if not lock_waits:
                # 尝试旧版本的表
                cursor.execute("""
                    SELECT
                        requesting_trx_id,
                        blocking_trx_id
                    FROM information_schema.innodb_lock_waits
                    LIMIT 50
                """)
                lock_waits = cursor.fetchall()

            return lock_waits
        except Exception as e:
            print(f"检查锁状态失败: {e}")
            return []


def get_long_running_queries(min_time=30) -> List[Dict[str, Any]]:
    """获取长时间运行的查询"""
    processes = check_processlist()
    long_queries = []

    for proc in processes:
        if (proc.get('Time', 0) > min_time and
            proc.get('Command') not in ['Sleep', 'Binlog Dump'] and
            proc.get('Info') and proc.get('Info') != 'NULL'):
            long_queries.append(proc)

    return long_queries


def kill_blocking_processes(dry_run=True) -> List[int]:
    """杀掉阻塞的进程"""
    killed_pids = []

    # 首先检查锁等待
    lock_waits = check_innodb_locks()
    blocking_threads = set()

    for lock in lock_waits:
        blocking_thread = lock.get('blocking_thread')
        if blocking_thread:
            blocking_threads.add(blocking_thread)

    # 获取长时间运行的查询
    long_queries = get_long_running_queries(min_time=60)  # 超过1分钟的查询

    print(f"发现 {len(blocking_threads)} 个阻塞线程")
    print(f"发现 {len(long_queries)} 个长时间运行的查询")

    # 合并需要杀掉的进程
    processes_to_kill = blocking_threads.copy()
    for query in long_queries:
        if query.get('Id'):
            processes_to_kill.add(query['Id'])

    if not dry_run and processes_to_kill:
        with emergency_db_session() as conn:
            cursor = conn.cursor()
            for pid in processes_to_kill:
                try:
                    cursor.execute(f"KILL {pid}")
                    killed_pids.append(pid)
                    print(f"已杀掉进程: {pid}")
                except Exception as e:
                    print(f"杀掉进程 {pid} 失败: {e}")
    else:
        print(f"[DRY RUN] 将要杀掉的进程: {list(processes_to_kill)}")

    return killed_pids


def optimize_mysql_settings():
    """优化MySQL设置"""
    optimizations = [
        "SET GLOBAL innodb_lock_wait_timeout = 10",
        "SET GLOBAL wait_timeout = 300",
        "SET GLOBAL interactive_timeout = 300",
        "SET GLOBAL max_connections = 500",
        "SET GLOBAL thread_cache_size = 50",
    ]

    with emergency_db_session() as conn:
        cursor = conn.cursor()
        for sql in optimizations:
            try:
                cursor.execute(sql)
                print(f"执行成功: {sql}")
            except Exception as e:
                print(f"执行失败 {sql}: {e}")


def emergency_cleanup():
    """紧急清理"""
    print("=== 数据库紧急修复开始 ===")

    # 1. 检查当前状态
    print("\n1. 检查当前进程状态...")
    processes = check_processlist()
    print(f"当前活跃连接数: {len(processes)}")

    # 2. 检查锁状态
    print("\n2. 检查锁状态...")
    locks = check_innodb_locks()
    if locks:
        print(f"发现 {len(locks)} 个锁等待")
        for lock in locks:
            print(f"  阻塞线程: {lock.get('blocking_thread')} -> 等待线程: {lock.get('waiting_thread')}")
    else:
        print("没有发现锁等待")

    # 3. 检查长时间运行的查询
    print("\n3. 检查长时间查询...")
    long_queries = get_long_running_queries(30)
    if long_queries:
        print(f"发现 {len(long_queries)} 个长时间查询:")
        for query in long_queries:
            print(f"  PID: {query.get('Id')}, 时间: {query.get('Time')}s, 查询: {query.get('Info', '')[:100]}")
    else:
        print("没有发现长时间查询")

    # 4. 询问是否执行清理
    response = input("\n是否执行紧急清理? (y/N): ").strip().lower()
    if response == 'y':
        print("\n4. 执行进程清理...")
        killed = kill_blocking_processes(dry_run=False)
        print(f"已清理 {len(killed)} 个进程")

        print("\n5. 优化MySQL设置...")
        optimize_mysql_settings()

        # 等待一下让设置生效
        time.sleep(2)

        print("\n6. 验证清理结果...")
        new_processes = check_processlist()
        print(f"清理后活跃连接数: {len(new_processes)}")

    print("\n=== 紧急修复完成 ===")


if __name__ == "__main__":
    try:
        emergency_cleanup()
    except KeyboardInterrupt:
        print("\n用户中断操作")
    except Exception as e:
        print(f"紧急修复失败: {e}")
        sys.exit(1)