#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
修复数据库锁问题
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db_context

def fix_database_locks():
    """修复数据库锁问题"""
    print("=== 修复数据库锁问题 ===\n")

    with get_db_context() as db:
        # 1. 先查看当前阻塞情况
        print("1. 当前长时间运行的事务:")
        long_trx = db.execute(text("""
            SELECT
                trx_id,
                trx_mysql_thread_id,
                trx_state,
                trx_started,
                TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_seconds,
                trx_query
            FROM information_schema.innodb_trx
            WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 30
            ORDER BY duration_seconds DESC
        """)).fetchall()

        blocking_threads = []
        for trx in long_trx:
            print(f"   事务 {trx.trx_id} (线程{trx.trx_mysql_thread_id}):")
            print(f"     运行时长: {trx.duration_seconds}秒")
            print(f"     状态: {trx.trx_state}")

            # 超过300秒(5分钟)的事务视为可能的阻塞者
            if trx.duration_seconds > 300:
                blocking_threads.append(trx.trx_mysql_thread_id)
                print(f"     -> 标记为阻塞线程")

        # 2. 杀死阻塞线程
        if blocking_threads:
            print(f"\n2. 清理阻塞线程:")
            for thread_id in blocking_threads:
                try:
                    db.execute(text(f"KILL {thread_id}"))
                    print(f"   已杀死线程 {thread_id}")
                except Exception as e:
                    print(f"   杀死线程 {thread_id} 失败: {e}")
        else:
            print(f"\n2. 没有发现需要清理的阻塞线程")

        # 3. 检查修复后的状态
        print(f"\n3. 修复后状态检查:")

        # 检查还有没有长时间事务
        remaining_trx = db.execute(text("""
            SELECT COUNT(*) as count
            FROM information_schema.innodb_trx
            WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 30
        """)).scalar()

        print(f"   剩余长时间事务: {remaining_trx}")

        # 检查表锁
        table_locks = db.execute(text("SHOW OPEN TABLES WHERE In_use > 0")).fetchall()
        if table_locks:
            print(f"   剩余表锁: {len(table_locks)}个")
            for lock in table_locks:
                print(f"     {lock.Database}.{lock.Table}: In_use={lock.In_use}")
        else:
            print(f"   剩余表锁: 0个")

        # 检查连接状态
        connections = db.execute(text("SHOW PROCESSLIST")).fetchall()
        active_queries = [conn for conn in connections if conn.Info and conn.Info.strip()]
        print(f"   活跃查询数: {len(active_queries)}")

if __name__ == "__main__":
    fix_database_locks()