#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
检查数据库锁情况
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db_context

def check_database_locks():
    """检查数据库锁情况"""
    print("=== 数据库锁情况检查 ===\n")

    with get_db_context() as db:
        # 1. 检查当前连接数
        print("1. 当前数据库连接数:")
        connections = db.execute(text("SHOW PROCESSLIST")).fetchall()
        print(f"   总连接数: {len(connections)}")

        # 按状态统计
        status_count = {}
        for conn in connections:
            status = conn.State or 'NULL'
            status_count[status] = status_count.get(status, 0) + 1

        for status, count in status_count.items():
            print(f"   {status}: {count}")

        # 2. 检查锁等待
        print(f"\n2. 当前锁等待情况:")
        try:
            locks = db.execute(text("""
                SELECT
                    r.trx_id waiting_trx_id,
                    r.trx_mysql_thread_id waiting_thread,
                    r.trx_query waiting_query,
                    b.trx_id blocking_trx_id,
                    b.trx_mysql_thread_id blocking_thread,
                    b.trx_query blocking_query
                FROM information_schema.innodb_lock_waits w
                INNER JOIN information_schema.innodb_trx b
                  ON b.trx_id = w.blocking_trx_id
                INNER JOIN information_schema.innodb_trx r
                  ON r.trx_id = w.requesting_trx_id
            """)).fetchall()

            if locks:
                print(f"   发现 {len(locks)} 个锁等待:")
                for lock in locks:
                    print(f"   等待事务 {lock.waiting_trx_id} (线程{lock.waiting_thread})")
                    print(f"   被阻塞by 事务 {lock.blocking_trx_id} (线程{lock.blocking_thread})")
                    print(f"   等待查询: {lock.waiting_query}")
                    print(f"   阻塞查询: {lock.blocking_query}")
                    print()
            else:
                print("   当前没有锁等待")

        except Exception as e:
            print(f"   检查锁等待失败: {e}")

        # 3. 检查长时间运行的事务
        print(f"\n3. 长时间运行的事务:")
        try:
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

            if long_trx:
                print(f"   发现 {len(long_trx)} 个长时间事务:")
                for trx in long_trx:
                    print(f"   事务 {trx.trx_id} (线程{trx.trx_mysql_thread_id}):")
                    print(f"     状态: {trx.trx_state}")
                    print(f"     开始时间: {trx.trx_started}")
                    print(f"     运行时长: {trx.duration_seconds}秒")
                    print(f"     查询: {trx.trx_query}")
                    print()
            else:
                print("   没有发现长时间运行的事务")

        except Exception as e:
            print(f"   检查长事务失败: {e}")

        # 4. 检查正在处理statistical_aggregations表的连接
        print(f"\n4. 正在操作statistical_aggregations表的连接:")
        agg_connections = [conn for conn in connections
                          if conn.Info and 'statistical_aggregations' in str(conn.Info)]

        if agg_connections:
            print(f"   发现 {len(agg_connections)} 个相关连接:")
            for conn in agg_connections:
                print(f"   线程 {conn.Id}: {conn.Command} | {conn.State}")
                print(f"     查询: {conn.Info}")
                print()
        else:
            print("   没有发现操作statistical_aggregations表的连接")

        # 5. 检查表锁
        print(f"\n5. 表锁情况:")
        try:
            table_locks = db.execute(text("SHOW OPEN TABLES WHERE In_use > 0")).fetchall()
            if table_locks:
                print(f"   发现 {len(table_locks)} 个被锁的表:")
                for lock in table_locks:
                    print(f"   {lock.Database}.{lock.Table}: In_use={lock.In_use}")
            else:
                print("   没有发现被锁的表")
        except Exception as e:
            print(f"   检查表锁失败: {e}")

def kill_blocking_processes():
    """杀死可能的阻塞进程"""
    print(f"\n=== 清理可能的阻塞进程 ===\n")

    with get_db_context() as db:
        # 查找可能的阻塞进程
        connections = db.execute(text("SHOW PROCESSLIST")).fetchall()

        # 找出长时间运行且可能阻塞的连接
        blocking_candidates = []
        for conn in connections:
            # 跳过当前连接
            if conn.Id == db.connection.info.connection.thread_id():
                continue

            # 查找长时间运行的连接
            if conn.Time and conn.Time > 300:  # 超过5分钟
                blocking_candidates.append(conn)

        if blocking_candidates:
            print(f"发现 {len(blocking_candidates)} 个可能的阻塞连接:")
            for conn in blocking_candidates:
                print(f"  线程 {conn.Id}: 运行{conn.Time}秒, 状态: {conn.State}")
                print(f"    查询: {conn.Info}")

                # 杀死长时间运行的连接
                try:
                    db.execute(text(f"KILL {conn.Id}"))
                    print(f"    已杀死线程 {conn.Id}")
                except Exception as e:
                    print(f"    杀死线程 {conn.Id} 失败: {e}")
        else:
            print("没有发现需要清理的阻塞连接")

if __name__ == "__main__":
    check_database_locks()

    # 询问是否需要清理
    print(f"\n是否需要清理可能的阻塞进程? (y/n): ", end="")
    response = input().strip().lower()
    if response == 'y':
        kill_blocking_processes()
    else:
        print("跳过清理操作")