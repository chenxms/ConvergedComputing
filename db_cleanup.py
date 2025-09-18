#!/usr/bin/env python3
"""
数据库连接清理脚本
"""

import time
import logging
from datetime import datetime
from sqlalchemy import text
from app.database.connection import get_db_context

def cleanup_long_sleeping_connections():
    """清理长时间睡眠的连接"""
    print("=" * 50)
    print("Database Connection Cleanup")
    print("=" * 50)
    print(f"Cleanup Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    killed_connections = []
    errors = []

    try:
        with get_db_context() as db:
            # 获取长时间睡眠的连接
            long_sleep = db.execute(text("""
                SELECT id, user, host, time
                FROM information_schema.processlist
                WHERE command = 'Sleep'
                  AND time > 600  -- 超过10分钟
                  AND user != 'system user'
                  AND id != CONNECTION_ID()  -- 不终止当前连接
                ORDER BY time DESC
            """)).fetchall()

            print(f"\nFound {len(long_sleep)} long sleeping connections to clean:")

            for conn in long_sleep:
                try:
                    # 终止连接
                    db.execute(text(f"KILL {conn[0]}"))
                    minutes = conn[3] // 60

                    killed_info = {
                        'id': conn[0],
                        'user': conn[1],
                        'host': conn[2],
                        'sleep_time_minutes': minutes
                    }
                    killed_connections.append(killed_info)

                    print(f"KILLED: ID={conn[0]} User={conn[1]} Host={conn[2]} Sleep={minutes}min")

                except Exception as e:
                    error_msg = f"Failed to kill connection {conn[0]}: {str(e)}"
                    errors.append(error_msg)
                    print(f"ERROR: {error_msg}")

            db.commit()

    except Exception as e:
        error_msg = f"Cleanup failed: {str(e)}"
        errors.append(error_msg)
        print(f"ERROR: {error_msg}")

    return {
        'killed_count': len(killed_connections),
        'killed_connections': killed_connections,
        'errors': errors
    }

def cleanup_long_running_queries():
    """清理长时间运行的查询"""
    print("\n[Long Running Query Cleanup]")

    killed_queries = []
    errors = []

    try:
        with get_db_context() as db:
            # 获取长时间运行的查询 (超过5分钟)
            long_queries = db.execute(text("""
                SELECT id, user, host, time, command, state,
                       SUBSTRING(COALESCE(info, ''), 1, 100) as query_preview
                FROM information_schema.processlist
                WHERE command != 'Sleep'
                  AND time > 300  -- 超过5分钟
                  AND user != 'system user'
                  AND user != 'event_scheduler'
                  AND id != CONNECTION_ID()
                ORDER BY time DESC
            """)).fetchall()

            print(f"Found {len(long_queries)} long running queries to clean:")

            for query in long_queries:
                try:
                    # 终止查询
                    db.execute(text(f"KILL {query[0]}"))
                    minutes = query[3] // 60

                    killed_info = {
                        'id': query[0],
                        'user': query[1],
                        'host': query[2],
                        'runtime_minutes': minutes,
                        'command': query[4],
                        'state': query[5],
                        'query_preview': query[6]
                    }
                    killed_queries.append(killed_info)

                    print(f"KILLED: ID={query[0]} User={query[1]} Runtime={minutes}min")
                    print(f"        Command={query[4]} State={query[5]}")
                    if query[6]:
                        print(f"        Query={query[6]}")

                except Exception as e:
                    error_msg = f"Failed to kill query {query[0]}: {str(e)}"
                    errors.append(error_msg)
                    print(f"ERROR: {error_msg}")

            db.commit()

    except Exception as e:
        error_msg = f"Query cleanup failed: {str(e)}"
        errors.append(error_msg)
        print(f"ERROR: {error_msg}")

    return {
        'killed_count': len(killed_queries),
        'killed_queries': killed_queries,
        'errors': errors
    }

def optimize_database():
    """执行数据库优化"""
    print("\n[Database Optimization]")

    optimizations = []
    errors = []

    try:
        with get_db_context() as db:
            # 1. 刷新查询缓存
            try:
                db.execute(text("FLUSH QUERY CACHE"))
                optimizations.append("Query cache flushed")
                print("SUCCESS: Query cache flushed")
            except Exception as e:
                errors.append(f"Query cache flush failed: {str(e)}")
                print(f"WARNING: Query cache flush failed: {str(e)}")

            # 2. 刷新表缓存
            try:
                db.execute(text("FLUSH TABLES"))
                optimizations.append("Table cache flushed")
                print("SUCCESS: Table cache flushed")
            except Exception as e:
                errors.append(f"Table cache flush failed: {str(e)}")
                print(f"ERROR: Table cache flush failed: {str(e)}")

            db.commit()

    except Exception as e:
        error_msg = f"Database optimization failed: {str(e)}"
        errors.append(error_msg)
        print(f"ERROR: {error_msg}")

    return {
        'optimizations': optimizations,
        'errors': errors
    }

def check_after_cleanup():
    """清理后检查状态"""
    print("\n[Post-Cleanup Status Check]")

    try:
        with get_db_context() as db:
            # 检查当前连接状态
            total = db.execute(text("SELECT COUNT(*) FROM information_schema.processlist")).fetchone()[0]
            active = db.execute(text("SELECT COUNT(*) FROM information_schema.processlist WHERE command != 'Sleep'")).fetchone()[0]
            sleeping = total - active

            print(f"Current Status:")
            print(f"  Total Connections: {total}")
            print(f"  Active Connections: {active}")
            print(f"  Sleeping Connections: {sleeping}")

            # 检查是否还有长时间连接
            long_sleep_count = db.execute(text("""
                SELECT COUNT(*) FROM information_schema.processlist
                WHERE command = 'Sleep' AND time > 600
            """)).fetchone()[0]

            long_query_count = db.execute(text("""
                SELECT COUNT(*) FROM information_schema.processlist
                WHERE command != 'Sleep' AND time > 300 AND user != 'event_scheduler'
            """)).fetchone()[0]

            print(f"  Long Sleeping Connections (>10min): {long_sleep_count}")
            print(f"  Long Running Queries (>5min): {long_query_count}")

            if long_sleep_count == 0 and long_query_count == 0:
                print("SUCCESS: No problematic connections remaining")
            else:
                print("WARNING: Some problematic connections still exist")

            return {
                'total': total,
                'active': active,
                'sleeping': sleeping,
                'long_sleep': long_sleep_count,
                'long_queries': long_query_count
            }

    except Exception as e:
        print(f"ERROR: Post-cleanup check failed: {str(e)}")
        return None

def main():
    """主清理流程"""
    print("Starting database cleanup process...")

    # 1. 清理长时间睡眠连接
    sleep_result = cleanup_long_sleeping_connections()

    # 2. 清理长时间运行查询
    query_result = cleanup_long_running_queries()

    # 3. 数据库优化
    opt_result = optimize_database()

    # 4. 清理后状态检查
    final_status = check_after_cleanup()

    # 5. 总结报告
    print("\n" + "=" * 50)
    print("CLEANUP SUMMARY REPORT")
    print("=" * 50)

    print(f"Sleeping Connections Killed: {sleep_result['killed_count']}")
    print(f"Long Running Queries Killed: {query_result['killed_count']}")
    print(f"Optimizations Applied: {len(opt_result['optimizations'])}")

    total_errors = len(sleep_result['errors']) + len(query_result['errors']) + len(opt_result['errors'])
    print(f"Total Errors: {total_errors}")

    if total_errors > 0:
        print("\nERRORS ENCOUNTERED:")
        for error in sleep_result['errors'] + query_result['errors'] + opt_result['errors']:
            print(f"  - {error}")

    if final_status:
        print(f"\nFINAL STATUS:")
        print(f"  Total Connections: {final_status['total']}")
        print(f"  Problematic Connections: {final_status['long_sleep'] + final_status['long_queries']}")

    print("\nCleanup process completed!")
    print("Recommendation: Monitor database connections regularly")

if __name__ == "__main__":
    main()