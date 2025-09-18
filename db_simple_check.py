#!/usr/bin/env python3
"""
数据库连接简单检查 - ASCII版本
"""

import time
import logging
from datetime import datetime
from sqlalchemy import text
from app.database.connection import get_db_context

def check_database():
    """检查数据库连接状态"""
    print("=" * 50)
    print("Database Connection Status Check")
    print("=" * 50)
    print(f"Check Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        with get_db_context() as db:
            # 1. 基本统计
            print("\n[Connection Statistics]")

            total = db.execute(text("SELECT COUNT(*) FROM information_schema.processlist")).fetchone()[0]
            active = db.execute(text("SELECT COUNT(*) FROM information_schema.processlist WHERE command != 'Sleep'")).fetchone()[0]
            sleeping = total - active

            print(f"Total Connections: {total}")
            print(f"Active Connections: {active}")
            print(f"Sleeping Connections: {sleeping}")

            # 2. 长时间查询
            print("\n[Long Running Queries]")

            long_queries = db.execute(text("""
                SELECT id, user, host, time, command, state
                FROM information_schema.processlist
                WHERE command != 'Sleep' AND time > 10 AND user != 'event_scheduler'
                ORDER BY time DESC LIMIT 5
            """)).fetchall()

            if long_queries:
                print(f"Found {len(long_queries)} long running queries:")
                for q in long_queries:
                    print(f"  ID:{q[0]} User:{q[1]} Time:{q[3]}s Command:{q[4]}")
            else:
                print("No long running queries found")

            # 3. 长时间睡眠连接
            print("\n[Long Sleeping Connections]")

            long_sleep = db.execute(text("""
                SELECT id, user, host, time
                FROM information_schema.processlist
                WHERE command = 'Sleep' AND time > 600
                ORDER BY time DESC LIMIT 5
            """)).fetchall()

            if long_sleep:
                print(f"Found {len(long_sleep)} long sleeping connections:")
                for conn in long_sleep:
                    minutes = conn[3] // 60
                    print(f"  ID:{conn[0]} User:{conn[1]} Host:{conn[2]} Sleep:{minutes}min")
            else:
                print("No long sleeping connections found")

            # 4. 数据库版本和配置
            print("\n[Database Configuration]")

            version = db.execute(text("SELECT VERSION()")).fetchone()[0]
            print(f"Database Version: {version}")

            max_conn = db.execute(text("SHOW VARIABLES LIKE 'max_connections'")).fetchone()
            if max_conn:
                max_connections = int(max_conn[1])
                utilization = (total / max_connections * 100)
                print(f"Max Connections: {max_connections}")
                print(f"Connection Usage: {utilization:.1f}%")

            # 5. 慢查询配置
            print("\n[Slow Query Configuration]")

            slow_log = db.execute(text("SHOW VARIABLES LIKE 'slow_query_log'")).fetchone()
            long_time = db.execute(text("SHOW VARIABLES LIKE 'long_query_time'")).fetchone()

            print(f"Slow Query Log: {slow_log[1] if slow_log else 'Unknown'}")
            print(f"Long Query Time: {long_time[1] if long_time else 'Unknown'}s")

            # 6. 清理建议
            print("\n" + "=" * 50)
            print("[Cleanup Recommendations]")

            if long_sleep:
                print(f"1. Clean up {len(long_sleep)} long sleeping connections")
                print("   Commands to execute:")
                for conn in long_sleep[:3]:
                    print(f"   KILL {conn[0]};  -- User:{conn[1]}, Sleep:{conn[3]//60}min")

            if long_queries:
                print(f"2. Check {len(long_queries)} long running queries")
                print("   Consider optimizing these queries")

            if total > 50:
                print(f"3. High connection count ({total})")
                print("   Check application connection pool settings")

            if not long_sleep and not long_queries and total <= 50:
                print("Database connection status is healthy")

            print("\n[Optimization Commands]")
            print("FLUSH QUERY CACHE;  -- Clear query cache")
            print("FLUSH TABLES;       -- Refresh table cache")

            print("\n" + "=" * 50)
            print("Check completed!")

            return {
                'total': total,
                'active': active,
                'sleeping': sleeping,
                'long_queries': len(long_queries),
                'long_sleep': len(long_sleep)
            }

    except Exception as e:
        print(f"Error during check: {e}")
        return None

if __name__ == "__main__":
    result = check_database()

    if result:
        print(f"\nSummary:")
        print(f"- Total connections: {result['total']}")
        print(f"- Long running queries: {result['long_queries']}")
        print(f"- Long sleeping connections: {result['long_sleep']}")

        if result['long_queries'] > 0 or result['long_sleep'] > 0:
            print("\nAction Required: Consider cleaning up problematic connections")
        else:
            print("\nStatus: Database connections are healthy")