
#!/usr/bin/env python3
"""
数据库连接池监控脚本 - 定期运行检查
"""

import time
from datetime import datetime
from app.database.connection import engine, get_db_context
from sqlalchemy import text

def monitor_connections():
    """监控连接状态"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 连接池状态
    pool = engine.pool
    pool_stats = {
        "timestamp": timestamp,
        "pool_size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "utilization": (pool.checkedout() + pool.overflow()) / (pool.size() + engine.pool._max_overflow) * 100
    }

    # 数据库连接状态
    try:
        with get_db_context() as db:
            total_conn = db.execute(text("SELECT COUNT(*) FROM information_schema.processlist")).fetchone()[0]
            active_conn = db.execute(text("SELECT COUNT(*) FROM information_schema.processlist WHERE command != 'Sleep'")).fetchone()[0]

        db_stats = {
            "total_connections": total_conn,
            "active_connections": active_conn,
            "sleeping_connections": total_conn - active_conn
        }
    except Exception as e:
        db_stats = {"error": str(e)}

    # 输出监控信息
    print(f"[{timestamp}] Pool: {pool_stats['utilization']:.1f}% | "
          f"DB Connections: {db_stats.get('total_connections', 'N/A')} | "
          f"Active: {db_stats.get('active_connections', 'N/A')}")

    # 警告检查
    if pool_stats['utilization'] > 80:
        print(f"WARNING: High pool utilization ({pool_stats['utilization']:.1f}%)")

    if db_stats.get('total_connections', 0) > 50:
        print(f"WARNING: High database connection count ({db_stats['total_connections']})")

    return pool_stats, db_stats

if __name__ == "__main__":
    # 可以作为定期任务运行
    # 或者添加到 crontab: */5 * * * * python db_monitor.py
    monitor_connections()
