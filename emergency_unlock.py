#!/usr/bin/env python3
"""
紧急解锁脚本 - statistical_aggregations 表
仅在DBA确认的紧急情况下使用
"""

from sqlalchemy import text
from app.database.connection import get_db_context
from datetime import datetime

def emergency_unlock():
    """紧急解锁 statistical_aggregations 表"""
    print(f"Emergency unlock started at: {datetime.now()}")

    try:
        with get_db_context() as db:
            # 1. 强制终止所有相关的长时间进程
            print("Step 1: Killing long running processes...")

            long_processes = db.execute(text("""
                SELECT id, user, time, info
                FROM information_schema.processlist
                WHERE (info LIKE '%statistical_aggregations%' OR info LIKE '%G4-2025%')
                  AND time > 60  -- 超过1分钟
                  AND id != CONNECTION_ID()
            """)).fetchall()

            killed_count = 0
            for proc in long_processes:
                try:
                    db.execute(text(f"KILL {proc[0]}"))
                    print(f"  Killed process {proc[0]} (user: {proc[1]}, time: {proc[2]}s)")
                    killed_count += 1
                except Exception as e:
                    print(f"  Failed to kill process {proc[0]}: {e}")

            print(f"Killed {killed_count} processes")

            # 2. 刷新表锁
            print("Step 2: Flushing table locks...")
            try:
                db.execute(text("FLUSH TABLES statistical_aggregations"))
                print("  Table locks flushed successfully")
            except Exception as e:
                print(f"  Table flush failed: {e}")

            # 3. 验证表可访问性
            print("Step 3: Verifying table accessibility...")
            try:
                result = db.execute(text("SELECT COUNT(*) FROM statistical_aggregations LIMIT 1")).fetchone()
                print(f"  Table accessible, sample count: {result[0] if result else 'N/A'}")
            except Exception as e:
                print(f"  Table access failed: {e}")

            db.commit()
            print("Emergency unlock completed successfully")

    except Exception as e:
        print(f"Emergency unlock failed: {e}")

if __name__ == "__main__":
    print("WARNING: This is an emergency unlock script!")
    print("Only use with DBA approval in critical situations!")
    emergency_unlock()
