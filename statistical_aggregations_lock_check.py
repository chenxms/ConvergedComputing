#!/usr/bin/env python3
"""
statistical_aggregations 表锁定和长事务排查脚本
专门针对 G4-2025 批次的问题排查
"""

import time
import json
from datetime import datetime
from sqlalchemy import text
from app.database.connection import get_db_context

def check_statistical_aggregations_locks():
    """检查 statistical_aggregations 表的锁定情况"""
    print("=" * 80)
    print("STATISTICAL_AGGREGATIONS TABLE LOCK ANALYSIS")
    print("=" * 80)
    print(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        with get_db_context() as db:
            # 1. 检查当前表锁
            print("\n[1] TABLE LOCK STATUS")

            # 检查当前是否有表级锁
            table_locks = db.execute(text("""
                SHOW OPEN TABLES
                WHERE Database = DATABASE()
                AND Table LIKE '%statistical_aggregations%'
                AND In_use > 0
            """)).fetchall()

            if table_locks:
                print("Found active table locks:")
                for lock in table_locks:
                    print(f"  Database: {lock[0]}, Table: {lock[1]}, In_use: {lock[2]}, Name_locked: {lock[3]}")
            else:
                print("No active table locks found")

            # 2. 检查 InnoDB 锁等待
            print("\n[2] INNODB LOCK WAITS")

            try:
                lock_waits = db.execute(text("""
                    SELECT
                        r.trx_id as requesting_trx_id,
                        r.trx_mysql_thread_id as requesting_thread,
                        r.trx_query as requesting_query,
                        r.trx_operation_state as requesting_state,
                        r.trx_started as requesting_started,
                        b.trx_id as blocking_trx_id,
                        b.trx_mysql_thread_id as blocking_thread,
                        b.trx_query as blocking_query,
                        b.trx_operation_state as blocking_state,
                        b.trx_started as blocking_started,
                        l.lock_table,
                        l.lock_index,
                        l.lock_mode,
                        l.lock_type
                    FROM
                        information_schema.innodb_lock_waits w
                        INNER JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id
                        INNER JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id
                        INNER JOIN information_schema.innodb_locks l ON l.lock_trx_id = w.blocking_trx_id
                    WHERE
                        l.lock_table LIKE '%statistical_aggregations%'
                """)).fetchall()

                if lock_waits:
                    print("Found InnoDB lock waits:")
                    for wait in lock_waits:
                        print(f"  Requesting Transaction: {wait[0]} (Thread: {wait[1]})")
                        print(f"    Query: {wait[2][:100] if wait[2] else 'NULL'}")
                        print(f"    Started: {wait[4]}")
                        print(f"  Blocking Transaction: {wait[5]} (Thread: {wait[6]})")
                        print(f"    Query: {wait[7][:100] if wait[7] else 'NULL'}")
                        print(f"    Started: {wait[9]}")
                        print(f"  Lock Info: Table={wait[10]}, Index={wait[11]}, Mode={wait[12]}, Type={wait[13]}")
                        print("-" * 60)
                else:
                    print("No InnoDB lock waits found")
            except Exception as e:
                print(f"InnoDB lock check failed (MySQL version may not support): {e}")

            # 3. 检查长时间运行的事务
            print("\n[3] LONG RUNNING TRANSACTIONS")

            try:
                long_transactions = db.execute(text("""
                    SELECT
                        trx_id,
                        trx_state,
                        trx_started,
                        trx_mysql_thread_id,
                        trx_query,
                        trx_operation_state,
                        trx_tables_in_use,
                        trx_tables_locked,
                        trx_lock_structs,
                        trx_rows_locked,
                        trx_rows_modified,
                        TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_seconds
                    FROM
                        information_schema.innodb_trx
                    WHERE
                        TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 30  -- 超过30秒的事务
                    ORDER BY trx_started
                """)).fetchall()

                if long_transactions:
                    print(f"Found {len(long_transactions)} long running transactions:")
                    for trx in long_transactions:
                        duration_minutes = trx[11] // 60
                        print(f"  Transaction ID: {trx[0]}")
                        print(f"    State: {trx[1]}")
                        print(f"    Thread ID: {trx[3]}")
                        print(f"    Started: {trx[2]}")
                        print(f"    Duration: {duration_minutes} minutes ({trx[11]} seconds)")
                        print(f"    Tables in use: {trx[6]}, Tables locked: {trx[7]}")
                        print(f"    Lock structs: {trx[8]}, Rows locked: {trx[9]}, Rows modified: {trx[10]}")
                        if trx[4]:
                            print(f"    Query: {trx[4][:150]}...")
                        print("-" * 60)
                else:
                    print("No long running transactions found")
            except Exception as e:
                print(f"Transaction check failed: {e}")

            # 4. 检查 G4-2025 批次相关的进程
            print("\n[4] G4-2025 BATCH RELATED PROCESSES")

            g4_processes = db.execute(text("""
                SELECT
                    id,
                    user,
                    host,
                    db,
                    command,
                    time,
                    state,
                    info
                FROM
                    information_schema.processlist
                WHERE
                    (info LIKE '%G4-2025%'
                     OR info LIKE '%statistical_aggregations%'
                     OR command != 'Sleep')
                    AND id != CONNECTION_ID()
                ORDER BY time DESC
            """)).fetchall()

            if g4_processes:
                print(f"Found {len(g4_processes)} processes related to G4-2025 or statistical_aggregations:")
                for proc in g4_processes:
                    print(f"  Process ID: {proc[0]}")
                    print(f"    User: {proc[1]}, Host: {proc[2]}, Database: {proc[3]}")
                    print(f"    Command: {proc[4]}, Time: {proc[5]}s, State: {proc[6]}")
                    if proc[7]:
                        print(f"    Query: {proc[7][:200]}...")
                    print("-" * 60)
            else:
                print("No G4-2025 related processes found")

            return {
                'table_locks': len(table_locks) if table_locks else 0,
                'lock_waits': len(lock_waits) if 'lock_waits' in locals() else 0,
                'long_transactions': len(long_transactions) if 'long_transactions' in locals() else 0,
                'g4_processes': len(g4_processes) if g4_processes else 0
            }

    except Exception as e:
        print(f"Analysis failed: {e}")
        return None

def check_g4_2025_specific_locks():
    """专门检查 G4-2025 批次的锁定情况"""
    print("\n" + "=" * 80)
    print("G4-2025 BATCH SPECIFIC LOCK ANALYSIS")
    print("=" * 80)

    try:
        with get_db_context() as db:
            # 1. 检查 G4-2025 批次的数据量
            print("\n[1] G4-2025 DATA VOLUME CHECK")

            try:
                g4_count = db.execute(text("""
                    SELECT COUNT(*) as count
                    FROM statistical_aggregations
                    WHERE batch_code = 'G4-2025'
                """)).fetchone()

                print(f"G4-2025 records in statistical_aggregations: {g4_count[0] if g4_count else 0}")
            except Exception as e:
                print(f"Data volume check failed: {e}")

            # 2. 检查最近的 G4-2025 相关操作
            print("\n[2] RECENT G4-2025 OPERATIONS")

            try:
                recent_ops = db.execute(text("""
                    SELECT
                        MAX(updated_at) as last_update,
                        COUNT(*) as total_records,
                        COUNT(DISTINCT school_code) as school_count
                    FROM statistical_aggregations
                    WHERE batch_code = 'G4-2025'
                """)).fetchone()

                if recent_ops:
                    print(f"Last update: {recent_ops[0]}")
                    print(f"Total records: {recent_ops[1]}")
                    print(f"School count: {recent_ops[2]}")
            except Exception as e:
                print(f"Recent operations check failed: {e}")

            # 3. 检查可能的死锁历史
            print("\n[3] DEADLOCK HISTORY CHECK")

            try:
                deadlock_info = db.execute(text("SHOW ENGINE INNODB STATUS")).fetchone()
                if deadlock_info:
                    innodb_status = deadlock_info[2]

                    # 查找死锁相关信息
                    if "LATEST DETECTED DEADLOCK" in innodb_status:
                        deadlock_section = innodb_status.split("LATEST DETECTED DEADLOCK")[1].split("WE ROLL BACK")[0]
                        if "statistical_aggregations" in deadlock_section:
                            print("Found deadlock history involving statistical_aggregations table")
                            print("Deadlock details (last 200 chars):")
                            print(deadlock_section[-200:])
                        else:
                            print("No recent deadlocks involving statistical_aggregations")
                    else:
                        print("No recent deadlock history found")
            except Exception as e:
                print(f"Deadlock history check failed: {e}")

            # 4. 检查表的索引使用情况
            print("\n[4] INDEX USAGE ANALYSIS")

            try:
                index_usage = db.execute(text("""
                    SHOW INDEX FROM statistical_aggregations
                """)).fetchall()

                print("Available indexes:")
                for idx in index_usage:
                    print(f"  Index: {idx[2]}, Column: {idx[4]}, Unique: {idx[1]}")
            except Exception as e:
                print(f"Index analysis failed: {e}")

    except Exception as e:
        print(f"G4-2025 specific analysis failed: {e}")

def generate_lock_release_commands():
    """生成锁释放命令"""
    print("\n" + "=" * 80)
    print("LOCK RELEASE COMMANDS")
    print("=" * 80)

    try:
        with get_db_context() as db:
            # 1. 获取需要终止的进程
            problem_processes = []

            # 检查长时间运行的 G4-2025 相关进程
            long_g4_processes = db.execute(text("""
                SELECT
                    id, user, host, time, state, info
                FROM information_schema.processlist
                WHERE
                    (info LIKE '%G4-2025%' OR info LIKE '%statistical_aggregations%')
                    AND time > 300  -- 超过5分钟
                    AND command != 'Sleep'
                    AND id != CONNECTION_ID()
                ORDER BY time DESC
            """)).fetchall()

            # 检查长时间事务的线程
            long_trx_threads = db.execute(text("""
                SELECT DISTINCT
                    trx_mysql_thread_id,
                    trx_id,
                    trx_state,
                    TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration
                FROM information_schema.innodb_trx
                WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 300  -- 超过5分钟
            """)).fetchall()

            print("\n[PROBLEMATIC PROCESSES TO KILL]")

            # G4-2025 相关的长时间进程
            if long_g4_processes:
                print("\nLong running G4-2025 processes:")
                for proc in long_g4_processes:
                    minutes = proc[3] // 60
                    problem_processes.append({
                        'id': proc[0],
                        'type': 'G4-process',
                        'user': proc[1],
                        'duration_minutes': minutes,
                        'query': proc[5][:100] if proc[5] else 'NULL'
                    })
                    print(f"  KILL {proc[0]};  -- User: {proc[1]}, Runtime: {minutes}min")
                    print(f"    Query: {proc[5][:100] if proc[5] else 'NULL'}")

            # 长时间事务
            if long_trx_threads:
                print("\nLong running transactions:")
                for trx in long_trx_threads:
                    if trx[0] not in [p['id'] for p in problem_processes]:  # 避免重复
                        minutes = trx[3] // 60
                        problem_processes.append({
                            'id': trx[0],
                            'type': 'long-transaction',
                            'trx_id': trx[1],
                            'duration_minutes': minutes,
                            'state': trx[2]
                        })
                        print(f"  KILL {trx[0]};  -- Transaction: {trx[1]}, Duration: {minutes}min, State: {trx[2]}")

            if not problem_processes:
                print("No problematic processes found to kill")

            # 2. 生成安全释放脚本
            print("\n[SAFE RELEASE SCRIPT]")
            if problem_processes:
                print("-- Execute these commands one by one and monitor the results:")
                print("-- Before executing, ensure you have proper backup and DBA approval")
                print()

                for i, proc in enumerate(problem_processes, 1):
                    print(f"-- Step {i}: Kill {proc['type']} (ID: {proc['id']})")
                    print(f"KILL {proc['id']};")
                    print("-- Check status after each kill:")
                    print("SELECT COUNT(*) FROM information_schema.processlist WHERE id = " + str(proc['id']) + ";")
                    print()

                print("-- After killing processes, check for remaining locks:")
                print("SELECT COUNT(*) FROM information_schema.innodb_trx;")
                print("SHOW PROCESSLIST;")
                print()
                print("-- If needed, flush tables to release any remaining locks:")
                print("FLUSH TABLES statistical_aggregations;")

            # 3. 生成监控脚本
            print("\n[POST-RELEASE MONITORING]")
            print("-- Monitor these queries after lock release:")
            print("""
-- 1. Check remaining transactions
SELECT
    trx_id, trx_state, trx_started, trx_mysql_thread_id,
    TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_seconds
FROM information_schema.innodb_trx;

-- 2. Check G4-2025 data integrity
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT school_code) as schools,
    MAX(updated_at) as last_update
FROM statistical_aggregations
WHERE batch_code = 'G4-2025';

-- 3. Check for any remaining locks
SHOW OPEN TABLES WHERE In_use > 0;

-- 4. Verify table accessibility
SELECT COUNT(*) FROM statistical_aggregations LIMIT 1;
""")

            return problem_processes

    except Exception as e:
        print(f"Failed to generate release commands: {e}")
        return []

def create_emergency_unlock_script():
    """创建紧急解锁脚本"""
    emergency_script = '''#!/usr/bin/env python3
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
'''

    with open('emergency_unlock.py', 'w', encoding='utf-8') as f:
        f.write(emergency_script)

    print("\n[EMERGENCY SCRIPT CREATED]")
    print("Created: emergency_unlock.py")
    print("WARNING: Only use with DBA approval in critical situations!")

def main():
    """主函数"""
    print("Starting statistical_aggregations lock analysis for G4-2025...")

    # 1. 检查表锁定情况
    lock_analysis = check_statistical_aggregations_locks()

    # 2. G4-2025 特定检查
    check_g4_2025_specific_locks()

    # 3. 生成释放命令
    problem_processes = generate_lock_release_commands()

    # 4. 创建紧急脚本
    create_emergency_unlock_script()

    # 5. 总结报告
    print("\n" + "=" * 80)
    print("ANALYSIS SUMMARY")
    print("=" * 80)

    if lock_analysis:
        print(f"Table locks found: {lock_analysis['table_locks']}")
        print(f"Lock waits found: {lock_analysis['lock_waits']}")
        print(f"Long transactions found: {lock_analysis['long_transactions']}")
        print(f"G4-2025 processes found: {lock_analysis['g4_processes']}")

    print(f"Problematic processes to kill: {len(problem_processes)}")

    if problem_processes:
        print("\nIMMEDIATE ACTION REQUIRED:")
        print("1. Review the generated KILL commands above")
        print("2. Get DBA approval before executing")
        print("3. Execute commands one by one")
        print("4. Monitor table accessibility after each kill")
        print("5. Use emergency_unlock.py only if standard kills fail")
    else:
        print("\nNo immediate action required - no problematic locks found")

    print("\nNext steps:")
    print("- Share this analysis with DBA team")
    print("- Execute approved KILL commands")
    print("- Monitor G4-2025 batch processing")
    print("- Consider optimizing statistical_aggregations operations")

if __name__ == "__main__":
    main()