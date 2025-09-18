#!/usr/bin/env python3
"""
修复版本 - statistical_aggregations表锁定分析脚本
专门针对G4-2025批次的长事务和锁定连接排查
"""

import time
import logging
from datetime import datetime
from sqlalchemy import text
from app.database.connection import get_db_context

def analyze_g4_locks():
    """分析G4-2025批次的锁定情况"""
    print("=" * 80)
    print("FIXED STATISTICAL_AGGREGATIONS LOCK ANALYSIS FOR G4-2025")
    print("=" * 80)
    print(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    results = {
        'long_transactions': [],
        'blocking_processes': [],
        'g4_records': 0,
        'kill_commands': [],
        'errors': []
    }

    try:
        with get_db_context() as db:
            # 1. 检查当前活动事务
            print("\n[1] ACTIVE TRANSACTIONS ANALYSIS")
            try:
                active_trx = db.execute(text("""
                    SELECT
                        trx_id,
                        trx_state,
                        trx_started,
                        trx_mysql_thread_id,
                        TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_seconds,
                        trx_query,
                        trx_operation_state,
                        trx_tables_in_use,
                        trx_tables_locked
                    FROM information_schema.innodb_trx
                    WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 60
                    ORDER BY trx_started ASC
                """)).fetchall()

                if active_trx:
                    print(f"Found {len(active_trx)} long-running transactions:")
                    for trx in active_trx:
                        duration_min = trx[4] // 60
                        print(f"  TRX ID: {trx[0]} | Thread: {trx[3]} | Duration: {duration_min}min")
                        print(f"    State: {trx[1]} | Tables in use: {trx[7]} | Tables locked: {trx[8]}")
                        if trx[5]:
                            query_preview = trx[5][:100] + "..." if len(trx[5]) > 100 else trx[5]
                            print(f"    Query: {query_preview}")

                        results['long_transactions'].append({
                            'trx_id': trx[0],
                            'thread_id': trx[3],
                            'duration_seconds': trx[4],
                            'state': trx[1],
                            'query': trx[5]
                        })
                else:
                    print("No long-running transactions found")

            except Exception as e:
                error_msg = f"Transaction analysis failed: {str(e)}"
                results['errors'].append(error_msg)
                print(f"ERROR: {error_msg}")

            # 2. 检查进程列表中的长时间连接
            print("\n[2] PROCESS LIST ANALYSIS")
            try:
                long_processes = db.execute(text("""
                    SELECT
                        id,
                        user,
                        host,
                        db,
                        command,
                        time,
                        state,
                        SUBSTRING(COALESCE(info, ''), 1, 200) as query_info
                    FROM information_schema.processlist
                    WHERE time > 300
                      AND id != CONNECTION_ID()
                      AND command != 'Sleep'
                      AND user NOT IN ('system user', 'event_scheduler')
                    ORDER BY time DESC
                """)).fetchall()

                if long_processes:
                    print(f"Found {len(long_processes)} long-running processes:")
                    for proc in long_processes:
                        time_min = proc[5] // 60
                        print(f"  ID: {proc[0]} | User: {proc[1]} | DB: {proc[3]} | Time: {time_min}min")
                        print(f"    Command: {proc[4]} | State: {proc[6]}")
                        if proc[7]:
                            print(f"    Query: {proc[7]}")

                        # 检查是否与statistical_aggregations相关
                        if (proc[7] and ('statistical_aggregations' in proc[7] or 'G4-2025' in proc[7])):
                            results['blocking_processes'].append({
                                'id': proc[0],
                                'user': proc[1],
                                'time_seconds': proc[5],
                                'query': proc[7]
                            })
                            results['kill_commands'].append(f"KILL {proc[0]};  -- {proc[1]}, {time_min}min, G4/stat_agg related")
                        elif proc[5] > 1800:  # 超过30分钟的任何查询
                            results['blocking_processes'].append({
                                'id': proc[0],
                                'user': proc[1],
                                'time_seconds': proc[5],
                                'query': proc[7]
                            })
                            results['kill_commands'].append(f"KILL {proc[0]};  -- {proc[1]}, {time_min}min, very long running")
                else:
                    print("No long-running processes found")

            except Exception as e:
                error_msg = f"Process list analysis failed: {str(e)}"
                results['errors'].append(error_msg)
                print(f"ERROR: {error_msg}")

            # 3. 检查G4-2025数据量
            print("\n[3] G4-2025 DATA VOLUME")
            try:
                g4_count = db.execute(text("""
                    SELECT COUNT(*) FROM statistical_aggregations
                    WHERE batch_code = 'G4-2025'
                """)).fetchone()[0]

                results['g4_records'] = g4_count
                print(f"G4-2025 records in statistical_aggregations: {g4_count}")

                if g4_count > 0:
                    # 检查G4-2025记录的状态
                    g4_status = db.execute(text("""
                        SELECT
                            aggregation_level,
                            calculation_status,
                            COUNT(*) as count,
                            MAX(updated_at) as last_update
                        FROM statistical_aggregations
                        WHERE batch_code = 'G4-2025'
                        GROUP BY aggregation_level, calculation_status
                        ORDER BY aggregation_level, calculation_status
                    """)).fetchall()

                    print("G4-2025 records by level and status:")
                    for status in g4_status:
                        print(f"  Level: {status[0]} | Status: {status[1]} | Count: {status[2]} | Last Update: {status[3]}")

            except Exception as e:
                error_msg = f"G4-2025 data check failed: {str(e)}"
                results['errors'].append(error_msg)
                print(f"ERROR: {error_msg}")

            # 4. 检查表级锁定
            print("\n[4] TABLE LOCK STATUS")
            try:
                # 使用修复后的查询语法
                open_tables = db.execute(text("""
                    SHOW OPEN TABLES WHERE In_use > 0
                """)).fetchall()

                if open_tables:
                    print("Tables currently in use:")
                    for table in open_tables:
                        if 'statistical_aggregations' in str(table):
                            print(f"  ALERT: statistical_aggregations table is locked: {table}")
                        else:
                            print(f"  {table}")
                else:
                    print("No tables currently locked")

            except Exception as e:
                error_msg = f"Table lock check failed: {str(e)}"
                results['errors'].append(error_msg)
                print(f"WARNING: {error_msg}")

            # 5. 测试表可访问性
            print("\n[5] TABLE ACCESSIBILITY TEST")
            try:
                test_query = db.execute(text("""
                    SELECT COUNT(*) FROM statistical_aggregations LIMIT 1
                """)).fetchone()[0]
                print(f"SUCCESS: statistical_aggregations table is accessible (row count check passed)")
            except Exception as e:
                error_msg = f"Table accessibility test failed: {str(e)}"
                results['errors'].append(error_msg)
                print(f"CRITICAL: Table not accessible - {error_msg}")

    except Exception as e:
        error_msg = f"Database connection failed: {str(e)}"
        results['errors'].append(error_msg)
        print(f"FATAL ERROR: {error_msg}")

    return results

def generate_action_plan(results):
    """生成具体的解决方案"""
    print("\n" + "=" * 80)
    print("ACTION PLAN FOR G4-2025 LOCK ISSUES")
    print("=" * 80)

    if results['kill_commands']:
        print(f"\n[IMMEDIATE ACTIONS REQUIRED]")
        print(f"Found {len(results['kill_commands'])} processes that should be terminated:")
        print("\n-- Execute these KILL commands (with DBA approval):")
        for cmd in results['kill_commands']:
            print(cmd)

        print(f"\n-- After killing processes, run these verification queries:")
        print("SELECT COUNT(*) FROM information_schema.innodb_trx;")
        print("SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = 'G4-2025';")
        print("SHOW OPEN TABLES WHERE In_use > 0;")

    else:
        print("\n[STATUS: HEALTHY]")
        print("No problematic connections found that require immediate action")

    if results['g4_records'] == 0:
        print(f"\n[WARNING: NO G4-2025 DATA]")
        print("No G4-2025 records found in statistical_aggregations table")
        print("This may indicate:")
        print("- Data has been deleted or moved")
        print("- Batch processing has not started")
        print("- Table structure issues")

    if results['errors']:
        print(f"\n[ERRORS ENCOUNTERED]")
        for error in results['errors']:
            print(f"- {error}")

    print(f"\n[MONITORING RECOMMENDATIONS]")
    print("1. Set up monitoring for long-running transactions:")
    print("   SELECT * FROM information_schema.innodb_trx WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 300;")
    print("\n2. Monitor G4-2025 batch processing:")
    print("   SELECT calculation_status, COUNT(*) FROM statistical_aggregations WHERE batch_code = 'G4-2025' GROUP BY calculation_status;")
    print("\n3. Check for deadlocks:")
    print("   SHOW ENGINE INNODB STATUS\\G")

def main():
    """主函数"""
    print("Starting G4-2025 batch lock analysis...")

    # 执行分析
    results = analyze_g4_locks()

    # 生成行动计划
    generate_action_plan(results)

    # 创建解锁脚本（如果需要）
    if results['kill_commands']:
        create_unlock_script(results['kill_commands'])

    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETED")
    print(f"{'='*80}")
    print(f"Long transactions found: {len(results['long_transactions'])}")
    print(f"Blocking processes found: {len(results['blocking_processes'])}")
    print(f"G4-2025 records: {results['g4_records']}")
    print(f"Kill commands generated: {len(results['kill_commands'])}")
    print(f"Errors encountered: {len(results['errors'])}")

def create_unlock_script(kill_commands):
    """创建紧急解锁脚本"""
    script_content = f'''#!/usr/bin/env python3
"""
G4-2025批次紧急解锁脚本
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
WARNING: 仅在DBA批准下使用！
"""

from sqlalchemy import text
from app.database.connection import get_db_context

def emergency_unlock():
    print("执行G4-2025紧急解锁...")

    kill_commands = {kill_commands}

    try:
        with get_db_context() as db:
            for cmd in kill_commands:
                try:
                    print(f"执行: {{cmd}}")
                    db.execute(text(cmd.split(';')[0]))  # 只执行KILL命令部分
                    print("SUCCESS")
                except Exception as e:
                    print(f"FAILED: {{e}}")

            db.commit()
            print("解锁完成！")

    except Exception as e:
        print(f"紧急解锁失败: {{e}}")

if __name__ == "__main__":
    confirm = input("确认执行G4-2025紧急解锁? (输入 'EMERGENCY' 继续): ")
    if confirm == 'EMERGENCY':
        emergency_unlock()
    else:
        print("操作已取消")
'''

    with open('g4_2025_emergency_unlock.py', 'w', encoding='utf-8') as f:
        f.write(script_content)

    print(f"\n[EMERGENCY SCRIPT CREATED]")
    print("Created: g4_2025_emergency_unlock.py")
    print("WARNING: Only use with DBA approval!")

if __name__ == "__main__":
    main()