#!/usr/bin/env python3
"""
全面检查数据库是否处于空闲状态，无活跃读写操作
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def verify_database_idle():
    """验证数据库是否处于空闲状态"""
    
    try:
        with get_db_context() as session:
            print("=== 数据库空闲状态全面检查 ===")
            print(f"检查时间: {datetime.now()}\n")
            
            # 1. 检查所有活跃连接
            print("1. 检查所有数据库连接状态:")
            result = session.execute(text("""
                SELECT 
                    ID,
                    USER,
                    HOST,
                    DB,
                    COMMAND,
                    TIME,
                    STATE,
                    LEFT(COALESCE(INFO, ''), 100) as INFO_SNIPPET
                FROM INFORMATION_SCHEMA.PROCESSLIST
                ORDER BY TIME DESC
            """))
            
            all_processes = result.fetchall()
            
            active_count = 0
            sleep_count = 0
            system_count = 0
            
            for proc in all_processes:
                command = proc[4]
                time_running = proc[5]
                state = proc[6] or ""
                info = proc[7] or ""
                
                print(f"进程ID: {proc[0]}")
                print(f"  用户: {proc[1]}")
                print(f"  主机: {proc[2]}")
                print(f"  数据库: {proc[3]}")
                print(f"  命令: {command}")
                print(f"  运行时间: {time_running}秒")
                print(f"  状态: {state}")
                if info:
                    print(f"  查询: {info}")
                
                # 分类统计
                if command == 'Sleep':
                    sleep_count += 1
                elif command in ['Daemon', 'Binlog Dump']:
                    system_count += 1
                else:
                    active_count += 1
                    if time_running > 5:  # 运行超过5秒的认为是活跃操作
                        print(f"  [WARNING] 长时间运行的操作: {time_running}秒")
                
                print()
            
            print(f"连接统计:")
            print(f"  活跃连接: {active_count}")
            print(f"  睡眠连接: {sleep_count}")
            print(f"  系统连接: {system_count}")
            print(f"  总连接数: {len(all_processes)}")
            
            # 2. 检查InnoDB事务状态
            print("\n2. 检查InnoDB事务状态:")
            result = session.execute(text("""
                SELECT 
                    trx_id,
                    trx_state,
                    trx_started,
                    trx_mysql_thread_id,
                    TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_seconds,
                    trx_tables_in_use,
                    trx_tables_locked,
                    trx_rows_locked,
                    trx_rows_modified,
                    LEFT(COALESCE(trx_query, ''), 100) as query_snippet
                FROM INFORMATION_SCHEMA.INNODB_TRX
                ORDER BY trx_started
            """))
            
            active_transactions = result.fetchall()
            
            if active_transactions:
                print(f"发现 {len(active_transactions)} 个活跃事务:")
                for trx in active_transactions:
                    print(f"  事务ID: {trx[0]}")
                    print(f"  状态: {trx[1]}")
                    print(f"  持续时间: {trx[4]}秒")
                    print(f"  线程ID: {trx[3]}")
                    print(f"  使用表数: {trx[5]}")
                    print(f"  锁定表数: {trx[6]}")
                    print(f"  锁定行数: {trx[7]}")
                    print(f"  修改行数: {trx[8]}")
                    if trx[9]:
                        print(f"  查询: {trx[9]}")
                    print()
            else:
                print("[OK] 无活跃事务")
            
            # 3. 检查表锁情况
            print("\n3. 检查表锁状态:")
            result = session.execute(text("""
                SHOW OPEN TABLES WHERE In_use > 0
            """))
            
            locked_tables = result.fetchall()
            if locked_tables:
                print("发现被锁定的表:")
                for table in locked_tables:
                    print(f"  数据库: {table[0]}")
                    print(f"  表名: {table[1]}")
                    print(f"  使用中: {table[2]}")
                    print(f"  名称锁定: {table[3]}")
                    print()
            else:
                print("[OK] 无表锁")
            
            # 4. 检查性能指标
            print("\n4. 检查数据库性能指标:")
            result = session.execute(text("""
                SHOW STATUS WHERE Variable_name IN (
                    'Threads_connected',
                    'Threads_running',
                    'Questions',
                    'Uptime',
                    'Innodb_rows_read',
                    'Innodb_rows_inserted',
                    'Innodb_rows_updated',
                    'Innodb_rows_deleted',
                    'Com_select',
                    'Com_insert',
                    'Com_update',
                    'Com_delete'
                )
            """))
            
            status_vars = result.fetchall()
            for var in status_vars:
                print(f"  {var[0]}: {var[1]}")
            
            # 5. 检查慢查询
            print("\n5. 检查是否有慢查询:")
            try:
                result = session.execute(text("""
                    SELECT COUNT(*) as slow_query_count
                    FROM INFORMATION_SCHEMA.PROCESSLIST
                    WHERE COMMAND != 'Sleep' 
                      AND COMMAND != 'Daemon'
                      AND TIME > 10
                """))
                slow_count = result.fetchone()[0]
                
                if slow_count > 0:
                    print(f"[WARNING] 发现 {slow_count} 个可能的慢查询")
                else:
                    print("[OK] 无慢查询")
            except Exception as e:
                print(f"检查慢查询失败: {e}")
            
            # 6. 最终评估
            print("\n=== 数据库状态评估 ===")
            
            issues = []
            
            if active_count > 1:  # 除了当前检查连接
                issues.append(f"存在 {active_count} 个活跃连接")
            
            if active_transactions:
                issues.append(f"存在 {len(active_transactions)} 个活跃事务")
            
            if locked_tables:
                issues.append(f"存在 {len(locked_tables)} 个锁定的表")
            
            if not issues:
                print("[SUCCESS] 数据库当前处于空闲状态")
                print("✓ 无活跃读写操作")
                print("✓ 无长时间运行事务")
                print("✓ 无表锁")
                print("✓ 可以安全进行维护操作")
            else:
                print("[WARNING] 数据库可能不完全空闲:")
                for issue in issues:
                    print(f"  - {issue}")
                print("\n建议等待这些操作完成或手动处理后再进行维护")
            
    except Exception as e:
        print(f"检查失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_database_idle()