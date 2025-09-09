#!/usr/bin/env python3
"""
检查和处理长时间运行的事务
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def check_long_transaction():
    """检查长时间运行的事务详情"""
    
    try:
        with get_db_context() as session:
            print("=== 长事务详细检查 ===")
            print(f"检查时间: {datetime.now()}\n")
            
            # 1. 检查长时间运行的事务详情
            print("1. 长时间运行事务详情:")
            result = session.execute(text("""
                SELECT 
                    trx_id,
                    trx_state,
                    trx_started,
                    trx_mysql_thread_id,
                    trx_query,
                    TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_seconds,
                    trx_tables_in_use,
                    trx_tables_locked,
                    trx_lock_structs,
                    trx_rows_locked,
                    trx_rows_modified
                FROM INFORMATION_SCHEMA.INNODB_TRX
                WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 60
                ORDER BY trx_started
            """))
            
            long_transactions = result.fetchall()
            
            for trx in long_transactions:
                print(f"事务ID: {trx[0]}")
                print(f"状态: {trx[1]}")
                print(f"开始时间: {trx[2]}")
                print(f"持续时间: {trx[5]}秒 ({trx[5]//3600}小时{(trx[5]%3600)//60}分钟)")
                print(f"线程ID: {trx[3]}")
                print(f"使用表数: {trx[6]}")
                print(f"锁定表数: {trx[7]}")
                print(f"锁结构数: {trx[8]}")
                print(f"锁定行数: {trx[9]}")
                print(f"修改行数: {trx[10]}")
                if trx[4]:
                    query = str(trx[4])
                    print(f"当前查询: {query[:200]}...")
                print()
            
            # 2. 检查具体的问题线程
            print("2. 检查问题线程详情:")
            result = session.execute(text("""
                SELECT 
                    ID,
                    USER,
                    HOST,
                    DB,
                    COMMAND,
                    TIME,
                    STATE,
                    INFO
                FROM INFORMATION_SCHEMA.PROCESSLIST
                WHERE ID = 2526
            """))
            
            thread_info = result.fetchone()
            if thread_info:
                print(f"线程ID: {thread_info[0]}")
                print(f"用户: {thread_info[1]}")
                print(f"主机: {thread_info[2]}")
                print(f"数据库: {thread_info[3]}")
                print(f"命令: {thread_info[4]}")
                print(f"运行时间: {thread_info[5]}秒")
                print(f"状态: {thread_info[6]}")
                if thread_info[7]:
                    print(f"完整查询: {thread_info[7]}")
            
            # 3. 检查是否有锁等待（使用MySQL 8.0的新视图）
            print("\n3. 检查锁等待情况:")
            try:
                result = session.execute(text("""
                    SELECT 
                        waiting_trx_id,
                        waiting_thread,
                        waiting_query,
                        blocking_trx_id,
                        blocking_thread,
                        blocking_query
                    FROM sys.innodb_lock_waits
                """))
                lock_waits = result.fetchall()
                
                if lock_waits:
                    for wait in lock_waits:
                        print(f"等待事务: {wait[0]} (线程: {wait[1]})")
                        print(f"等待查询: {wait[2]}")
                        print(f"阻塞事务: {wait[3]} (线程: {wait[4]})")
                        print(f"阻塞查询: {wait[5]}")
                        print()
                else:
                    print("当前无锁等待")
            except Exception as e:
                print(f"检查sys.innodb_lock_waits失败: {e}")
            
            # 4. 建议的解决方案
            print("\n4. 建议的解决方案:")
            if long_transactions:
                print("发现长时间运行的事务，建议:")
                print("1. 如果事务正常执行但耗时太长，可以等待其完成")
                print("2. 如果事务陷入死循环或死锁，建议KILL掉")
                print("3. KILL命令: KILL 2526;")
                print("4. 或者强制KILL: KILL CONNECTION 2526;")
                
                # 询问是否要执行KILL操作
                print("\n是否需要KILL长时间运行的线程? (输入'yes'确认)")
                # 注意：在生产环境中，这个操作需要谨慎
                
    except Exception as e:
        print(f"检查过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

def kill_long_transaction(thread_id: int):
    """KILL长时间运行的事务"""
    
    try:
        with get_db_context() as session:
            print(f"正在KILL线程 {thread_id}...")
            
            # 执行KILL操作
            session.execute(text(f"KILL {thread_id}"))
            session.commit()
            
            print(f"线程 {thread_id} 已被终止")
            
            # 再次检查状态
            result = session.execute(text("""
                SELECT COUNT(*) as count
                FROM INFORMATION_SCHEMA.PROCESSLIST
                WHERE ID = :thread_id
            """), {"thread_id": thread_id})
            
            count = result.fetchone()[0]
            if count == 0:
                print("线程已成功终止")
            else:
                print("线程可能仍在运行")
                
    except Exception as e:
        print(f"KILL操作失败: {e}")

if __name__ == "__main__":
    check_long_transaction()
    
    # 如果需要KILL线程，取消下面的注释
    # kill_long_transaction(2526)