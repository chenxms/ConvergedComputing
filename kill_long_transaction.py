#!/usr/bin/env python3
"""
安全地终止长时间运行的事务
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def kill_long_transaction():
    """安全地终止长时间运行的事务"""
    
    thread_id = 2526
    
    try:
        with get_db_context() as session:
            print(f"=== 终止长时间运行的事务 ===")
            print(f"时间: {datetime.now()}")
            print(f"目标线程ID: {thread_id}\n")
            
            # 1. 再次确认线程状态
            result = session.execute(text("""
                SELECT 
                    ID,
                    TIME,
                    STATE,
                    LEFT(INFO, 100) as query_snippet
                FROM INFORMATION_SCHEMA.PROCESSLIST
                WHERE ID = :thread_id
            """), {"thread_id": thread_id})
            
            thread_info = result.fetchone()
            if thread_info:
                print(f"确认线程信息:")
                print(f"  ID: {thread_info[0]}")
                print(f"  运行时间: {thread_info[1]}秒")
                print(f"  状态: {thread_info[2]}")
                print(f"  查询: {thread_info[3]}")
                
                # 2. 执行KILL操作
                print(f"\n正在终止线程 {thread_id}...")
                session.execute(text(f"KILL {thread_id}"))
                session.commit()
                print("KILL命令已执行")
                
                # 3. 等待几秒后检查结果
                import time
                time.sleep(3)
                
                result = session.execute(text("""
                    SELECT COUNT(*) as count
                    FROM INFORMATION_SCHEMA.PROCESSLIST
                    WHERE ID = :thread_id
                """), {"thread_id": thread_id})
                
                count = result.fetchone()[0]
                if count == 0:
                    print("✅ 线程已成功终止")
                else:
                    print("⚠️  线程可能仍在运行，尝试强制终止...")
                    session.execute(text(f"KILL CONNECTION {thread_id}"))
                    session.commit()
                    
                    time.sleep(2)
                    result = session.execute(text("""
                        SELECT COUNT(*) as count
                        FROM INFORMATION_SCHEMA.PROCESSLIST
                        WHERE ID = :thread_id
                    """), {"thread_id": thread_id})
                    
                    count = result.fetchone()[0]
                    if count == 0:
                        print("✅ 线程已强制终止")
                    else:
                        print("❌ 线程终止失败")
                
                # 4. 检查事务状态
                print("\n检查剩余长时间事务...")
                result = session.execute(text("""
                    SELECT 
                        trx_id,
                        trx_mysql_thread_id,
                        TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_seconds
                    FROM INFORMATION_SCHEMA.INNODB_TRX
                    WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 60
                """))
                
                remaining_trx = result.fetchall()
                if remaining_trx:
                    print("剩余长时间事务:")
                    for trx in remaining_trx:
                        print(f"  事务ID: {trx[0]}, 线程ID: {trx[1]}, 持续: {trx[2]}秒")
                else:
                    print("✅ 无剩余长时间事务")
                    
            else:
                print(f"线程 {thread_id} 不存在或已终止")
                
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    kill_long_transaction()