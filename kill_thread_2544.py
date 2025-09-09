#!/usr/bin/env python3
"""
终止线程2544的长时间运行事务
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def kill_thread_2544():
    """终止线程2544"""
    
    thread_id = 2544
    
    try:
        with get_db_context() as session:
            print(f"=== 终止线程 {thread_id} ===")
            print(f"时间: {datetime.now()}\n")
            
            # 1. 确认线程详情
            result = session.execute(text("""
                SELECT 
                    ID,
                    TIME,
                    STATE,
                    INFO
                FROM INFORMATION_SCHEMA.PROCESSLIST
                WHERE ID = :thread_id
            """), {"thread_id": thread_id})
            
            thread_info = result.fetchone()
            if thread_info:
                print(f"线程详情:")
                print(f"  ID: {thread_info[0]}")
                print(f"  运行时间: {thread_info[1]}秒")
                print(f"  状态: {thread_info[2]}")
                if thread_info[3]:
                    print(f"  查询: {str(thread_info[3])[:200]}...")
                
                # 2. 执行KILL
                print(f"\n正在终止线程 {thread_id}...")
                session.execute(text(f"KILL {thread_id}"))
                session.commit()
                print("KILL命令已执行")
                
                # 3. 等待并检查结果
                import time
                time.sleep(3)
                
                result = session.execute(text("""
                    SELECT COUNT(*) as count
                    FROM INFORMATION_SCHEMA.PROCESSLIST
                    WHERE ID = :thread_id
                """), {"thread_id": thread_id})
                
                count = result.fetchone()[0]
                if count == 0:
                    print("[OK] 线程已成功终止")
                else:
                    print("[WARNING] 线程可能仍在运行，尝试强制终止...")
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
                        print("[OK] 线程已强制终止")
                    else:
                        print("[ERROR] 线程终止失败")
                
                # 4. 检查数据库状态
                print("\n检查清理后的数据库状态:")
                
                # 检查剩余长时间事务
                result = session.execute(text("""
                    SELECT COUNT(*) as count
                    FROM INFORMATION_SCHEMA.INNODB_TRX
                    WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 60
                """))
                long_trx_count = result.fetchone()[0]
                
                # 检查活跃非Sleep连接
                result = session.execute(text("""
                    SELECT COUNT(*) as count
                    FROM INFORMATION_SCHEMA.PROCESSLIST
                    WHERE COMMAND != 'Sleep' 
                      AND COMMAND != 'Daemon'
                      AND ID != CONNECTION_ID()
                """))
                active_count = result.fetchone()[0]
                
                # 检查表锁
                result = session.execute(text("SHOW OPEN TABLES WHERE In_use > 0"))
                locked_tables = len(result.fetchall())
                
                print(f"  长时间事务: {long_trx_count}")
                print(f"  活跃连接: {active_count}")
                print(f"  锁定表: {locked_tables}")
                
                if long_trx_count == 0 and active_count == 0 and locked_tables == 0:
                    print("\n[SUCCESS] 数据库现在处于空闲状态!")
                    print("所有长时间运行的操作已被清理")
                else:
                    print("\n[WARNING] 可能仍有其他活跃操作")
                    
            else:
                print(f"线程 {thread_id} 不存在或已终止")
                
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    kill_thread_2544()