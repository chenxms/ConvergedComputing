#!/usr/bin/env python3
"""
检查KILL操作结果
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def check_kill_result():
    """检查KILL操作结果"""
    
    try:
        with get_db_context() as session:
            print("=== KILL操作结果检查 ===")
            print(f"检查时间: {datetime.now()}\n")
            
            # 1. 检查目标线程是否还存在
            result = session.execute(text("""
                SELECT COUNT(*) as count
                FROM INFORMATION_SCHEMA.PROCESSLIST
                WHERE ID = 2526
            """))
            
            count = result.fetchone()[0]
            if count == 0:
                print("[OK] 目标线程 2526 已被终止")
            else:
                print("[WARNING] 目标线程 2526 仍然存在")
            
            # 2. 检查剩余长时间事务
            result = session.execute(text("""
                SELECT 
                    trx_id,
                    trx_mysql_thread_id,
                    trx_state,
                    TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_seconds
                FROM INFORMATION_SCHEMA.INNODB_TRX
                WHERE TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 60
                ORDER BY duration_seconds DESC
            """))
            
            long_trx = result.fetchall()
            if long_trx:
                print("剩余长时间事务:")
                for trx in long_trx:
                    print(f"  事务ID: {trx[0]}")
                    print(f"  线程ID: {trx[1]}")
                    print(f"  状态: {trx[2]}")
                    print(f"  持续时间: {trx[3]}秒")
                    print()
            else:
                print("[OK] 无剩余长时间事务")
            
            # 3. 检查当前活跃连接
            result = session.execute(text("""
                SELECT 
                    ID,
                    USER,
                    HOST,
                    DB,
                    COMMAND,
                    TIME,
                    STATE
                FROM INFORMATION_SCHEMA.PROCESSLIST
                WHERE COMMAND != 'Sleep' AND TIME > 30
                ORDER BY TIME DESC
            """))
            
            active_processes = result.fetchall()
            if active_processes:
                print("当前活跃进程:")
                for proc in active_processes:
                    print(f"  进程ID: {proc[0]}")
                    print(f"  用户: {proc[1]}")
                    print(f"  命令: {proc[4]}")
                    print(f"  运行时间: {proc[5]}秒")
                    print(f"  状态: {proc[6]}")
                    print()
            else:
                print("[OK] 无长时间运行的活跃进程")
            
            # 4. 检查锁等待情况（如果支持）
            try:
                result = session.execute(text("""
                    SELECT COUNT(*) as lock_wait_count
                    FROM sys.innodb_lock_waits
                """))
                lock_waits = result.fetchone()[0]
                if lock_waits == 0:
                    print("[OK] 当前无锁等待")
                else:
                    print(f"[WARNING] 仍有 {lock_waits} 个锁等待")
            except:
                print("无法检查锁等待情况（可能是MySQL版本问题）")
            
            # 5. 总结
            print("\n=== 操作结果总结 ===")
            if count == 0 and not long_trx:
                print("[SUCCESS] 长时间事务已成功终止，数据库锁问题已解决")
                print("建议:")
                print("1. 可以重新尝试之前被阻塞的操作")
                print("2. 如需重新处理G4-2025的数据，请重新运行数据清洗")
                print("3. 考虑优化类似的大批量UPDATE操作")
            else:
                print("[PARTIAL] 操作部分成功，可能仍有问题需要处理")
                
    except Exception as e:
        print(f"检查失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_kill_result()