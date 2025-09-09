#!/usr/bin/env python3
"""
检查InnoDB锁等待和事务状态
"""

from app.database.connection import get_db_context
from sqlalchemy import text
import json
from datetime import datetime

def check_innodb_locks():
    """检查InnoDB锁等待情况"""
    
    try:
        with get_db_context() as session:
            print("=== InnoDB锁等待和事务状态检查 ===")
            print(f"检查时间: {datetime.now()}\n")
            
            # 1. 检查当前锁等待情况
            print("1. 检查锁等待情况 (INFORMATION_SCHEMA.INNODB_LOCK_WAITS)")
            try:
                result = session.execute(text("""
                    SELECT 
                        requesting_trx_id as 请求事务ID,
                        requested_lock_id as 请求锁ID,
                        blocking_trx_id as 阻塞事务ID,
                        blocking_lock_id as 阻塞锁ID
                    FROM INFORMATION_SCHEMA.INNODB_LOCK_WAITS
                """))
                lock_waits = result.fetchall()
                
                if lock_waits:
                    print("发现锁等待:")
                    for wait in lock_waits:
                        print(f"  请求事务 {wait[0]} 被事务 {wait[2]} 阻塞")
                        print(f"  请求锁ID: {wait[1]}")
                        print(f"  阻塞锁ID: {wait[3]}")
                        print()
                else:
                    print("✅ 当前无锁等待")
            except Exception as e:
                print(f"查询锁等待失败: {e}")
            
            # 2. 检查当前活跃事务
            print("\n2. 检查活跃事务 (INFORMATION_SCHEMA.INNODB_TRX)")
            try:
                result = session.execute(text("""
                    SELECT 
                        trx_id as 事务ID,
                        trx_state as 事务状态,
                        trx_started as 开始时间,
                        trx_requested_lock_id as 等待锁ID,
                        trx_wait_started as 等待开始时间,
                        trx_weight as 权重,
                        trx_mysql_thread_id as 线程ID,
                        trx_query as 当前查询,
                        TIMESTAMPDIFF(SECOND, trx_started, NOW()) as 持续时间秒
                    FROM INFORMATION_SCHEMA.INNODB_TRX
                    ORDER BY trx_started
                """))
                transactions = result.fetchall()
                
                if transactions:
                    print("当前活跃事务:")
                    for trx in transactions:
                        print(f"  事务ID: {trx[0]}")
                        print(f"  状态: {trx[1]}")
                        print(f"  开始时间: {trx[2]}")
                        print(f"  持续时间: {trx[8]}秒")
                        print(f"  线程ID: {trx[6]}")
                        if trx[3]:
                            print(f"  等待锁ID: {trx[3]}")
                        if trx[4]:
                            print(f"  等待开始: {trx[4]}")
                        if trx[7]:
                            print(f"  当前查询: {str(trx[7])[:100]}...")
                        print()
                else:
                    print("✅ 当前无长时间运行事务")
            except Exception as e:
                print(f"查询事务状态失败: {e}")
            
            # 3. 检查锁信息
            print("\n3. 检查锁信息 (INFORMATION_SCHEMA.INNODB_LOCKS)")
            try:
                result = session.execute(text("""
                    SELECT 
                        lock_id as 锁ID,
                        lock_trx_id as 事务ID,
                        lock_mode as 锁模式,
                        lock_type as 锁类型,
                        lock_table as 表名,
                        lock_index as 索引名,
                        lock_space as 表空间ID,
                        lock_page as 页号,
                        lock_rec as 记录号
                    FROM INFORMATION_SCHEMA.INNODB_LOCKS
                """))
                locks = result.fetchall()
                
                if locks:
                    print("当前锁信息:")
                    for lock in locks:
                        print(f"  锁ID: {lock[0]}")
                        print(f"  事务ID: {lock[1]}")
                        print(f"  锁模式: {lock[2]}")
                        print(f"  锁类型: {lock[3]}")
                        print(f"  表名: {lock[4]}")
                        if lock[5]:
                            print(f"  索引: {lock[5]}")
                        print()
                else:
                    print("✅ 当前无显示锁")
            except Exception as e:
                print(f"查询锁信息失败: {e}")
            
            # 4. 检查进程列表
            print("\n4. 检查MySQL进程列表")
            try:
                result = session.execute(text("""
                    SELECT 
                        ID,
                        USER as 用户,
                        HOST as 主机,
                        DB as 数据库,
                        COMMAND as 命令,
                        TIME as 运行时间,
                        STATE as 状态,
                        LEFT(INFO, 100) as 查询信息
                    FROM INFORMATION_SCHEMA.PROCESSLIST
                    WHERE COMMAND != 'Sleep' OR TIME > 300
                    ORDER BY TIME DESC
                """))
                processes = result.fetchall()
                
                if processes:
                    print("活跃或长时间运行的进程:")
                    for proc in processes:
                        print(f"  进程ID: {proc[0]}")
                        print(f"  用户: {proc[1]}")
                        print(f"  主机: {proc[2]}")
                        print(f"  数据库: {proc[3]}")
                        print(f"  命令: {proc[4]}")
                        print(f"  运行时间: {proc[5]}秒")
                        print(f"  状态: {proc[6]}")
                        if proc[7]:
                            print(f"  查询: {proc[7]}")
                        print()
                else:
                    print("✅ 无异常进程")
            except Exception as e:
                print(f"查询进程列表失败: {e}")
            
            # 5. 获取InnoDB状态
            print("\n5. InnoDB引擎状态概要")
            try:
                result = session.execute(text("SHOW ENGINE INNODB STATUS"))
                innodb_status = result.fetchone()
                
                if innodb_status:
                    status_text = innodb_status[2]  # STATUS字段
                    
                    # 提取关键信息
                    lines = status_text.split('\n')
                    
                    # 寻找TRANSACTIONS部分
                    in_transactions = False
                    transactions_info = []
                    
                    for line in lines:
                        line = line.strip()
                        if 'TRANSACTIONS' in line:
                            in_transactions = True
                            transactions_info.append(line)
                        elif in_transactions and line.startswith('---'):
                            in_transactions = False
                        elif in_transactions:
                            transactions_info.append(line)
                        
                        # 查找锁等待信息
                        if 'LOCK WAIT' in line or 'waiting for this lock' in line:
                            print(f"⚠️  发现锁等待: {line}")
                    
                    if transactions_info:
                        print("\nTRANSACTIONS部分摘要:")
                        for info in transactions_info[:10]:  # 只显示前10行
                            print(f"  {info}")
                    
                    # 检查死锁信息
                    if 'LATEST DETECTED DEADLOCK' in status_text:
                        print("\n⚠️  发现最近的死锁信息，请查看完整的SHOW ENGINE INNODB STATUS输出")
                    
                else:
                    print("无法获取InnoDB状态")
            except Exception as e:
                print(f"获取InnoDB状态失败: {e}")
            
            # 6. 检查statistical_aggregations表的锁情况
            print("\n6. 检查statistical_aggregations表相关操作")
            try:
                result = session.execute(text("""
                    SELECT 
                        trx_id,
                        trx_mysql_thread_id,
                        trx_query,
                        trx_state,
                        TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_seconds
                    FROM INFORMATION_SCHEMA.INNODB_TRX 
                    WHERE trx_query LIKE '%statistical_aggregations%' 
                       OR trx_query LIKE '%statistical%'
                """))
                table_trx = result.fetchall()
                
                if table_trx:
                    print("发现statistical_aggregations相关事务:")
                    for trx in table_trx:
                        print(f"  事务ID: {trx[0]}")
                        print(f"  线程ID: {trx[1]}")  
                        print(f"  查询: {trx[2]}")
                        print(f"  状态: {trx[3]}")
                        print(f"  持续时间: {trx[4]}秒")
                        print()
                else:
                    print("✅ 无statistical_aggregations相关的长事务")
            except Exception as e:
                print(f"检查表相关事务失败: {e}")
            
            print("\n=== 检查完成 ===")
            
    except Exception as e:
        print(f"检查过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_innodb_locks()