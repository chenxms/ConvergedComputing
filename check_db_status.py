#!/usr/bin/env python3
import pymysql

def check_db_status():
    conn = pymysql.connect(
        host='117.72.14.166', 
        port=23506, 
        user='root', 
        password='mysql_Lujing2022', 
        db='appraisal_test'
    )
    cursor = conn.cursor()

    try:
        print('=== 当前活跃连接详情 ===')
        cursor.execute('SHOW PROCESSLIST')
        processes = cursor.fetchall()
        
        for proc in processes:
            proc_id = proc[0]
            user = proc[1] 
            host = proc[2]
            db = proc[3]
            command = proc[4]
            time_sec = proc[5]
            state = proc[6] if len(proc) > 6 else 'None'
            info = proc[7] if len(proc) > 7 else None
            
            print(f'ID: {proc_id}, User: {user}, Command: {command}, Time: {time_sec}s')
            print(f'  Host: {host}, DB: {db}, State: {state}')
            if info and str(info) != 'None':
                query_str = str(info)
                if len(query_str) > 100:
                    print(f'  Query: {query_str[:100]}...')
                else:
                    print(f'  Query: {query_str}')
            print()
        
        print('=== 活跃事务详情 ===')
        cursor.execute('''
            SELECT trx_id, trx_state, trx_started, trx_mysql_thread_id, trx_query, trx_operation_state
            FROM information_schema.innodb_trx
            ORDER BY trx_started
        ''')
        transactions = cursor.fetchall()
        
        if transactions:
            for trx in transactions:
                trx_id = trx[0]
                state = trx[1]
                started = trx[2]
                thread_id = trx[3]
                query = trx[4]
                op_state = trx[5]
                
                print(f'Transaction ID: {trx_id}')
                print(f'  State: {state}, Started: {started}')
                print(f'  Thread ID: {thread_id}, Op State: {op_state}')
                if query:
                    query_str = str(query)
                    if len(query_str) > 150:
                        print(f'  Query: {query_str[:150]}...')
                    else:
                        print(f'  Query: {query_str}')
                print()
        else:
            print('没有活跃事务')
            
    except Exception as e:
        print(f'查询失败: {e}')
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_db_status()