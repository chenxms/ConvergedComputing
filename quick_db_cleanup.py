#!/usr/bin/env python3
"""快速数据库清理脚本 - 无需交互"""

import os
import pymysql


def quick_cleanup():
    """快速清理数据库连接"""
    host = os.getenv("DATABASE_HOST", "117.72.14.166")
    port = int(os.getenv("DATABASE_PORT", "23506"))
    user = os.getenv("DATABASE_USER", "root")
    password = os.getenv("DATABASE_PASSWORD", "mysql_Lujing2022")
    database = os.getenv("DATABASE_NAME", "appraisal_test")

    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset='utf8mb4',
            autocommit=True
        )

        cursor = conn.cursor(pymysql.cursors.DictCursor)

        print("1. 检查当前连接...")
        cursor.execute("SHOW PROCESSLIST")
        processes = cursor.fetchall()
        print(f"当前连接数: {len(processes)}")

        # 找出长时间运行的查询(超过30秒)
        long_queries = []
        for proc in processes:
            if (proc.get('Time', 0) > 30 and
                proc.get('Command') not in ['Sleep', 'Binlog Dump'] and
                proc.get('Info') and proc.get('Info') != 'NULL'):
                long_queries.append(proc)

        print(f"发现 {len(long_queries)} 个长时间查询")

        # 杀掉长时间查询
        killed_count = 0
        for query in long_queries:
            pid = query.get('Id')
            query_text = query.get('Info', '')[:100]
            try:
                cursor.execute(f"KILL {pid}")
                print(f"已杀掉进程 {pid}: {query_text}")
                killed_count += 1
            except Exception as e:
                print(f"杀掉进程 {pid} 失败: {e}")

        print(f"2. 优化连接设置...")
        optimizations = [
            ("SET GLOBAL wait_timeout = 300", "等待超时"),
            ("SET GLOBAL interactive_timeout = 300", "交互超时"),
            ("SET GLOBAL max_connections = 500", "最大连接数"),
            ("SET GLOBAL innodb_lock_wait_timeout = 10", "锁等待超时"),
        ]

        for sql, desc in optimizations:
            try:
                cursor.execute(sql)
                print(f"[成功] {desc}")
            except Exception as e:
                print(f"[失败] {desc}: {e}")

        print(f"3. 清理完成，共杀掉 {killed_count} 个进程")

        # 最终检查
        cursor.execute("SHOW PROCESSLIST")
        final_processes = cursor.fetchall()
        print(f"清理后连接数: {len(final_processes)}")

        conn.close()

    except Exception as e:
        print(f"清理失败: {e}")
        return False

    return True


if __name__ == "__main__":
    print("=== 数据库快速清理 ===")
    success = quick_cleanup()
    if success:
        print("清理成功，前端访问应该已恢复")
    else:
        print("清理失败，请检查数据库连接")