#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""监控G7-2025数据库活动"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime, timedelta

def check_recent_g7_writes():
    """检查最近的G7-2025写入活动"""
    print("=== 检查最近G7-2025写入活动 ===")

    # 检查statistical_aggregations表
    query = text("""
        SELECT id, batch_code, aggregation_level, school_id, updated_at,
               TIMESTAMPDIFF(SECOND, updated_at, NOW()) as seconds_ago
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025'
          AND updated_at >= DATE_SUB(NOW(), INTERVAL 30 MINUTE)
        ORDER BY updated_at DESC
        LIMIT 10
    """)

    try:
        with get_db_context() as db:
            result = db.execute(query)
            records = result.fetchall()

            if not records:
                print("[SUCCESS] 最近30分钟没有G7-2025写入活动")
                return True
            else:
                print(f"[WARNING] 发现 {len(records)} 条最近的G7-2025写入:")
                for record in records:
                    id_val, batch, level, school, updated_at, seconds_ago = record
                    print(f"  ID:{id_val} {level} 学校:{school} - {seconds_ago}秒前 ({updated_at})")
                return False

    except Exception as e:
        print(f"[ERROR] 查询写入活动失败: {e}")
        return False

def check_guard_logs_recent():
    """检查最近的守卫日志"""
    print("\n=== 检查最近守卫活动 ===")

    # 根据实际表结构查询
    query = text("""
        SELECT event, action, message, batch_code, decision,
               created_at, TIMESTAMPDIFF(SECOND, created_at, NOW()) as seconds_ago
        FROM g7_enhanced_guard_log
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL 10 MINUTE)
        ORDER BY created_at DESC
        LIMIT 20
    """)

    try:
        with get_db_context() as db:
            result = db.execute(query)
            logs = result.fetchall()

            if not logs:
                print("[INFO] 最近10分钟没有守卫活动")
                return []
            else:
                print(f"[INFO] 最近10分钟守卫活动 ({len(logs)} 条):")
                for log in logs:
                    event, action, message, batch, decision, created_at, seconds_ago = log
                    print(f"  {seconds_ago}秒前: {event}/{action} {batch} -> {decision}")
                    if message:
                        print(f"    消息: {message}")
                return logs

    except Exception as e:
        print(f"[ERROR] 查询守卫日志失败: {e}")
        return []

def test_guard_blocking():
    """测试守卫是否会阻断新的G7-2025写入"""
    print("\n=== 测试守卫阻断机制 ===")

    # 尝试一个安全的测试写入（会被守卫阻断）
    test_query = text("""
        INSERT INTO statistical_aggregations
        (batch_code, aggregation_level, school_id, statistics_data, created_at, updated_at)
        VALUES ('G7-2025', 'TEST', 'TEST_SCHOOL', '{"test": true}', NOW(), NOW())
    """)

    try:
        with get_db_context() as db:
            db.execute(test_query)
            db.commit()
            print("[ERROR] 测试写入成功 - 守卫未阻断！")

            # 清理测试数据
            cleanup_query = text("""
                DELETE FROM statistical_aggregations
                WHERE batch_code = 'G7-2025' AND aggregation_level = 'TEST' AND school_id = 'TEST_SCHOOL'
            """)
            db.execute(cleanup_query)
            db.commit()
            print("[INFO] 已清理测试数据")
            return False

    except Exception as e:
        error_msg = str(e)
        if "G7-2025 writes blocked" in error_msg:
            print("[SUCCESS] 守卫正常工作 - G7-2025写入被阻断")
            print(f"[INFO] 阻断消息: {error_msg}")
            return True
        else:
            print(f"[ERROR] 测试失败，非预期错误: {error_msg}")
            return False

def check_database_connections():
    """检查当前数据库连接"""
    print("\n=== 检查数据库连接状态 ===")

    query = text("""
        SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO
        FROM INFORMATION_SCHEMA.PROCESSLIST
        WHERE DB IS NOT NULL
          AND COMMAND != 'Sleep'
          AND USER != 'system user'
        ORDER BY TIME DESC
        LIMIT 10
    """)

    try:
        with get_db_context() as db:
            result = db.execute(query)
            processes = result.fetchall()

            if not processes:
                print("[INFO] 没有活跃的数据库进程")
                return []
            else:
                print(f"[INFO] 活跃数据库连接 ({len(processes)} 个):")
                for proc in processes:
                    proc_id, user, host, db_name, command, time_sec, state, info = proc
                    info_short = (info[:50] + '...') if info and len(info) > 50 else info
                    print(f"  ID:{proc_id} {user}@{host} {db_name} [{command}] {time_sec}s - {info_short}")
                return processes

    except Exception as e:
        print(f"[ERROR] 查询数据库连接失败: {e}")
        return []

def main():
    print(f"[START] G7-2025数据库活动监控 - {datetime.now()}")

    # 检查最近写入
    no_recent_writes = check_recent_g7_writes()

    # 检查守卫日志
    recent_logs = check_guard_logs_recent()

    # 测试守卫阻断
    guard_working = test_guard_blocking()

    # 检查数据库连接
    active_connections = check_database_connections()

    # 总结分析
    print("\n" + "="*60)
    print("[ANALYSIS] G7-2025活动监控结果:")

    if no_recent_writes:
        print("  [SUCCESS] 最近没有G7-2025数据写入")
    else:
        print("  [WARNING] 发现最近的G7-2025数据写入活动")

    if guard_working:
        print("  [SUCCESS] 守卫系统正常阻断G7-2025写入")
    else:
        print("  [ERROR] 守卫系统未能阻断G7-2025写入")

    if len(recent_logs) > 0:
        blocked_count = sum(1 for log in recent_logs if log[4] == 'BLOCKED')
        print(f"  [INFO] 最近守卫活动: {blocked_count} 次阻断")

    print(f"  [INFO] 当前活跃数据库连接: {len(active_connections)} 个")

    # 最终状态
    system_secure = no_recent_writes and guard_working
    print(f"\n[RESULT] G7-2025保护状态: {'[SECURE]' if system_secure else '[AT RISK]'}")

    if not system_secure:
        print("\n[ACTION] 建议立即:")
        if not guard_working:
            print("- 检查守卫触发器完整性")
            print("- 验证维护模式确实已禁用")
        if not no_recent_writes:
            print("- 停止所有G7-2025相关处理进程")
            print("- 分析数据写入来源")

    return system_secure

if __name__ == "__main__":
    main()