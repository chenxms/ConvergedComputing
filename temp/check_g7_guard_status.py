#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""独立G7守卫状态检查脚本"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime, timedelta

def check_triggers():
    """检查G7守卫触发器状态"""
    print("=== 检查G7守卫触发器状态 ===")

    query = text("""
        SELECT TRIGGER_NAME, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_TIMING
        FROM INFORMATION_SCHEMA.TRIGGERS
        WHERE TRIGGER_NAME LIKE '%g7%guard%'
        ORDER BY TRIGGER_NAME
    """)

    try:
        with get_db_context() as db:
            result = db.execute(query)
            triggers = result.fetchall()

            if not triggers:
                print("[ERROR] 未找到G7守卫触发器！")
                return False

            print(f"[INFO] 找到 {len(triggers)} 个G7守卫触发器:")
            for trigger in triggers:
                name, event, table, timing = trigger
                print(f"  - {name}: {timing} {event} ON {table}")

            # 检查期望的触发器
            expected_triggers = ['g7_enhanced_guard_insert', 'g7_enhanced_guard_update']
            found_names = [t[0] for t in triggers]

            all_found = all(name in found_names for name in expected_triggers)
            if all_found:
                print("[SUCCESS] 所有期望的G7守卫触发器都存在")
                return True
            else:
                missing = [name for name in expected_triggers if name not in found_names]
                print(f"[ERROR] 缺少触发器: {missing}")
                return False

    except Exception as e:
        print(f"[ERROR] 查询触发器失败: {e}")
        return False

def check_guard_tables():
    """检查G7守卫相关表"""
    print("\n=== 检查G7守卫相关表 ===")

    tables_to_check = [
        'g7_enhanced_guard_log',
        'g7_guard_whitelist',
        'g7_guard_config'
    ]

    results = {}

    try:
        with get_db_context() as db:
            for table in tables_to_check:
                try:
                    count_query = text(f"SELECT COUNT(*) FROM {table}")
                    result = db.execute(count_query)
                    count = result.fetchone()[0]
                    results[table] = count
                    print(f"[INFO] {table}: {count} 条记录")
                except Exception as e:
                    results[table] = f"ERROR: {e}"
                    print(f"[ERROR] {table}: {e}")

        return results
    except Exception as e:
        print(f"[ERROR] 检查表失败: {e}")
        return {}

def check_recent_guard_logs():
    """检查最近的守卫日志"""
    print("\n=== 检查最近的G7守卫日志 ===")

    query = text("""
        SELECT operation_type, batch_code, decision, user_info,
               created_at, execution_time_ms, details
        FROM g7_enhanced_guard_log
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL 2 HOUR)
        ORDER BY created_at DESC
        LIMIT 20
    """)

    try:
        with get_db_context() as db:
            result = db.execute(query)
            logs = result.fetchall()

            if not logs:
                print("[WARNING] 最近2小时没有G7守卫日志记录")
                return []

            print(f"[INFO] 最近2小时的守卫活动 ({len(logs)} 条):")
            for log in logs:
                op_type, batch_code, decision, user_info, created_at, exec_time, details = log
                print(f"  {created_at}: {op_type} {batch_code} -> {decision} ({user_info}) [{exec_time}ms]")
                if details:
                    print(f"    详情: {details}")

            return logs
    except Exception as e:
        print(f"[ERROR] 查询守卫日志失败: {e}")
        return []

def check_whitelist():
    """检查白名单状态"""
    print("\n=== 检查G7守卫白名单 ===")

    query = text("""
        SELECT user_pattern, is_active, created_at, updated_at
        FROM g7_guard_whitelist
        WHERE is_active = 1
        ORDER BY created_at DESC
    """)

    try:
        with get_db_context() as db:
            result = db.execute(query)
            whitelist = result.fetchall()

            if not whitelist:
                print("[WARNING] 没有活跃的白名单用户")
                return []

            print(f"[INFO] 活跃白名单用户 ({len(whitelist)} 个):")
            for entry in whitelist:
                pattern, is_active, created_at, updated_at = entry
                print(f"  - {pattern} (创建: {created_at})")

            return whitelist
    except Exception as e:
        print(f"[ERROR] 查询白名单失败: {e}")
        return []

def check_maintenance_mode():
    """检查维护模式状态"""
    print("\n=== 检查维护模式状态 ===")

    query = text("""
        SELECT config_key, config_value, updated_at
        FROM g7_guard_config
        WHERE config_key IN ('maintenance_mode', 'guard_enabled')
        ORDER BY config_key
    """)

    try:
        with get_db_context() as db:
            result = db.execute(query)
            configs = result.fetchall()

            if not configs:
                print("[WARNING] 没有找到守卫配置")
                return {}

            config_dict = {}
            print("[INFO] 守卫配置状态:")
            for config in configs:
                key, value, updated_at = config
                config_dict[key] = value
                print(f"  {key}: {value} (更新: {updated_at})")

            return config_dict
    except Exception as e:
        print(f"[ERROR] 查询守卫配置失败: {e}")
        return {}

def main():
    print("[START] 开始G7守卫系统状态检查...")
    print(f"[TIME] 检查时间: {datetime.now()}")

    # 执行各项检查
    triggers_ok = check_triggers()
    tables_status = check_guard_tables()
    recent_logs = check_recent_guard_logs()
    whitelist = check_whitelist()
    config = check_maintenance_mode()

    # 分析结果
    print("\n" + "="*60)
    print("[ANALYSIS] G7守卫系统状态分析:")

    if not triggers_ok:
        print("  [CRITICAL] 触发器未正确安装 - 守卫系统无法工作！")
    else:
        print("  [OK] 触发器正常安装")

    if 'g7_enhanced_guard_log' not in tables_status:
        print("  [ERROR] 守卫日志表不存在")
    else:
        log_count = tables_status.get('g7_enhanced_guard_log', 0)
        if isinstance(log_count, int) and log_count > 0:
            print(f"  [INFO] 守卫日志表正常 ({log_count} 条记录)")
        else:
            print("  [WARNING] 守卫日志表为空或异常")

    # 检查维护模式
    maintenance_mode = config.get('maintenance_mode', 'unknown')
    if maintenance_mode == 'true':
        print("  [WARNING] 维护模式已启用 - G7-2025写入不受限制！")
    elif maintenance_mode == 'false':
        print("  [OK] 维护模式已禁用 - G7-2025写入受保护")
    else:
        print(f"  [WARNING] 维护模式状态未知: {maintenance_mode}")

    # 检查白名单
    if len(whitelist) > 0:
        print(f"  [INFO] 白名单包含 {len(whitelist)} 个用户模式")
    else:
        print("  [WARNING] 没有白名单用户")

    # 检查最近活动
    if len(recent_logs) > 0:
        blocked_count = sum(1 for log in recent_logs if log[2] == 'BLOCKED')
        allowed_count = sum(1 for log in recent_logs if log[2] == 'ALLOWED')
        print(f"  [INFO] 最近活动: {blocked_count} 次阻断, {allowed_count} 次允许")
    else:
        print("  [WARNING] 最近没有守卫活动记录")

    # 总体状态
    overall_ok = triggers_ok and maintenance_mode != 'true'
    print(f"\n[RESULT] G7守卫系统: {'[ACTIVE]' if overall_ok else '[INACTIVE/BYPASSED]'}")

    if not overall_ok:
        print("\n[URGENT] 守卫系统可能失效，G7-2025数据可能处于无保护状态！")

    return overall_ok

if __name__ == "__main__":
    main()