#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""紧急禁用G7维护模式"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def disable_maintenance_mode():
    """紧急禁用维护模式"""
    print("[URGENT] 紧急禁用G7-2025维护模式...")

    update_query = text("""
        UPDATE g7_guard_config
        SET config_value = 'false', updated_at = NOW()
        WHERE config_key = 'maintenance_mode'
    """)

    try:
        with get_db_context() as db:
            result = db.execute(update_query)
            affected_rows = result.rowcount
            db.commit()

            if affected_rows > 0:
                print(f"[SUCCESS] 维护模式已禁用 (影响 {affected_rows} 行)")

                # 验证更新
                verify_query = text("SELECT config_value, updated_at FROM g7_guard_config WHERE config_key = 'maintenance_mode'")
                verify_result = db.execute(verify_query)
                row = verify_result.fetchone()

                if row:
                    value, updated_at = row
                    print(f"[VERIFY] 当前维护模式状态: {value} (更新时间: {updated_at})")
                    return value == 'false'
                else:
                    print("[ERROR] 无法验证维护模式状态")
                    return False
            else:
                print("[ERROR] 没有更新任何记录")
                return False

    except Exception as e:
        print(f"[ERROR] 禁用维护模式失败: {e}")
        return False

def insert_emergency_log():
    """插入紧急操作日志"""
    print("[LOG] 记录紧急操作日志...")

    # 先检查日志表结构
    check_columns_query = text("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'g7_enhanced_guard_log'
        ORDER BY ORDINAL_POSITION
    """)

    try:
        with get_db_context() as db:
            result = db.execute(check_columns_query)
            columns = [row[0] for row in result.fetchall()]
            print(f"[INFO] 日志表字段: {columns}")

            # 根据实际字段构造插入语句
            if 'action_type' in columns:  # 使用action_type而不是operation_type
                insert_query = text("""
                    INSERT INTO g7_enhanced_guard_log
                    (action_type, batch_code, decision, user_info, created_at, details)
                    VALUES ('EMERGENCY_DISABLE_MAINTENANCE', 'G7-2025', 'SYSTEM', 'EMERGENCY_SCRIPT', NOW(),
                            'Emergency maintenance mode disable due to continuous aggregation')
                """)
            elif 'operation_type' in columns:
                insert_query = text("""
                    INSERT INTO g7_enhanced_guard_log
                    (operation_type, batch_code, decision, user_info, created_at, details)
                    VALUES ('EMERGENCY_DISABLE_MAINTENANCE', 'G7-2025', 'SYSTEM', 'EMERGENCY_SCRIPT', NOW(),
                            'Emergency maintenance mode disable due to continuous aggregation')
                """)
            else:
                # 简化版本，只插入必要字段
                insert_query = text("""
                    INSERT INTO g7_enhanced_guard_log
                    (batch_code, decision, user_info, created_at)
                    VALUES ('G7-2025', 'EMERGENCY_DISABLE', 'EMERGENCY_SCRIPT', NOW())
                """)

            db.execute(insert_query)
            db.commit()
            print("[SUCCESS] 紧急操作日志已记录")

    except Exception as e:
        print(f"[WARNING] 记录操作日志失败: {e}")

def main():
    print(f"[START] 紧急G7维护模式禁用操作 - {datetime.now()}")

    # 禁用维护模式
    success = disable_maintenance_mode()

    if success:
        print("\n[SUCCESS] G7-2025保护机制已恢复！")

        # 记录日志
        insert_emergency_log()

        print("\n[NEXT] 建议操作:")
        print("1. 立即停止任何G7-2025数据处理操作")
        print("2. 检查最近的数据写入记录")
        print("3. 验证守卫系统开始阻断新的写入")

    else:
        print("\n[FAILED] 维护模式禁用失败！")
        print("需要手动数据库操作或联系管理员")

    return success

if __name__ == "__main__":
    main()