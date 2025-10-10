#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""紧急重置G7守卫触发器"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from app.database.connection import get_db_context
from sqlalchemy import text

def drop_existing_triggers():
    """删除现有触发器"""
    print("[1] 删除现有G7守卫触发器...")

    trigger_names = [
        'g7_enhanced_guard_insert',
        'g7_enhanced_guard_update'
    ]

    dropped_count = 0

    try:
        with get_db_context() as db:
            for trigger_name in trigger_names:
                try:
                    drop_sql = f"DROP TRIGGER IF EXISTS {trigger_name}"
                    db.execute(text(drop_sql))
                    print(f"  - 删除触发器: {trigger_name}")
                    dropped_count += 1
                except Exception as e:
                    print(f"  - 删除 {trigger_name} 失败: {e}")

            db.commit()
            print(f"[SUCCESS] 删除了 {dropped_count} 个触发器")
            return True

    except Exception as e:
        print(f"[ERROR] 删除触发器失败: {e}")
        return False

def create_blocking_trigger():
    """创建简单的阻断触发器"""
    print("[2] 创建紧急阻断触发器...")

    # 简单粗暴的阻断触发器
    trigger_sql = text("""
        CREATE TRIGGER g7_emergency_block_insert
        BEFORE INSERT ON statistical_aggregations
        FOR EACH ROW
        BEGIN
            IF NEW.batch_code = 'G7-2025' THEN
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes EMERGENCY BLOCKED';
            END IF;
        END
    """)

    trigger_sql_update = text("""
        CREATE TRIGGER g7_emergency_block_update
        BEFORE UPDATE ON statistical_aggregations
        FOR EACH ROW
        BEGIN
            IF NEW.batch_code = 'G7-2025' THEN
                SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes EMERGENCY BLOCKED';
            END IF;
        END
    """)

    try:
        with get_db_context() as db:
            # 创建INSERT触发器
            db.execute(trigger_sql)
            print("  - 创建 INSERT 阻断触发器")

            # 创建UPDATE触发器
            db.execute(trigger_sql_update)
            print("  - 创建 UPDATE 阻断触发器")

            db.commit()
            print("[SUCCESS] 紧急阻断触发器已创建")
            return True

    except Exception as e:
        print(f"[ERROR] 创建阻断触发器失败: {e}")
        return False

def test_blocking():
    """测试阻断是否生效"""
    print("[3] 测试紧急阻断...")

    test_sql = text("""
        INSERT INTO statistical_aggregations
        (batch_code, aggregation_level, school_id, statistics_data, created_at, updated_at)
        VALUES ('G7-2025', 'SCHOOL', 'TEST', '{}', NOW(), NOW())
    """)

    try:
        with get_db_context() as db:
            db.execute(test_sql)
            db.commit()
            print("[ERROR] 测试失败 - 写入未被阻断！")
            return False

    except Exception as e:
        error_msg = str(e)
        if "G7-2025 writes EMERGENCY BLOCKED" in error_msg:
            print("[SUCCESS] 紧急阻断正常工作！")
            return True
        else:
            print(f"[ERROR] 未预期的错误: {error_msg}")
            return False

def main():
    print("=== 紧急G7守卫重置 ===")

    # 步骤1：删除现有触发器
    if not drop_existing_triggers():
        print("[FAILED] 无法删除现有触发器")
        return False

    # 步骤2：创建紧急阻断触发器
    if not create_blocking_trigger():
        print("[FAILED] 无法创建阻断触发器")
        return False

    # 步骤3：测试阻断
    if test_blocking():
        print("\n[SUCCESS] G7-2025 紧急保护已生效！")
        print("[INFO] 所有G7-2025写入将被完全阻断")
        print("[WARNING] 这是临时紧急措施，需要后续恢复正常守卫")
        return True
    else:
        print("\n[FAILED] 紧急保护未生效")
        return False

if __name__ == "__main__":
    main()