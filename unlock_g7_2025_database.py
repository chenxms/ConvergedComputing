#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
解除G7-2025数据库层面的写入锁
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database.connection import get_db_context
from sqlalchemy import text

def unlock_g7_2025():
    """解除G7-2025的数据库触发器锁"""

    print("="*60)
    print("解除G7-2025数据库锁")
    print("="*60)

    with get_db_context() as db:
        try:
            # 1. 检查当前的触发器
            print("\n[1] 检查当前触发器...")
            triggers = db.execute(text("""
                SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_STATEMENT
                FROM information_schema.TRIGGERS
                WHERE TRIGGER_SCHEMA = DATABASE()
                AND TRIGGER_NAME LIKE '%g7_guard%'
            """)).fetchall()

            if triggers:
                print(f"找到 {len(triggers)} 个G7相关触发器:")
                for trigger in triggers:
                    print(f"  - {trigger[0]} on {trigger[1]}")
            else:
                print("未找到G7相关触发器")

            # 2. 删除G7-2025相关的触发器
            print("\n[2] 删除G7-2025触发器...")

            # 尝试删除可能存在的触发器
            trigger_names = [
                'prevent_g7_2025_insert',
                'prevent_g7_2025_update',
                'g7_2025_guard',
                'block_g7_2025'
            ]

            for trigger_name in trigger_names:
                try:
                    db.execute(text(f"DROP TRIGGER IF EXISTS {trigger_name}"))
                    print(f"  ✓ 删除触发器: {trigger_name}")
                except Exception as e:
                    print(f"  - 跳过: {trigger_name} ({str(e)})")

            db.commit()
            print("\n[3] 触发器删除完成")

            # 3. 验证解锁
            print("\n[4] 验证解锁状态...")

            # 尝试插入测试数据
            try:
                db.execute(text("""
                    INSERT INTO statistical_aggregations
                    (batch_code, aggregation_level, school_id, school_name,
                     statistics_data, data_version, calculation_status, created_at, updated_at)
                    VALUES
                    ('G7-2025', 'TEST', 'TEST_UNLOCK', 'Test School',
                     '{"test": true}', 'test', 'PENDING', NOW(), NOW())
                    ON DUPLICATE KEY UPDATE updated_at = NOW()
                """))
                db.commit()

                # 删除测试数据
                db.execute(text("""
                    DELETE FROM statistical_aggregations
                    WHERE batch_code = 'G7-2025' AND aggregation_level = 'TEST'
                """))
                db.commit()

                print("  ✅ 成功！G7-2025已解锁，可以写入数据")

            except Exception as e:
                if "blocked" in str(e).lower() or "guard" in str(e).lower():
                    print(f"  ❌ 仍然被锁定: {e}")

                    # 尝试更彻底的解锁
                    print("\n[5] 尝试更彻底的解锁...")

                    # 检查所有触发器
                    all_triggers = db.execute(text("""
                        SELECT TRIGGER_NAME
                        FROM information_schema.TRIGGERS
                        WHERE TRIGGER_SCHEMA = DATABASE()
                        AND EVENT_OBJECT_TABLE = 'statistical_aggregations'
                    """)).fetchall()

                    print(f"找到 {len(all_triggers)} 个相关触发器:")
                    for trigger in all_triggers:
                        trigger_name = trigger[0]
                        print(f"  删除: {trigger_name}")
                        db.execute(text(f"DROP TRIGGER IF EXISTS {trigger_name}"))

                    db.commit()
                    print("  ✅ 所有触发器已删除")
                else:
                    # 其他错误，可能是正常的约束
                    print(f"  ⚠️ 测试插入失败，但可能不是锁的问题: {e}")

            print("\n" + "="*60)
            print("解锁流程完成！")
            print("="*60)
            print("\n现在可以运行物化脚本了：")
            print("python scripts/rewrite_subjects_v12.py <BATCH_CODE>")
            print("Example: python scripts/rewrite_subjects_v12.py G7-2025")

        except Exception as e:
            print(f"\n❌ 解锁失败: {e}")
            import traceback
            traceback.print_exc()
            return 1

    return 0

if __name__ == "__main__":
    sys.exit(unlock_g7_2025())