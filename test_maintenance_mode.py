#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
维护模式测试脚本
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text
from app.database.connection import get_db


def test_maintenance_mode():
    """测试维护模式功能"""
    print("Testing Maintenance Mode")
    print("=" * 40)

    with next(get_db()) as db:
        test_school_id = 'TEST_MAINTENANCE'

        try:
            # 1. 确保维护模式关闭
            print("1. Setting maintenance mode OFF...")
            db.execute(text("""
                UPDATE g7_guard_config
                SET config_value = 'false'
                WHERE config_key = 'maintenance_mode'
            """))
            db.commit()

            # 测试G7-2025被阻断
            print("2. Testing G7-2025 blocking (maintenance OFF)...")
            try:
                db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        statistics_data, data_version, calculation_status, created_at, updated_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                    )
                """), {'school_id': f'{test_school_id}_OFF'})

                db.commit()
                print("   FAIL: Not blocked (unexpected)")

                # 清理数据
                db.execute(text("""
                    DELETE FROM statistical_aggregations
                    WHERE school_id = :school_id
                """), {'school_id': f'{test_school_id}_OFF'})
                db.commit()

            except Exception as e:
                if "G7-2025 writes blocked" in str(e):
                    print("   OK: Correctly blocked")
                else:
                    print(f"   ? Other error: {e}")

            # 2. 启用维护模式
            print("3. Setting maintenance mode ON...")
            db.execute(text("""
                UPDATE g7_guard_config
                SET config_value = 'true'
                WHERE config_key = 'maintenance_mode'
            """))
            db.commit()

            # 测试G7-2025被允许
            print("4. Testing G7-2025 allowing (maintenance ON)...")
            try:
                db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        statistics_data, data_version, calculation_status, created_at, updated_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                    )
                """), {'school_id': f'{test_school_id}_ON'})

                db.commit()
                print("   OK: Write allowed in maintenance mode")

                # 清理数据
                db.execute(text("""
                    DELETE FROM statistical_aggregations
                    WHERE school_id = :school_id
                """), {'school_id': f'{test_school_id}_ON'})
                db.commit()

            except Exception as e:
                print(f"   FAIL: Write blocked in maintenance mode: {e}")

            # 3. 检查日志记录
            print("5. Checking guard logs...")
            result = db.execute(text("""
                SELECT event, action, decision, maintenance_mode, message
                FROM g7_enhanced_guard_log
                WHERE batch_code = 'G7-2025'
                ORDER BY created_at DESC
                LIMIT 5
            """))

            logs = result.fetchall()
            if logs:
                print("   Recent G7-2025 guard logs:")
                for log in logs:
                    print(f"     - {log[0]} {log[1]} -> {log[2]} (maintenance: {log[3]}) | {log[4]}")
            else:
                print("   No G7-2025 guard logs found")

            # 4. 恢复维护模式关闭
            print("6. Restoring maintenance mode OFF...")
            db.execute(text("""
                UPDATE g7_guard_config
                SET config_value = 'false'
                WHERE config_key = 'maintenance_mode'
            """))
            db.commit()

            print("\nMaintenance mode test completed")

        except Exception as e:
            print(f"Test error: {e}")
            db.rollback()


def test_whitelist_functionality():
    """测试白名单功能"""
    print("\n" + "=" * 40)
    print("Testing Whitelist Functionality")
    print("=" * 40)

    with next(get_db()) as db:
        test_school_id = 'TEST_WHITELIST'

        try:
            # 获取当前用户
            result = db.execute(text("SELECT USER(), CURRENT_USER()"))
            current_user, current_user_full = result.fetchone()
            print(f"Current user: {current_user}")

            # 确保维护模式关闭
            db.execute(text("""
                UPDATE g7_guard_config
                SET config_value = 'false'
                WHERE config_key = 'maintenance_mode'
            """))
            db.commit()

            # 1. 添加当前用户到白名单
            print("1. Adding current user to whitelist...")
            user_pattern = current_user.split('@')[0] + '%'
            db.execute(text("""
                INSERT INTO g7_guard_whitelist (user_pattern, added_by, notes)
                VALUES (:pattern, USER(), 'Test whitelist entry')
                ON DUPLICATE KEY UPDATE is_active = TRUE, notes = 'Test whitelist entry'
            """), {'pattern': user_pattern})
            db.commit()
            print(f"   Added pattern: {user_pattern}")

            # 2. 测试白名单用户可以写入G7-2025
            print("2. Testing whitelisted user G7-2025 write...")
            try:
                db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        statistics_data, data_version, calculation_status, created_at, updated_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                    )
                """), {'school_id': f'{test_school_id}_WHITELIST'})

                db.commit()
                print("   OK: Whitelisted user can write G7-2025")

                # 清理数据
                db.execute(text("""
                    DELETE FROM statistical_aggregations
                    WHERE school_id = :school_id
                """), {'school_id': f'{test_school_id}_WHITELIST'})
                db.commit()

            except Exception as e:
                print(f"   FAIL: Whitelisted user write failed: {e}")

            # 3. 移除白名单
            print("3. Removing user from whitelist...")
            db.execute(text("""
                UPDATE g7_guard_whitelist
                SET is_active = FALSE
                WHERE user_pattern = :pattern
            """), {'pattern': user_pattern})
            db.commit()

            # 4. 测试移除后被阻断
            print("4. Testing write after whitelist removal...")
            try:
                db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        statistics_data, data_version, calculation_status, created_at, updated_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                    )
                """), {'school_id': f'{test_school_id}_REMOVED'})

                db.commit()
                print("   FAIL: Write not blocked after whitelist removal")

                # 清理数据
                db.execute(text("""
                    DELETE FROM statistical_aggregations
                    WHERE school_id = :school_id
                """), {'school_id': f'{test_school_id}_REMOVED'})
                db.commit()

            except Exception as e:
                if "G7-2025 writes blocked" in str(e):
                    print("   OK: Correctly blocked after whitelist removal")
                else:
                    print(f"   ? Other error: {e}")

            print("\nWhitelist test completed")

        except Exception as e:
            print(f"Whitelist test error: {e}")
            db.rollback()


if __name__ == '__main__':
    try:
        test_maintenance_mode()
        test_whitelist_functionality()
        print("\n" + "=" * 40)
        print("All tests completed")
    except Exception as e:
        print(f"Test suite failed: {e}")