#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7守卫系统测试脚本
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text
from app.database.connection import get_db


def test_guard_system():
    """测试G7守卫系统"""
    print("G7 Guard System Test")
    print("=" * 40)

    with next(get_db()) as db:
        # 1. 检查触发器状态
        print("1. Checking trigger status...")
        result = db.execute(text("SHOW TRIGGERS LIKE 'statistical_aggregations'"))
        triggers = result.fetchall()

        g7_triggers = [t for t in triggers if 'g7' in t[0].lower()]
        print(f"   Found {len(g7_triggers)} G7 related triggers:")
        for trigger in g7_triggers:
            print(f"   - {trigger[0]} ({trigger[1]})")

        # 2. 检查表存在性
        print("\n2. Checking related tables...")
        tables = [
            'g7_guard_log',
            'g7_enhanced_guard_log',
            'g7_guard_whitelist',
            'g7_guard_config'
        ]

        for table in tables:
            try:
                result = db.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.fetchone()[0]
                print(f"   OK {table}: {count} records")
            except Exception as e:
                print(f"   FAIL {table}: not exist or inaccessible ({e})")

        # 3. 检查配置状态
        print("\n3. Checking configuration status...")
        try:
            result = db.execute(text("""
                SELECT config_key, config_value
                FROM g7_guard_config
                ORDER BY config_key
            """))
            configs = result.fetchall()

            if configs:
                for config in configs:
                    print(f"   - {config[0]}: {config[1]}")
            else:
                print("   No configuration records")
        except:
            print("   Configuration table not exists")

        # 4. 检查最近的日志
        print("\n4. Checking recent logs...")
        log_tables = ['g7_enhanced_guard_log', 'g7_guard_log']

        for table in log_tables:
            try:
                result = db.execute(text(f"""
                    SELECT event, action, decision, created_at
                    FROM {table}
                    ORDER BY created_at DESC
                    LIMIT 3
                """))
                logs = result.fetchall()

                if logs:
                    print(f"   {table} recent records:")
                    for log in logs:
                        print(f"     - {log[0]} {log[1]} {log[2]} ({log[3]})")
                else:
                    print(f"   {table}: no records")
            except:
                print(f"   {table}: table not exists")


def test_basic_functionality():
    """测试基础功能"""
    print("\n" + "=" * 40)
    print("Basic Functionality Test")
    print("=" * 40)

    with next(get_db()) as db:
        test_school_id = 'TEST_GUARD_BASIC'

        try:
            # 确保维护模式关闭
            try:
                db.execute(text("""
                    UPDATE g7_guard_config
                    SET config_value = 'false'
                    WHERE config_key = 'maintenance_mode'
                """))
                db.commit()
                print("Maintenance mode disabled")
            except:
                print("Cannot set maintenance mode (table may not exist)")

            # 测试G7-2025阻断
            print("\nTesting G7-2025 write blocking...")
            try:
                db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        statistics_data, data_version, calculation_status, created_at
                    ) VALUES (
                        'G7-2025', 'SCHOOL', :school_id,
                        '{}', 'v1.0', 'COMPLETED', NOW()
                    )
                """), {'school_id': test_school_id})

                db.commit()
                print("   FAIL Not blocked (unexpected)")

                # 清理测试数据
                db.execute(text("""
                    DELETE FROM statistical_aggregations
                    WHERE school_id = :school_id
                """), {'school_id': test_school_id})
                db.commit()

            except Exception as e:
                if "G7-2025 writes blocked" in str(e):
                    print("   OK Correctly blocked")
                else:
                    print(f"   ? Other error: {e}")

            # 测试非G7数据
            print("\nTesting non-G7 data write...")
            try:
                db.execute(text("""
                    INSERT INTO statistical_aggregations (
                        batch_code, aggregation_level, school_id,
                        statistics_data, data_version, calculation_status, created_at, updated_at
                    ) VALUES (
                        'TEST-BATCH', 'SCHOOL', :school_id,
                        '{}', 'v1.0', 'COMPLETED', NOW(), NOW()
                    )
                """), {'school_id': test_school_id})

                db.commit()
                print("   OK Non-G7 data write successful")

                # 清理测试数据
                db.execute(text("""
                    DELETE FROM statistical_aggregations
                    WHERE school_id = :school_id
                """), {'school_id': test_school_id})
                db.commit()

            except Exception as e:
                print(f"   FAIL Non-G7 data write failed: {e}")

        except Exception as e:
            print(f"测试过程中发生错误: {e}")


if __name__ == '__main__':
    try:
        test_guard_system()
        test_basic_functionality()
        print("\n" + "=" * 40)
        print("Test completed")
    except Exception as e:
        print(f"Test failed: {e}")