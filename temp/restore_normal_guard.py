#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""恢复正常G7守卫系统（支持白名单）"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def remove_emergency_triggers():
    """删除紧急阻断触发器"""
    print("[1] 删除紧急阻断触发器...")

    emergency_triggers = [
        'g7_emergency_block_insert',
        'g7_emergency_block_update'
    ]

    removed_count = 0

    try:
        with get_db_context() as db:
            for trigger_name in emergency_triggers:
                try:
                    drop_sql = f"DROP TRIGGER IF EXISTS {trigger_name}"
                    db.execute(text(drop_sql))
                    print(f"  - 删除: {trigger_name}")
                    removed_count += 1
                except Exception as e:
                    print(f"  - 删除 {trigger_name} 失败: {e}")

            db.commit()
            print(f"[SUCCESS] 删除了 {removed_count} 个紧急触发器")
            return True

    except Exception as e:
        print(f"[ERROR] 删除紧急触发器失败: {e}")
        return False

def create_enhanced_guard_triggers():
    """创建增强G7守卫触发器（支持白名单和维护模式）"""
    print("[2] 创建增强G7守卫触发器...")

    # INSERT触发器
    insert_trigger_sql = text("""
        CREATE TRIGGER g7_enhanced_guard_insert
        BEFORE INSERT ON statistical_aggregations
        FOR EACH ROW
        BEGIN
            DECLARE v_maintenance_mode VARCHAR(10) DEFAULT 'false';
            DECLARE v_is_whitelisted INT DEFAULT 0;
            DECLARE v_user_host VARCHAR(255);
            DECLARE v_current_user VARCHAR(255);
            DECLARE v_decision VARCHAR(20) DEFAULT 'BLOCKED';
            DECLARE v_start_time BIGINT;

            IF NEW.batch_code = 'G7-2025' THEN
                SET v_start_time = UNIX_TIMESTAMP(NOW(3)) * 1000;
                SET v_user_host = USER();
                SET v_current_user = CURRENT_USER();

                -- 检查维护模式
                SELECT config_value INTO v_maintenance_mode
                FROM g7_guard_config
                WHERE config_key = 'maintenance_mode'
                LIMIT 1;

                -- 检查白名单
                SELECT COUNT(*) INTO v_is_whitelisted
                FROM g7_guard_whitelist
                WHERE (v_user_host LIKE user_pattern OR v_current_user LIKE user_pattern)
                  AND is_active = 1
                LIMIT 1;

                -- 决策逻辑
                IF v_maintenance_mode = 'true' OR v_is_whitelisted > 0 THEN
                    SET v_decision = 'ALLOWED';
                ELSE
                    SET v_decision = 'BLOCKED';
                END IF;

                -- 记录日志
                INSERT INTO g7_enhanced_guard_log (
                    event, action, message, batch_code, aggregation_level, school_id,
                    user_host, current_user_name, connection_id, is_whitelisted,
                    maintenance_mode, decision, execution_time_ms, created_at
                ) VALUES (
                    'GUARD', 'INSERT',
                    CONCAT('Decision: ', v_decision,
                           IF(v_maintenance_mode = 'true', ' (maintenance mode)', ''),
                           IF(v_is_whitelisted > 0, ' (whitelisted)', '')),
                    NEW.batch_code, NEW.aggregation_level, NEW.school_id,
                    v_user_host, v_current_user, CONNECTION_ID(),
                    IF(v_is_whitelisted > 0, 1, 0),
                    IF(v_maintenance_mode = 'true', 1, 0),
                    v_decision,
                    UNIX_TIMESTAMP(NOW(3)) * 1000 - v_start_time,
                    NOW()
                );

                -- 执行决策
                IF v_decision = 'BLOCKED' THEN
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by enhanced guard';
                END IF;
            END IF;
        END
    """)

    # UPDATE触发器
    update_trigger_sql = text("""
        CREATE TRIGGER g7_enhanced_guard_update
        BEFORE UPDATE ON statistical_aggregations
        FOR EACH ROW
        BEGIN
            DECLARE v_maintenance_mode VARCHAR(10) DEFAULT 'false';
            DECLARE v_is_whitelisted INT DEFAULT 0;
            DECLARE v_user_host VARCHAR(255);
            DECLARE v_current_user VARCHAR(255);
            DECLARE v_decision VARCHAR(20) DEFAULT 'BLOCKED';
            DECLARE v_start_time BIGINT;

            IF NEW.batch_code = 'G7-2025' OR OLD.batch_code = 'G7-2025' THEN
                SET v_start_time = UNIX_TIMESTAMP(NOW(3)) * 1000;
                SET v_user_host = USER();
                SET v_current_user = CURRENT_USER();

                -- 检查维护模式
                SELECT config_value INTO v_maintenance_mode
                FROM g7_guard_config
                WHERE config_key = 'maintenance_mode'
                LIMIT 1;

                -- 检查白名单
                SELECT COUNT(*) INTO v_is_whitelisted
                FROM g7_guard_whitelist
                WHERE (v_user_host LIKE user_pattern OR v_current_user LIKE user_pattern)
                  AND is_active = 1
                LIMIT 1;

                -- 决策逻辑
                IF v_maintenance_mode = 'true' OR v_is_whitelisted > 0 THEN
                    SET v_decision = 'ALLOWED';
                ELSE
                    SET v_decision = 'BLOCKED';
                END IF;

                -- 记录日志
                INSERT INTO g7_enhanced_guard_log (
                    event, action, message, batch_code, aggregation_level, school_id,
                    user_host, current_user_name, connection_id, is_whitelisted,
                    maintenance_mode, decision, execution_time_ms, created_at
                ) VALUES (
                    'GUARD', 'UPDATE',
                    CONCAT('Decision: ', v_decision,
                           IF(v_maintenance_mode = 'true', ' (maintenance mode)', ''),
                           IF(v_is_whitelisted > 0, ' (whitelisted)', '')),
                    COALESCE(NEW.batch_code, OLD.batch_code),
                    COALESCE(NEW.aggregation_level, OLD.aggregation_level),
                    COALESCE(NEW.school_id, OLD.school_id),
                    v_user_host, v_current_user, CONNECTION_ID(),
                    IF(v_is_whitelisted > 0, 1, 0),
                    IF(v_maintenance_mode = 'true', 1, 0),
                    v_decision,
                    UNIX_TIMESTAMP(NOW(3)) * 1000 - v_start_time,
                    NOW()
                );

                -- 执行决策
                IF v_decision = 'BLOCKED' THEN
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by enhanced guard';
                END IF;
            END IF;
        END
    """)

    try:
        with get_db_context() as db:
            # 创建INSERT触发器
            db.execute(insert_trigger_sql)
            print("  - 创建 INSERT 增强守卫触发器")

            # 创建UPDATE触发器
            db.execute(update_trigger_sql)
            print("  - 创建 UPDATE 增强守卫触发器")

            db.commit()
            print("[SUCCESS] 增强G7守卫触发器创建完成")
            return True

    except Exception as e:
        print(f"[ERROR] 创建增强守卫触发器失败: {e}")
        return False

def verify_guard_system():
    """验证守卫系统状态"""
    print("[3] 验证守卫系统状态...")

    try:
        with get_db_context() as db:
            # 检查触发器
            result = db.execute(text("""
                SELECT TRIGGER_NAME, EVENT_MANIPULATION
                FROM INFORMATION_SCHEMA.TRIGGERS
                WHERE TRIGGER_NAME LIKE 'g7_enhanced_guard_%'
                ORDER BY TRIGGER_NAME
            """))
            triggers = result.fetchall()

            print(f"  - 增强守卫触发器: {len(triggers)} 个")
            for trigger in triggers:
                print(f"    * {trigger[0]} ({trigger[1]})")

            # 检查配置
            config_result = db.execute(text("""
                SELECT config_key, config_value
                FROM g7_guard_config
                WHERE config_key IN ('maintenance_mode', 'guard_enabled')
                ORDER BY config_key
            """))
            configs = dict(config_result.fetchall())

            print("  - 守卫配置:")
            for key, value in configs.items():
                print(f"    * {key}: {value}")

            # 检查白名单
            whitelist_result = db.execute(text("""
                SELECT user_pattern, is_active
                FROM g7_guard_whitelist
                WHERE is_active = 1
            """))
            whitelist = whitelist_result.fetchall()

            print(f"  - 活跃白名单: {len(whitelist)} 个")
            for pattern, active in whitelist:
                print(f"    * {pattern}")

            expected_triggers = 2
            system_ok = (
                len(triggers) == expected_triggers and
                configs.get('guard_enabled') == 'true' and
                configs.get('maintenance_mode') == 'false'
            )

            if system_ok:
                print("[SUCCESS] 守卫系统状态正常")
            else:
                print("[WARNING] 守卫系统状态异常")

            return system_ok

    except Exception as e:
        print(f"[ERROR] 验证守卫系统失败: {e}")
        return False

def test_whitelist_function():
    """测试白名单功能"""
    print("[4] 测试白名单功能...")

    try:
        with get_db_context() as db:
            # 获取当前用户信息
            user_result = db.execute(text("SELECT USER(), CURRENT_USER()"))
            user_info = user_result.fetchone()
            current_user, current_user_full = user_info

            print(f"  - 当前用户: {current_user}")
            print(f"  - 完整用户: {current_user_full}")

            # 检查是否在白名单中
            whitelist_check = db.execute(text("""
                SELECT user_pattern, is_active
                FROM g7_guard_whitelist
                WHERE (:user LIKE user_pattern OR :current_user LIKE user_pattern)
                  AND is_active = 1
            """), {"user": current_user, "current_user": current_user_full})

            whitelist_matches = whitelist_check.fetchall()

            if whitelist_matches:
                print(f"  - 白名单匹配: {len(whitelist_matches)} 个")
                for pattern, active in whitelist_matches:
                    print(f"    * 匹配模式: {pattern}")

                # 测试INSERT操作
                print("  - 测试白名单INSERT...")
                try:
                    db.execute(text("""
                        INSERT INTO statistical_aggregations
                        (batch_code, aggregation_level, school_id, statistics_data,
                         data_version, created_at, updated_at)
                        VALUES ('G7-2025', 'SCHOOL', 'WHITELIST_TEST', '{}',
                               'v1.2', NOW(), NOW())
                    """))
                    db.commit()
                    print("    ✅ 白名单用户写入成功")

                    # 清理测试数据
                    db.execute(text("""
                        DELETE FROM statistical_aggregations
                        WHERE batch_code = 'G7-2025' AND school_id = 'WHITELIST_TEST'
                    """))
                    db.commit()
                    print("    ✅ 测试数据已清理")

                    return True

                except Exception as e:
                    print(f"    ❌ 白名单测试失败: {e}")
                    return False

            else:
                print("  - 当前用户不在白名单中")
                return False

    except Exception as e:
        print(f"[ERROR] 测试白名单功能失败: {e}")
        return False

def main():
    print("=== 恢复正常G7守卫系统 ===")
    print(f"开始时间: {datetime.now()}")

    # 步骤1：删除紧急触发器
    if not remove_emergency_triggers():
        print("[FAILED] 无法删除紧急触发器")
        return False

    # 步骤2：创建增强守卫触发器
    if not create_enhanced_guard_triggers():
        print("[FAILED] 无法创建增强守卫触发器")
        return False

    # 步骤3：验证守卫系统
    system_ok = verify_guard_system()

    # 步骤4：测试白名单功能
    whitelist_ok = test_whitelist_function()

    print("\n" + "="*50)
    print("[SUMMARY] 守卫系统恢复结果:")
    print(f"  系统状态: {'正常' if system_ok else '异常'}")
    print(f"  白名单功能: {'正常' if whitelist_ok else '异常'}")

    if system_ok and whitelist_ok:
        print("\n[SUCCESS] G7守卫系统已完全恢复！")
        print("  - 支持白名单用户G7-2025操作")
        print("  - 阻断非授权用户访问")
        print("  - 维护模式功能可用")
        print("\n现在可以安全运行:")
        print("  python run_g7_pipeline_wrapper.py --batch G7-2025 --env production")
        return True
    else:
        print("\n[FAILED] 守卫系统恢复不完整")
        print("请检查错误信息并重试")
        return False

if __name__ == "__main__":
    main()