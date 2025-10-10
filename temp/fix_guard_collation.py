#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""修复G7守卫触发器字符集问题"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from app.database.connection import get_db_context
from sqlalchemy import text

def fix_guard_triggers():
    """修复守卫触发器字符集问题"""
    print("[1] 删除现有触发器...")

    try:
        with get_db_context() as db:
            # 删除现有触发器
            db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_insert"))
            db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_update"))

            print("[2] 创建修复版触发器...")

            # 修复版INSERT触发器（使用COLLATE统一字符集）
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

                    IF NEW.batch_code = 'G7-2025' THEN
                        SET v_user_host = USER();
                        SET v_current_user = CURRENT_USER();

                        -- 检查维护模式
                        SELECT config_value INTO v_maintenance_mode
                        FROM g7_guard_config
                        WHERE config_key = 'maintenance_mode'
                        LIMIT 1;

                        -- 检查白名单（修复字符集问题）
                        SELECT COUNT(*) INTO v_is_whitelisted
                        FROM g7_guard_whitelist
                        WHERE (v_user_host COLLATE utf8mb4_general_ci LIKE user_pattern COLLATE utf8mb4_general_ci
                               OR v_current_user COLLATE utf8mb4_general_ci LIKE user_pattern COLLATE utf8mb4_general_ci)
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
                            v_decision, 0, NOW()
                        );

                        -- 执行决策
                        IF v_decision = 'BLOCKED' THEN
                            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by enhanced guard';
                        END IF;
                    END IF;
                END
            """)

            # 修复版UPDATE触发器
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

                    IF NEW.batch_code = 'G7-2025' OR OLD.batch_code = 'G7-2025' THEN
                        SET v_user_host = USER();
                        SET v_current_user = CURRENT_USER();

                        -- 检查维护模式
                        SELECT config_value INTO v_maintenance_mode
                        FROM g7_guard_config
                        WHERE config_key = 'maintenance_mode'
                        LIMIT 1;

                        -- 检查白名单（修复字符集问题）
                        SELECT COUNT(*) INTO v_is_whitelisted
                        FROM g7_guard_whitelist
                        WHERE (v_user_host COLLATE utf8mb4_general_ci LIKE user_pattern COLLATE utf8mb4_general_ci
                               OR v_current_user COLLATE utf8mb4_general_ci LIKE user_pattern COLLATE utf8mb4_general_ci)
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
                            v_decision, 0, NOW()
                        );

                        -- 执行决策
                        IF v_decision = 'BLOCKED' THEN
                            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by enhanced guard';
                        END IF;
                    END IF;
                END
            """)

            # 创建触发器
            db.execute(insert_trigger_sql)
            print("  - 创建 INSERT 触发器")

            db.execute(update_trigger_sql)
            print("  - 创建 UPDATE 触发器")

            db.commit()
            print("[SUCCESS] 修复版守卫触发器创建完成")
            return True

    except Exception as e:
        print(f"[ERROR] 修复触发器失败: {e}")
        return False

def test_fixed_triggers():
    """测试修复后的触发器"""
    print("[3] 测试修复后的触发器...")

    try:
        with get_db_context() as db:
            # 测试INSERT
            print("  - 测试 INSERT...")
            db.execute(text("""
                INSERT INTO statistical_aggregations
                (batch_code, aggregation_level, school_id, statistics_data,
                 data_version, created_at, updated_at)
                VALUES ('G7-2025', 'SCHOOL', 'COLLATION_TEST', '{}',
                       'v1.2', NOW(), NOW())
            """))
            print("    SUCCESS: INSERT 允许")

            # 测试UPDATE
            print("  - 测试 UPDATE...")
            db.execute(text("""
                UPDATE statistical_aggregations
                SET statistics_data = '{"test": "updated"}'
                WHERE batch_code = 'G7-2025' AND school_id = 'COLLATION_TEST'
            """))
            print("    SUCCESS: UPDATE 允许")

            db.commit()
            print("    SUCCESS: 事务提交成功")

            # 清理测试数据
            db.execute(text("""
                DELETE FROM statistical_aggregations
                WHERE batch_code = 'G7-2025' AND school_id = 'COLLATION_TEST'
            """))
            db.commit()
            print("    SUCCESS: 测试数据已清理")

            return True

    except Exception as e:
        print(f"    ERROR: 测试失败: {e}")
        return False

def main():
    print("=== 修复G7守卫触发器字符集问题 ===")

    # 修复触发器
    if not fix_guard_triggers():
        return False

    # 测试触发器
    if test_fixed_triggers():
        print("\n[SUCCESS] G7守卫系统完全恢复正常！")
        print("  - 白名单功能正常工作")
        print("  - 字符集问题已解决")
        print("  - 可以安全运行G7-2025汇聚任务")
        print("\n现在可以执行:")
        print("  python run_g7_pipeline_wrapper.py --batch G7-2025 --env production")
        return True
    else:
        print("\n[FAILED] 触发器测试失败")
        return False

if __name__ == "__main__":
    main()