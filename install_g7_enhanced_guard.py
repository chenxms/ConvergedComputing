#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
简化的G7增强守卫安装脚本
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text
from app.database.connection import get_db


def install_enhanced_guard():
    """安装增强守卫系统"""
    print("Installing enhanced G7 guard system...")

    with next(get_db()) as db:
        try:
            # 1. 创建增强日志表
            print("Creating enhanced log table...")
            db.execute(text("""
                CREATE TABLE IF NOT EXISTS g7_enhanced_guard_log (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    event VARCHAR(20) NOT NULL,
                    action VARCHAR(20) NOT NULL,
                    message TEXT NULL,
                    batch_code VARCHAR(50) NULL,
                    aggregation_level VARCHAR(30) NULL,
                    school_id VARCHAR(60) NULL,
                    user_host VARCHAR(128) NULL,
                    current_user_name VARCHAR(128) NULL,
                    connection_id BIGINT NULL,
                    is_whitelisted BOOLEAN DEFAULT FALSE,
                    maintenance_mode BOOLEAN DEFAULT FALSE,
                    decision VARCHAR(20) NOT NULL,
                    execution_time_ms INT NULL,
                    created_at DATETIME NOT NULL DEFAULT NOW(),
                    INDEX idx_created_at (created_at),
                    INDEX idx_event (event),
                    INDEX idx_decision (decision),
                    INDEX idx_batch_code (batch_code)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))

            # 2. 创建白名单表
            print("Creating whitelist table...")
            db.execute(text("""
                CREATE TABLE IF NOT EXISTS g7_guard_whitelist (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    user_pattern VARCHAR(128) NOT NULL UNIQUE,
                    added_by VARCHAR(128) NOT NULL,
                    notes TEXT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at DATETIME NOT NULL DEFAULT NOW(),
                    INDEX idx_user_pattern (user_pattern),
                    INDEX idx_is_active (is_active)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))

            # 3. 创建配置表
            print("Creating config table...")
            db.execute(text("""
                CREATE TABLE IF NOT EXISTS g7_guard_config (
                    config_key VARCHAR(50) PRIMARY KEY,
                    config_value TEXT NOT NULL,
                    description TEXT NULL,
                    updated_by VARCHAR(128) NULL,
                    updated_at DATETIME NOT NULL DEFAULT NOW() ON UPDATE NOW()
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))

            # 4. 清理旧触发器
            print("Cleaning old triggers...")
            db.execute(text("DROP TRIGGER IF EXISTS g7_guard_insert"))
            db.execute(text("DROP TRIGGER IF EXISTS g7_guard_update"))
            db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_insert"))
            db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_update"))

            # 5. 安装增强触发器 - INSERT
            print("Installing INSERT trigger...")
            db.execute(text("""
                CREATE TRIGGER g7_enhanced_guard_insert
                BEFORE INSERT ON statistical_aggregations
                FOR EACH ROW
                BEGIN
                    DECLARE nb VARCHAR(64);
                    DECLARE maintenance_enabled BOOLEAN DEFAULT FALSE;
                    DECLARE user_whitelisted BOOLEAN DEFAULT FALSE;
                    DECLARE start_time BIGINT DEFAULT UNIX_TIMESTAMP(NOW(3)) * 1000;
                    DECLARE decision VARCHAR(20) DEFAULT 'BLOCKED';

                    SET nb = TRIM(REPLACE(REPLACE(REPLACE(NEW.batch_code, '–','-'),'−','-'),'—','-'));

                    IF nb = 'G7-2025' THEN
                        -- 检查维护模式
                        SELECT CASE WHEN config_value = 'true' THEN TRUE ELSE FALSE END
                        INTO maintenance_enabled
                        FROM g7_guard_config
                        WHERE config_key = 'maintenance_mode'
                        LIMIT 1;

                        -- 检查白名单
                        SELECT COUNT(*) > 0
                        INTO user_whitelisted
                        FROM g7_guard_whitelist
                        WHERE is_active = TRUE
                        AND (USER() LIKE user_pattern OR CURRENT_USER() LIKE user_pattern);

                        -- 决策逻辑
                        IF maintenance_enabled OR user_whitelisted THEN
                            SET decision = 'ALLOWED';
                        ELSE
                            SET decision = 'BLOCKED';
                        END IF;

                        -- 记录日志
                        INSERT INTO g7_enhanced_guard_log(
                            event, action, message, batch_code, aggregation_level, school_id,
                            user_host, current_user_name, connection_id, is_whitelisted,
                            maintenance_mode, decision, execution_time_ms
                        ) VALUES (
                            'GUARD', 'INSERT',
                            CONCAT('Decision: ', decision,
                                   CASE WHEN maintenance_enabled THEN ' (maintenance mode)' ELSE '' END,
                                   CASE WHEN user_whitelisted THEN ' (whitelisted user)' ELSE '' END),
                            nb, NEW.aggregation_level, NEW.school_id,
                            USER(), CURRENT_USER(), CONNECTION_ID(), user_whitelisted,
                            maintenance_enabled, decision,
                            UNIX_TIMESTAMP(NOW(3)) * 1000 - start_time
                        );

                        -- 如果不是维护模式且用户不在白名单中，则阻断
                        IF decision = 'BLOCKED' THEN
                            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by enhanced guard';
                        END IF;
                    END IF;
                END
            """))

            # 6. 安装增强触发器 - UPDATE
            print("Installing UPDATE trigger...")
            db.execute(text("""
                CREATE TRIGGER g7_enhanced_guard_update
                BEFORE UPDATE ON statistical_aggregations
                FOR EACH ROW
                BEGIN
                    DECLARE nb VARCHAR(64);
                    DECLARE maintenance_enabled BOOLEAN DEFAULT FALSE;
                    DECLARE user_whitelisted BOOLEAN DEFAULT FALSE;
                    DECLARE start_time BIGINT DEFAULT UNIX_TIMESTAMP(NOW(3)) * 1000;
                    DECLARE decision VARCHAR(20) DEFAULT 'BLOCKED';

                    SET nb = TRIM(REPLACE(REPLACE(REPLACE(NEW.batch_code, '–','-'),'−','-'),'—','-'));

                    IF nb = 'G7-2025' THEN
                        -- 检查维护模式
                        SELECT CASE WHEN config_value = 'true' THEN TRUE ELSE FALSE END
                        INTO maintenance_enabled
                        FROM g7_guard_config
                        WHERE config_key = 'maintenance_mode'
                        LIMIT 1;

                        -- 检查白名单
                        SELECT COUNT(*) > 0
                        INTO user_whitelisted
                        FROM g7_guard_whitelist
                        WHERE is_active = TRUE
                        AND (USER() LIKE user_pattern OR CURRENT_USER() LIKE user_pattern);

                        -- 决策逻辑
                        IF maintenance_enabled OR user_whitelisted THEN
                            SET decision = 'ALLOWED';
                        ELSE
                            SET decision = 'BLOCKED';
                        END IF;

                        -- 记录日志
                        INSERT INTO g7_enhanced_guard_log(
                            event, action, message, batch_code, aggregation_level, school_id,
                            user_host, current_user_name, connection_id, is_whitelisted,
                            maintenance_mode, decision, execution_time_ms
                        ) VALUES (
                            'GUARD', 'UPDATE',
                            CONCAT('Decision: ', decision,
                                   CASE WHEN maintenance_enabled THEN ' (maintenance mode)' ELSE '' END,
                                   CASE WHEN user_whitelisted THEN ' (whitelisted user)' ELSE '' END),
                            nb, NEW.aggregation_level, NEW.school_id,
                            USER(), CURRENT_USER(), CONNECTION_ID(), user_whitelisted,
                            maintenance_enabled, decision,
                            UNIX_TIMESTAMP(NOW(3)) * 1000 - start_time
                        );

                        -- 如果不是维护模式且用户不在白名单中，则阻断
                        IF decision = 'BLOCKED' THEN
                            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by enhanced guard';
                        END IF;
                    END IF;
                END
            """))

            # 7. 初始化配置
            print("Initializing configuration...")
            configs = [
                ('maintenance_mode', 'false', 'Maintenance mode switch'),
                ('guard_enabled', 'true', 'Guard enable status'),
                ('log_retention_days', '30', 'Log retention days')
            ]

            for key, value, desc in configs:
                db.execute(text("""
                    INSERT IGNORE INTO g7_guard_config (config_key, config_value, description, updated_by)
                    VALUES (:key, :value, :desc, USER())
                """), {'key': key, 'value': value, 'desc': desc})

            db.commit()
            print("Enhanced G7 guard system installed successfully!")

            # 8. 验证安装
            print("\nVerifying installation...")
            result = db.execute(text("SHOW TRIGGERS LIKE 'statistical_aggregations'"))
            triggers = result.fetchall()
            g7_triggers = [t for t in triggers if 'g7_enhanced_guard' in t[0]]

            if len(g7_triggers) == 2:
                print(f"OK: {len(g7_triggers)} triggers installed")
                for trigger in g7_triggers:
                    print(f"  - {trigger[0]} ({trigger[1]})")
            else:
                print(f"WARNING: Expected 2 triggers, found {len(g7_triggers)}")

            # 检查表
            tables = ['g7_enhanced_guard_log', 'g7_guard_whitelist', 'g7_guard_config']
            for table in tables:
                try:
                    result = db.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.fetchone()[0]
                    print(f"OK: Table {table} exists with {count} records")
                except Exception as e:
                    print(f"ERROR: Table {table} check failed: {e}")

        except Exception as e:
            db.rollback()
            print(f"Installation failed: {e}")
            raise


if __name__ == '__main__':
    try:
        install_enhanced_guard()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)