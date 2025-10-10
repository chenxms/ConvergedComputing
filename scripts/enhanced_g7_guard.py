#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
增强的 G7-2025 写入守卫管理器：
- 支持白名单机制，允许特定用户/连接绕过保护
- 支持维护模式切换
- 增强的日志记录和监控功能
- 支持回滚和恢复机制

用法：
  python scripts/enhanced_g7_guard.py install           # 安装增强守卫
  python scripts/enhanced_g7_guard.py uninstall         # 卸载守卫
  python scripts/enhanced_g7_guard.py add-whitelist <user>  # 添加白名单用户
  python scripts/enhanced_g7_guard.py remove-whitelist <user>  # 移除白名单用户
  python scripts/enhanced_g7_guard.py enable-maintenance    # 启用维护模式
  python scripts/enhanced_g7_guard.py disable-maintenance   # 禁用维护模式
  python scripts/enhanced_g7_guard.py status               # 查看状态
  python scripts/enhanced_g7_guard.py validate             # 验证触发器

说明：
- 增强守卫支持白名单机制，白名单用户可以绕过保护
- 维护模式下，所有写入操作都被允许（用于紧急修复）
- 所有操作都会记录详细日志
"""

import argparse
import sys
from datetime import datetime
from sqlalchemy import text
from app.database.connection import get_db


class EnhancedG7Guard:
    """增强的G7-2025守卫管理器"""

    def __init__(self):
        self.db = next(get_db())

    def install(self):
        """安装增强守卫系统"""
        print("🔧 正在安装增强G7守卫系统...")

        try:
            # 1. 创建增强日志表
            self._create_enhanced_log_table()

            # 2. 创建白名单表
            self._create_whitelist_table()

            # 3. 创建配置表
            self._create_config_table()

            # 4. 清理旧触发器
            self._cleanup_old_triggers()

            # 5. 安装增强触发器
            self._install_enhanced_triggers()

            # 6. 初始化配置
            self._initialize_config()

            self.db.commit()
            print("✅ 增强G7守卫系统安装完成")
            self._print_status()

        except Exception as e:
            self.db.rollback()
            print(f"❌ 安装失败: {e}")
            raise

    def uninstall(self):
        """卸载守卫系统"""
        print("🗑️ 正在卸载G7守卫系统...")

        try:
            # 移除触发器
            self.db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_insert"))
            self.db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_update"))

            # 清理配置（设置为维护模式以避免意外阻塞）
            self.db.execute(text("""
                UPDATE g7_guard_config
                SET config_value = 'true'
                WHERE config_key = 'maintenance_mode'
            """))

            self.db.commit()
            print("✅ 守卫触发器已卸载（保留配置表和日志表）")

        except Exception as e:
            self.db.rollback()
            print(f"❌ 卸载失败: {e}")
            raise

    def add_whitelist(self, user_pattern):
        """添加白名单用户"""
        print(f"➕ 添加白名单用户: {user_pattern}")

        try:
            self.db.execute(text("""
                INSERT INTO g7_guard_whitelist (user_pattern, added_by, notes)
                VALUES (:user_pattern, USER(), :notes)
            """), {
                'user_pattern': user_pattern,
                'notes': f'Added via CLI at {datetime.now()}'
            })

            self._log_action('WHITELIST_ADD', f'Added user pattern: {user_pattern}')
            self.db.commit()
            print("✅ 白名单用户添加成功")

        except Exception as e:
            self.db.rollback()
            print(f"❌ 添加失败: {e}")
            raise

    def remove_whitelist(self, user_pattern):
        """移除白名单用户"""
        print(f"➖ 移除白名单用户: {user_pattern}")

        try:
            result = self.db.execute(text("""
                DELETE FROM g7_guard_whitelist
                WHERE user_pattern = :user_pattern
            """), {'user_pattern': user_pattern})

            if result.rowcount > 0:
                self._log_action('WHITELIST_REMOVE', f'Removed user pattern: {user_pattern}')
                self.db.commit()
                print("✅ 白名单用户移除成功")
            else:
                print("⚠️ 未找到指定的白名单用户")

        except Exception as e:
            self.db.rollback()
            print(f"❌ 移除失败: {e}")
            raise

    def enable_maintenance(self):
        """启用维护模式"""
        print("🔧 启用维护模式...")

        try:
            self._set_config('maintenance_mode', 'true')
            self._log_action('MAINTENANCE_ON', 'Maintenance mode enabled')
            self.db.commit()
            print("✅ 维护模式已启用（所有G7-2025写入操作将被允许）")

        except Exception as e:
            self.db.rollback()
            print(f"❌ 启用失败: {e}")
            raise

    def disable_maintenance(self):
        """禁用维护模式"""
        print("🔒 禁用维护模式...")

        try:
            self._set_config('maintenance_mode', 'false')
            self._log_action('MAINTENANCE_OFF', 'Maintenance mode disabled')
            self.db.commit()
            print("✅ 维护模式已禁用（G7-2025写入保护已恢复）")

        except Exception as e:
            self.db.rollback()
            print(f"❌ 禁用失败: {e}")
            raise

    def status(self):
        """查看守卫状态"""
        print("📊 G7守卫系统状态:")
        self._print_status()

    def validate(self):
        """验证触发器完整性"""
        print("🔍 验证触发器完整性...")

        try:
            # 检查触发器存在性
            result = self.db.execute(text("""
                SHOW TRIGGERS LIKE 'statistical_aggregations'
            """))

            triggers = result.fetchall()
            g7_triggers = [t for t in triggers if 'g7_enhanced_guard' in t[0]]

            if len(g7_triggers) == 2:
                print("✅ 触发器完整性验证通过")
                for trigger in g7_triggers:
                    print(f"  - {trigger[0]} ({trigger[1]})")
            else:
                print(f"❌ 触发器不完整，期望2个，实际{len(g7_triggers)}个")
                return False

            # 检查表存在性
            tables = ['g7_enhanced_guard_log', 'g7_guard_whitelist', 'g7_guard_config']
            for table in tables:
                try:
                    self.db.execute(text(f"SELECT 1 FROM {table} LIMIT 1"))
                    print(f"✅ 表 {table} 存在")
                except:
                    print(f"❌ 表 {table} 不存在")
                    return False

            return True

        except Exception as e:
            print(f"❌ 验证失败: {e}")
            return False

    def _create_enhanced_log_table(self):
        """创建增强日志表"""
        self.db.execute(text("""
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

    def _create_whitelist_table(self):
        """创建白名单表"""
        self.db.execute(text("""
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

    def _create_config_table(self):
        """创建配置表"""
        self.db.execute(text("""
            CREATE TABLE IF NOT EXISTS g7_guard_config (
                config_key VARCHAR(50) PRIMARY KEY,
                config_value TEXT NOT NULL,
                description TEXT NULL,
                updated_by VARCHAR(128) NULL,
                updated_at DATETIME NOT NULL DEFAULT NOW() ON UPDATE NOW()
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

    def _cleanup_old_triggers(self):
        """清理旧触发器"""
        self.db.execute(text("DROP TRIGGER IF EXISTS g7_guard_insert"))
        self.db.execute(text("DROP TRIGGER IF EXISTS g7_guard_update"))
        self.db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_insert"))
        self.db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_update"))

    def _install_enhanced_triggers(self):
        """安装增强触发器"""
        # INSERT 触发器
        self.db.execute(text("""
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

        # UPDATE 触发器
        self.db.execute(text("""
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

    def _initialize_config(self):
        """初始化配置"""
        configs = [
            ('maintenance_mode', 'false', '维护模式开关'),
            ('guard_enabled', 'true', '守卫启用状态'),
            ('log_retention_days', '30', '日志保留天数')
        ]

        for key, value, desc in configs:
            self.db.execute(text("""
                INSERT IGNORE INTO g7_guard_config (config_key, config_value, description, updated_by)
                VALUES (:key, :value, :desc, USER())
            """), {'key': key, 'value': value, 'desc': desc})

    def _set_config(self, key, value):
        """设置配置值"""
        self.db.execute(text("""
            UPDATE g7_guard_config
            SET config_value = :value, updated_by = USER()
            WHERE config_key = :key
        """), {'key': key, 'value': value})

    def _log_action(self, action, message):
        """记录管理操作"""
        self.db.execute(text("""
            INSERT INTO g7_enhanced_guard_log(
                event, action, message, user_host, current_user_name,
                connection_id, decision
            ) VALUES (
                'ADMIN', :action, :message, USER(), CURRENT_USER(),
                CONNECTION_ID(), 'LOGGED'
            )
        """), {'action': action, 'message': message})

    def _print_status(self):
        """打印系统状态"""
        try:
            # 检查触发器
            result = self.db.execute(text("SHOW TRIGGERS LIKE 'statistical_aggregations'"))
            triggers = result.fetchall()
            g7_triggers = [t for t in triggers if 'g7_enhanced_guard' in t[0]]

            print(f"\n📋 触发器状态: {'✅ 已安装' if len(g7_triggers) == 2 else '❌ 未安装'}")
            for trigger in g7_triggers:
                print(f"  - {trigger[0]} ({trigger[1]})")

            # 检查配置
            try:
                result = self.db.execute(text("""
                    SELECT config_key, config_value, updated_at
                    FROM g7_guard_config
                    ORDER BY config_key
                """))
                configs = result.fetchall()

                print(f"\n⚙️ 系统配置:")
                for config in configs:
                    print(f"  - {config[0]}: {config[1]} (更新时间: {config[2]})")

                # 特别标注维护模式状态
                maintenance = next((c[1] for c in configs if c[0] == 'maintenance_mode'), 'false')
                if maintenance == 'true':
                    print("  🔧 当前处于维护模式 - 所有G7-2025写入操作被允许")
                else:
                    print("  🔒 当前处于保护模式 - G7-2025写入操作被阻断")

            except:
                print("⚠️ 配置表不存在")

            # 检查白名单
            try:
                result = self.db.execute(text("""
                    SELECT user_pattern, added_by, created_at
                    FROM g7_guard_whitelist
                    WHERE is_active = TRUE
                    ORDER BY created_at DESC
                """))
                whitelist = result.fetchall()

                print(f"\n👥 白名单用户 ({len(whitelist)}个):")
                if whitelist:
                    for user in whitelist:
                        print(f"  - {user[0]} (添加者: {user[1]}, 时间: {user[2]})")
                else:
                    print("  无白名单用户")

            except:
                print("⚠️ 白名单表不存在")

            # 检查日志统计
            try:
                result = self.db.execute(text("""
                    SELECT decision, COUNT(*) as count, MAX(created_at) as latest
                    FROM g7_enhanced_guard_log
                    WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                    GROUP BY decision
                    ORDER BY count DESC
                """))
                logs = result.fetchall()

                print(f"\n📊 24小时日志统计:")
                if logs:
                    for log in logs:
                        print(f"  - {log[0]}: {log[1]}次 (最新: {log[2]})")
                else:
                    print("  无日志记录")

            except:
                print("⚠️ 增强日志表不存在")

        except Exception as e:
            print(f"⚠️ 状态检查失败: {e}")

    def __del__(self):
        """清理资源"""
        if hasattr(self, 'db'):
            self.db.close()


def main():
    parser = argparse.ArgumentParser(description='增强G7守卫管理器')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # 安装命令
    subparsers.add_parser('install', help='安装增强守卫')

    # 卸载命令
    subparsers.add_parser('uninstall', help='卸载守卫')

    # 白名单管理
    add_parser = subparsers.add_parser('add-whitelist', help='添加白名单用户')
    add_parser.add_argument('user', help='用户模式 (支持通配符)')

    remove_parser = subparsers.add_parser('remove-whitelist', help='移除白名单用户')
    remove_parser.add_argument('user', help='用户模式')

    # 维护模式
    subparsers.add_parser('enable-maintenance', help='启用维护模式')
    subparsers.add_parser('disable-maintenance', help='禁用维护模式')

    # 状态和验证
    subparsers.add_parser('status', help='查看状态')
    subparsers.add_parser('validate', help='验证触发器')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    guard = EnhancedG7Guard()

    try:
        if args.command == 'install':
            guard.install()
        elif args.command == 'uninstall':
            guard.uninstall()
        elif args.command == 'add-whitelist':
            guard.add_whitelist(args.user)
        elif args.command == 'remove-whitelist':
            guard.remove_whitelist(args.user)
        elif args.command == 'enable-maintenance':
            guard.enable_maintenance()
        elif args.command == 'disable-maintenance':
            guard.disable_maintenance()
        elif args.command == 'status':
            guard.status()
        elif args.command == 'validate':
            if guard.validate():
                sys.exit(0)
            else:
                sys.exit(1)

    except Exception as e:
        print(f"❌ 操作失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()