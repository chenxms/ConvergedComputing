#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025 守卫回滚脚本：
- 支持安全回滚到之前的状态
- 保留日志数据进行审计
- 支持紧急解锁功能
- 支持备份和恢复配置

用法：
  python scripts/g7_guard_rollback.py backup           # 备份当前配置
  python scripts/g7_guard_rollback.py rollback        # 回滚到安装前状态
  python scripts/g7_guard_rollback.py emergency       # 紧急解锁（移除所有限制）
  python scripts/g7_guard_rollback.py restore         # 从备份恢复配置
  python scripts/g7_guard_rollback.py clean           # 清理所有守卫相关数据
"""

import argparse
import json
import os
import sys
from datetime import datetime
from sqlalchemy import text
from app.database.connection import get_db


class G7GuardRollback:
    """G7守卫回滚管理器"""

    def __init__(self):
        self.db = next(get_db())
        self.backup_file = 'g7_guard_backup.json'

    def backup_config(self):
        """备份当前配置"""
        print("💾 备份当前G7守卫配置...")

        try:
            backup_data = {
                'timestamp': datetime.now().isoformat(),
                'triggers': [],
                'config': [],
                'whitelist': [],
                'log_count': 0
            }

            # 备份触发器信息
            result = self.db.execute(text("SHOW TRIGGERS LIKE 'statistical_aggregations'"))
            triggers = result.fetchall()

            for trigger in triggers:
                if 'g7' in trigger[0].lower():
                    backup_data['triggers'].append({
                        'name': trigger[0],
                        'event': trigger[1],
                        'table': trigger[2],
                        'timing': trigger[4]
                    })

            # 备份配置
            try:
                result = self.db.execute(text("SELECT * FROM g7_guard_config"))
                configs = result.fetchall()
                for config in configs:
                    backup_data['config'].append({
                        'key': config[0],
                        'value': config[1],
                        'description': config[2],
                        'updated_by': config[3],
                        'updated_at': config[4].isoformat() if config[4] else None
                    })
            except:
                print("  ⚠️ 配置表不存在，跳过备份")

            # 备份白名单
            try:
                result = self.db.execute(text("SELECT * FROM g7_guard_whitelist WHERE is_active = TRUE"))
                whitelist = result.fetchall()
                for user in whitelist:
                    backup_data['whitelist'].append({
                        'user_pattern': user[1],
                        'added_by': user[2],
                        'notes': user[3],
                        'created_at': user[5].isoformat() if user[5] else None
                    })
            except:
                print("  ⚠️ 白名单表不存在，跳过备份")

            # 统计日志数量
            try:
                result = self.db.execute(text("SELECT COUNT(*) FROM g7_enhanced_guard_log"))
                backup_data['log_count'] = result.fetchone()[0]
            except:
                print("  ⚠️ 日志表不存在，跳过统计")

            # 保存备份文件
            with open(self.backup_file, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, indent=2, ensure_ascii=False)

            print(f"✅ 配置已备份到: {self.backup_file}")
            print(f"  - 触发器: {len(backup_data['triggers'])}个")
            print(f"  - 配置项: {len(backup_data['config'])}个")
            print(f"  - 白名单: {len(backup_data['whitelist'])}个")
            print(f"  - 日志记录: {backup_data['log_count']}条")

        except Exception as e:
            print(f"❌ 备份失败: {e}")
            raise

    def rollback(self):
        """回滚到安装前状态"""
        print("🔄 回滚G7守卫到安装前状态...")

        try:
            # 1. 移除触发器
            print("  📋 移除触发器...")
            self.db.execute(text("DROP TRIGGER IF EXISTS g7_guard_insert"))
            self.db.execute(text("DROP TRIGGER IF EXISTS g7_guard_update"))
            self.db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_insert"))
            self.db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_update"))
            print("    ✅ 所有G7相关触发器已移除")

            # 2. 禁用配置（如果表存在）
            try:
                self.db.execute(text("""
                    UPDATE g7_guard_config
                    SET config_value = 'true'
                    WHERE config_key = 'maintenance_mode'
                """))
                print("    ✅ 已启用维护模式（安全措施）")
            except:
                print("    ⚠️ 配置表不存在，跳过配置更新")

            # 3. 记录回滚操作
            try:
                self.db.execute(text("""
                    INSERT INTO g7_enhanced_guard_log(
                        event, action, message, user_host, current_user_name,
                        connection_id, decision
                    ) VALUES (
                        'ADMIN', 'ROLLBACK', 'System rolled back to pre-installation state',
                        USER(), CURRENT_USER(), CONNECTION_ID(), 'LOGGED'
                    )
                """))
                print("    ✅ 回滚操作已记录")
            except:
                print("    ⚠️ 无法记录回滚操作（日志表可能不存在）")

            self.db.commit()
            print("✅ 回滚完成 - 系统已恢复到安装前状态")
            print("  🔧 当前处于维护模式，所有G7-2025操作都被允许")
            print("  📊 日志和配置表已保留，可以随时查看历史记录")

        except Exception as e:
            self.db.rollback()
            print(f"❌ 回滚失败: {e}")
            raise

    def emergency_unlock(self):
        """紧急解锁（移除所有限制）"""
        print("🚨 执行紧急解锁程序...")

        try:
            # 1. 强制移除所有相关触发器
            triggers_to_remove = [
                'g7_guard_insert', 'g7_guard_update',
                'g7_enhanced_guard_insert', 'g7_enhanced_guard_update'
            ]

            for trigger in triggers_to_remove:
                self.db.execute(text(f"DROP TRIGGER IF EXISTS {trigger}"))
                print(f"    🗑️ 移除触发器: {trigger}")

            # 2. 强制启用维护模式
            try:
                self.db.execute(text("""
                    UPDATE g7_guard_config
                    SET config_value = 'true'
                    WHERE config_key = 'maintenance_mode'
                """))
                print("    🔧 强制启用维护模式")
            except:
                # 如果配置表不存在，创建一个临时的
                try:
                    self.db.execute(text("""
                        CREATE TABLE IF NOT EXISTS g7_guard_config (
                            config_key VARCHAR(50) PRIMARY KEY,
                            config_value TEXT NOT NULL,
                            description TEXT NULL,
                            updated_by VARCHAR(128) NULL,
                            updated_at DATETIME NOT NULL DEFAULT NOW() ON UPDATE NOW()
                        )
                    """))
                    self.db.execute(text("""
                        INSERT INTO g7_guard_config (config_key, config_value, description, updated_by)
                        VALUES ('maintenance_mode', 'true', 'Emergency unlock', USER())
                        ON DUPLICATE KEY UPDATE config_value = 'true', updated_by = USER()
                    """))
                    print("    🔧 创建临时配置表并启用维护模式")
                except:
                    print("    ⚠️ 无法设置维护模式配置")

            # 3. 记录紧急解锁
            try:
                self.db.execute(text("""
                    CREATE TABLE IF NOT EXISTS emergency_log (
                        id BIGINT PRIMARY KEY AUTO_INCREMENT,
                        action VARCHAR(50) NOT NULL,
                        message TEXT NULL,
                        user_info VARCHAR(256) NULL,
                        created_at DATETIME NOT NULL DEFAULT NOW()
                    )
                """))
                self.db.execute(text("""
                    INSERT INTO emergency_log (action, message, user_info)
                    VALUES ('EMERGENCY_UNLOCK', 'All G7 guards removed via emergency procedure',
                            CONCAT('User: ', USER(), ', Connection: ', CONNECTION_ID()))
                """))
                print("    📝 紧急解锁已记录")
            except:
                print("    ⚠️ 无法记录紧急解锁操作")

            self.db.commit()
            print("✅ 紧急解锁完成")
            print("  🚨 所有G7-2025写入限制已移除")
            print("  🔧 系统处于完全开放状态")
            print("  ⚠️ 请在问题解决后重新安装守卫系统")

        except Exception as e:
            self.db.rollback()
            print(f"❌ 紧急解锁失败: {e}")
            raise

    def restore_config(self):
        """从备份恢复配置"""
        print("📥 从备份恢复G7守卫配置...")

        if not os.path.exists(self.backup_file):
            print(f"❌ 备份文件不存在: {self.backup_file}")
            return

        try:
            with open(self.backup_file, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)

            print(f"  📅 备份时间: {backup_data['timestamp']}")

            # 恢复配置
            if backup_data['config']:
                print("  ⚙️ 恢复配置...")
                for config in backup_data['config']:
                    self.db.execute(text("""
                        INSERT INTO g7_guard_config (config_key, config_value, description, updated_by)
                        VALUES (:key, :value, :desc, :updated_by)
                        ON DUPLICATE KEY UPDATE
                        config_value = VALUES(config_value),
                        description = VALUES(description),
                        updated_by = VALUES(updated_by)
                    """), {
                        'key': config['key'],
                        'value': config['value'],
                        'desc': config['description'],
                        'updated_by': config['updated_by']
                    })
                print(f"    ✅ 恢复了 {len(backup_data['config'])} 个配置项")

            # 恢复白名单
            if backup_data['whitelist']:
                print("  👥 恢复白名单...")
                for user in backup_data['whitelist']:
                    self.db.execute(text("""
                        INSERT INTO g7_guard_whitelist (user_pattern, added_by, notes)
                        VALUES (:pattern, :added_by, :notes)
                        ON DUPLICATE KEY UPDATE
                        is_active = TRUE,
                        notes = CONCAT(notes, ' [Restored from backup]')
                    """), {
                        'pattern': user['user_pattern'],
                        'added_by': user['added_by'],
                        'notes': user['notes'] + ' [Restored from backup]'
                    })
                print(f"    ✅ 恢复了 {len(backup_data['whitelist'])} 个白名单用户")

            # 记录恢复操作
            try:
                self.db.execute(text("""
                    INSERT INTO g7_enhanced_guard_log(
                        event, action, message, user_host, current_user_name,
                        connection_id, decision
                    ) VALUES (
                        'ADMIN', 'RESTORE', :message,
                        USER(), CURRENT_USER(), CONNECTION_ID(), 'LOGGED'
                    )
                """), {
                    'message': f'Configuration restored from backup: {backup_data["timestamp"]}'
                })
            except:
                print("    ⚠️ 无法记录恢复操作")

            self.db.commit()
            print("✅ 配置恢复完成")
            print("  ⚠️ 触发器需要手动重新安装")

        except Exception as e:
            self.db.rollback()
            print(f"❌ 恢复失败: {e}")
            raise

    def clean_all(self):
        """清理所有守卫相关数据"""
        print("🧹 清理所有G7守卫相关数据...")
        print("⚠️ 这将删除所有配置、日志和白名单数据！")

        confirm = input("确认要继续吗？输入 'YES' 确认: ")
        if confirm != 'YES':
            print("❌ 操作已取消")
            return

        try:
            # 移除触发器
            triggers = [
                'g7_guard_insert', 'g7_guard_update',
                'g7_enhanced_guard_insert', 'g7_enhanced_guard_update'
            ]

            for trigger in triggers:
                self.db.execute(text(f"DROP TRIGGER IF EXISTS {trigger}"))
                print(f"  🗑️ 移除触发器: {trigger}")

            # 删除表
            tables = [
                'g7_enhanced_guard_log',
                'g7_guard_log',
                'g7_guard_whitelist',
                'g7_guard_config',
                'emergency_log'
            ]

            for table in tables:
                self.db.execute(text(f"DROP TABLE IF EXISTS {table}"))
                print(f"  🗑️ 删除表: {table}")

            # 删除备份文件
            if os.path.exists(self.backup_file):
                os.remove(self.backup_file)
                print(f"  🗑️ 删除备份文件: {self.backup_file}")

            self.db.commit()
            print("✅ 清理完成 - 所有G7守卫数据已删除")
            print("  🚨 系统已完全解除保护，可以自由写入G7-2025数据")

        except Exception as e:
            self.db.rollback()
            print(f"❌ 清理失败: {e}")
            raise

    def status(self):
        """显示当前状态"""
        print("📊 G7守卫系统状态:")

        try:
            # 检查触发器
            result = self.db.execute(text("SHOW TRIGGERS LIKE 'statistical_aggregations'"))
            triggers = result.fetchall()
            g7_triggers = [t for t in triggers if 'g7' in t[0].lower()]

            print(f"  📋 触发器: {len(g7_triggers)}个")
            for trigger in g7_triggers:
                print(f"    - {trigger[0]} ({trigger[1]})")

            # 检查配置
            try:
                result = self.db.execute(text("SELECT config_key, config_value FROM g7_guard_config"))
                configs = result.fetchall()
                print(f"  ⚙️ 配置: {len(configs)}个")
                for config in configs:
                    print(f"    - {config[0]}: {config[1]}")
            except:
                print("  ⚙️ 配置表不存在")

            # 检查备份
            if os.path.exists(self.backup_file):
                try:
                    with open(self.backup_file, 'r', encoding='utf-8') as f:
                        backup_data = json.load(f)
                    print(f"  💾 备份文件存在 (时间: {backup_data['timestamp']})")
                except:
                    print("  💾 备份文件存在但格式错误")
            else:
                print("  💾 无备份文件")

        except Exception as e:
            print(f"❌ 状态检查失败: {e}")

    def __del__(self):
        """清理资源"""
        if hasattr(self, 'db'):
            self.db.close()


def main():
    parser = argparse.ArgumentParser(description='G7守卫回滚工具')
    parser.add_argument('action', choices=['backup', 'rollback', 'emergency', 'restore', 'clean', 'status'],
                       help='要执行的操作')

    args = parser.parse_args()

    rollback = G7GuardRollback()

    try:
        if args.action == 'backup':
            rollback.backup_config()
        elif args.action == 'rollback':
            rollback.rollback()
        elif args.action == 'emergency':
            rollback.emergency_unlock()
        elif args.action == 'restore':
            rollback.restore_config()
        elif args.action == 'clean':
            rollback.clean_all()
        elif args.action == 'status':
            rollback.status()

    except Exception as e:
        print(f"❌ 操作失败: {e}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())