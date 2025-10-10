#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
维护窗口管理器：
- 安全管理维护窗口期间的G7守卫状态
- 支持计划性维护和紧急维护
- 自动日志记录和通知
- 支持回滚计划

用法：
  python scripts/maintenance_window_manager.py start --duration 60    # 开始60分钟维护窗口
  python scripts/maintenance_window_manager.py start --emergency      # 开始紧急维护
  python scripts/maintenance_window_manager.py extend --minutes 30    # 延长30分钟
  python scripts/maintenance_window_manager.py stop                   # 结束维护窗口
  python scripts/maintenance_window_manager.py status                 # 查看状态
  python scripts/maintenance_window_manager.py schedule               # 查看维护计划
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from sqlalchemy import text
from app.database.connection import get_db


class MaintenanceWindowManager:
    """维护窗口管理器"""

    def __init__(self):
        self.db = next(get_db())

    def start_maintenance(self, duration_minutes=None, emergency=False):
        """开始维护窗口"""
        if emergency:
            print("🚨 启动紧急维护窗口...")
            duration_minutes = 120  # 紧急维护默认2小时
        else:
            print(f"🔧 启动计划维护窗口 (持续{duration_minutes}分钟)...")

        try:
            # 检查当前状态
            current_status = self._get_maintenance_status()
            if current_status['active']:
                print(f"⚠️ 维护窗口已在进行中，结束时间: {current_status['end_time']}")
                return

            # 设置维护窗口
            start_time = datetime.now()
            end_time = start_time + timedelta(minutes=duration_minutes) if duration_minutes else None

            # 备份当前守卫状态
            self._backup_guard_state()

            # 启用维护模式
            self._set_maintenance_mode(True)

            # 记录维护窗口
            window_id = self._create_maintenance_window(start_time, end_time, emergency)

            # 记录操作日志
            self._log_maintenance_action(
                'START',
                f'Maintenance window started. Type: {"Emergency" if emergency else "Planned"}, '
                f'Duration: {duration_minutes} minutes, Window ID: {window_id}'
            )

            print("✅ 维护窗口已启动")
            print(f"  🆔 窗口ID: {window_id}")
            print(f"  ⏰ 开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            if end_time:
                print(f"  ⏰ 结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  🔧 模式: {'紧急维护' if emergency else '计划维护'}")
            print("  🔓 G7-2025写入限制已解除")

            return window_id

        except Exception as e:
            print(f"❌ 启动维护窗口失败: {e}")
            raise

    def extend_maintenance(self, additional_minutes):
        """延长维护窗口"""
        print(f"⏰ 延长维护窗口 {additional_minutes} 分钟...")

        try:
            current_status = self._get_maintenance_status()
            if not current_status['active']:
                print("❌ 当前没有活动的维护窗口")
                return

            window_id = current_status['window_id']
            old_end_time = current_status['end_time']

            # 计算新的结束时间
            if old_end_time:
                new_end_time = old_end_time + timedelta(minutes=additional_minutes)
            else:
                new_end_time = datetime.now() + timedelta(minutes=additional_minutes)

            # 更新维护窗口
            self.db.execute(text("""
                UPDATE maintenance_windows
                SET end_time = :end_time,
                    updated_at = NOW(),
                    notes = CONCAT(IFNULL(notes, ''), '; Extended by ', :minutes, ' minutes at ', NOW())
                WHERE id = :window_id
            """), {
                'end_time': new_end_time,
                'minutes': additional_minutes,
                'window_id': window_id
            })

            # 记录操作日志
            self._log_maintenance_action(
                'EXTEND',
                f'Maintenance window extended by {additional_minutes} minutes. '
                f'New end time: {new_end_time.strftime("%Y-%m-%d %H:%M:%S")}'
            )

            self.db.commit()

            print("✅ 维护窗口已延长")
            print(f"  🆔 窗口ID: {window_id}")
            print(f"  ⏰ 新结束时间: {new_end_time.strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as e:
            self.db.rollback()
            print(f"❌ 延长维护窗口失败: {e}")
            raise

    def stop_maintenance(self):
        """结束维护窗口"""
        print("🔒 结束维护窗口...")

        try:
            current_status = self._get_maintenance_status()
            if not current_status['active']:
                print("❌ 当前没有活动的维护窗口")
                return

            window_id = current_status['window_id']
            end_time = datetime.now()

            # 关闭维护窗口
            self.db.execute(text("""
                UPDATE maintenance_windows
                SET actual_end_time = :end_time,
                    status = 'COMPLETED',
                    updated_at = NOW()
                WHERE id = :window_id
            """), {
                'end_time': end_time,
                'window_id': window_id
            })

            # 恢复守卫状态
            self._restore_guard_state()

            # 记录操作日志
            self._log_maintenance_action(
                'STOP',
                f'Maintenance window completed. Duration: {self._calculate_duration(current_status["start_time"], end_time)}'
            )

            self.db.commit()

            print("✅ 维护窗口已结束")
            print(f"  🆔 窗口ID: {window_id}")
            print(f"  ⏰ 结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  ⏱️ 持续时间: {self._calculate_duration(current_status['start_time'], end_time)}")
            print("  🔒 G7-2025写入保护已恢复")

        except Exception as e:
            self.db.rollback()
            print(f"❌ 结束维护窗口失败: {e}")
            raise

    def get_status(self):
        """获取维护状态"""
        print("📊 维护窗口状态:")

        try:
            current_status = self._get_maintenance_status()

            if current_status['active']:
                print("🔧 当前状态: 维护模式")
                print(f"  🆔 窗口ID: {current_status['window_id']}")
                print(f"  📅 开始时间: {current_status['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")

                if current_status['end_time']:
                    remaining = current_status['end_time'] - datetime.now()
                    if remaining.total_seconds() > 0:
                        print(f"  ⏰ 计划结束: {current_status['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"  ⏱️ 剩余时间: {self._format_duration(remaining)}")
                    else:
                        print("  ⚠️ 维护窗口已超时")
                else:
                    print("  ⏰ 无预定结束时间（手动结束）")

                elapsed = datetime.now() - current_status['start_time']
                print(f"  ⏱️ 已运行: {self._format_duration(elapsed)}")
                print(f"  🔧 类型: {current_status['type']}")
                print("  🔓 G7-2025写入: 允许")
            else:
                print("🔒 当前状态: 正常运行")
                print("  🛡️ G7-2025写入: 受保护")

                # 显示最近的维护窗口
                self.db.execute(text("""
                    SELECT id, start_time, actual_end_time, maintenance_type
                    FROM maintenance_windows
                    WHERE status = 'COMPLETED'
                    ORDER BY start_time DESC
                    LIMIT 1
                """))
                recent = self.db.fetchone()

                if recent:
                    print(f"  📅 最近维护: {recent[1].strftime('%Y-%m-%d %H:%M:%S')} - {recent[2].strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"    类型: {recent[3]}")

        except Exception as e:
            print(f"❌ 获取状态失败: {e}")

    def show_schedule(self):
        """显示维护计划"""
        print("📅 维护窗口历史:")

        try:
            result = self.db.execute(text("""
                SELECT id, start_time, end_time, actual_end_time, maintenance_type, status, created_by
                FROM maintenance_windows
                ORDER BY start_time DESC
                LIMIT 10
            """))

            windows = result.fetchall()

            if not windows:
                print("  无维护记录")
                return

            print(f"{'ID':<6} {'类型':<8} {'状态':<10} {'开始时间':<20} {'结束时间':<20} {'创建者':<15}")
            print("-" * 90)

            for window in windows:
                end_time_str = window[3].strftime('%Y-%m-%d %H:%M:%S') if window[3] else (
                    window[2].strftime('%Y-%m-%d %H:%M:%S') if window[2] else '未设定')

                status_display = {
                    'ACTIVE': '🔧进行中',
                    'COMPLETED': '✅已完成',
                    'CANCELLED': '❌已取消'
                }.get(window[5], window[5])

                print(f"{window[0]:<6} {window[4]:<8} {status_display:<10} "
                      f"{window[1].strftime('%Y-%m-%d %H:%M:%S'):<20} {end_time_str:<20} {window[6]:<15}")

        except Exception as e:
            print(f"❌ 获取历史失败: {e}")

    def _get_maintenance_status(self):
        """获取当前维护状态"""
        try:
            # 检查活动的维护窗口
            result = self.db.execute(text("""
                SELECT id, start_time, end_time, maintenance_type
                FROM maintenance_windows
                WHERE status = 'ACTIVE'
                ORDER BY start_time DESC
                LIMIT 1
            """))

            window = result.fetchone()

            if window:
                return {
                    'active': True,
                    'window_id': window[0],
                    'start_time': window[1],
                    'end_time': window[2],
                    'type': window[3]
                }
            else:
                return {'active': False}

        except:
            # 如果表不存在，创建表结构
            self._ensure_tables_exist()
            return {'active': False}

    def _set_maintenance_mode(self, enabled):
        """设置维护模式"""
        try:
            self.db.execute(text("""
                UPDATE g7_guard_config
                SET config_value = :value, updated_by = USER()
                WHERE config_key = 'maintenance_mode'
            """), {'value': 'true' if enabled else 'false'})

            if self.db.rowcount == 0:
                # 如果配置不存在，创建它
                self.db.execute(text("""
                    INSERT INTO g7_guard_config (config_key, config_value, description, updated_by)
                    VALUES ('maintenance_mode', :value, 'Maintenance mode flag', USER())
                """), {'value': 'true' if enabled else 'false'})

        except:
            # 如果配置表不存在，创建它
            self._ensure_tables_exist()
            self.db.execute(text("""
                INSERT INTO g7_guard_config (config_key, config_value, description, updated_by)
                VALUES ('maintenance_mode', :value, 'Maintenance mode flag', USER())
            """), {'value': 'true' if enabled else 'false'})

    def _backup_guard_state(self):
        """备份当前守卫状态"""
        try:
            backup_data = {
                'timestamp': datetime.now().isoformat(),
                'maintenance_mode': False
            }

            # 获取当前维护模式状态
            try:
                result = self.db.execute(text("""
                    SELECT config_value FROM g7_guard_config
                    WHERE config_key = 'maintenance_mode'
                """))
                row = result.fetchone()
                if row:
                    backup_data['maintenance_mode'] = row[0] == 'true'
            except:
                pass

            # 保存到维护状态表
            self.db.execute(text("""
                INSERT INTO maintenance_state_backup (backup_data, created_at)
                VALUES (:data, NOW())
            """), {'data': json.dumps(backup_data)})

        except:
            # 如果备份失败，不影响主流程
            pass

    def _restore_guard_state(self):
        """恢复守卫状态"""
        try:
            # 获取最新的备份
            result = self.db.execute(text("""
                SELECT backup_data FROM maintenance_state_backup
                ORDER BY created_at DESC
                LIMIT 1
            """))

            row = result.fetchone()
            if row:
                backup_data = json.loads(row[0])
                self._set_maintenance_mode(backup_data.get('maintenance_mode', False))
            else:
                # 默认关闭维护模式
                self._set_maintenance_mode(False)

        except:
            # 如果恢复失败，默认关闭维护模式
            self._set_maintenance_mode(False)

    def _create_maintenance_window(self, start_time, end_time, emergency):
        """创建维护窗口记录"""
        result = self.db.execute(text("""
            INSERT INTO maintenance_windows (
                start_time, end_time, maintenance_type, status, created_by, created_at
            ) VALUES (
                :start_time, :end_time, :type, 'ACTIVE', USER(), NOW()
            )
        """), {
            'start_time': start_time,
            'end_time': end_time,
            'type': 'EMERGENCY' if emergency else 'PLANNED'
        })

        self.db.commit()
        return result.lastrowid

    def _log_maintenance_action(self, action, message):
        """记录维护操作"""
        try:
            self.db.execute(text("""
                INSERT INTO g7_enhanced_guard_log(
                    event, action, message, user_host, current_user_name,
                    connection_id, decision
                ) VALUES (
                    'MAINTENANCE', :action, :message,
                    USER(), CURRENT_USER(), CONNECTION_ID(), 'LOGGED'
                )
            """), {'action': action, 'message': message})
        except:
            # 如果日志表不存在，不影响主流程
            pass

    def _ensure_tables_exist(self):
        """确保所需表存在"""
        try:
            # 创建维护窗口表
            self.db.execute(text("""
                CREATE TABLE IF NOT EXISTS maintenance_windows (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    start_time DATETIME NOT NULL,
                    end_time DATETIME NULL,
                    actual_end_time DATETIME NULL,
                    maintenance_type VARCHAR(20) NOT NULL DEFAULT 'PLANNED',
                    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
                    created_by VARCHAR(128) NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT NOW(),
                    updated_at DATETIME NOT NULL DEFAULT NOW() ON UPDATE NOW(),
                    notes TEXT NULL,
                    INDEX idx_status (status),
                    INDEX idx_start_time (start_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))

            # 创建状态备份表
            self.db.execute(text("""
                CREATE TABLE IF NOT EXISTS maintenance_state_backup (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    backup_data JSON NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT NOW()
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))

            # 创建配置表（如果不存在）
            self.db.execute(text("""
                CREATE TABLE IF NOT EXISTS g7_guard_config (
                    config_key VARCHAR(50) PRIMARY KEY,
                    config_value TEXT NOT NULL,
                    description TEXT NULL,
                    updated_by VARCHAR(128) NULL,
                    updated_at DATETIME NOT NULL DEFAULT NOW() ON UPDATE NOW()
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))

            self.db.commit()

        except Exception as e:
            self.db.rollback()
            raise

    def _calculate_duration(self, start_time, end_time):
        """计算持续时间"""
        delta = end_time - start_time
        return self._format_duration(delta)

    def _format_duration(self, delta):
        """格式化持续时间"""
        total_seconds = int(delta.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        if hours > 0:
            return f"{hours}小时{minutes}分钟"
        elif minutes > 0:
            return f"{minutes}分钟{seconds}秒"
        else:
            return f"{seconds}秒"

    def __del__(self):
        """清理资源"""
        if hasattr(self, 'db'):
            self.db.close()


def main():
    parser = argparse.ArgumentParser(description='维护窗口管理器')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # 开始维护窗口
    start_parser = subparsers.add_parser('start', help='开始维护窗口')
    group = start_parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--duration', type=int, help='维护窗口持续时间（分钟）')
    group.add_argument('--emergency', action='store_true', help='紧急维护模式')

    # 延长维护窗口
    extend_parser = subparsers.add_parser('extend', help='延长维护窗口')
    extend_parser.add_argument('--minutes', type=int, required=True, help='延长时间（分钟）')

    # 其他命令
    subparsers.add_parser('stop', help='结束维护窗口')
    subparsers.add_parser('status', help='查看维护状态')
    subparsers.add_parser('schedule', help='查看维护历史')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    manager = MaintenanceWindowManager()

    try:
        if args.command == 'start':
            if args.emergency:
                manager.start_maintenance(emergency=True)
            else:
                manager.start_maintenance(duration_minutes=args.duration)
        elif args.command == 'extend':
            manager.extend_maintenance(args.minutes)
        elif args.command == 'stop':
            manager.stop_maintenance()
        elif args.command == 'status':
            manager.get_status()
        elif args.command == 'schedule':
            manager.show_schedule()

    except Exception as e:
        print(f"❌ 操作失败: {e}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())