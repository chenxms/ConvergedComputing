#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025 守卫模式切换工具：
- 在全面拦截模式和白名单模式之间切换
- 提供临时禁用/启用守卫功能
- 支持维护窗口管理

用法：
  python scripts/g7_guard_switch.py status                    # 查看当前状态
  python scripts/g7_guard_switch.py block-all                # 切换到全面拦截模式
  python scripts/g7_guard_switch.py whitelist                # 切换到白名单模式
  python scripts/g7_guard_switch.py disable                  # 临时禁用所有守卫
  python scripts/g7_guard_switch.py enable                   # 重新启用守卫
  python scripts/g7_guard_switch.py maintenance on [reason]  # 开启维护模式
  python scripts/g7_guard_switch.py maintenance off          # 关闭维护模式
"""

import sys
from datetime import datetime
from sqlalchemy import text
from app.database.connection import get_db
from scripts.enhanced_g7_guard import EnhancedG7Guard


def get_current_status():
    """获取当前守卫状态"""
    with next(get_db()) as db:
        # 检查触发器
        triggers = db.execute(text(
            """
            SELECT TRIGGER_NAME, EVENT_MANIPULATION
            FROM information_schema.TRIGGERS
            WHERE TRIGGER_SCHEMA = DATABASE()
              AND (TRIGGER_NAME LIKE 'g7_guard_%' OR TRIGGER_NAME LIKE '%g7%')
            ORDER BY TRIGGER_NAME
            """
        )).fetchall()

        # 检查维护模式配置
        try:
            maintenance = db.execute(text(
                """
                SELECT * FROM g7_guard_config
                WHERE config_key = 'maintenance_mode'
                """
            )).fetchone()
        except:
            maintenance = None

        # 检查白名单条目数量
        try:
            whitelist_count = db.execute(text(
                "SELECT COUNT(*) as count FROM g7_guard_whitelist WHERE is_active = TRUE"
            )).fetchone()
            whitelist_total = whitelist_count.count if whitelist_count else 0
        except:
            whitelist_total = 0

        return {
            'triggers': triggers,
            'maintenance': maintenance,
            'whitelist_count': whitelist_total
        }


def show_status():
    """显示当前状态"""
    status = get_current_status()

    print("[GUARD] G7-2025 守卫状态报告")
    print("=" * 50)

    # 触发器状态
    print("\n[TRIGGERS] 触发器状态：")
    if not status['triggers']:
        print("  [X] 无活动守卫触发器")
    else:
        for trigger in status['triggers']:
            print(f"  [OK] {trigger.TRIGGER_NAME} ({trigger.EVENT_MANIPULATION})")

    # 维护模式
    print(f"\n[MAINTENANCE] 维护模式：")
    if status['maintenance'] and status['maintenance'].config_value == 'true':
        print(f"  [MAINT] 维护模式已启用")
        print(f"  [INFO] 原因: {status['maintenance'].description or '未指定'}")
        print(f"  [TIME] 开始时间: {status['maintenance'].updated_at}")
    else:
        print("  [OK] 正常运行模式")

    # 白名单状态
    print(f"\n[WHITELIST] 白名单状态：")
    print(f"  [COUNT] 活跃条目: {status['whitelist_count']} 个")

    # 判断当前模式
    print(f"\n[MODE] 当前模式：")
    if not status['triggers']:
        print("  [DISABLED] 守卫已禁用")
    elif any('whitelist' in t.TRIGGER_NAME for t in status['triggers']):
        print("  [WHITELIST] 白名单模式")
    else:
        print("  [BLOCK_ALL] 全面拦截模式")


def create_config_table():
    """创建配置表"""
    with next(get_db()) as db:
        db.execute(text(
            """
            CREATE TABLE IF NOT EXISTS g7_guard_config (
                id INT PRIMARY KEY AUTO_INCREMENT,
                config_key VARCHAR(64) NOT NULL UNIQUE,
                config_value TEXT,
                description VARCHAR(255),
                created_at DATETIME NOT NULL DEFAULT NOW(),
                updated_at DATETIME NOT NULL DEFAULT NOW() ON UPDATE NOW()
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        ))
        db.commit()


def switch_to_block_all():
    """切换到全面拦截模式"""
    print("[SWITCH] 切换到全面拦截模式...")

    with next(get_db()) as db:
        # 删除所有现有触发器
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_insert"))
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_update"))
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_whitelist_insert"))
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_whitelist_update"))

        # 创建全面拦截触发器
        db.execute(text(
            """
            CREATE TRIGGER g7_guard_insert
            BEFORE INSERT ON statistical_aggregations
            FOR EACH ROW
            BEGIN
                DECLARE nb VARCHAR(64);
                SET nb = TRIM(REPLACE(REPLACE(REPLACE(NEW.batch_code, '–','-'),'−','-'),'—','-'));
                IF nb = 'G7-2025' THEN
                    INSERT INTO g7_guard_log(
                        event, action, message, batch_code, aggregation_level, school_id,
                        user_host, current_user_name, connection_id
                    ) VALUES (
                        'INSERT', 'blocked', 'blocked by guard (block-all mode)', nb, NEW.aggregation_level,
                        NEW.school_id, USER(), CURRENT_USER(), CONNECTION_ID()
                    );
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by guard (block-all mode)';
                END IF;
            END
            """
        ))

        db.execute(text(
            """
            CREATE TRIGGER g7_guard_update
            BEFORE UPDATE ON statistical_aggregations
            FOR EACH ROW
            BEGIN
                DECLARE nb VARCHAR(64);
                SET nb = TRIM(REPLACE(REPLACE(REPLACE(NEW.batch_code, '–','-'),'−','-'),'—','-'));
                IF nb = 'G7-2025' THEN
                    INSERT INTO g7_guard_log(
                        event, action, message, batch_code, aggregation_level, school_id,
                        user_host, current_user_name, connection_id
                    ) VALUES (
                        'UPDATE', 'blocked', 'blocked by guard (block-all mode)', nb, NEW.aggregation_level,
                        NEW.school_id, USER(), CURRENT_USER(), CONNECTION_ID()
                    );
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by guard (block-all mode)';
                END IF;
            END
            """
        ))

        db.commit()
        print("[OK] 已切换到全面拦截模式")


def switch_to_whitelist():
    """切换到白名单模式"""
    print("[SWITCH] 切换到白名单模式...")

    # 统一到增强守卫实现：安装增强触发器 + 写入默认白名单
    # 1) 清理历史触发器
    with next(get_db()) as db:
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_insert"))
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_update"))
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_whitelist_insert"))
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_whitelist_update"))
        db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_insert"))
        db.execute(text("DROP TRIGGER IF EXISTS g7_enhanced_guard_update"))
        db.commit()

    # 2) 安装增强守卫并添加默认白名单
    EnhancedG7Guard().install()
    try:
        from scripts.install_g7_guard_with_whitelist import install_whitelist_guard as _seed
        _seed()
    except Exception:
        # 如果默认白名单注入失败，不影响模式切换
        pass

    print("[OK] 已切换到白名单模式（增强守卫）")


def disable_guard():
    """禁用所有守卫"""
    print("[DISABLE] 禁用所有G7守卫...")

    with next(get_db()) as db:
        # 删除所有触发器
        triggers_to_remove = [
            'g7_guard_insert', 'g7_guard_update',
            'g7_guard_whitelist_insert', 'g7_guard_whitelist_update',
            'g7_guard_insert_copy1', 'g7_guard_insert_copy2',
            'g7_guard_update_copy1', 'g7_guard_update_copy2'
        ]

        for trigger in triggers_to_remove:
            db.execute(text(f"DROP TRIGGER IF EXISTS {trigger}"))

        # 记录禁用操作
        try:
            db.execute(text(
                """
                INSERT INTO g7_guard_log(
                    event, action, message, batch_code, user_host, current_user_name, connection_id
                ) VALUES (
                    'ADMIN', 'bypassed', 'guard disabled by admin', 'G7-2025', USER(), CURRENT_USER(), CONNECTION_ID()
                )
                """
            ))
        except:
            pass  # 如果日志表不存在则忽略

        db.commit()
        print("[OK] 所有守卫已禁用")


def enable_guard():
    """启用守卫（默认白名单模式）"""
    print("[ENABLE] 启用G7守卫（白名单模式）...")
    switch_to_whitelist()


def set_maintenance_mode(enabled, reason=None):
    """设置维护模式"""
    create_config_table()

    with next(get_db()) as db:
        if enabled:
            db.execute(text(
                """
                INSERT INTO g7_guard_config (config_key, config_value, description)
                VALUES ('maintenance_mode', 'true', :reason)
                ON DUPLICATE KEY UPDATE
                config_value = 'true', description = :reason, updated_at = NOW()
                """
            ), {"reason": reason or "维护模式已启用"})
            print(f"[MAINT] 维护模式已启用: {reason or '未指定原因'}")
        else:
            db.execute(text(
                """
                INSERT INTO g7_guard_config (config_key, config_value, description)
                VALUES ('maintenance_mode', 'false', '维护模式已关闭')
                ON DUPLICATE KEY UPDATE
                config_value = 'false', description = '维护模式已关闭', updated_at = NOW()
                """
            ))
            print("[OK] 维护模式已关闭")

        db.commit()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    command = sys.argv[1].lower()

    if command == 'status':
        show_status()

    elif command == 'block-all':
        switch_to_block_all()

    elif command == 'whitelist':
        switch_to_whitelist()

    elif command == 'disable':
        disable_guard()

    elif command == 'enable':
        enable_guard()

    elif command == 'maintenance':
        if len(sys.argv) < 3:
            print("[ERROR] 用法：maintenance <on|off> [reason]")
            return

        action = sys.argv[2].lower()
        if action == 'on':
            reason = " ".join(sys.argv[3:]) if len(sys.argv) > 3 else None
            set_maintenance_mode(True, reason)
        elif action == 'off':
            set_maintenance_mode(False)
        else:
            print("[ERROR] 维护模式操作必须是 'on' 或 'off'")

    else:
        print("[ERROR] 未知命令。请查看帮助信息。")
        print(__doc__)


if __name__ == '__main__':
    main()
