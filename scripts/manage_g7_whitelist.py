#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025 白名单管理工具：
- 查看当前白名单配置
- 添加/删除/启用/禁用白名单条目
- 查看守卫日志和统计信息

用法：
  python scripts/manage_g7_whitelist.py list                           # 查看白名单
  python scripts/manage_g7_whitelist.py add user pipeline_user        # 添加用户到白名单
  python scripts/manage_g7_whitelist.py add application materialize   # 添加应用到白名单
  python scripts/manage_g7_whitelist.py disable user old_user         # 禁用白名单条目
  python scripts/manage_g7_whitelist.py enable user old_user          # 启用白名单条目
  python scripts/manage_g7_whitelist.py remove user old_user          # 删除白名单条目
  python scripts/manage_g7_whitelist.py logs                          # 查看守卫日志
  python scripts/manage_g7_whitelist.py stats                         # 查看统计信息
"""

import sys
from datetime import datetime, timedelta
from sqlalchemy import text
from app.database.connection import get_db


def list_whitelist():
    """查看当前白名单配置"""
    with next(get_db()) as db:
        result = db.execute(text(
            """
            SELECT type, value, description, enabled, created_at, updated_at
            FROM g7_guard_whitelist
            ORDER BY type, enabled DESC, value
            """
        )).fetchall()

        if not result:
            print("📋 白名单为空")
            return

        print("📋 G7-2025 白名单配置：")
        print("-" * 80)
        print(f"{'类型':<12} {'值':<20} {'状态':<8} {'描述':<25} {'创建时间'}")
        print("-" * 80)

        for row in result:
            status = "✅启用" if row.enabled else "❌禁用"
            print(f"{row.type:<12} {row.value:<20} {status:<8} {row.description or '':<25} {row.created_at}")


def add_whitelist(wl_type, value, description=None):
    """添加白名单条目"""
    if wl_type not in ['user', 'application', 'host']:
        print("❌ 错误：类型必须是 user, application 或 host")
        return

    with next(get_db()) as db:
        try:
            db.execute(text(
                """
                INSERT INTO g7_guard_whitelist (type, value, description)
                VALUES (:type, :value, :description)
                """
            ), {"type": wl_type, "value": value, "description": description})
            db.commit()
            print(f"✅ 已添加白名单条目：{wl_type}={value}")
        except Exception as e:
            print(f"❌ 添加失败：{str(e)}")


def update_whitelist_status(wl_type, value, enabled):
    """启用/禁用白名单条目"""
    with next(get_db()) as db:
        result = db.execute(text(
            """
            UPDATE g7_guard_whitelist
            SET enabled = :enabled, updated_at = NOW()
            WHERE type = :type AND value = :value
            """
        ), {"enabled": enabled, "type": wl_type, "value": value})

        if result.rowcount > 0:
            status = "启用" if enabled else "禁用"
            print(f"✅ 已{status}白名单条目：{wl_type}={value}")
            db.commit()
        else:
            print(f"❌ 未找到白名单条目：{wl_type}={value}")


def remove_whitelist(wl_type, value):
    """删除白名单条目"""
    with next(get_db()) as db:
        result = db.execute(text(
            """
            DELETE FROM g7_guard_whitelist
            WHERE type = :type AND value = :value
            """
        ), {"type": wl_type, "value": value})

        if result.rowcount > 0:
            print(f"✅ 已删除白名单条目：{wl_type}={value}")
            db.commit()
        else:
            print(f"❌ 未找到白名单条目：{wl_type}={value}")


def show_logs(limit=20):
    """查看守卫日志"""
    with next(get_db()) as db:
        result = db.execute(text(
            """
            SELECT event, action, message, current_user_name, application_name,
                   whitelist_match, created_at
            FROM g7_guard_log
            ORDER BY created_at DESC
            LIMIT :limit
            """
        ), {"limit": limit}).fetchall()

        if not result:
            print("📝 暂无守卫日志")
            return

        print(f"📝 最近 {len(result)} 条守卫日志：")
        print("-" * 120)
        print(f"{'时间':<19} {'事件':<6} {'动作':<8} {'用户':<20} {'应用':<15} {'白名单匹配':<20} {'消息'}")
        print("-" * 120)

        for row in result:
            print(f"{row.created_at} {row.event:<6} {row.action:<8} "
                  f"{row.current_user_name or '':<20} {row.application_name or '':<15} "
                  f"{row.whitelist_match or '':<20} {row.message}")


def show_stats():
    """查看统计信息"""
    with next(get_db()) as db:
        # 今日统计
        today_stats = db.execute(text(
            """
            SELECT action, COUNT(*) as count
            FROM g7_guard_log
            WHERE DATE(created_at) = CURDATE()
            GROUP BY action
            ORDER BY action
            """
        )).fetchall()

        # 近7日统计
        week_stats = db.execute(text(
            """
            SELECT action, COUNT(*) as count
            FROM g7_guard_log
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            GROUP BY action
            ORDER BY action
            """
        )).fetchall()

        # 白名单条目统计
        whitelist_stats = db.execute(text(
            """
            SELECT type, enabled, COUNT(*) as count
            FROM g7_guard_whitelist
            GROUP BY type, enabled
            ORDER BY type, enabled DESC
            """
        )).fetchall()

        print("📊 G7-2025 守卫统计信息：")
        print("-" * 50)

        print("\n📅 今日活动统计：")
        if today_stats:
            for stat in today_stats:
                print(f"  {stat.action}: {stat.count} 次")
        else:
            print("  无活动")

        print("\n📈 近7日活动统计：")
        if week_stats:
            for stat in week_stats:
                print(f"  {stat.action}: {stat.count} 次")
        else:
            print("  无活动")

        print("\n🔧 白名单配置统计：")
        if whitelist_stats:
            for stat in whitelist_stats:
                status = "启用" if stat.enabled else "禁用"
                print(f"  {stat.type} ({status}): {stat.count} 条")
        else:
            print("  白名单为空")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    command = sys.argv[1].lower()

    if command == 'list':
        list_whitelist()

    elif command == 'add':
        if len(sys.argv) < 4:
            print("❌ 用法：add <type> <value> [description]")
            return
        wl_type = sys.argv[2]
        value = sys.argv[3]
        description = sys.argv[4] if len(sys.argv) > 4 else None
        add_whitelist(wl_type, value, description)

    elif command == 'enable':
        if len(sys.argv) < 4:
            print("❌ 用法：enable <type> <value>")
            return
        update_whitelist_status(sys.argv[2], sys.argv[3], True)

    elif command == 'disable':
        if len(sys.argv) < 4:
            print("❌ 用法：disable <type> <value>")
            return
        update_whitelist_status(sys.argv[2], sys.argv[3], False)

    elif command == 'remove':
        if len(sys.argv) < 4:
            print("❌ 用法：remove <type> <value>")
            return
        remove_whitelist(sys.argv[2], sys.argv[3])

    elif command == 'logs':
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        show_logs(limit)

    elif command == 'stats':
        show_stats()

    else:
        print("❌ 未知命令。请查看帮助信息。")
        print(__doc__)


if __name__ == '__main__':
    main()