#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
卸载 G7-2025 写入守卫（移除触发器，保留日志表以便回溯）。

用法：
  python scripts/uninstall_g7_guard.py
"""

from sqlalchemy import text
from app.database.connection import get_db


def uninstall_guard():
    with next(get_db()) as db:
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_insert"))
        db.execute(text("DROP TRIGGER IF EXISTS g7_guard_update"))
        db.commit()
        print('✅ 已卸载 G7-2025 写入守卫触发器（日志表 g7_guard_log 保留）')


if __name__ == '__main__':
    uninstall_guard()

