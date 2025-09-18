#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
查看最近的 G7-2025 写入拦截日志（谁在写、何时写、哪种操作）。

用法：
  python scripts/show_g7_guard_log.py            # 最近 50 条
  python scripts/show_g7_guard_log.py 200       # 最近 200 条
"""

import sys
from sqlalchemy import text
from app.database.connection import get_db


def show(limit: int = 50):
    with next(get_db()) as db:
        rows = db.execute(text(
            """
            SELECT id, created_at, event, batch_code, aggregation_level, school_id,
                   user_host, current_user_name, connection_id
              FROM g7_guard_log
             ORDER BY id DESC
             LIMIT :limit
            """
        ), {"limit": limit}).fetchall()

        if not rows:
            print("(no guard logs)")
            return

        for r in rows:
            print(f"#{r.id} [{r.created_at}] {r.event} {r.batch_code}/{r.aggregation_level} school={r.school_id} user={r.user_host} cur={r.current_user_name} conn={r.connection_id}")


if __name__ == '__main__':
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    show(n)
