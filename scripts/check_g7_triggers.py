#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sqlalchemy import text
from app.database.connection import get_db


def main():
    with next(get_db()) as db:
        rows = db.execute(text(
            """
            SELECT TRIGGER_NAME, EVENT_MANIPULATION
            FROM information_schema.TRIGGERS
            WHERE TRIGGER_SCHEMA = DATABASE()
              AND TRIGGER_NAME LIKE 'g7_guard_%'
            ORDER BY TRIGGER_NAME
            """
        )).fetchall()
        if not rows:
            print("(no guard triggers found)")
        else:
            for name, ev in rows:
                print(f"trigger: {name} ({ev})")


if __name__ == '__main__':
    main()

