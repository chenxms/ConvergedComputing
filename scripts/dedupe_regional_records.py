#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
去重指定批次的 REGIONAL 级别记录，仅保留最新一条（按 updated_at DESC, id DESC）。

用法：
  python scripts/dedupe_regional_records.py G4-2025 [G7-2025 ...]
"""
from __future__ import annotations
import sys
from typing import List
from sqlalchemy import text
import os

# 确保可导入 app.*
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
import sys
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from app.database.connection import get_db


def dedupe_one(batch: str) -> int:
    db = next(get_db())
    # 找出需保留的最新一条 id
    row = db.execute(
        text(
            "SELECT id FROM statistical_aggregations "
            "WHERE batch_code=:b AND aggregation_level='REGIONAL' "
            "ORDER BY updated_at DESC, id DESC LIMIT 1"
        ),
        {"b": batch},
    ).fetchone()
    if not row:
        return 0
    keep_id = int(row[0])
    # 删除其他区域记录
    res = db.execute(
        text(
            "DELETE FROM statistical_aggregations "
            "WHERE batch_code=:b AND aggregation_level='REGIONAL' AND id != :kid"
        ),
        {"b": batch, "kid": keep_id},
    )
    db.commit()
    return res.rowcount or 0


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: python scripts/dedupe_regional_records.py <BATCH> [<BATCH> ...]")
        return 1
    total_deleted = 0
    for b in argv[1:]:
        deleted = dedupe_one(b)
        print(f"{b}: deleted {deleted} duplicate regional rows")
        total_deleted += deleted
    print(f"Total deleted: {total_deleted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
