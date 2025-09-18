#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
验证指定批次在 statistical_aggregations(区域级) 是否在 60 秒内继续增长。

用法：
  python scripts/verify_no_growth.py --batch G7-2025

依赖：环境变量 DATABASE_URL
"""

import os
import sys
import time
import argparse
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", required=True, help="批次代码，如 G7-2025")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: 未设置 DATABASE_URL 环境变量")
        sys.exit(2)

    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)

    with Session() as db:
        print(f"[{datetime.now()}] 检查批次 {args.batch} 的区域级记录增长情况...")

        def snapshot():
            row = db.execute(text(
                """
                SELECT COUNT(*) AS c, COALESCE(MAX(id), 0) AS max_id, COALESCE(MAX(updated_at), '1970-01-01') AS last_ts
                FROM statistical_aggregations
                WHERE batch_code=:b AND aggregation_level='REGIONAL'
                """
            ), {"b": args.batch}).fetchone()
            return int(row[0]), int(row[1] or 0), str(row[2])

        c1, max1, ts1 = snapshot()
        print(f"初始：count={c1}, max_id={max1}, last_updated={ts1}")
        print("等待 60 秒...")
        time.sleep(60)
        c2, max2, ts2 = snapshot()
        print(f"再次：count={c2}, max_id={max2}, last_updated={ts2}")

        if c2 > c1 or max2 > max1:
            print("❌ 警告：区域级记录仍在增长")
            sys.exit(1)
        print("✅ 验证通过：区域级记录无增长")


if __name__ == "__main__":
    main()

