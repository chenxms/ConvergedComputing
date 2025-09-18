#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
清理并规范化区域级统计记录：
- 统一区域级 school_id = 'REGIONAL'
- 保留同批次最新一条区域级记录，删除其余重复

用法：
  python scripts/fix_regional_duplicates.py --batch G7-2025            # 仅修复 G7-2025
  python scripts/fix_regional_duplicates.py --all                      # 修复所有批次
  python scripts/fix_regional_duplicates.py --batch G7-2025 --dry-run  # 仅打印将要执行的操作

依赖：环境变量 DATABASE_URL（如 docker-compose 已设置），示例：
  mysql+pymysql://user:pass@host:port/dbname?charset=utf8mb4
"""

import os
import sys
import argparse
from datetime import datetime
from typing import List, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fix regional duplicates in statistical_aggregations")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch", dest="batch", help="指定批次，如 G7-2025")
    group.add_argument("--all", dest="fix_all", action="store_true", help="修复所有批次")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="仅打印不执行")
    return parser.parse_args()


def get_engine() -> Tuple:
    url = os.getenv("DATABASE_URL")
    if not url:
        print("ERROR: 未设置环境变量 DATABASE_URL")
        sys.exit(2)
    engine = create_engine(url)
    Session = sessionmaker(bind=engine)
    return engine, Session


def list_target_batches(session, only_batch: str = None) -> List[str]:
    if only_batch:
        return [only_batch]
    sql = text(
        """
        SELECT DISTINCT batch_code
        FROM statistical_aggregations
        WHERE aggregation_level = 'REGIONAL'
        ORDER BY batch_code
        """
    )
    return [row[0] for row in session.execute(sql).fetchall()]


def fix_batch(session, batch_code: str, dry_run: bool = False) -> None:
    print(f"\n>>> 修复批次: {batch_code}")

    # 1) 查询区域级记录（包括 school_id 为 NULL/'REGION'/'REGIONAL'/其他）
    rows = session.execute(
        text(
            """
            SELECT id, school_id, updated_at
            FROM statistical_aggregations
            WHERE batch_code = :b
              AND aggregation_level = 'REGIONAL'
            ORDER BY updated_at DESC, id DESC
            """
        ),
        {"b": batch_code},
    ).fetchall()

    if not rows:
        print("   无区域级记录，跳过")
        return

    keep_id = rows[0][0]
    to_delete = [r[0] for r in rows[1:]]
    print(f"   保留最新记录: id={keep_id}; 待删除重复: {len(to_delete)} 条")

    # 2) 删除重复记录
    if to_delete:
        # 由于 MySQL 不支持直接数组绑定，这里拼接 SQL 值列表
        del_sql_str = f"DELETE FROM statistical_aggregations WHERE id IN ({','.join(map(str, to_delete))})"
        if dry_run:
            print(f"   [DRY-RUN] {del_sql_str}")
        else:
            session.execute(text(del_sql_str))
            session.commit()
            print(f"   已删除: {len(to_delete)} 条")

    # 3) 统一保留记录的 school_id 为 'REGIONAL'
    upd_sql = text(
        """
        UPDATE statistical_aggregations
           SET school_id = 'REGIONAL'
         WHERE id = :keep_id
        """
    )
    if dry_run:
        print(f"   [DRY-RUN] 统一区域级标识: id={keep_id} -> 'REGIONAL'")
    else:
        session.execute(upd_sql, {"keep_id": keep_id})
        session.commit()
        print("   已统一区域级标识为 'REGIONAL'")


def main():
    args = parse_args()
    engine, Session = get_engine()
    with Session() as session:
        batches = list_target_batches(session, only_batch=args.batch)
        if not batches:
            print("未找到目标批次")
            return
        print(f"目标批次: {', '.join(batches)}")
        for b in batches:
            fix_batch(session, b, dry_run=args.dry_run)
    print("\n完成")


if __name__ == "__main__":
    main()

