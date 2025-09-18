#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
为 statistical_aggregations 增加生成列 + 唯一索引（无需 Alembic）：
- 生成列：school_id_norm = COALESCE(school_id, 'REGIONAL') STORED
- 唯一索引：uk_batch_level_school_norm(batch_code, aggregation_level, school_id_norm)

前置要求：需要先清理重复（否则唯一索引创建会失败）：
  python scripts/fix_regional_duplicates.py --all  (或 --batch G7-2025)

环境变量：DATABASE_URL（如：mysql+pymysql://user:pass@host:port/db?charset=utf8mb4）
"""

import os
import sys
from sqlalchemy import create_engine, text


def get_engine():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("ERROR: 未设置环境变量 DATABASE_URL")
        sys.exit(2)
    return create_engine(url)


def column_exists(conn, table: str, column: str) -> bool:
    sql = text(f"SHOW COLUMNS FROM {table} LIKE :col")
    return conn.execute(sql, {"col": column}).fetchone() is not None


def index_exists(conn, table: str, index_name: str) -> bool:
    sql = text(f"SHOW INDEX FROM {table} WHERE Key_name = :k")
    return conn.execute(sql, {"k": index_name}).fetchone() is not None


def check_normalized_duplicates(conn) -> int:
    sql = text(
        """
        SELECT COUNT(*) AS dup_cnt
        FROM (
            SELECT batch_code, aggregation_level, COALESCE(school_id,'REGIONAL') AS school_id_norm, COUNT(*) AS c
            FROM statistical_aggregations
            GROUP BY batch_code, aggregation_level, school_id_norm
            HAVING c > 1
        ) t
        """
    )
    return int(conn.execute(sql).scalar() or 0)


def main():
    engine = get_engine()
    with engine.begin() as conn:
        dup = check_normalized_duplicates(conn)
        if dup > 0:
            print(f"ERROR: 发现 {dup} 组规范化键重复，请先运行去重脚本：")
            print("       python scripts/fix_regional_duplicates.py --all  (或 --batch G7-2025)")
            sys.exit(1)

        if not column_exists(conn, 'statistical_aggregations', 'school_id_norm'):
            print("添加生成列 school_id_norm ...")
            conn.execute(text(
                """
                ALTER TABLE `statistical_aggregations`
                  ADD COLUMN `school_id_norm` VARCHAR(60)
                  GENERATED ALWAYS AS (COALESCE(`school_id`,'REGIONAL')) STORED
                """
            ))
        else:
            print("生成列 school_id_norm 已存在，跳过")

        if not index_exists(conn, 'statistical_aggregations', 'uk_batch_level_school_norm'):
            print("创建唯一索引 uk_batch_level_school_norm ...")
            conn.execute(text(
                """
                CREATE UNIQUE INDEX `uk_batch_level_school_norm`
                  ON `statistical_aggregations` (`batch_code`, `aggregation_level`, `school_id_norm`)
                """
            ))
        else:
            print("唯一索引 uk_batch_level_school_norm 已存在，跳过")

    print("完成：生成列与唯一索引就绪")


if __name__ == "__main__":
    main()

