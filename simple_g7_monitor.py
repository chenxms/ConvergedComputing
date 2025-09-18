#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
G7-2025全链路批次处理简化监控器
"""

import os
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def get_db_stats(batch_code: str):
    """获取数据库统计信息"""
    db_url = os.getenv("DATABASE_URL",
                       "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        # 检查statistical_aggregations记录数
        agg_count = session.execute(
            text("SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = :batch"),
            {"batch": batch_code}
        ).scalar() or 0

        # 检查清洗数据记录数
        cleaned_count = session.execute(
            text("SELECT COUNT(*) FROM student_cleaned_scores WHERE batch_code = :batch"),
            {"batch": batch_code}
        ).scalar() or 0

        # 检查原始数据记录数
        raw_count = session.execute(
            text("SELECT COUNT(*) FROM student_score_detail WHERE batch_code = :batch"),
            {"batch": batch_code}
        ).scalar() or 0

        return {
            'aggregations_count': agg_count,
            'cleaned_count': cleaned_count,
            'raw_count': raw_count,
            'timestamp': datetime.now()
        }
    finally:
        session.close()

def sample_statistics_data(batch_code: str, limit: int = 3):
    """抽样statistical_aggregations的statistics_data字段"""
    db_url = os.getenv("DATABASE_URL",
                       "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        result = session.execute(
            text("""
            SELECT school_id, aggregation_level, data_version,
                   JSON_EXTRACT(statistics_data, '$.schema_version') as schema_version,
                   JSON_LENGTH(JSON_EXTRACT(statistics_data, '$.subjects')) as subjects_count,
                   CHAR_LENGTH(statistics_data) as data_size
            FROM statistical_aggregations
            WHERE batch_code = :batch
            ORDER BY created_at DESC
            LIMIT :limit
            """),
            {"batch": batch_code, "limit": limit}
        ).fetchall()

        samples = []
        for row in result:
            samples.append({
                'school_id': row[0],
                'aggregation_level': row[1],
                'data_version': row[2],
                'schema_version': row[3],
                'subjects_count': row[4],
                'data_size_kb': round((row[5] or 0) / 1024, 1)
            })
        return samples
    finally:
        session.close()

def print_status(batch_code: str, stage: str):
    """打印当前状态"""
    stats = get_db_stats(batch_code)
    current_time = datetime.now().strftime('%H:%M:%S')
    print(f"[{current_time}] {stage}")
    print(f"  Raw data: {stats['raw_count']:,}")
    print(f"  Cleaned data: {stats['cleaned_count']:,}")
    print(f"  Aggregations: {stats['aggregations_count']:,}")

if __name__ == "__main__":
    import sys
    batch_code = sys.argv[1] if len(sys.argv) > 1 else "G7-2025"
    print_status(batch_code, "Status Check")