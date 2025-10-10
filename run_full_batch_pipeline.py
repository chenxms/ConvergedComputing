#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全量批次 清洗 → 汇聚（subjects v1.2 物化）一键脚本

用法（PowerShell 示例）:
  $env:DATABASE_URL="mysql+pymysql://user:pass@host:port/db?charset=utf8mb4"; \
  $env:EXCLUDE_ZERO_TOTAL_SCORE="1"; \
  python run_full_batch_pipeline.py G4-2025

说明：
- 先对指定批次执行全量数据清洗（考试+问卷）。
- 再基于清洗表 student_cleaned_scores 构建 subjects v1.2：区域级 + 全部学校级，并落地到 statistical_aggregations。
- 受开关 EXCLUDE_ZERO_TOTAL_SCORE 控制是否剔除总分=0（默认剔除）。
"""

import os
import sys
import asyncio
from typing import Tuple
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from data_cleaning_service import DataCleaningService
from app.database.connection import get_db
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus
from app.services.calculation_service import CalculationService


def _sanitize_db_url(url: str) -> str:
    """Sanitize whitespace in query keys/values (e.g., "? charset=")"""
    try:
        if not url or "?" not in url:
            return url
        parts = urlsplit(url)
        # Normalize query: trim spaces around keys/values and canonicalize charset
        q = []
        for k, v in parse_qsl(parts.query, keep_blank_values=True):
            k = (k or "").strip()
            v = (v or "").strip()
            if k.lower().replace("_", "") == "charset":
                k = "charset"
            q.append((k, v))
        new_query = urlencode(q)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))
    except Exception:
        return url


def _get_db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url and url.strip():
        return _sanitize_db_url(url)
    # 兼容默认连接（如未设置环境变量）
    return "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"


async def clean_batch(batch_code: str) -> dict:
    db_url = _get_db_url()
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        cleaner = DataCleaningService(session)
        result = await cleaner.clean_batch_scores(batch_code)
        return result
    finally:
        session.close()


async def materialize_subjects_v12(batch_code: str) -> Tuple[int, int]:
    db = next(get_db())
    try:
        calc_service = CalculationService(db)
        results = await calc_service.calculate_batch_statistics(batch_code)
        summary = results.get('school_statistics_summary', {}) or {}
        ok = summary.get('successful_schools', 0)
        total = summary.get('total_schools', 0)
        if not total:
            total = ok
        # 打印被跳过科目（若有）
        try:
            meta = (results.get('regional_statistics') or {}).get('calculation_metadata') or {}
            skipped = meta.get('skipped_subjects') or []
            if skipped:
                print(f"[提示] 以下科目清洗数据缺失，已在统计阶段跳过：{', '.join(map(str, skipped))}")
        except Exception:
            pass
        print(f'[汇聚] 增强计算完成：学校成功 {ok}/{total}')
        return ok, total
    finally:
        db.close()


async def main():
    if len(sys.argv) < 2:
        print("用法: python run_full_batch_pipeline.py <batch_code>")
        sys.exit(1)
    batch_code = sys.argv[1]

    print(f"=== 全量批次管道：清洗+汇聚 v1.2 ===")
    print(f"批次: {batch_code}")
    print(f"零分剔除规则: {'开启' if str(os.getenv('EXCLUDE_ZERO_TOTAL_SCORE', '1')).strip().lower() in ('1','true','yes','on') else '关闭'}")

    # 1) 全量清洗
    clean_result = await clean_batch(batch_code)
    print("\n[清洗结果]")
    try:
        subjects = clean_result.get('subjects', {})
        print(f"  科目数: {clean_result.get('subjects_processed', 0)}")
        print(f"  原始记录: {clean_result.get('total_raw_records', 0)}")
        print(f"  清洗记录: {clean_result.get('total_cleaned_records', 0)}")
        print(f"  异常记录: {clean_result.get('anomalous_records', 0)}")
        if subjects:
            for name, sr in list(subjects.items())[:5]:
                print(f"    - {name}: 原始 {sr.get('raw_records',0)} 清洗 {sr.get('cleaned_records',0)} 异常 {sr.get('anomalous_records',0)}")
            if len(subjects) > 5:
                print(f"    ... 共 {len(subjects)} 科目")
    except Exception:
        print(clean_result)

    # 2) 物化 subjects v1.2（区域+学校）
    ok, total = await materialize_subjects_v12(batch_code)
    print(f"\n[SUCCESS] 完成。学校级生成 {ok}/{total}。")


if __name__ == "__main__":
    asyncio.run(main())
