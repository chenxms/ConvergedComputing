#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
单科目清洗+汇聚全流程执行脚本

用法（PowerShell）：
  $env:DATABASE_URL="mysql+pymysql://user:pass@host:port/db?charset=utf8mb4"; \
  $env:EXCLUDE_ZERO_TOTAL_SCORE="1"; \
  python run_single_subject_pipeline.py G4-2025 数学

说明：
- 仅清洗指定批次的指定科目（不影响其他科目的清洗数据）。
- 汇聚（subjects v1.2）会按整批次重建区域/学校级的subjects结构，以确保一致性。
"""

import os
import sys
import asyncio
from typing import Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from data_cleaning_service import DataCleaningService
from app.database.connection import get_db
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus
from app.services.subjects_builder import SubjectsBuilder
from app.utils.precision import round2_json


def _get_db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url and url.strip():
        return url
    # 兼容默认连接（如未设置环境变量）
    return "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"


async def clean_subject(batch_code: str, subject_name: str) -> None:
    db_url = _get_db_url()
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        cleaner = DataCleaningService(session)
        result = await cleaner.clean_single_subject(batch_code, subject_name)
        print("\n[清洗结果]")
        print(result)
    finally:
        session.close()


def materialize_subjects_v12(batch_code: str) -> Tuple[int, int]:
    """重建并落地 v1.2 subjects：区域级 + 学校级（全量学校）。
    返回：(学校级生成数量, 学校总数)
    """
    db = next(get_db())
    try:
        repo = StatisticalAggregationRepository(db)
        builder = SubjectsBuilder()

        # 区域级
        regional_subjects = builder.build_regional_subjects(batch_code)
        regional_payload = {
            'schema_version': 'v1.2',
            'batch_code': batch_code,
            'aggregation_level': 'REGIONAL',
            'subjects': regional_subjects,
        }
        repo.upsert_statistics({
            'batch_code': batch_code,
            'aggregation_level': DBAggregationLevel.REGIONAL,
            'school_id': None,
            'school_name': None,
            'statistics_data': round2_json(regional_payload),
            'calculation_status': CalculationStatus.COMPLETED,
        })
        print("[汇聚] 区域级 subjects v1.2 已更新")

        # 学校级（全量）
        rows = db.execute(text("SELECT DISTINCT school_code FROM student_cleaned_scores WHERE batch_code=:b"), {"b": batch_code}).fetchall()
        total = 0
        ok = 0
        for (school_code,) in rows:
            total += 1
            try:
                school_subjects = builder.build_school_subjects(batch_code, school_code)
                school_payload = {
                    'schema_version': 'v1.2',
                    'batch_code': batch_code,
                    'aggregation_level': 'SCHOOL',
                    'school_code': school_code,
                    'subjects': school_subjects,
                }
                repo.upsert_statistics({
                    'batch_code': batch_code,
                    'aggregation_level': DBAggregationLevel.SCHOOL,
                    'school_id': school_code,
                    'school_name': None,
                    'statistics_data': round2_json(school_payload),
                    'calculation_status': CalculationStatus.COMPLETED,
                })
                ok += 1
                if ok % 20 == 0:
                    print(f"[汇聚] 学校级已生成 {ok}/{total} ...")
            except Exception as e:
                print(f"[WARN] 学校 {school_code} 生成失败: {e}")
        print(f"[汇聚] 学校级 subjects v1.2 更新完成：{ok}/{total}")
        return ok, total
    finally:
        db.close()


async def main():
    if len(sys.argv) < 3:
        print("用法: python run_single_subject_pipeline.py <batch_code> <subject_name>")
        sys.exit(1)
    batch_code = sys.argv[1]
    subject_name = sys.argv[2]

    print(f"=== 单科目管道：清洗+汇聚 v1.2 ===")
    print(f"批次: {batch_code}  科目: {subject_name}")
    print(f"零分剔除规则: {'开启' if str(os.getenv('EXCLUDE_ZERO_TOTAL_SCORE', '1')).strip().lower() in ('1','true','yes','on') else '关闭'}")

    await clean_subject(batch_code, subject_name)
    ok, total = materialize_subjects_v12(batch_code)
    print(f"\n[SUCCESS] 完成。学校级生成 {ok}/{total}。")


if __name__ == "__main__":
    asyncio.run(main())

