#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
带重试机制的v1.2版本物化脚本
"""

from __future__ import annotations
import sys
import os
import json
import time
from datetime import datetime, timezone
from typing import List

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db, get_db_context
from app.services.subjects_builder import SubjectsBuilder
from app.services.calculation_service import CalculationService
import asyncio
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def retry_on_connection_error(func, max_retries=3, delay=5):
    """装饰器：在连接错误时重试"""
    def wrapper(*args, **kwargs):
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except OperationalError as e:
                if "Lost connection" in str(e) or "2013" in str(e):
                    if attempt < max_retries - 1:
                        _log(f"连接丢失，{delay}秒后重试 (尝试 {attempt + 1}/{max_retries})")
                        time.sleep(delay)
                        continue
                raise
            except Exception as e:
                raise
        return None
    return wrapper


def process_single_school(batch_code: str, school_code: str, school_name: str, sb: SubjectsBuilder, idx: int, total: int):
    """处理单个学校，带重试机制"""
    max_retries = 3

    for attempt in range(max_retries):
        try:
            with get_db_context() as db:
                # 设置更长的超时时间
                db.execute(text("SET SESSION net_write_timeout=1200"))  # 20分钟
                db.execute(text("SET SESSION net_read_timeout=1200"))
                db.execute(text("SET SESSION wait_timeout=28800"))  # 8小时
                db.execute(text("SET SESSION interactive_timeout=28800"))

                calc = CalculationService(db)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                # 计算增强统计
                school_enhanced = None
                try:
                    school_df = loop.run_until_complete(calc._fetch_school_scores(batch_code, school_code))
                    if not school_df.empty:
                        school_enhanced = loop.run_until_complete(
                            calc._consolidate_multi_subject_results(batch_code, school_df)
                        )
                except Exception as e:
                    _log(f"    [WARN] 学校 {school_code} 增强统计失败: {e}")

                # 构建subjects
                school_subjects = sb.build_school_subjects_v12(
                    batch_code, school_code, enhanced_stats=school_enhanced
                )

                school_json = {
                    "schema_version": "v1.2",
                    "batch_code": batch_code,
                    "aggregation_level": "SCHOOL",
                    "school_id": school_code,
                    "school_name": school_name,
                    "subjects": school_subjects,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }

                # 写入数据库
                repo = StatisticalAggregationRepository(db)
                repo.upsert_statistics({
                    "batch_code": batch_code,
                    "aggregation_level": DBAggregationLevel.SCHOOL,
                    "school_id": school_code,
                    "school_name": school_name,
                    "statistics_data": school_json,
                    "data_version": "v1.2",
                    "calculation_status": CalculationStatus.COMPLETED,
                    "updated_at": datetime.now(),
                    "created_at": datetime.now(),
                })
                db.commit()

                _log(f"    进度 {idx}/{total} 学校: {school_code} ✓")
                return True

        except OperationalError as e:
            if "Lost connection" in str(e) or "2013" in str(e):
                if attempt < max_retries - 1:
                    _log(f"    学校 {school_code} 连接丢失，5秒后重试 (尝试 {attempt + 1}/{max_retries})")
                    time.sleep(5)
                    continue
                else:
                    _log(f"    [ERROR] 学校 {school_code} 处理失败（重试{max_retries}次后）: {e}")
                    return False
            else:
                _log(f"    [ERROR] 学校 {school_code} 处理失败: {e}")
                return False
        except Exception as e:
            _log(f"    [ERROR] 学校 {school_code} 处理失败: {e}")
            return False

    return False


def rewrite_batch(batch_code: str) -> None:
    sb = SubjectsBuilder()

    _log(f"Rewriting subjects for {batch_code} ...")

    # 1. 构建区域级subjects
    _log("[1/3] 构建区域级 subjects (v1.2) ...")

    with get_db_context() as db:
        calc = CalculationService(db)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # 设置超时
        try:
            db.execute(text("SET SESSION net_write_timeout=1200"))
            db.execute(text("SET SESSION net_read_timeout=1200"))
            db.execute(text("SET SESSION wait_timeout=28800"))
            db.execute(text("SET SESSION interactive_timeout=28800"))
        except:
            pass

        # 计算区域级增强统计
        regional_enhanced = None
        try:
            student_df = loop.run_until_complete(calc._fetch_student_scores(batch_code))
            if not student_df.empty:
                regional_enhanced = loop.run_until_complete(
                    calc._consolidate_multi_subject_results(batch_code, student_df)
                )
        except Exception as e:
            _log(f"[WARN] 区域增强统计获取失败，降级输出基础结构: {e}")

        # 构建区域级subjects
        regional_subjects = sb.build_regional_subjects_v12(
            batch_code,
            enhanced_stats=regional_enhanced,
            include_detail=True,
        )

        regional_json = {
            "schema_version": "v1.2",
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "subjects": regional_subjects,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "data_version": "v1.2",
        }

        # 写入区域级记录
        _log("[1.1] 写入区域级记录 ...")
        repo = StatisticalAggregationRepository(db)

        # 统计学校和学生数
        try:
            total_schools = db.execute(text(
                "SELECT COUNT(*) FROM school_master_data WHERE batch_code=:b AND status='ACTIVE'"
            ), {"b": batch_code}).scalar() or 0

            total_students = db.execute(text("""
                SELECT COUNT(DISTINCT scs.student_id)
                FROM student_cleaned_scores scs
                JOIN school_master_data smd
                  ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code
                 AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code
                 AND smd.status='ACTIVE'
                WHERE scs.batch_code=:b AND scs.subject_type IN ('exam','questionnaire')
            """), {"b": batch_code}).scalar() or 0
        except:
            total_schools = 0
            total_students = 0

        repo.upsert_statistics({
            "batch_code": batch_code,
            "aggregation_level": DBAggregationLevel.REGIONAL,
            "school_id": "REGIONAL",
            "school_name": "区域汇总",
            "statistics_data": regional_json,
            "data_version": "v1.2",
            "calculation_status": CalculationStatus.COMPLETED,
            "total_schools": total_schools,
            "total_students": total_students,
            "updated_at": datetime.now(),
            "created_at": datetime.now(),
        })
        _log("[1.2] 区域级完成。")

    # 2. 构建学校级subjects
    _log("[2/3] 读取学校列表 ...")

    with get_db_context() as db:
        schools = db.execute(
            text("""
                SELECT school_id, school_name
                FROM school_master_data
                WHERE batch_code=:b AND status='ACTIVE'
                ORDER BY school_id
            """),
            {"b": batch_code},
        ).fetchall()

    total = len(schools)
    _log(f"[2.1] 共 {total} 所学校，开始生成学校级 subjects (v1.2) ...")

    failed_schools = []
    for idx, (school_code, school_name) in enumerate(schools, start=1):
        success = process_single_school(batch_code, school_code, school_name, sb, idx, total)
        if not success:
            failed_schools.append((school_code, school_name))

    _log("[2.2] 学校级完成。")

    if failed_schools:
        _log(f"[WARNING] 以下 {len(failed_schools)} 所学校处理失败:")
        for code, name in failed_schools:
            _log(f"  - {code}: {name}")

        # 尝试重新处理失败的学校
        if len(failed_schools) <= 5:
            _log("尝试重新处理失败的学校...")
            for code, name in failed_schools:
                _log(f"重试学校 {code}...")
                process_single_school(batch_code, code, name, sb, 0, 0)

    _log(f"[3/3] 批次 {batch_code} 物化完成！")


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: python rewrite_subjects_v12_with_retry.py <batch_code>")
        return 1

    batch_code = argv[1]
    rewrite_batch(batch_code)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))