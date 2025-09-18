#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
仅重写并物化“区域级” v1.2 subjects 数据（不处理学校级）

用法:
  poetry run python scripts/rewrite_regional_v12.py G7-2025

说明:
- 计算并写入 REGIONAL 记录（含 percentiles/discrimination/grade_distribution 等增强指标）
- 不遍历学校，不产生 SCHOOL 记录，便于与学校级脚本分开执行
- 是否允许写入由环境变量 DISABLE_WRITES_FOR_BATCHES 控制（仓储层已内置拦截）
"""

from __future__ import annotations
import sys
import os
import asyncio
from datetime import datetime, timezone

from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db_context
from app.services.calculation_service import CalculationService
from app.services.subjects_builder import SubjectsBuilder
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus


def _log(msg: str) -> None:
    print(msg, flush=True)


def rewrite_regional(batch_code: str, include_detail: bool = True) -> None:
    """仅生成并写入区域级 v1.2 subjects 数据"""
    sb = SubjectsBuilder()
    with get_db_context() as db:
        calc = CalculationService(db)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        # 提高会话级超时，避免大 JSON 写入中断
        try:
            db.execute(text("SET SESSION net_write_timeout=600"))
            db.execute(text("SET SESSION net_read_timeout=600"))
            db.execute(text("SET SESSION wait_timeout=600"))
            db.execute(text("SET SESSION innodb_lock_wait_timeout=120"))
            db.execute(text("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        except Exception:
            pass

        _log(f"[REGIONAL] 计算增强统计（{batch_code}）...")
        enhanced = None
        try:
            df = loop.run_until_complete(calc._fetch_student_scores(batch_code))
            if df is not None and not df.empty:
                enhanced = loop.run_until_complete(
                    calc._consolidate_multi_subject_results(batch_code, df)
                )
        finally:
            try:
                loop.close()
            except Exception:
                pass

        _log("[REGIONAL] 构建 v1.2 subjects（含增强指标）...")
        subjects = sb.build_regional_subjects_v12(
            batch_code,
            enhanced_stats=enhanced,
            include_detail=include_detail,
        )

        # 统计口径：ACTIVE 学校与去重学生
        try:
            total_schools = db.execute(text(
                "SELECT COUNT(*) FROM school_master_data WHERE batch_code=:b AND status='ACTIVE'"
            ), {"b": batch_code}).scalar() or 0
            total_students = db.execute(text(
                """
                SELECT COUNT(DISTINCT scs.student_id)
                  FROM student_cleaned_scores scs
                  JOIN school_master_data smd
                    ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code
                   AND smd.school_id  COLLATE utf8mb4_unicode_ci = scs.school_code
                   AND smd.status='ACTIVE'
                 WHERE scs.batch_code=:b AND scs.subject_type IN ('exam','questionnaire')
                """
            ), {"b": batch_code}).scalar() or 0
        except Exception:
            total_schools = 0
            total_students = 0

        regional_json = {
            "schema_version": "v1.2",
            "data_version": "v1.2",
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "subjects": subjects,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        _log("[REGIONAL] 写入数据库（UPSERT）...")
        repo = StatisticalAggregationRepository(db)
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
            "calculation_duration": None,
            "updated_at": datetime.now(),
            "created_at": datetime.now(),
        })
        _log("[REGIONAL] 完成 ✅")


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: python scripts/rewrite_regional_v12.py <BATCH_CODE>")
        return 1
    batch = argv[1]
    rewrite_regional(batch)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

