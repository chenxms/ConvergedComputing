#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rewrite statistical_aggregations.statistics_data to v1.2 subjects schema

- Adds regional subjects with school_rankings (exam + questionnaire)
- Adds school subjects with region_rank/total_schools
- Adds school dimension ranks per subject
- Adds questionnaire option distributions (dimension/question)
"""

from __future__ import annotations
import sys
import os
import json
from datetime import datetime, timezone
import time
from typing import List

from sqlalchemy import text

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
from sqlalchemy import text


def _log(msg: str) -> None:
    print(msg, flush=True)


def rewrite_batch(batch_code: str) -> None:
    t_total_start = time.time()  # 记录总耗时
    sb = SubjectsBuilder()
    # 使用事务上下文，确保逐步提交并可见进度
    with get_db_context() as db:
        calc = CalculationService(db)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        # 尽量提高单连接的超时容忍度，避免大JSON写入超时
        try:
            db.execute(text("SET SESSION net_write_timeout=600"))
            db.execute(text("SET SESSION net_read_timeout=600"))
            db.execute(text("SET SESSION wait_timeout=600"))
            db.execute(text("SET SESSION innodb_lock_wait_timeout=120"))
            db.execute(text("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        except Exception:
            pass
        _log(f"[1/3] 构建区域级 subjects (v1.2) ...")
        t_regional_start = time.time()
        # 计算区域级增强统计（含百分位、区分度、等级分布）
        regional_enhanced = None
        try:
            student_df = loop.run_until_complete(calc._fetch_student_scores(batch_code))
            if not student_df.empty:
                regional_enhanced = loop.run_until_complete(
                    calc._consolidate_multi_subject_results(batch_code, student_df)
                )
        except Exception as e:
            _log(f"[WARN] 区域增强统计获取失败，降级输出基础结构: {e}")
            regional_enhanced = None

        # 使用 v1.2 增强版，包含问卷维度avg与维度下题目分布；并在区域级顶层保留细节
        regional_subjects = sb.build_regional_subjects_v12(
            batch_code,
            enhanced_stats=regional_enhanced,
            include_detail=True,
        )
        # 2) 提取维度排名缓存：{ school_id: { subject_name: { dimensions: { code: {rank, avg} } } } }
        _log("[1.0] 构建维度排名缓存（按科目分批）...")
        sb.reset_cache_stats()
        t_cache_start = time.time()
        dimension_rankings_cache = sb.build_dimension_rank_cache(batch_code)
        t_cache_elapsed = time.time() - t_cache_start
        _log(f"[1.0] 维度排名缓存构建完成，耗时: {t_cache_elapsed:.2f}秒")
        # 覆盖校验（学校覆盖）
        try:
            schools_expected = set([r[0] for r in db.execute(text("SELECT DISTINCT school_id FROM school_master_data WHERE batch_code=:b AND status='ACTIVE'"), {"b": batch_code}).fetchall()])
            schools_covered = set(dimension_rankings_cache.keys()) if isinstance(dimension_rankings_cache, dict) else set()
            _log(f"[1.0] 维度缓存学校覆盖: {len(schools_covered)}/{len(schools_expected)}")
        except Exception as e:
            _log(f"[WARN] 维度缓存覆盖校验失败: {e}")
        # 2) 提取学校排名缓存：{ school_id: { subject_name: {rank, avg} } }
        school_rankings_cache = {}
        try:
            for subj in regional_subjects or []:
                sname = subj.get('subject_name')
                for r in subj.get('school_rankings', []) or []:
                    sid = r.get('school_id')
                    if not sid:
                        continue
                    school_rankings_cache.setdefault(sid, {})[sname] = {
                        'rank': r.get('rank'),
                        'avg': r.get('avg') if 'avg' in r else r.get('avg_score')
                    }
        except Exception as e:
            _log(f"[WARN] 提取学校排名缓存失败（忽略优化，继续）：{e}")
        regional_json = {
            "schema_version": "v1.2",
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "subjects": regional_subjects,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "data_version": "v1.2",
        }
        # 使用带重试的仓库 upsert，避免锁等待失败
        _log("[1.1] 写入区域级记录 ...")
        repo = StatisticalAggregationRepository(db)
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
        _log("[1.2] 区域级完成。")
        t_regional_elapsed = time.time() - t_regional_start
        _log(f"[1.3] 区域级耗时: {t_regional_elapsed:.2f}秒")

        # School records
        _log("[2/3] 读取学校列表 ...")
        # 以主数据为准拉取学校列表与标准名
        schools = db.execute(
            text("""
                SELECT DISTINCT smd.school_id, smd.standard_school_name
                FROM school_master_data smd
                WHERE smd.batch_code=:b AND smd.status='ACTIVE'
                ORDER BY smd.school_id
            """),
            {"b": batch_code},
        ).fetchall()
        total = len(schools)
        _log(f"[2.1] 共 {total} 所学校，开始生成学校级 subjects (v1.2) ...")
        t_schools_start = time.time()
        # 预先统计 ACTIVE 学校总数（写入顶层字段一致性）
        try:
            total_active_schools = db.execute(text("SELECT COUNT(*) FROM school_master_data WHERE batch_code=:b AND status='ACTIVE'"), {"b": batch_code}).scalar() or len(schools)
        except Exception:
            total_active_schools = len(schools)

        for idx, (school_code, school_name) in enumerate(schools, start=1):
            if idx == 1 or idx % 5 == 0 or idx == total:
                _log(f"    进度 {idx}/{total} 学校: {school_code}")
            # 使用 v1.2 增强版，包含 regional_avg 与学校级问卷题目分布
            # 为避免 CPU 密集操作长时间无输出，逐校打印关键节点
            # 2.2 计算该校的增强统计（按科目整合），随后构建 subjects（包含顶层 rank/p10/p50/p90/区分度/等级占比）
            school_enhanced = None
            try:
                school_df = loop.run_until_complete(calc._fetch_school_scores(batch_code, school_code))
                if not school_df.empty:
                    school_enhanced = loop.run_until_complete(
                        calc._consolidate_multi_subject_results(batch_code, school_df)
                    )
            except Exception as e:
                _log(f"    [WARN] 学校 {school_code} 增强统计失败，降级输出基础结构: {e}")

            pre_ranks = school_rankings_cache.get(school_code) if isinstance(school_rankings_cache, dict) else None
            pre_dim_ranks = dimension_rankings_cache.get(school_code) if isinstance(dimension_rankings_cache, dict) else None
            school_subjects = sb.build_school_subjects_v12(
                batch_code, school_code, enhanced_stats=school_enhanced, precomputed_ranks=pre_ranks, precomputed_dim_ranks=pre_dim_ranks
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
            # 统计该校参与学生数（去重），用于顶层 total_students
            try:
                total_students = db.execute(text(
                    """
                    SELECT COUNT(DISTINCT scs.student_id)
                      FROM student_cleaned_scores scs
                      JOIN school_master_data smd
                        ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code
                       AND smd.school_id  COLLATE utf8mb4_unicode_ci = scs.school_code
                       AND smd.status='ACTIVE'
                     WHERE scs.batch_code=:b AND scs.school_code=:s AND scs.subject_type IN ('exam','questionnaire')
                    """
                ), {"b": batch_code, "s": school_code}).scalar() or 0
            except Exception:
                total_students = 0

            # 2.3 写入（带重试）
            repo.upsert_statistics({
                "batch_code": batch_code,
                "aggregation_level": DBAggregationLevel.SCHOOL,
                "school_id": school_code,
                "school_name": school_name,
                "statistics_data": school_json,
                "data_version": "v1.2",
                "calculation_status": CalculationStatus.COMPLETED,
                "total_students": int(total_students),
                "total_schools": int(total_active_schools),
                "calculation_duration": None,
                "updated_at": datetime.now(),
                "created_at": datetime.now(),
            })
            # 2.4 分批提交，避免超大事务
            if idx % 10 == 0:
                db.commit()
                _log(f"    已提交至第 {idx} 所学校")
        db.commit()

        # 学校处理耗时
        t_schools_elapsed = time.time() - t_schools_start
        _log(f"[2.2] 学校级处理耗时: {t_schools_elapsed:.2f}秒")

        # 输出缓存统计信息
        cache_stats = sb.get_cache_stats()
        _log(f"[2.3] 维度缓存统计:")
        _log(f"    - 缓存命中: {cache_stats['dim_cache_hits']}次")
        _log(f"    - 缓存未命中: {cache_stats['dim_cache_misses']}次")
        _log(f"    - 降级查询: {cache_stats['dim_cache_fallbacks']}次")
        if cache_stats['dim_cache_hits'] + cache_stats['dim_cache_misses'] > 0:
            hit_rate = cache_stats['dim_cache_hits'] / (cache_stats['dim_cache_hits'] + cache_stats['dim_cache_misses']) * 100
            _log(f"    - 缓存命中率: {hit_rate:.2f}%")

        try:
            loop.close()
        except Exception:
            pass
        _log("[3/3] 全部学校完成并提交。")

        # 输出总体耗时统计
        t_total_elapsed = time.time() - t_total_start
        _log(f"[统计] 物化总耗时: {t_total_elapsed:.2f}秒")
        _log(f"[统计] 其中维度缓存构建: {t_cache_elapsed:.2f}秒 ({t_cache_elapsed/t_total_elapsed*100:.1f}%)")
        _log(f"[统计] 平均每所学校: {(t_total_elapsed-t_cache_elapsed)/total:.2f}秒")


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: python scripts/rewrite_subjects_v12.py <BATCH> [<BATCH> ...]")
        return 1
    for b in argv[1:]:
        print(f"Rewriting subjects for {b} ...")
        rewrite_batch(b)
        print(f"Done: {b}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
