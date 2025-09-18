#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
带调试信息的v1.2物化脚本，用于诊断缓存命中率为0的问题
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


def _log(msg: str) -> None:
    print(msg, flush=True)


def rewrite_batch(batch_code: str) -> None:
    t_total_start = time.time()
    sb = SubjectsBuilder()

    with get_db_context() as db:
        calc = CalculationService(db)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

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

        regional_enhanced = None
        try:
            student_df = loop.run_until_complete(calc._fetch_student_scores(batch_code))
            if not student_df.empty:
                regional_enhanced = loop.run_until_complete(
                    calc._consolidate_multi_subject_results(batch_code, student_df)
                )
        except Exception as e:
            _log(f"[WARN] 区域增强统计获取失败: {e}")

        regional_subjects = sb.build_regional_subjects_v12(
            batch_code,
            enhanced_stats=regional_enhanced,
            include_detail=True,
        )

        # 构建维度排名缓存
        _log("[1.0] 构建维度排名缓存...")
        sb.reset_cache_stats()
        t_cache_start = time.time()
        dimension_rankings_cache = sb.build_dimension_rank_cache(batch_code)
        t_cache_elapsed = time.time() - t_cache_start
        _log(f"[1.0] 维度排名缓存构建完成，耗时: {t_cache_elapsed:.2f}秒")

        # 调试：输出缓存结构
        _log("[DEBUG] 维度缓存结构分析:")
        if dimension_rankings_cache:
            _log(f"  - 缓存中的学校数量: {len(dimension_rankings_cache)}")
            # 取第一个学校作为样例
            for school_id in list(dimension_rankings_cache.keys())[:1]:
                _log(f"  - 样例学校ID: '{school_id}' (类型: {type(school_id).__name__})")
                school_data = dimension_rankings_cache[school_id]
                if school_data:
                    _log(f"  - 该学校的科目: {list(school_data.keys())}")
                    for subject in list(school_data.keys())[:1]:
                        _log(f"    - 样例科目: '{subject}' (类型: {type(subject).__name__})")
                        if 'dimensions' in school_data[subject]:
                            dims = school_data[subject]['dimensions']
                            _log(f"    - 维度数量: {len(dims)}")
                            for dim_code in list(dims.keys())[:2]:
                                _log(f"      - 维度: '{dim_code}' -> {dims[dim_code]}")
        else:
            _log("  - 缓存为空!")

        # 提取学校排名缓存
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
            _log(f"[WARN] 提取学校排名缓存失败: {e}")

        # 写入区域级数据
        regional_json = {
            "schema_version": "v1.2",
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "subjects": regional_subjects,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "data_version": "v1.2",
        }

        _log("[1.1] 写入区域级记录 ...")
        repo = StatisticalAggregationRepository(db)

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
            "updated_at": datetime.now(),
            "created_at": datetime.now(),
        })
        _log("[1.2] 区域级完成。")
        t_regional_elapsed = time.time() - t_regional_start
        _log(f"[1.3] 区域级耗时: {t_regional_elapsed:.2f}秒")

        # 学校级处理
        _log("[2/3] 读取学校列表 ...")
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

        # 调试：检查学校ID类型
        if schools:
            first_school_id, first_school_name = schools[0]
            _log(f"[DEBUG] 第一所学校ID: '{first_school_id}' (类型: {type(first_school_id).__name__})")

        t_schools_start = time.time()
        for idx, (school_code, school_name) in enumerate(schools, start=1):
            if idx == 1:
                _log(f"[DEBUG] 处理学校 {school_code}:")
                _log(f"  - 查询缓存key: '{school_code}' (类型: {type(school_code).__name__})")
                _log(f"  - 缓存中是否存在: {school_code in dimension_rankings_cache}")
                _log(f"  - str(school_code)是否存在: {str(school_code) in dimension_rankings_cache}")

                # 尝试获取缓存
                pre_dim_ranks = dimension_rankings_cache.get(school_code)
                if pre_dim_ranks is None:
                    pre_dim_ranks = dimension_rankings_cache.get(str(school_code))
                    if pre_dim_ranks:
                        _log(f"  - 使用str()转换后找到缓存!")

                if pre_dim_ranks:
                    _log(f"  - 缓存内容: 包含科目 {list(pre_dim_ranks.keys())}")
                else:
                    _log(f"  - 缓存未命中!")

            if idx % 5 == 0 or idx == total:
                _log(f"    进度 {idx}/{total} 学校: {school_code}")

            school_enhanced = None
            try:
                school_df = loop.run_until_complete(calc._fetch_school_scores(batch_code, school_code))
                if not school_df.empty:
                    school_enhanced = loop.run_until_complete(
                        calc._consolidate_multi_subject_results(batch_code, school_df)
                    )
            except Exception as e:
                _log(f"    [WARN] 学校 {school_code} 增强统计失败: {e}")

            # 统一使用字符串类型的school_code作为key
            pre_ranks = school_rankings_cache.get(str(school_code)) if isinstance(school_rankings_cache, dict) else None
            pre_dim_ranks = dimension_rankings_cache.get(str(school_code)) if isinstance(dimension_rankings_cache, dict) else None

            school_subjects = sb.build_school_subjects_v12(
                batch_code, school_code, enhanced_stats=school_enhanced,
                precomputed_ranks=pre_ranks, precomputed_dim_ranks=pre_dim_ranks
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

            if idx % 10 == 0:
                db.commit()
                _log(f"    已提交至第 {idx} 所学校")

        db.commit()

        t_schools_elapsed = time.time() - t_schools_start
        _log(f"[2.2] 学校级处理耗时: {t_schools_elapsed:.2f}秒")

        # 输出缓存统计
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

        t_total_elapsed = time.time() - t_total_start
        _log(f"[统计] 物化总耗时: {t_total_elapsed:.2f}秒")
        _log(f"[统计] 其中维度缓存构建: {t_cache_elapsed:.2f}秒 ({t_cache_elapsed/t_total_elapsed*100:.1f}%)")
        _log(f"[统计] 平均每所学校: {(t_total_elapsed-t_cache_elapsed)/total:.2f}秒")


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: python scripts/rewrite_subjects_v12_debug.py <BATCH>")
        return 1
    for b in argv[1:]:
        print(f"Rewriting subjects for {b} with debug info...")
        rewrite_batch(b)
        print(f"Done: {b}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))