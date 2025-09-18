#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
增强型v1.2物化脚本
- 键规范化
- 分层命中统计
- 缓存完整性审计
"""

from __future__ import annotations
import sys
import os
import json
from datetime import datetime, timezone
import time
from typing import List, Dict, Any, Optional
import argparse

from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db, get_db_context
from app.services.subjects_builder import SubjectsBuilder
from app.services.calculation_service import CalculationService
from app.utils.cache_utils import KeyNormalizer, CacheHitStats, CacheAuditor
import asyncio
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus


def _log(msg: str) -> None:
    """带时间戳的日志输出"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}", flush=True)


def build_normalized_cache(raw_cache: Dict[str, Any], normalizer: KeyNormalizer) -> Dict[str, Any]:
    """
    构建规范化的缓存
    将原始缓存的所有键进行规范化处理
    """
    normalized_cache = {}
    collision_count = 0

    for school_id, school_data in raw_cache.items():
        norm_school = normalizer.normalize_school_id(school_id)

        # 检查学校ID碰撞
        if norm_school in normalized_cache:
            collision_count += 1
            _log(f"[WARN] 学校ID碰撞: '{school_id}' 和之前的键都规范化为 '{norm_school}'")

        if not isinstance(school_data, dict):
            continue

        normalized_cache[norm_school] = {}

        for subject_name, subject_data in school_data.items():
            norm_subject = normalizer.normalize_subject_name(subject_name)

            if not isinstance(subject_data, dict):
                continue

            normalized_cache[norm_school][norm_subject] = {}

            # 处理维度数据
            if 'dimensions' in subject_data and isinstance(subject_data['dimensions'], dict):
                normalized_cache[norm_school][norm_subject]['dimensions'] = {}

                for dim_code, dim_data in subject_data['dimensions'].items():
                    norm_dim = normalizer.normalize_dimension_code(dim_code)
                    normalized_cache[norm_school][norm_subject]['dimensions'][norm_dim] = dim_data

    if collision_count > 0:
        _log(f"[WARN] 检测到 {collision_count} 个键碰撞")

    return normalized_cache


def rewrite_batch(batch_code: str, limit: Optional[int] = None, debug: bool = False) -> None:
    """
    重写批次数据
    Args:
        batch_code: 批次代码
        limit: 限制处理的学校数量（用于测试）
        debug: 是否输出详细调试信息
    """
    t_total_start = time.time()
    sb = SubjectsBuilder()
    normalizer = KeyNormalizer()
    cache_stats = CacheHitStats()

    with get_db_context() as db:
        calc = CalculationService(db)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # 设置数据库超时
        try:
            db.execute(text("SET SESSION net_write_timeout=600"))
            db.execute(text("SET SESSION net_read_timeout=600"))
            db.execute(text("SET SESSION wait_timeout=600"))
            db.execute(text("SET SESSION innodb_lock_wait_timeout=120"))
            db.execute(text("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        except Exception:
            pass

        _log(f"[1/4] 构建区域级 subjects (v1.2) ...")
        t_regional_start = time.time()

        # 计算区域级增强统计
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
        _log("[2/4] 构建维度排名缓存...")
        sb.reset_cache_stats()
        t_cache_start = time.time()

        # 构建原始缓存
        raw_dimension_cache = sb.build_dimension_rank_cache(batch_code)
        t_cache_elapsed = time.time() - t_cache_start

        _log(f"  原始缓存构建完成，耗时: {t_cache_elapsed:.2f}秒")

        # 审计原始缓存
        _log("  审计原始缓存...")
        audit_result = CacheAuditor.audit_cache(raw_dimension_cache)
        _log(f"  缓存状态: {audit_result['status']}")
        _log(f"  学校数量: {audit_result['total_schools']}")
        _log(f"  维度总数: {audit_result['total_dimensions']}")

        if audit_result['warnings']:
            for warning in audit_result['warnings']:
                _log(f"  [WARN] {warning}")

        if debug and audit_result['school_samples']:
            _log(f"  学校样例: {audit_result['school_samples'][:3]}")
            _log(f"  科目发现: {audit_result['subjects_found']}")

        # 构建规范化缓存
        _log("  构建规范化缓存...")
        dimension_rankings_cache = build_normalized_cache(raw_dimension_cache, normalizer)

        # 对比原始和规范化缓存
        if debug:
            _log(f"  原始缓存学校数: {len(raw_dimension_cache)}")
            _log(f"  规范化缓存学校数: {len(dimension_rankings_cache)}")
            if len(raw_dimension_cache) != len(dimension_rankings_cache):
                _log(f"  [WARN] 规范化后学校数量变化，可能存在碰撞!")

        # 提取学校排名缓存
        school_rankings_cache = {}
        try:
            for subj in regional_subjects or []:
                sname = subj.get('subject_name')
                norm_subject = normalizer.normalize_subject_name(sname)

                for r in subj.get('school_rankings', []) or []:
                    sid = r.get('school_id')
                    if not sid:
                        continue
                    norm_school = normalizer.normalize_school_id(sid)
                    school_rankings_cache.setdefault(norm_school, {})[norm_subject] = {
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

        _log("[3/4] 写入区域级记录...")
        repo = StatisticalAggregationRepository(db)

        # 统计学校和学生数
        try:
            total_schools_db = db.execute(text(
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
            total_schools_db = 0
            total_students = 0

        repo.upsert_statistics({
            "batch_code": batch_code,
            "aggregation_level": DBAggregationLevel.REGIONAL,
            "school_id": "REGIONAL",
            "school_name": "区域汇总",
            "statistics_data": regional_json,
            "data_version": "v1.2",
            "calculation_status": CalculationStatus.COMPLETED,
            "total_schools": total_schools_db,
            "total_students": total_students,
            "updated_at": datetime.now(),
            "created_at": datetime.now(),
        })
        _log("  区域级完成")
        t_regional_elapsed = time.time() - t_regional_start
        _log(f"  区域级耗时: {t_regional_elapsed:.2f}秒")

        # 学校级处理
        _log("[4/4] 处理学校级数据...")
        schools = db.execute(
            text("""
                SELECT DISTINCT smd.school_id, smd.standard_school_name
                FROM school_master_data smd
                WHERE smd.batch_code=:b AND smd.status='ACTIVE'
                ORDER BY smd.school_id
            """),
            {"b": batch_code},
        ).fetchall()

        # 应用limit限制
        if limit:
            schools = schools[:limit]
            _log(f"  限制处理前 {limit} 所学校")

        total = len(schools)
        _log(f"  共 {total} 所学校待处理")

        t_schools_start = time.time()

        for idx, (school_code, school_name) in enumerate(schools, start=1):
            # 规范化学校ID
            norm_school_id = normalizer.normalize_school_id(school_code)

            # 详细调试第一所学校
            if idx == 1 and debug:
                _log(f"\n[DEBUG] 处理第一所学校:")
                _log(f"  原始ID: '{school_code}' (类型: {type(school_code).__name__})")
                _log(f"  规范化ID: '{norm_school_id}'")
                _log(f"  缓存中是否存在: {norm_school_id in dimension_rankings_cache}")

                if norm_school_id in dimension_rankings_cache:
                    school_cache = dimension_rankings_cache[norm_school_id]
                    _log(f"  找到缓存! 包含科目: {list(school_cache.keys())}")
                else:
                    # 尝试查找相似的键
                    available_keys = list(dimension_rankings_cache.keys())[:10]
                    _log(f"  未找到缓存!")
                    _log(f"  缓存中的前10个键: {available_keys}")

                    # 记录未命中
                    cache_stats.record_l1_miss(
                        school_code, norm_school_id,
                        list(dimension_rankings_cache.keys())
                    )

            if idx % 5 == 0 or idx == total:
                _log(f"  进度 {idx}/{total} 学校: {school_code}")

            # 计算学校增强统计
            school_enhanced = None
            try:
                school_df = loop.run_until_complete(calc._fetch_school_scores(batch_code, school_code))
                if not school_df.empty:
                    school_enhanced = loop.run_until_complete(
                        calc._consolidate_multi_subject_results(batch_code, school_df)
                    )
            except Exception as e:
                if debug:
                    _log(f"    [WARN] 学校 {school_code} 增强统计失败: {e}")

            # 使用规范化的键获取缓存
            pre_ranks = school_rankings_cache.get(norm_school_id)
            pre_dim_ranks = dimension_rankings_cache.get(norm_school_id)

            # 记录缓存命中情况
            if pre_dim_ranks:
                cache_stats.record_l1_hit()
            else:
                cache_stats.record_l1_miss(
                    school_code, norm_school_id,
                    list(dimension_rankings_cache.keys())
                )

            # 构建学校subjects
            school_subjects = sb.build_school_subjects_v12(
                batch_code, school_code,
                enhanced_stats=school_enhanced,
                precomputed_ranks=pre_ranks,
                precomputed_dim_ranks=pre_dim_ranks
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

            # 分批提交
            if idx % 10 == 0:
                db.commit()
                if debug:
                    _log(f"  已提交至第 {idx} 所学校")

        db.commit()

        t_schools_elapsed = time.time() - t_schools_start
        _log(f"  学校级处理耗时: {t_schools_elapsed:.2f}秒")

        # 输出原始缓存统计
        _log("\n[缓存统计] 原始统计:")
        cache_stats_raw = sb.get_cache_stats()
        _log(f"  - 缓存命中: {cache_stats_raw['dim_cache_hits']}次")
        _log(f"  - 缓存未命中: {cache_stats_raw['dim_cache_misses']}次")
        _log(f"  - 降级查询: {cache_stats_raw['dim_cache_fallbacks']}次")
        if cache_stats_raw['dim_cache_hits'] + cache_stats_raw['dim_cache_misses'] > 0:
            hit_rate = cache_stats_raw['dim_cache_hits'] / (cache_stats_raw['dim_cache_hits'] + cache_stats_raw['dim_cache_misses']) * 100
            _log(f"  - 缓存命中率: {hit_rate:.2f}%")

        # 输出分层统计
        _log("\n[缓存统计] 分层统计:")
        stats_summary = cache_stats.get_summary()

        _log(f"  L1-学校层:")
        _log(f"    命中: {stats_summary['l1_school']['hit']}")
        _log(f"    未命中: {stats_summary['l1_school']['miss']}")
        _log(f"    命中率: {stats_summary['l1_school']['hit_rate']:.2f}%")

        # 显示未命中样例
        if stats_summary['miss_samples']['l1'] and debug:
            _log(f"\n  L1未命中样例:")
            for sample in stats_summary['miss_samples']['l1'][:3]:
                _log(f"    原始: '{sample['original']}' ({sample['type']})")
                _log(f"    规范化: '{sample['normalized']}'")
                if sample['close_matches']:
                    _log(f"    相似键: {sample['close_matches']}")
                _log(f"    可用键样例: {sample['available_sample']}")

        try:
            loop.close()
        except Exception:
            pass

        _log("\n[完成] 全部学校处理完成")

        # 输出总体统计
        t_total_elapsed = time.time() - t_total_start
        _log(f"\n[性能统计]")
        _log(f"  物化总耗时: {t_total_elapsed:.2f}秒")
        _log(f"  维度缓存构建: {t_cache_elapsed:.2f}秒 ({t_cache_elapsed/t_total_elapsed*100:.1f}%)")
        if total > 0:
            _log(f"  平均每所学校: {t_schools_elapsed/total:.2f}秒")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description='增强型v1.2物化脚本')
    parser.add_argument('batch_code', help='批次代码')
    parser.add_argument('--limit', type=int, help='限制处理的学校数量（用于测试）')
    parser.add_argument('--debug', action='store_true', help='输出详细调试信息')

    args = parser.parse_args(argv[1:])

    _log(f"开始物化批次: {args.batch_code}")
    if args.limit:
        _log(f"限制模式: 仅处理前 {args.limit} 所学校")
    if args.debug:
        _log(f"调试模式: 已启用")

    rewrite_batch(args.batch_code, limit=args.limit, debug=args.debug)
    _log(f"批次 {args.batch_code} 物化完成")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))