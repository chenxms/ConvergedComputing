#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
逐校重写学校�?subjects v1.2（补�?P10/P50/P90、区分度、等级占比、排名）

用途：
- 针对已完成区�?学校级重算但学校级指标不全的情况，逐校现算并回�?- 不依�?fast 物化脚本，直接用 CalculationService 的增强整合逻辑 + v1.2 SubjectsBuilder 入库

执行�?  python scripts/rebuild_school_v12.py            # 默认批次 G4-2025
  python scripts/rebuild_school_v12.py G8-2025    # 指定批次

说明�?- 仅更�?statistical_aggregations 表中 aggregation_level='SCHOOL' �?JSON（schema_version/data_version 均为 v1.2�?- 统计口径：仅 ACTIVE 学校；学生数去重；两位小数精度；排名使用 DENSE_RANK
"""

from __future__ import annotations

import sys
import os
import asyncio
import time
import types
from typing import Optional, List, Dict, Any

from sqlalchemy import text

# 兼容从任意工作目录运行：将项目根目录加入 sys.path
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus
from app.services.calculation_service import CalculationService
from app.services.subjects_builder import SubjectsBuilder
from app.utils.precision import round2_json


DEFAULT_BATCH = "G4-2025"


async def _rebuild_school_v12(batch_code: str) -> None:
    db = next(get_db())
    try:
        t0 = time.perf_counter()
        calc = CalculationService(db)
        builder = SubjectsBuilder()
        repo = StatisticalAggregationRepository(db)

        # 读取 ACTIVE 学校列表（以主数据为准）
        rows = db.execute(
            text(
                "SELECT school_id, standard_school_name FROM school_master_data "
                "WHERE batch_code=:b AND status='ACTIVE' ORDER BY school_id"
            ),
            {"b": batch_code},
        ).fetchall()
        total = len(rows)
        print(f"[START] batch={batch_code} active_schools={total}")

        # 预取：学校名映射
        school_name_map = {r[0]: r[1] for r in rows}

        # 统计 ACTIVE 学校总数（用于排名口径一致性）
        total_schools = db.execute(
            text("SELECT COUNT(*) FROM school_master_data WHERE batch_code=:b AND status='ACTIVE'"),
            {"b": batch_code},
        ).scalar() or 0

        # 预取：每校参与学生数（一�?GROUP BY�?        print("[PREP] computing per-school distinct student counts...")
        student_counts_rows = db.execute(
            text(
                """
                SELECT scs.school_code AS school_id,
                       COUNT(DISTINCT scs.student_id) AS cnt
                  FROM student_cleaned_scores scs
                  JOIN school_master_data smd
                    ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
                   AND smd.school_id  COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
                   AND smd.status='ACTIVE'
                 WHERE scs.batch_code=:b AND scs.subject_type IN ('exam','questionnaire')
                 GROUP BY scs.school_code
                """
            ),
            {"b": batch_code},
        ).fetchall()
        total_students_map = {sid: int(cnt or 0) for sid, cnt in student_counts_rows}

        # 预计算：每科目“维度统计”（区域级），供后续所有学校复用，避免重复全表扫描
        print("[PREP] precomputing per-subject dimension statistics cache...")
        precomputed_dim_stats: Dict[str, Dict[str, Any]] = {}
        subjects_cfg = await calc._get_batch_subjects(batch_code)
        for s in subjects_cfg:
            sname = s.get('subject_name') if isinstance(s, dict) else None
            if not sname:
                continue
            try:
                ds = await calc._calculate_subject_dimensions(batch_code, sname)
                precomputed_dim_stats[sname] = ds or {}
            except Exception as e:
                print(f"  [WARN] precompute dimensions failed for subject={sname}: {e}")

        # 预计算：维度排名缓存（按学校×科目×维度�?        print("[PREP] building dimension rank cache (this may take a while)...")
        dim_rank_cache = builder.build_dimension_rank_cache(batch_code)

        # 预计算：科目区域排名（学校在区域内的科目名次�?        print("[PREP] precomputing school region ranks per subject...")
        from collections import defaultdict
        school_subject_ranks: Dict[str, Dict[str, Any]] = defaultdict(dict)
        try:
            subjects_for_rank = [s.get('subject_name') for s in subjects_cfg if isinstance(s, dict)]
            for sname in subjects_for_rank:
                try:
                    rankings = builder._compute_school_rankings(batch_code, sname)
                    for item in rankings:
                        sid = item.get('school_id')
                        rnk = item.get('rank')
                        if sid is not None and rnk is not None:
                            school_subject_ranks[str(sid)][sname] = {"rank": int(rnk)}
                except Exception as e:
                    print(f"  [WARN] precompute subject rank failed for subject={sname}: {e}")
        except Exception:
            pass

        # 预加载：科目满分（填充到 calc 内部缓存，后续避�?DB 访问�?        try:
            _ = calc._batch_get_max_scores(batch_code)
        except Exception as e:
            print(f"[WARN] batch max scores prefetch failed: {e}")

        # 预加载：年级（覆写方法避免并发阶段再次访问DB�?        try:
            grade_level_const = calc._get_batch_grade_level(batch_code)
            def _grade_level_override(self, _batch_code: str) -> str:
                return grade_level_const
            calc._get_batch_grade_level = types.MethodType(_grade_level_override, calc)
        except Exception as e:
            print(f"[WARN] batch grade prefetch failed: {e}")

        # 批量入库大小，可通过环境变量 REBUILD_BATCH_SIZE 覆盖，默�?00
        try:
            BATCH_SIZE = int(os.getenv('REBUILD_BATCH_SIZE', '50'))
        except Exception:
            BATCH_SIZE = 50

        # 可选：一次性拉取全批次清洗后的学生分数，避免逐校重复访问数据�?        # 大批次（内存允许）建议开启；可通过环境变量 REBUILD_USE_BATCH_FETCH=1 控制
        all_scores_df = None
        try:
            if os.getenv('REBUILD_USE_BATCH_FETCH', '1') == '1':
                print("[PREP] fetching ALL cleaned scores once for the batch...")
                all_scores_df = await calc._fetch_student_scores(batch_code)
                if all_scores_df is not None and not all_scores_df.empty:
                    print(f"[PREP] all_scores_df loaded: rows={len(all_scores_df)} subjects={all_scores_df['subject_name'].nunique()}")
                else:
                    all_scores_df = None
        except Exception as e:
            print(f"[WARN] batch-level fetch failed, fallback to per-school fetch: {e}")
            all_scores_df = None

        # 并发执行：计算并�?+ 单协程写�?        try:
            CONCURRENCY = min(4, max(1, int(os.getenv('REBUILD_CONCURRENCY', '4'))))
        except Exception:
            CONCURRENCY = 2
        print(f"[CONF] concurrency={CONCURRENCY}, batch_size={BATCH_SIZE}")

        write_queue: asyncio.Queue = asyncio.Queue(maxsize=CONCURRENCY * 10)
        stop_sentinel = object()

        async def writer():
            buf: List[Dict[str, Any]] = []
            processed_cnt = 0
            start_ts = time.perf_counter()
            while True:
                item = await write_queue.get()
                if item is stop_sentinel:
                    break
                buf.append(item)
                processed_cnt += 1
                if len(buf) >= BATCH_SIZE:
                    repo.batch_upsert_statistics(buf, batch_size=BATCH_SIZE)
                    buf.clear()
            if buf:
                repo.batch_upsert_statistics(buf, batch_size=BATCH_SIZE)
                buf.clear()
            db.commit()
            dur = time.perf_counter() - start_ts
            print(f"[WRITE] committed {processed_cnt} schools in {dur:.2f}s")

        sem = asyncio.Semaphore(CONCURRENCY)

        async def process_school(idx0: int, school_id: str):
            async with sem:
                try:
                    # 取数（优先批次缓存；回退单校新会话，避免共享db�?                    if all_scores_df is not None:
                        try:
                            if 'school_id' in all_scores_df.columns:
                                school_df = all_scores_df[all_scores_df['school_id'] == school_id].copy()
                            elif 'school_code' in all_scores_df.columns:
                                school_df = all_scores_df[all_scores_df['school_code'] == school_id].copy()
                            else:
                                with next(get_db()) as db_ro:
                                    calc_ro = CalculationService(db_ro)
                                    school_df = await calc_ro._fetch_school_scores(batch_code, school_id)
                        except Exception:
                            with next(get_db()) as db_ro:
                                calc_ro = CalculationService(db_ro)
                                school_df = await calc_ro._fetch_school_scores(batch_code, school_id)
                    else:
                        with next(get_db()) as db_ro:
                            calc_ro = CalculationService(db_ro)
                            school_df = await calc_ro._fetch_school_scores(batch_code, school_id)

                    if school_df is None or school_df.empty:
                        if (idx0 + 1) % 20 == 0 or (idx0 + 1) == total:
                            print(f"  [{idx0+1}/{total}] {school_id}: no cleaned scores, skip")
                        return

                    # 统一列名
                    if "total_score" in school_df.columns:
                        school_df = school_df.rename(columns={"total_score": "score"})

                    # 计算增强统计（全内存�?                    enhanced = await calc._consolidate_multi_subject_results(
                        batch_code,
                        school_df,
                        validation_result=None,
                        precomputed_dimension_stats=precomputed_dim_stats,
                    )

                    # 组装 subjects（使用预计算排名缓存�?                    subjects = builder.build_school_subjects_v12(
                        batch_code,
                        school_id,
                        enhanced_stats=enhanced,
                        precomputed_ranks=school_subject_ranks.get(str(school_id)),
                        precomputed_dim_ranks=dim_rank_cache.get(str(school_id), {}),
                    )
                    _assert_enhanced_subjects(subjects, school_id)

                    school_name: Optional[str] = school_name_map.get(school_id)
                    payload = {
                        "schema_version": "v1.2",
                        "batch_code": batch_code,
                        "aggregation_level": "SCHOOL",
                        "school_id": school_id,
                        "school_name": school_name,
                        "subjects": subjects,
                    }
                    processed = round2_json(payload)
                    total_students = total_students_map.get(school_id, 0)
                    await write_queue.put({
                        "batch_code": batch_code,
                        "aggregation_level": DBAggregationLevel.SCHOOL,
                        "school_id": school_id,
                        "school_name": school_name,
                        "statistics_data": processed,
                        "data_version": "v1.2",
                        "calculation_status": CalculationStatus.COMPLETED,
                        "total_students": total_students,
                        "total_schools": total_schools,
                    })

                    if (idx0 + 1) % 20 == 0 or (idx0 + 1) == total:
                        print(f"  [{idx0+1}/{total}] {school_id}: queued (students={total_students})")

                except Exception as e:
                    print(f"  [{idx0+1}/{total}] {school_id}: ERROR {e}")`r`n                    raise

        # 启动 writer �?workers
        writer_task = asyncio.create_task(writer())
        worker_tasks = [asyncio.create_task(process_school(i, sid)) for i, (sid, _nm) in enumerate(rows)]
        await asyncio.gather(*worker_tasks)
        await write_queue.put(stop_sentinel)
        await writer_task

        t1 = time.perf_counter()
        print(f"[DONE] school-level v1.2 rewrite completed in {(t1 - t0):.2f}s, schools={total}, concurrency={CONCURRENCY}")

    finally:
        db.close()


def main() -> int:
    batch = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BATCH
    asyncio.run(_rebuild_school_v12(batch))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())






