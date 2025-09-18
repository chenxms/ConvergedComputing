#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
仅重算并回填“问卷科目”的区域汇聚（v1.2）

用途
- 避免整批次区域重建耗时，针对“问卷部分失效/需刷新”的场景，
  只重算问卷科目并合并到现有 REGIONAL JSON 中。

做了什么
- 可选刷新问卷题目选项分布（保证 option_label 与正/反向一致）。
- 仅拉取问卷科目增强统计（百分位/区分度/等级分布）。
- 只为指定问卷科目构建 subjects 片段并合并替换，保留其余科目不变。

注意
- 需要数据库已存在该批次 REGIONAL 记录（默认不创建）。
- 写入采用与主脚本一致的容错：优先 UPSERT，必要时“删除后插入”。

示例
  python scripts/patch_regional_questionnaire_v12.py --batch G7-2025 \
         --include-detail --subjects 问卷A,问卷B
"""

from __future__ import annotations
import argparse
import asyncio
import json
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import text

import os, sys
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus
from app.database.repositories import StatisticalAggregationRepository, RepositoryError
from app.services.subjects_builder import SubjectsBuilder
from app.services.calculation_service import CalculationService
from app.services.question_option_distribution_service import populate_questionnaire_distributions


async def _load_enhanced_for_questionnaires(db, batch_code: str, target_subjects: Set[str]) -> Optional[Dict[str, Any]]:
    """仅对问卷科目计算增强统计并返回兼容 SubjectsBuilder 的结构。"""
    try:
        calc = CalculationService(db)
        scores_df = await calc._fetch_student_scores(batch_code)
        if scores_df is None or scores_df.empty:
            return None
        # 仅保留问卷 + 目标科目
        if 'subject_type' in scores_df.columns:
            scores_df = scores_df[scores_df['subject_type'] == 'questionnaire']
        if target_subjects:
            scores_df = scores_df[scores_df['subject_name'].isin(list(target_subjects))]
        if scores_df is None or scores_df.empty:
            return None
        if 'total_score' in scores_df.columns and 'score' not in scores_df.columns:
            scores_df['score'] = scores_df['total_score']
        regional_stats = await calc._consolidate_multi_subject_results(
            batch_code=batch_code,
            scores_df=scores_df,
            validation_result={'is_valid': True, 'warnings': []}
        )
        # 兼容：SubjectsBuilder 读取时会直接从 map 中取该科目的统计，保持原样返回
        return regional_stats
    except Exception:
        return None


def _pick_questionnaire_subjects(sb: SubjectsBuilder, batch_code: str, subjects_arg: Optional[str]) -> List[str]:
    all_subs = sb.list_subjects(batch_code)
    q_names = [s.name for s in all_subs if s.type == 'questionnaire']
    if subjects_arg:
        wanted = {x.strip() for x in subjects_arg.split(',') if x.strip()}
        return [s for s in q_names if s in wanted]
    return q_names


def _build_one_questionnaire_subject(sb: SubjectsBuilder, batch_code: str, subject_name: str, include_detail: bool, subject_enhanced_stats: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """复用 SubjectsBuilder 内部方法，仅构建单个问卷科目的区域 subjects 条目。"""
    # 基础 metrics + 百分位/区分度注入
    metrics = sb._compute_subject_metrics(batch_code, subject_name, enhanced_stats=subject_enhanced_stats)
    # 问卷区域 school_rankings 也保留
    rankings = sb._compute_school_rankings(batch_code, subject_name)

    # 维度与分布
    dim_name_map = {}
    from app.database.connection import get_db_context
    with get_db_context() as db_tmp:
        dim_name_map = sb._batch_load_dimension_names(db_tmp, batch_code, subject_name)
    dim_avg_map = sb._compute_regional_dimension_avgs(batch_code, subject_name)
    try:
        dims_questions = sb._compute_questionnaire_dimension_question_option_distribution(batch_code, subject_name)
    except Exception:
        dims_questions = {}
    try:
        dims_od = sb._compute_questionnaire_dimension_option_distribution(batch_code, subject_name)
    except Exception:
        dims_od = {}
    dim_max_map = sb._compute_regional_dimension_max_scores(batch_code, subject_name)

    dim_codes = set(dim_avg_map.keys()) | set(dims_questions.keys()) | set(dims_od.keys())
    dims_list: List[Dict[str, Any]] = []
    for dim_code in sorted(dim_codes):
        entry: Dict[str, Any] = {
            'code': dim_code,
            'name': dim_name_map.get(dim_code, dim_code),
        }
        if dim_code in dim_avg_map:
            entry['avg'] = dim_avg_map[dim_code]
            mx = dim_max_map.get(dim_code)
            if mx and mx > 0:
                entry['score_rate'] = round(dim_avg_map[dim_code] * 100.0 / mx, 2)
        if dim_code in dims_od:
            entry['option_distribution'] = dims_od[dim_code]
        if dim_code in dims_questions:
            entry['questions'] = [
                {'question_id': qid, 'option_distribution': dist}
                for qid, dist in dims_questions[dim_code].items()
            ]
        dims_list.append(entry)

    subj: Dict[str, Any] = {
        'subject_name': subject_name,
        'type': 'questionnaire',
        'metrics': metrics,
        'school_rankings': rankings,
    }
    if dims_list:
        subj['dimensions'] = dims_list

    # include_detail 控制是否保留顶层的 detail 字段（与主构建保持一致策略）
    if not include_detail:
        subj.pop('p10', None)
        subj.pop('p50', None)
        subj.pop('p90', None)
        subj.pop('discrimination', None)
        subj.pop('grade_distribution', None)

    return subj


async def main_async(args: argparse.Namespace) -> int:
    batch_code: str = args.batch
    include_detail: bool = args.include_detail
    subjects_arg: Optional[str] = args.subjects
    refresh: bool = not args.no_refresh

    sb = SubjectsBuilder()
    with next(get_db()) as db:
        # 提高本会话锁等待，避免 1205
        try:
            db.execute(text("SET SESSION innodb_lock_wait_timeout=120"))
        except Exception:
            pass

        # 1) 目标问卷科目集合
        targets = _pick_questionnaire_subjects(sb, batch_code, subjects_arg)
        if not targets:
            print(f"[patch] 批次 {batch_code} 无问卷科目，或未匹配到指定科目。")
            return 0
        print(f"[patch] 目标问卷科目: {targets}")

        # 2) 可选：刷新题目选项分布（逐科目，减少一次性锁与压力）
        if refresh:
            ok = True
            for sname in targets:
                try:
                    res = populate_questionnaire_distributions(batch_code, sname)
                    s_ok = bool(res.get('success', True) if isinstance(res, dict) else True)
                    print(f"[patch] 刷新分布: {sname} success={s_ok}")
                    ok = ok and s_ok
                except Exception as e:
                    print(f"[patch] 刷新分布失败（忽略继续）: {sname} err={e}")
            if not ok:
                print("[patch] 分布刷新存在失败项，将继续尝试重算并回填。")

        # 3) 增强统计（仅问卷科目）
        enhanced = await _load_enhanced_for_questionnaires(db, batch_code, set(targets))
        # 从增强结果中按科目取子集，兼容 SubjectsBuilder 读取方式
        per_subject_enhanced: Dict[str, Dict[str, Any]] = {}
        if isinstance(enhanced, dict):
            for key in ('academic_subjects', 'non_academic_subjects'):
                part = enhanced.get(key, {}) if isinstance(enhanced.get(key), dict) else {}
                for sname, sdata in part.items():
                    if sname in targets:
                        per_subject_enhanced[sname] = sdata

        # 4) 读取现有 REGIONAL JSON（必须存在，默认不创建）
        repo = StatisticalAggregationRepository(db)
        existing = repo.get_regional_statistics(batch_code)
        if not existing or not existing.statistics_data:
            print("[patch] 未找到现有区域记录，默认不创建。请先执行一次完整构建或指定创建策略。")
            return 2
        data = existing.statistics_data if isinstance(existing.statistics_data, dict) else json.loads(existing.statistics_data)
        old_subjects: List[Dict[str, Any]] = data.get('subjects', [])

        # 5) 构建新问卷 subjects 片段
        new_subjects: List[Dict[str, Any]] = []
        for sname in targets:
            s_enh = per_subject_enhanced.get(sname)
            new_subjects.append(_build_one_questionnaire_subject(sb, batch_code, sname, include_detail, s_enh))

        # 6) 合并：替换同名问卷科目，保留其余科目不变
        keep: List[Dict[str, Any]] = []
        names_set = set(targets)
        for s in old_subjects:
            try:
                if s.get('type') == 'questionnaire' and s.get('subject_name') in names_set:
                    continue  # 替换
            except Exception:
                pass
            keep.append(s)
        merged_subjects = keep + new_subjects
        data['subjects'] = merged_subjects
        data['updated_at'] = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()

        # 7) 写回（容错：优先 upsert，必要时删除后插入）
        try:
            repo.upsert_statistics({
                'batch_code': batch_code,
                'aggregation_level': DBAggregationLevel.REGIONAL,
                'school_id': 'REGIONAL',
                'school_name': '区域汇聚',
                'statistics_data': data,
                'data_version': 'v1.2',
                'calculation_status': CalculationStatus.COMPLETED,
                'total_students': existing.total_students or 0,
                'total_schools': existing.total_schools or 0,
            })
            print(f"[patch] 完成回填，subjects={len(merged_subjects)} (问卷替换 {len(new_subjects)} 个)")
        except RepositoryError as e:
            print(f"[patch] upsert 冲突，尝试删除后插入：{e}")
            try:
                db.execute(
                    text("DELETE FROM statistical_aggregations WHERE batch_code=:b AND aggregation_level='REGIONAL' AND (school_id='REGIONAL' OR school_id IS NULL)"),
                    {"b": batch_code},
                )
                db.execute(
                    text(
                        """
                        INSERT INTO statistical_aggregations
                          (batch_code, aggregation_level, school_id, school_name, statistics_data, data_version, calculation_status, created_at, updated_at, total_students, total_schools)
                        VALUES
                          (:b, 'REGIONAL', 'REGIONAL', :name, :data, 'v1.2', 'COMPLETED', NOW(), NOW(), :ts, :tc)
                        """
                    ),
                    {
                        'b': batch_code,
                        'name': '区域汇聚',
                        'data': json.dumps(data, ensure_ascii=False),
                        'ts': int(existing.total_students or 0),
                        'tc': int(existing.total_schools or 0),
                    },
                )
                db.commit()
                print("[patch] 删除后插入完成。")
            except Exception as e2:
                print(f"[patch] 删除后插入失败：{e2}")
                raise
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='仅重算并回填问卷科目（区域 v1.2 部分重建）')
    p.add_argument('--batch', '-b', required=True, help='批次代码，如 G7-2025')
    p.add_argument('--subjects', help='仅处理这些问卷科目，逗号分隔；缺省为批次中全部问卷科目')
    p.add_argument('--include-detail', action='store_true', help='保留区域顶层 detail 字段（p10/p50/p90/grade_distribution）')
    p.add_argument('--no-refresh', action='store_true', help='不刷新问卷分布（默认会按科目刷新）')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    return asyncio.run(main_async(args))


if __name__ == '__main__':
    raise SystemExit(main())

