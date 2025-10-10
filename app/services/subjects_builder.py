#!/usr/bin/env python3

# -*- coding: utf-8 -*-



"""

SubjectsBuilder



渚濇嵁銆婃眹鑱氭ā鍧椾慨澶嶅疄鏂芥柟妗?v1.2銆嬶紝浠庢暟鎹簱璁＄畻骞剁敓鎴愮粺涓€鐨?subjects 缁撴瀯锛?

鍖呭惈鑰冭瘯涓庨棶鍗风殑锛?

- 绉戠洰灞傛寚鏍囷紙avg/stddev/max/min锛屽彲鎵╁睍 p10/p50/p90锛?

- 鍖哄煙灞傚鏍℃帓鍚?school_rankings锛堣€冭瘯/闂嵎鍧囧弬涓庯級

- 瀛︽牎灞傛垜鏍″悕娆?region_rank/total_schools

- 瀛︽牎灞傜淮搴︽帓鍚?dimensions[].rank锛堟寜瀛︽牎缁村害鍧囧垎锛?

- 闂嵎缁村害/棰樼洰閫夐」鍗犳瘮 option_distribution



杈撳嚭宸插仛涓や綅灏忔暟绮惧害缁熶竴锛堝€间笌鐧惧垎姣斿瓧娈碉級銆?

"""



from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from decimal import Decimal

from dataclasses import dataclass

import json

from collections import defaultdict



from sqlalchemy import text



from app.database.connection import get_db, get_db_context

from app.database.models import BatchDimensionDefinition

from app.database.repositories import PrecomputedMetricsRepository, DataIntegrityError

from app.utils.precision import round2, round2_json





@dataclass

class SubjectInfo:

    name: str

    type: str  # 'exam' | 'questionnaire' | 'interaction'





class SubjectsBuilder:

    def __init__(self) -> None:

        # 缁村害鍚嶇О缂撳瓨 {batch_code: {subject_name: {dimension_code: dimension_name}}}

        self._dimension_name_cache: Dict[str, Dict[str, Dict[str, str]]] = {}

        # 缁村害鎺掑悕缂撳瓨 {batch_code: {school_id: {subject_name: {dimension_code: {...}}}}}

        self._dimension_rank_cache: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {}

        # 鍖哄煙瀛︽牎鎬绘暟缂撳瓨 {(batch_code, subject_name): total_schools}

        self._total_school_cache: Dict[Tuple[str, str], int] = {}

        # ACTIVE瀛︽牎鍚嶇О缂撳瓨 {batch_code: {school_id: school_name}}

        self._school_name_cache: Dict[str, Dict[str, str]] = {}

        self._subject_metric_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}

        self._school_metric_cache: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

        self._cache_stats: Dict[str, int] = self._init_cache_stats()



    def _init_cache_stats(self) -> Dict[str, int]:

        return {

            'dim_cache_hits': 0,

            'dim_cache_misses': 0,

            'dim_cache_fallbacks': 0,

        }



    def reset_cache_stats(self, clear_dimension_cache: bool = True) -> None:

        """Reset cached statistics counters and optionally drop cached dimension ranks."""

        self._cache_stats = self._init_cache_stats()

        if clear_dimension_cache:

            self._dimension_rank_cache.clear()

        self._total_school_cache.clear()

        self._subject_metric_cache.clear()

        self._school_metric_cache.clear()



    def get_cache_stats(self) -> Dict[str, int]:

        """Return a shallow copy of current cache statistics."""

        return dict(self._cache_stats)



    def _record_cache_hit(self) -> None:

        self._cache_stats['dim_cache_hits'] += 1



    def _record_cache_miss(self) -> None:

        self._cache_stats['dim_cache_misses'] += 1



    def _record_cache_fallback(self) -> None:

        self._cache_stats['dim_cache_fallbacks'] += 1

    @staticmethod
    def _dedupe_option_list(options: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """按 option_level 对选项列表去重并按等级排序。

        - 若同一等级出现多次，保留第一次出现的记录（避免重复平铺导致的视觉“无限重复”）。
        - 始终按 option_level 升序输出，保证前端稳定渲染。
        """
        if not isinstance(options, list) or not options:
            return options or []
        seen: Dict[int, Dict[str, Any]] = {}
        for item in options:
            try:
                lvl = int(item.get('option_level')) if item and item.get('option_level') is not None else None
            except Exception:
                lvl = None
            if lvl is None:
                continue
            if lvl not in seen:
                # 仅保留第一条，避免由于重复JOIN/平铺导致的重复项
                seen[lvl] = dict(item)
        # 返回按等级排序后的去重结果
        return sorted(seen.values(), key=lambda it: it.get('option_level', 0))

    @staticmethod
    def _normalize_school_code_value(value: Any) -> str:
        """将学校编码规范为字符串，去除尾部无意义的小数部分（如 '5001.0' -> '5001'）。"""
        try:
            s = str(value).strip()
            if not s:
                return s
            if "." in s:
                head, tail = s.split(".", 1)
                if tail and set(tail) <= {"0"} and head.lstrip("-+").isdigit():
                    return head
            return s
        except Exception:
            return str(value)



    @staticmethod

    def _normalize_option_label(value: Any) -> Optional[str]:

        if value is None:

            return None

        if isinstance(value, bytes):

            try:

                value = value.decode('utf-8')

            except Exception:

                value = value.decode(errors='ignore')

        text = str(value).strip()

        return text or None



    def _safe_load_json(self, payload: Any) -> Any:

        """Safely decode JSON payloads that may already be parsed."""

        if payload is None:

            return None

        if isinstance(payload, (dict, list)):

            return payload

        if isinstance(payload, (bytes, bytearray)):

            try:

                payload = payload.decode('utf-8')

            except Exception:

                return None

        if isinstance(payload, str):

            data = payload.strip()

            if not data:

                return None

            try:

                return json.loads(data)

            except Exception:

                return None

        return None



    

    def _simplify_grade_distribution(self, grade_dist: Dict[str, Any]) -> Optional[Dict[str, Any]]:

        """灏嗙瓑绾у垎甯冪簿绠€涓?{counts, percentages} 缁撴瀯銆?

        鍏煎涓ょ杈撳叆锛?

        - 鐩存帴鏄?{counts, rates, percentages, labels}

        - 鎴栧寘鍚?distribution 閿細{distribution: {...}}

        杩斿洖 None 琛ㄧず鏃犳硶璇嗗埆鏈夋晥缁撴瀯銆?

        """

        try:

            dist = grade_dist.get('distribution') if isinstance(grade_dist, dict) and 'distribution' in grade_dist else grade_dist

            if not isinstance(dist, dict):

                return None

            counts = dist.get('counts')

            percentages = dist.get('percentages')

            if isinstance(counts, dict) and isinstance(percentages, dict):

                return {

                    'counts': counts,

                    'percentages': percentages,

                }

            # 鍏煎鏃у舰鎬侊細姣忕瓑绾т负 {count, percentage}

            per_level = {k: v for k, v in dist.items() if isinstance(v, dict) and 'count' in v and 'percentage' in v}

            if per_level:

                return {

                    'counts': {k: v.get('count') for k, v in per_level.items()},

                    'percentages': {k: v.get('percentage') for k, v in per_level.items()},

                }

        except Exception:

            return None

        return None

    

    def _get_dimension_name(self, db, batch_code: str, subject_name: str, dimension_code: str) -> str:

        """鑾峰彇缁村害涓枃鍚嶇О锛屽甫缂撳瓨鏈哄埗"""

        # 妫€鏌ョ紦瀛?

        if (batch_code in self._dimension_name_cache and 

            subject_name in self._dimension_name_cache[batch_code] and

            dimension_code in self._dimension_name_cache[batch_code][subject_name]):

            return self._dimension_name_cache[batch_code][subject_name][dimension_code]

        

        # 鏌ヨ鏁版嵁搴?

        try:

            dimension_def = db.query(BatchDimensionDefinition).filter(

                BatchDimensionDefinition.batch_code == batch_code,

                BatchDimensionDefinition.subject_name == subject_name,

                BatchDimensionDefinition.dimension_code == dimension_code

            ).first()

            

            dimension_name = dimension_def.dimension_name if dimension_def else dimension_code

            

            # 鏇存柊缂撳瓨

            if batch_code not in self._dimension_name_cache:

                self._dimension_name_cache[batch_code] = {}

            if subject_name not in self._dimension_name_cache[batch_code]:

                self._dimension_name_cache[batch_code][subject_name] = {}

            self._dimension_name_cache[batch_code][subject_name][dimension_code] = dimension_name

            

            return dimension_name

            

        except Exception as e:

            print(f"鑾峰彇缁村害鍚嶇О澶辫触: batch_code={batch_code}, subject_name={subject_name}, dimension_code={dimension_code}, error={e}")

            return dimension_code

    

    def _batch_load_dimension_names(self, db, batch_code: str, subject_name: str) -> Dict[str, str]:

        """Load dimension name mapping from batch definition."""

        try:

            dimension_defs = (

                db.query(BatchDimensionDefinition)

                .filter(

                    BatchDimensionDefinition.batch_code == batch_code,

                    BatchDimensionDefinition.subject_name == subject_name,

                )

                .all()

            )



            dimension_mapping: Dict[str, str] = {}

            for def_record in dimension_defs:

                if not def_record or not def_record.dimension_code:

                    continue

                dimension_mapping[def_record.dimension_code] = (

                    def_record.dimension_name or def_record.dimension_code

                )



            if batch_code not in self._dimension_name_cache:

                self._dimension_name_cache[batch_code] = {}

            if subject_name not in self._dimension_name_cache[batch_code]:

                self._dimension_name_cache[batch_code][subject_name] = {}

            self._dimension_name_cache[batch_code][subject_name].update(dimension_mapping)



            return dimension_mapping



        except Exception as exc:

            print(

                "批量加载维度名称失败:",

                f"batch_code={batch_code}, subject_name={subject_name}, error={exc}",

            )

            return {}

    def list_subjects(self, batch_code: str) -> List[SubjectInfo]:

        """Return subjects using precomputed core metrics."""

        with get_db_context() as db:

            repo = PrecomputedMetricsRepository(db)

            rows = repo.list_subjects(batch_code)

        return [

            SubjectInfo(name=row["subject_name"], type=(row["subject_type"] or "exam").lower())

            for row in rows

        ]



    def build_dimension_rank_cache(self, batch_code: str) -> Dict[str, Dict[str, Dict[str, Any]]]:

        """Build a cache of per-school dimension averages and ranks for a batch."""

        if batch_code in self._dimension_rank_cache:

            return self._dimension_rank_cache[batch_code]



        aggregates: Dict[Tuple[str, str], Dict[str, Dict[str, float]]] = defaultdict(

            lambda: defaultdict(lambda: {'sum': 0.0, 'count': 0})

        )

        dimension_max_scores: Dict[Tuple[str, str], float] = {}



        with get_db_context() as db:

            sql = text(

                """

                SELECT scs.school_code,

                       scs.subject_name,

                       scs.dimension_scores,

                       scs.dimension_max_scores

                FROM student_cleaned_scores scs

                JOIN school_master_data smd

                  ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci

                 AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci

                 AND smd.status = 'ACTIVE'

                WHERE scs.batch_code = :batch

                  AND LOWER(scs.subject_type) IN ('exam','questionnaire')

                  AND scs.dimension_scores IS NOT NULL

                  AND scs.dimension_scores <> ''

                """

            )

            rows = db.execute(sql, {"batch": batch_code}).fetchall()



        for school_code, subject_name, dimension_scores_raw, dimension_max_raw in rows:

            if not subject_name:

                continue

            subject = subject_name

            school = str(school_code)

            dimension_scores = self._safe_load_json(dimension_scores_raw)

            dimension_max = self._safe_load_json(dimension_max_raw)

            if not isinstance(dimension_scores, dict):

                continue

            for dim_code, payload in dimension_scores.items():

                if not isinstance(payload, dict):

                    continue

                score = payload.get('score')

                if score is None:

                    continue

                try:

                    score_value = float(score)

                except (TypeError, ValueError):

                    continue

                key = (subject, str(dim_code))

                stats = aggregates[key][school]

                stats['sum'] += score_value

                stats['count'] += 1



                if isinstance(dimension_max, dict):

                    max_payload = dimension_max.get(dim_code)

                    max_value = None

                    if isinstance(max_payload, dict):

                        max_value = max_payload.get('max_score') or max_payload.get('max')

                    else:

                        max_value = max_payload

                    try:

                        max_float = float(max_value) if max_value is not None else None

                    except (TypeError, ValueError):

                        max_float = None

                    if max_float and max_float > 0:

                        existing = dimension_max_scores.get(key)

                        if not existing or max_float > existing:

                            dimension_max_scores[key] = max_float



        cache = defaultdict(lambda: defaultdict(dict))

        for (subject_name, dim_code), per_school in aggregates.items():

            averages = []

            for school_id, stats in per_school.items():

                if stats['count'] <= 0:

                    continue

                avg_value = stats['sum'] / stats['count']

                averages.append((school_id, avg_value))

            if not averages:

                continue

            averages.sort(key=lambda item: (-item[1], item[0]))

            rank = 0

            last_value = None

            max_score = dimension_max_scores.get((subject_name, dim_code))

            for index, (school_id, avg_value) in enumerate(averages):

                if last_value is None or abs(avg_value - last_value) > 1e-9:

                    rank = index + 1

                    last_value = avg_value

                score_rate = None

                if max_score and max_score != 0:

                    score_rate = round2((avg_value / max_score) * 100.0)

                cache[school_id][subject_name][str(dim_code)] = {

                    'avg': round2(avg_value),

                    'rank': rank,

                    'max_score': float(max_score) if max_score else None,

                    'score_rate': score_rate,

                }



        final_cache: Dict[str, Dict[str, Dict[str, Any]]] = {

            school: {subject: dict(dim_map) for subject, dim_map in subject_map.items()}

            for school, subject_map in cache.items()

        }

        self._dimension_rank_cache[batch_code] = final_cache

        return final_cache



    def build_regional_subjects(

        self,

        batch_code: str,

        enhanced_stats: Optional[Dict[str, Any]] = None

    ) -> List[Dict[str, Any]]:

        subjects: List[Dict[str, Any]] = []

        for s in self.list_subjects(batch_code):

            subject_enhanced_stats = None

            if enhanced_stats:

                academic_subjects = enhanced_stats.get('academic_subjects', {})

                non_academic_subjects = enhanced_stats.get('non_academic_subjects', {})

                subject_enhanced_stats = academic_subjects.get(s.name) or non_academic_subjects.get(s.name)



            metrics, grade_distribution = self._compute_subject_metrics(

                batch_code,

                s.name,

                enhanced_stats=subject_enhanced_stats,

            )

            subj: Dict[str, Any] = {

                "subject_name": s.name,

                "type": s.type,

                "metrics": metrics,

                "school_rankings": self._compute_school_rankings(batch_code, s.name),

            }

            if grade_distribution:

                subj["grade_distribution"] = grade_distribution



            try:

                with get_db_context() as db_tmp:

                    dim_name_map = self._batch_load_dimension_names(db_tmp, batch_code, s.name)

            except Exception:

                dim_name_map = {}



            dim_metrics = self._compute_regional_dimension_metrics(batch_code, s.name)

            dims_questions: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

            dims_option_distribution: Dict[str, List[Dict[str, Any]]] = {}

            dim_question_summary: Dict[str, Dict[str, Any]] = {}

            if s.type == 'questionnaire':

                try:

                    dims_questions = self._compute_questionnaire_dimension_question_option_distribution(batch_code, s.name)

                except Exception as exc:

                    print(f"维度题目分布计算失败: {exc}")

                    dims_questions = {}

                try:

                    dims_option_distribution = self._compute_questionnaire_dimension_option_distribution(batch_code, s.name)

                except Exception as exc:

                    print(f"维度选项分布计算失败: {exc}")

                    dims_option_distribution = {}

                try:

                    dim_question_summary = self._compute_questionnaire_dimension_question_summary(batch_code, s.name)

                except Exception as exc:

                    print(f"维度题目摘要计算失败: {exc}")

                    dim_question_summary = {}



            all_dim_codes = (

                set(dim_metrics.keys())

                | set(dims_option_distribution.keys())

                | set(dims_questions.keys())

                | set(dim_question_summary.keys())

            )



            dim_entries: Dict[str, Dict[str, Any]] = {}



            def ensure_entry(code: str) -> Dict[str, Any]:

                entry = dim_entries.get(code)

                if entry is None:

                    entry = {"code": code, "name": dim_name_map.get(code, code)}

                    dim_entries[code] = entry

                return entry



            for dim_code in sorted(all_dim_codes):

                entry = ensure_entry(dim_code)



                metrics_payload = dim_metrics.get(dim_code) or {}

                if isinstance(metrics_payload, dict):

                    avg_value = metrics_payload.get('avg')

                    if avg_value is not None:

                        entry['avg'] = avg_value

                    score_rate_value = metrics_payload.get('score_rate')

                    if score_rate_value is not None:

                        entry['score_rate'] = score_rate_value

                    max_score_value = metrics_payload.get('max_score')

                    if max_score_value is not None:

                        entry['max_score'] = max_score_value

                    student_count_value = metrics_payload.get('student_count')

                    if student_count_value:

                        try:

                            entry['student_count'] = int(student_count_value)

                        except (TypeError, ValueError):

                            entry['student_count'] = student_count_value



                if s.type == 'questionnaire':

                    if dim_code in dims_option_distribution:

                        option_dist = dims_option_distribution.get(dim_code) or []

                        if isinstance(option_dist, list):

                            option_dist = self._dedupe_option_list(option_dist)

                        entry['option_distribution'] = option_dist

                    if dim_code in dim_question_summary:

                        summary_payload = dim_question_summary.get(dim_code) or {}

                        questions_list: List[Dict[str, Any]] = []

                        if isinstance(summary_payload, dict):

                            for qid, qinfo in summary_payload.items():

                                q_entry = {

                                    'question_id': qid,

                                    'question_name': qinfo.get('question_name') or qid,

                                    'score': qinfo.get('score'),

                                    'score_rate': qinfo.get('score_rate'),

                                }

                                dist_list = []

                                question_dist = dims_questions.get(dim_code, {}).get(qid) if isinstance(dims_questions.get(dim_code), dict) else None

                                if isinstance(question_dist, list):

                                    dist_list = self._dedupe_option_list(question_dist)

                                q_entry['option_distribution'] = dist_list

                                questions_list.append(q_entry)

                        if questions_list:

                            questions_list.sort(key=lambda item: item.get('question_id'))

                            entry['questions'] = questions_list



            if dim_entries:

                subj["dimensions"] = [dim_entries[key] for key in sorted(dim_entries.keys())]



            subjects.append(round2_json(subj))

        return subjects



    def build_regional_subjects_v12(self, batch_code: str, enhanced_stats: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:



        """v1.2 版本的区域 subjects 输出，复用增强结构."""







        subjects = self.build_regional_subjects(batch_code, enhanced_stats=enhanced_stats)



        for subj in subjects:



            try:



                if subj.get("type") != "questionnaire":



                    continue



                dimensions = subj.get("dimensions")



                if isinstance(dimensions, list):



                    dimensions.sort(key=lambda item: item.get("code"))



                    for dim in dimensions:



                        rankings = dim.get("school_rankings")



                        if isinstance(rankings, list):



                            rankings.sort(key=lambda item: (item.get("rank") or float('inf'), item.get("school_id")))



            except Exception as e:



                print(f"区域 subjects v1.2 清理失败: {e}")



        return subjects









    def build_school_subjects(

        self,

        batch_code: str,

        school_code: str,

        enhanced_stats: Optional[Dict[str, Any]] = None,

        precomputed_ranks: Optional[Dict[str, Any]] = None,

        precomputed_dim_ranks: Optional[Dict[str, Dict[str, Any]]] = None,

    ) -> List[Dict[str, Any]]:

        subjects: List[Dict[str, Any]] = []

        rank_cache = precomputed_ranks or {}

        dim_cache = precomputed_dim_ranks or {}



        for s in self.list_subjects(batch_code):

            subject_enhanced_stats = None

            if enhanced_stats:

                if s.name in enhanced_stats:

                    subject_enhanced_stats = enhanced_stats[s.name]

                else:

                    academic_subjects = enhanced_stats.get('academic_subjects', {})

                    non_academic_subjects = enhanced_stats.get('non_academic_subjects', {})

                    subject_enhanced_stats = academic_subjects.get(s.name) or non_academic_subjects.get(s.name)



            metrics, grade_distribution = self._compute_subject_metrics(

                batch_code,

                s.name,

                school_code,

                enhanced_stats=subject_enhanced_stats,

            )



            region_rank = self._compute_school_region_rank(

                batch_code,

                s.name,

                school_code,

                precomputed_rank=rank_cache.get(s.name),

            )



            dims = self._compute_school_dimensions_with_rank(

                batch_code,

                s.name,

                school_code,

                enhanced_stats=subject_enhanced_stats,

                precomputed_dim_ranks=dim_cache.get(s.name),

            )

            if region_rank.get('region_rank') is not None:

                metrics['rank'] = int(region_rank['region_rank'])



            subj: Dict[str, Any] = {

                "subject_name": s.name,

                "type": s.type,

                "metrics": metrics,

            }

            if grade_distribution:

                subj["grade_distribution"] = grade_distribution

            if dims:

                subj["dimensions"] = dims



            if s.type == 'questionnaire':

                try:

                    qs_school = self._compute_questionnaire_question_option_distribution_school(

                        batch_code, s.name, school_code

                    )

                except Exception as exc:

                    print(f"瀛︽牎棰樼洰閫夐」鍒嗗竷璁＄畻澶辫触: {exc}")

                    qs_school = {}

                question_dim_map = self._get_question_dimension_map(batch_code, s.name)

                dim_question_summary = self._compute_school_dimension_question_summary(

                    batch_code, s.name, school_code

                )

                dim_question_options: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(dict)

                for qid_raw, dist in qs_school.items():

                    qid_str = str(qid_raw)

                    dim_code = question_dim_map.get(qid_str)

                    if not dim_code:

                        continue

                    dist_list = dist if isinstance(dist, list) else []

                    if isinstance(dist_list, list):

                        dist_list = self._dedupe_option_list(dist_list)

                    dim_question_options[dim_code][qid_str] = dist_list

                if isinstance(dims, list):

                    dims_by_code = {entry.get('code'): entry for entry in dims if isinstance(entry, dict) and entry.get('code')}

                    for dim_code, entry in dims_by_code.items():

                        summary_payload = dim_question_summary.get(dim_code) or {}

                        option_payload = dim_question_options.get(dim_code, {})

                        question_ids = sorted(set(summary_payload.keys()) | set(option_payload.keys()))

                        if not question_ids:

                            continue

                        questions_list: List[Dict[str, Any]] = []

                        for qid in question_ids:

                            qinfo = summary_payload.get(qid, {})

                            q_entry = {

                                'question_id': qid,

                                'question_name': qinfo.get('question_name') or qid,

                                'score': qinfo.get('score'),

                                'score_rate': qinfo.get('score_rate'),

                                'option_distribution': self._dedupe_option_list(option_payload.get(qid, []) if isinstance(option_payload.get(qid, []), list) else []),

                            }

                            questions_list.append(q_entry)

                        if questions_list:

                            entry['questions'] = questions_list



            subjects.append(round2_json(subj))

        return subjects

    def build_school_subjects_v12(

        self,

        batch_code: str,

        school_code: str,

        enhanced_stats: Optional[Dict[str, Any]] = None,

        precomputed_ranks: Optional[Dict[str, Any]] = None,

        precomputed_dim_ranks: Optional[Dict[str, Dict[str, Any]]] = None,

    ) -> List[Dict[str, Any]]:

        """v1.2 version of school-level subjects builder."""

        subjects = self.build_school_subjects(

            batch_code,

            school_code,

            enhanced_stats=enhanced_stats,

            precomputed_ranks=precomputed_ranks,

            precomputed_dim_ranks=precomputed_dim_ranks,

        )

        for subj in subjects:

            try:

                if isinstance(subj.get("dimensions"), list):

                    for d in subj["dimensions"]:

                        d.pop("regional_avg", None)



            except Exception as e:

                print(f"School subjects v1.2 cleanup failed: {e}")

        return subjects

    # --- Internals ---



    







    def _compute_subject_metrics(

        self,

        batch_code: str,

        subject_name: str,

        school_code: Optional[str] = None,

        enhanced_stats: Optional[Dict[str, Any]] = None,

    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:

        """Load subject-level metrics and return (metrics, grade_distribution)."""

        cache_key: Tuple[str, ...]

        cache_store: Dict[Tuple[str, ...], Dict[str, Any]]

        if school_code:

            cache_key = (batch_code, subject_name, school_code)

            cache_store = self._school_metric_cache

        else:

            cache_key = (batch_code, subject_name)

            cache_store = self._subject_metric_cache



        cached = cache_store.get(cache_key)

        if cached is None:

            try:

                with get_db_context() as db:

                    repo = PrecomputedMetricsRepository(db)

                    if school_code:

                        record = repo.get_subject_school_metric(batch_code, subject_name, school_code)

                    else:

                        record = repo.get_subject_metric(batch_code, subject_name)

            except DataIntegrityError as exc:

                scope = f"{batch_code}/{subject_name}"

                if school_code:

                    scope = f"{scope}/{school_code}"

                raise ValueError(f"Precomputed metrics missing for {scope}") from exc



            subject_type = (getattr(record, 'subject_type', None) or 'exam').lower()



            avg = float(getattr(record, 'avg_score', 0) or 0)

            stddev = float(getattr(record, 'std_score', 0) or 0)

            max_v = float(getattr(record, 'max_score_achieved', 0) or 0)

            min_v = float(getattr(record, 'min_score', 0) or 0)

            full_score = float(getattr(record, 'max_score', 0) or 0)

            student_count = int(getattr(record, 'student_count', 0) or 0)

            score_rate_value = getattr(record, 'score_rate', None)

            difficulty_value = getattr(record, 'difficulty_coefficient', None)



            base_metrics: Dict[str, Any] = {

                "avg": round2(avg),

                "stddev": round2(stddev),

                "max": round2(max_v),

                "min": round2(min_v),

                "subject_full_score": round2(full_score),

                "student_count": student_count,

                "score_rate": round2(score_rate_value or ((avg / full_score) * 100.0 if full_score else 0.0)),

            }



            if subject_type != 'questionnaire':

                difficulty = difficulty_value

                if difficulty is None and full_score:

                    difficulty = (avg / full_score) if full_score else None

                if difficulty is not None:

                    base_metrics['difficulty'] = round2(difficulty)



            cache_payload = dict(base_metrics)

            cache_payload['__subject_type'] = subject_type

            cache_store[cache_key] = cache_payload

        else:

            cache_payload = dict(cached)

            subject_type = str(cache_payload.pop('__subject_type', 'exam'))

            base_metrics = cache_payload



        metrics = dict(base_metrics)

        grade_distribution: Optional[Dict[str, Any]] = None



        if enhanced_stats:

            percentiles = enhanced_stats.get('percentiles') or {}

            if isinstance(percentiles, dict) and percentiles:

                pct_struct = {

                    'P10': round2(percentiles.get('P10', 0)),

                    'P50': round2(percentiles.get('P50', 0)),

                    'P90': round2(percentiles.get('P90', 0)),

                }

                metrics['percentiles'] = pct_struct

                metrics['p10'] = pct_struct['P10']

                metrics['p50'] = pct_struct['P50']

                metrics['p90'] = pct_struct['P90']



            statistical_indicators = enhanced_stats.get('statistical_indicators') or {}

            discr = statistical_indicators.get('discrimination_index')

            if discr is None:

                discr_payload = enhanced_stats.get('discrimination')

                if isinstance(discr_payload, dict):

                    discr = discr_payload.get('discrimination_index') or discr_payload.get('value')

                elif discr_payload is not None:

                    discr = discr_payload

            if discr is not None:

                metrics['discrimination'] = round2(discr)



            grade_payload = enhanced_stats.get('grade_distribution')

            if isinstance(grade_payload, dict):

                simplified = self._simplify_grade_distribution(grade_payload)

                if simplified:

                    grade_distribution = simplified

                    percentages = simplified.get('percentages') or {}

                    mapping = {

                        'fail': ('rate_fail', 'fail_rate'),

                        'pass': ('rate_pass', 'pass_rate'),

                        'good': ('rate_good', 'good_rate'),

                        'excellent': ('rate_excellent', 'excellent_rate'),

                    }

                    for grade_key, (enhanced_key, legacy_key) in mapping.items():

                        value = percentages.get(grade_key)

                        if value is None:

                            continue

                        rounded = round2(value)

                        metrics[enhanced_key] = rounded

                        metrics[legacy_key] = rounded



        if subject_type != 'questionnaire' and 'difficulty' not in metrics:

            avg_val = metrics.get('avg')

            full_score_val = metrics.get('subject_full_score')

            try:

                if avg_val is not None and full_score_val:

                    metrics['difficulty'] = round2(float(avg_val) / float(full_score_val))

            except Exception:

                pass



        if 'score_rate' not in metrics:

            avg_val = metrics.get('avg')

            full_score_val = metrics.get('subject_full_score')

            try:

                metrics['score_rate'] = round2((float(avg_val) / float(full_score_val)) * 100.0 if full_score_val else 0.0)

            except Exception:

                metrics['score_rate'] = round2(0.0)



        metrics.setdefault('student_count', base_metrics.get('student_count', 0))

        metrics.setdefault('subject_full_score', base_metrics.get('subject_full_score', 0))



        if subject_type != 'questionnaire':

            fallback_rates = {

                'fail': getattr(record, 'fail_rate', None),

                'pass': getattr(record, 'pass_rate', None),

                'good': getattr(record, 'good_rate', None),

                'excellent': getattr(record, 'excellent_rate', None),

            }

            if any(v is not None for v in fallback_rates.values()):

                for grade_key, rate_val in fallback_rates.items():

                    if rate_val is None:

                        continue

                    try:

                        numeric = float(rate_val)

                    except (TypeError, ValueError):

                        continue

                    if numeric > 1.0:

                        numeric = numeric / 100.0

                    rounded = round2(numeric)

                    metrics.setdefault(f'rate_{grade_key}', rounded)

                    metrics.setdefault(f'{grade_key}_rate', rounded)

                if grade_distribution is None:

                    student_total = metrics.get('student_count', student_count) or 0

                    counts = {}

                    percentages = {}

                    for grade_key, rate_val in fallback_rates.items():

                        rate_val = float(rate_val or 0)

                        if rate_val > 1.0:

                            rate_val = rate_val / 100.0

                        percentages[grade_key] = round2(rate_val)

                        counts[grade_key] = round2(student_total * rate_val) if student_total else 0

                    grade_distribution = {

                        'counts': counts,

                        'percentages': percentages,

                    }



        return metrics, grade_distribution



    def _compute_school_rankings(self, batch_code: str, subject_name: str) -> List[Dict[str, Any]]:

        try:

            with get_db_context() as db:

                repo = PrecomputedMetricsRepository(db)

                rows = repo.list_subject_school_rankings(batch_code, subject_name)

        except DataIntegrityError as exc:

            raise ValueError(

                f"Precomputed school rankings missing for {batch_code}/{subject_name}"

            ) from exc



        enriched: List[Dict[str, Any]] = []

        for row in rows:

            entry: Dict[str, Any] = {

                "school_id": row.school_code,

                "school_name": row.school_name,

                "avg": round2(getattr(row, 'avg_score', 0) or 0),

                "score_rate": round2(getattr(row, 'score_rate', 0) or 0),

                "rank": int(getattr(row, 'rank', 0) or 0),

            }

            total_schools = getattr(row, 'total_schools', None)

            if total_schools is not None:

                try:

                    entry['total_schools'] = int(total_schools)

                except Exception:

                    pass

            student_count = getattr(row, 'student_count', None)

            if student_count is not None:

                try:

                    entry['student_count'] = int(student_count)

                except Exception:

                    pass

            enriched.append(entry)

        return enriched









    def _get_total_active_schools(self, batch_code: str, subject_name: str) -> int:

        key = (batch_code, subject_name)

        if key in self._total_school_cache:

            return self._total_school_cache[key]



        try:

            with get_db_context() as db:

                repo = PrecomputedMetricsRepository(db)

                total = repo.get_total_active_schools(batch_code, subject_name)

        except DataIntegrityError as exc:

            raise ValueError(

                f"Total school count missing for {batch_code}/{subject_name}"

            ) from exc



        total_int = int(total)

        self._total_school_cache[key] = total_int

        return total_int



    def _get_school_name_map(self, batch_code: str) -> Dict[str, str]:

        if batch_code in self._school_name_cache:

            return self._school_name_cache[batch_code]



        try:

            with get_db_context() as db:

                rows = db.execute(

                    text(

                        """

                        SELECT school_id, COALESCE(standard_school_name, school_id) AS school_name

                        FROM school_master_data

                        WHERE batch_code=:batch AND status='ACTIVE'

                        """

                    ),

                    {"batch": batch_code},

                ).fetchall()

        except Exception:

            rows = []



        mapping = {}

        for row in rows:

            school_id = str(row[0]) if row and row[0] is not None else None

            if not school_id:

                continue

            school_name = row[1]

            if isinstance(school_name, bytes):

                try:

                    school_name = school_name.decode('utf-8')

                except Exception:

                    school_name = school_name.decode(errors='ignore')

            mapping[school_id] = str(school_name) if school_name is not None else school_id



        self._school_name_cache[batch_code] = mapping

        return mapping

    def _compute_school_region_rank(

        self,

        batch_code: str,

        subject_name: str,

        school_code: str,

        precomputed_rank: Optional[Dict[str, Any]] = None,

    ) -> Dict[str, Any]:

        if isinstance(precomputed_rank, dict):

            rank_value = precomputed_rank.get('rank')

            total_value = precomputed_rank.get('total_schools')

            try:

                rank_int = int(rank_value) if rank_value is not None else None

            except (TypeError, ValueError):

                rank_int = None

            if total_value is None:

                total_int = self._get_total_active_schools(batch_code, subject_name)

            else:

                try:

                    total_int = int(total_value)

                except (TypeError, ValueError):

                    total_int = self._get_total_active_schools(batch_code, subject_name)

            return {"region_rank": rank_int, "total_schools": total_int}



        sql = text(

            """

            WITH ranks AS (

              SELECT scs.school_code,

                     DENSE_RANK() OVER (ORDER BY AVG(scs.total_score) DESC, scs.school_code ASC) AS r

              FROM student_cleaned_scores scs

              JOIN school_master_data smd 

                ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code

               AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code

               AND smd.status = 'ACTIVE'

              WHERE scs.batch_code = :batch AND scs.subject_name = :subject

                AND LOWER(scs.subject_type) IN ('exam','questionnaire')

              GROUP BY scs.school_code

            )

            SELECT r AS region_rank,

                   (SELECT COUNT(DISTINCT scs2.school_code)

                      FROM student_cleaned_scores scs2

                      JOIN school_master_data smd2 

                        ON smd2.batch_code COLLATE utf8mb4_unicode_ci = scs2.batch_code COLLATE utf8mb4_unicode_ci

                       AND smd2.school_id COLLATE utf8mb4_unicode_ci = scs2.school_code COLLATE utf8mb4_unicode_ci

                       AND smd2.status = 'ACTIVE'

                      WHERE scs2.batch_code = :batch AND scs2.subject_name = :subject

                        AND LOWER(scs2.subject_type) IN ('exam','questionnaire')) AS total_schools

            FROM ranks WHERE school_code = :school

            """

        )

        with get_db_context() as db:

            row = db.execute(sql, {"batch": batch_code, "subject": subject_name, "school": school_code}).fetchone()

        if not row:

            total = self._get_total_active_schools(batch_code, subject_name)

            return {"region_rank": None, "total_schools": total}



        region_rank = int(row[0]) if row[0] is not None else None

        total_schools_val = row[1]

        if total_schools_val is not None:

            try:

                total_schools = int(total_schools_val)

            except (TypeError, ValueError):

                total_schools = self._get_total_active_schools(batch_code, subject_name)

        else:

            total_schools = self._get_total_active_schools(batch_code, subject_name)

        self._total_school_cache[(batch_code, subject_name)] = total_schools



        return {"region_rank": region_rank, "total_schools": total_schools}



    def _discover_dimension_codes(self, batch_code: str, subject_name: str) -> List[str]:

        """???????? JSON ???????????"""

        sql = text(

            """

            SELECT dimension_scores

            FROM student_cleaned_scores

            WHERE batch_code=:batch

              AND subject_name=:subject

              AND LOWER(subject_type) IN ('exam','questionnaire')

              AND dimension_scores IS NOT NULL

              AND dimension_scores != ''

            LIMIT 500

            """

        )

        codes: Dict[str, int] = {}

        with get_db_context() as db:

            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall()

        for (payload,) in rows:

            if payload is None:

                continue

            raw = payload

            if isinstance(raw, (bytes, bytearray)):

                try:

                    raw = raw.decode('utf-8')

                except Exception:

                    raw = raw.decode('utf-8', errors='ignore')

            if isinstance(raw, str):

                try:

                    parsed = json.loads(raw)

                except Exception:

                    continue

            elif isinstance(raw, dict):

                parsed = raw

            else:

                continue

            if isinstance(parsed, dict):

                for key in parsed.keys():

                    codes[str(key)] = 1

        return sorted(codes.keys())



    def _compute_school_dimensions_with_rank(

            self,

            batch_code: str,

            subject_name: str,

            school_code: str,

            enhanced_stats: Optional[Dict[str, Any]] = None,

            precomputed_dim_ranks: Optional[Dict[str, Any]] = None,

        ) -> List[Dict[str, Any]]:

            dims_out: List[Dict[str, Any]] = []

            dim_cache = precomputed_dim_ranks or {}



            dimension_keys: set[str] = set()

            if isinstance(dim_cache, dict):

                dimension_keys.update(str(key) for key in dim_cache.keys())

            if isinstance(enhanced_stats, dict):

                dims_stats_map = enhanced_stats.get('dimensions', {})

                if isinstance(dims_stats_map, dict):

                    dimension_keys.update(str(key) for key in dims_stats_map.keys())

            else:

                dims_stats_map = {}

            if not dimension_keys:

                discovered = self._discover_dimension_codes(batch_code, subject_name)

                dimension_keys.update(str(key) for key in discovered)

            if not dimension_keys:

                return dims_out



            with get_db_context() as db:

                dimension_name_mapping = self._batch_load_dimension_names(db, batch_code, subject_name)

                max_score_map: Dict[str, float] = {}

                try:

                    sql_bdd = text(

                        """

                        SELECT dimension_code, dimension_max_score

                        FROM batch_dimension_definition

                        WHERE batch_code=:batch AND subject_name=:subject AND dimension_max_score IS NOT NULL

                        """

                    )

                    rows_bdd = db.execute(sql_bdd, {"batch": batch_code, "subject": subject_name}).fetchall()

                    for dc, ms in rows_bdd:

                        try:

                            max_score_map[str(dc)] = float(ms)

                        except Exception:

                            continue

                except Exception:

                    pass



                for dim in sorted(dimension_keys):

                    dim_str = str(dim)

                    cache_entry = dim_cache.get(dim_str) if isinstance(dim_cache, dict) else None

                    dim_avg: Optional[float] = None

                    dim_rank: Optional[int] = None

                    max_score: Optional[float] = max_score_map.get(dim_str)

                    score_rate: Optional[float] = None



                    if isinstance(cache_entry, dict):

                        self._record_cache_hit()

                        cached_avg = cache_entry.get('avg')

                        cached_rank = cache_entry.get('rank')

                        cached_max = cache_entry.get('max_score')

                        cached_rate = cache_entry.get('score_rate')

                        try:

                            dim_avg = float(cached_avg) if cached_avg is not None else None

                        except (TypeError, ValueError):

                            dim_avg = None

                        try:

                            dim_rank = int(cached_rank) if cached_rank is not None else None

                        except (TypeError, ValueError):

                            dim_rank = None

                        try:

                            if cached_max is not None:

                                max_score = float(cached_max)

                        except (TypeError, ValueError):

                            pass

                        try:

                            if cached_rate is not None:

                                score_rate = round2(cached_rate)

                        except Exception:

                            score_rate = None

                        if dim_avg is None or dim_rank is None:

                            self._record_cache_fallback()

                    else:

                        self._record_cache_miss()



                    if dim_avg is None or dim_rank is None:

                        dim_avg, dim_rank, max_score = self._compute_dimension_stats_from_db(

                            db,

                            batch_code,

                            subject_name,

                            school_code,

                            dim_str,

                            max_score_map,

                        )

                        if dim_avg is None and dim_rank is None:

                            continue

                        score_rate = round2((dim_avg / max_score * 100.0) if (dim_avg is not None and max_score) else None)

                    elif score_rate is None and max_score:

                        score_rate = round2((dim_avg / max_score * 100.0) if dim_avg is not None else None)



                    dimension_name = dimension_name_mapping.get(dim_str, dim_str)

                    dim_result: Dict[str, Any] = {

                        "code": dim_str,

                        "name": dimension_name,

                        "avg": round2(dim_avg) if dim_avg is not None else None,

                        "score_rate": score_rate,

                        "rank": dim_rank,

                    }



                    dim_enhanced = {}

                    if isinstance(dims_stats_map, dict):

                        dim_enhanced = dims_stats_map.get(dim_str, {}) or {}

                    if isinstance(dim_enhanced, dict) and dim_enhanced:

                        indicators = dim_enhanced.get('statistical_indicators')

                        if isinstance(indicators, dict):

                            if 'difficulty_coefficient' in indicators:

                                dim_result['difficulty'] = round2(indicators.get('difficulty_coefficient'))

                            if 'discrimination_index' in indicators:

                                dim_result['discrimination'] = round2(indicators.get('discrimination_index'))

                        percentiles = dim_enhanced.get('percentiles')

                        if isinstance(percentiles, dict):

                            dim_result['percentiles'] = {

                                'P10': round2(percentiles.get('P10', 0)),

                                'P50': round2(percentiles.get('P50', 0)),

                                'P90': round2(percentiles.get('P90', 0)),

                            }



                    dims_out.append(dim_result)

            return dims_out



    def _compute_dimension_stats_from_db(

        self,

        db,

        batch_code: str,

        subject_name: str,

        school_code: str,

        dimension_code: str,

        max_score_map: Dict[str, float],

        ) -> Tuple[Optional[float], Optional[int], Optional[float]]:

        sql_rank = text(

            f"""

            WITH per_school AS (

              SELECT scs.school_code,

                     ROUND(AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(CAST(scs.dimension_scores AS JSON), '$."{dimension_code}".score')) AS DECIMAL(10,4))), 2) AS dim_avg

              FROM student_cleaned_scores scs

              JOIN school_master_data smd 

                ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code

               AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code

               AND smd.status = 'ACTIVE'

              WHERE scs.batch_code=:batch AND scs.subject_name=:subject

                AND LOWER(scs.subject_type) IN ('exam','questionnaire')

                AND JSON_EXTRACT(CAST(scs.dimension_scores AS JSON), '$."{dimension_code}".score') IS NOT NULL

              GROUP BY scs.school_code

            ), ranked AS (

              SELECT school_code, dim_avg,

                     DENSE_RANK() OVER (ORDER BY dim_avg DESC, school_code ASC) AS rnk

              FROM per_school

            )

            SELECT dim_avg, rnk FROM ranked WHERE school_code=:school

            """

        )

        row = db.execute(sql_rank, {"batch": batch_code, "subject": subject_name, "school": school_code}).fetchone()

        if row:

            dim_avg = float(row[0]) if row[0] is not None else None

            dim_rank = int(row[1]) if row[1] is not None else None

        else:

            dim_avg = None

            dim_rank = None

    

        if dim_avg is None:

            try:

                sql_rank_q = text(

                    """

                    WITH per_school AS (

                      SELECT qqs.school_id AS school_code,

                             ROUND(AVG(qqs.original_score), 2) AS dim_avg

                      FROM questionnaire_question_scores qqs

                      JOIN question_dimension_mapping qdm

                        ON qdm.batch_code COLLATE utf8mb4_unicode_ci = qqs.batch_code COLLATE utf8mb4_unicode_ci

                       AND qdm.question_id COLLATE utf8mb4_unicode_ci = qqs.question_id COLLATE utf8mb4_unicode_ci

                      JOIN school_master_data smd 

                        ON smd.batch_code COLLATE utf8mb4_unicode_ci = qqs.batch_code COLLATE utf8mb4_unicode_ci

                       AND smd.school_id COLLATE utf8mb4_unicode_ci = qqs.school_id COLLATE utf8mb4_unicode_ci

                       AND smd.status = 'ACTIVE'

                      WHERE qqs.batch_code COLLATE utf8mb4_unicode_ci = CAST(:batch AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                        AND qqs.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                        AND qdm.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                        AND qdm.dimension_code COLLATE utf8mb4_unicode_ci = CAST(:dim AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                      GROUP BY qqs.school_id

                    ), ranked AS (

                      SELECT school_code, dim_avg,

                             DENSE_RANK() OVER (ORDER BY dim_avg DESC, school_code ASC) AS rnk

                      FROM per_school

                    )

                    SELECT dim_avg, rnk FROM ranked WHERE school_code=:school

                    """

                )

                row_q = db.execute(sql_rank_q, {"batch": batch_code, "subject": subject_name, "school": school_code, "dim": dimension_code}).fetchone()

                if row_q:

                    dim_avg = float(row_q[0]) if row_q[0] is not None else None

                    dim_rank = int(row_q[1]) if row_q[1] is not None else None

            except Exception:

                pass

    

        max_score = max_score_map.get(dimension_code)

        if max_score is None or max_score == 0.0:

            sql_max = text(

                f"""

                SELECT ROUND(AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(CAST(scs.dimension_max_scores AS JSON), '$."{dimension_code}".max_score')) AS DECIMAL(10,4))), 2) AS max_score

                FROM student_cleaned_scores scs

                JOIN school_master_data smd 

                  ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code

                 AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code

                 AND smd.status = 'ACTIVE'

                WHERE scs.batch_code=:batch AND scs.subject_name=:subject

                  AND LOWER(scs.subject_type) IN ('exam','questionnaire')

                  AND JSON_EXTRACT(CAST(scs.dimension_max_scores AS JSON), '$."{dimension_code}".max_score') IS NOT NULL

                """

            )

            max_row = db.execute(sql_max, {"batch": batch_code, "subject": subject_name}).fetchone()

            if max_row and max_row[0] is not None:

                try:

                    max_score = float(max_row[0])

                except (TypeError, ValueError):

                    max_score = None

    

        if (max_score is None or max_score == 0.0) and dimension_code in max_score_map:

            max_score = max_score_map[dimension_code]

    

        if max_score is None or max_score == 0.0:

            try:

                sql_max2 = text(

                    """

                    SELECT SUM(sqc.max_score) AS dim_max

                    FROM subject_question_config sqc

                    JOIN question_dimension_mapping qdm

                      ON qdm.batch_code = sqc.batch_code

                     AND qdm.subject_name = sqc.subject_name

                     AND qdm.question_id = sqc.question_id

                    WHERE sqc.batch_code=:batch AND sqc.subject_name=:subject AND qdm.dimension_code=:dim

                    """

                )

                max_row2 = db.execute(sql_max2, {"batch": batch_code, "subject": subject_name, "dim": dimension_code}).fetchone()

                if max_row2 and max_row2[0] is not None:

                    try:

                        max_score = float(max_row2[0])

                    except (TypeError, ValueError):

                        max_score = None

            except Exception:

                pass

    

        return dim_avg, dim_rank, max_score

    def _compute_questionnaire_dimension_question_option_distribution(

        self, batch_code: str, subject_name: str

    ) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:

        """返回维度>题目>选项分布，用于区域层问卷输出。"""



        subject_label_map = self._get_scale_label_map(batch_code, subject_name) or {}

        question_label_maps = self._get_question_option_label_maps(batch_code, subject_name)



        sql = text(

            """

            SELECT qdm.dimension_code,

                   qqd.question_id,

                   qqd.option_level,

                   ROUND(

                     SUM(qqd.count) * 100.0 /

                     NULLIF(
                       SUM(SUM(qqd.count)) OVER (
                         PARTITION BY qdm.dimension_code, qqd.question_id
                       ),
                       0
                     ),

                     2

                   ) AS pct

            FROM questionnaire_option_distribution qqd

            JOIN question_dimension_mapping qdm

              ON qdm.batch_code COLLATE utf8mb4_unicode_ci = qqd.batch_code COLLATE utf8mb4_unicode_ci

             AND qdm.question_id COLLATE utf8mb4_unicode_ci = qqd.question_id COLLATE utf8mb4_unicode_ci

            WHERE qqd.batch_code COLLATE utf8mb4_unicode_ci = CAST(:batch AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

              AND qqd.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

              AND qdm.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

            GROUP BY qdm.dimension_code, qqd.question_id, qqd.option_level

            ORDER BY qdm.dimension_code, qqd.question_id, qqd.option_level

            """

        )



        out: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

        with get_db_context() as db:

            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall()



        for dim, question_id, level_value, pct in rows:

            dim_code = str(dim) if dim is not None else None

            if not dim_code:

                continue

            qid_str = str(question_id)

            level = int(level_value)

            question_map = question_label_maps.get(qid_str, {}) if question_label_maps else {}

            label = (

                question_map.get(level)

                if isinstance(question_map, dict)

                else None

            ) or subject_label_map.get(level) or f"选项{level}"



            out.setdefault(dim_code, {}).setdefault(qid_str, []).append(

                {

                    "option_level": level,

                    "option_label": label,

                    "pct": float(pct) if pct is not None else None,

                }

            )



        return out



    def _compute_questionnaire_dimension_option_distribution(

        self, batch_code: str, subject_name: str

    ) -> Dict[str, List[Dict[str, Any]]]:

        """返回维度级选项分布，用于区域层维度统计。"""



        label_map = self._get_scale_label_map(batch_code, subject_name) or {}



        sql = text(

            """

            SELECT qdm.dimension_code,

                   qqd.option_level,

                   ROUND(

                     SUM(qqd.count) * 100.0 /

                     NULLIF(SUM(SUM(qqd.count)) OVER (PARTITION BY qdm.dimension_code), 0),

                     2

                   ) AS pct

            FROM questionnaire_option_distribution qqd

            JOIN question_dimension_mapping qdm

              ON qdm.batch_code COLLATE utf8mb4_unicode_ci = qqd.batch_code COLLATE utf8mb4_unicode_ci

             AND qdm.question_id COLLATE utf8mb4_unicode_ci = qqd.question_id COLLATE utf8mb4_unicode_ci

            WHERE qqd.batch_code COLLATE utf8mb4_unicode_ci = CAST(:batch AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

              AND qqd.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

              AND qdm.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

            GROUP BY qdm.dimension_code, qqd.option_level

            ORDER BY qdm.dimension_code, qqd.option_level

            """

        )



        out: Dict[str, List[Dict[str, Any]]] = {}

        with get_db_context() as db:

            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall()



        for dim, level_value, pct in rows:

            dim_code = str(dim) if dim is not None else None

            if not dim_code:

                continue

            level = int(level_value)

            label = label_map.get(level) or f"选项{level}"

            out.setdefault(dim_code, []).append(

                {

                    "option_level": level,

                    "option_label": label,

                    "pct": float(pct) if pct is not None else None,

                }

            )



        return out



    def _get_question_dimension_map(

        self, batch_code: str, subject_name: str

    ) -> Dict[str, str]:

        """返回题目 -> 维度编码映射。"""



        sql = text(

            """

            SELECT question_id, dimension_code

            FROM question_dimension_mapping

            WHERE batch_code=:batch AND subject_name=:subject

            """

        )

        mapping: Dict[str, str] = {}

        with get_db_context() as db:

            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall()

        for qid, dim_code in rows:

            if qid is None or dim_code is None:

                continue

            mapping[str(qid)] = str(dim_code)

        return mapping



    def _compute_questionnaire_dimension_question_summary(

        self, batch_code: str, subject_name: str

    ) -> Dict[str, Dict[str, Any]]:

        """返回维度->题目摘要（题目名称、得分、得分率），供区域维度展示使用。"""



        sql = text(

            """

            SELECT qdm.dimension_code,

                   qqs.question_id,

                   ROUND(AVG(qqs.original_score), 2) AS avg_score,

                   MAX(qqs.max_score) AS max_score,

                   MAX(COALESCE(sqc.question_no, qqs.question_id)) AS question_name

            FROM questionnaire_question_scores qqs

            JOIN question_dimension_mapping qdm

              ON qdm.batch_code COLLATE utf8mb4_unicode_ci = qqs.batch_code COLLATE utf8mb4_unicode_ci

             AND qdm.subject_name COLLATE utf8mb4_unicode_ci = qqs.subject_name COLLATE utf8mb4_unicode_ci

             AND qdm.question_id COLLATE utf8mb4_unicode_ci = qqs.question_id COLLATE utf8mb4_unicode_ci

            LEFT JOIN subject_question_config sqc

              ON sqc.batch_code COLLATE utf8mb4_unicode_ci = qqs.batch_code COLLATE utf8mb4_unicode_ci

             AND sqc.subject_name COLLATE utf8mb4_unicode_ci = qqs.subject_name COLLATE utf8mb4_unicode_ci

             AND sqc.question_id COLLATE utf8mb4_unicode_ci = qqs.question_id COLLATE utf8mb4_unicode_ci

            WHERE qqs.batch_code COLLATE utf8mb4_unicode_ci = CAST(:batch AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

              AND qqs.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

              AND qdm.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

            GROUP BY qdm.dimension_code, qqs.question_id

            ORDER BY qdm.dimension_code, qqs.question_id

            """

        )



        results: Dict[str, Dict[str, Any]] = {}

        with get_db_context() as db:

            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall()



        for dim, qid, avg_score, max_score, question_label in rows:

            dim_code = str(dim) if dim is not None else None

            if not dim_code:

                continue

            qid_str = str(qid)

            entry = results.setdefault(dim_code, {})

            score_rate = None

            try:

                max_score_val = float(max_score) if max_score is not None else 0.0

                avg_score_val = float(avg_score) if avg_score is not None else 0.0

                if max_score_val > 0:

                    score_rate = round2((avg_score_val / max_score_val) * 100.0)

            except Exception:

                score_rate = None

            display_name = self._normalize_option_label(question_label) or qid_str

            entry[qid_str] = {

                'question_name': display_name,

                'score': round2(avg_score) if avg_score is not None else None,

                'score_rate': score_rate,

            }



        return results



    def _compute_school_dimension_question_summary(

        self, batch_code: str, subject_name: str, school_code: str

    ) -> Dict[str, Dict[str, Any]]:

        """返回指定学校的维度 -> 题目摘要。"""



        sql = text(

            """

            SELECT qdm.dimension_code,

                   qqs.question_id,

                   ROUND(AVG(qqs.original_score), 2) AS avg_score,

                   MAX(qqs.max_score) AS max_score,

                   MAX(COALESCE(sqc.question_no, qqs.question_id)) AS question_name

            FROM questionnaire_question_scores qqs

            JOIN question_dimension_mapping qdm

              ON qdm.batch_code COLLATE utf8mb4_unicode_ci = qqs.batch_code COLLATE utf8mb4_unicode_ci

             AND qdm.subject_name COLLATE utf8mb4_unicode_ci = qqs.subject_name COLLATE utf8mb4_unicode_ci

             AND qdm.question_id COLLATE utf8mb4_unicode_ci = qqs.question_id COLLATE utf8mb4_unicode_ci

            LEFT JOIN subject_question_config sqc

              ON sqc.batch_code COLLATE utf8mb4_unicode_ci = qqs.batch_code COLLATE utf8mb4_unicode_ci

             AND sqc.subject_name COLLATE utf8mb4_unicode_ci = qqs.subject_name COLLATE utf8mb4_unicode_ci

             AND sqc.question_id COLLATE utf8mb4_unicode_ci = qqs.question_id COLLATE utf8mb4_unicode_ci

            WHERE qqs.batch_code COLLATE utf8mb4_unicode_ci = CAST(:batch AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

              AND qqs.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

              AND qqs.school_id COLLATE utf8mb4_unicode_ci = CAST(:school AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

              AND qdm.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

            GROUP BY qdm.dimension_code, qqs.question_id

            ORDER BY qdm.dimension_code, qqs.question_id

            """

        )



        results: Dict[str, Dict[str, Any]] = {}

        with get_db_context() as db:

            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name, "school": school_code}).fetchall()



        for dim, qid, avg_score, max_score, question_label in rows:

            dim_code = str(dim) if dim is not None else None

            if not dim_code:

                continue

            qid_str = str(qid)

            entry = results.setdefault(dim_code, {})

            score_rate = None

            try:

                max_score_val = float(max_score) if max_score is not None else 0.0

                avg_score_val = float(avg_score) if avg_score is not None else 0.0

                if max_score_val > 0:

                    score_rate = round2((avg_score_val / max_score_val) * 100.0)

            except Exception:

                score_rate = None

            display_name = self._normalize_option_label(question_label) or qid_str

            entry[qid_str] = {

                'question_name': display_name,

                'score': round2(avg_score) if avg_score is not None else None,

                'score_rate': score_rate,

            }



        return results



    def _get_question_option_label_maps(self, batch_code: str, subject_name: str) -> Dict[str, Dict[int, str]]:

        sql = text(

            """

            SELECT question_id, instrument_type, scale_level, MAX(COALESCE(is_reverse, 0)) AS is_reverse, COUNT(*) AS cnt

            FROM questionnaire_question_scores

            WHERE batch_code=:batch AND subject_name=:subject

              AND instrument_type IS NOT NULL AND scale_level IS NOT NULL

            GROUP BY question_id, instrument_type, scale_level

            ORDER BY question_id, cnt DESC

            """

        )

        with get_db_context() as db:

            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall()

            main_scale: Dict[str, Dict[str, Any]] = {}

            for qid, inst, scale, rev, cnt in rows:

                qid_str = str(qid)

                if qid_str not in main_scale:

                    main_scale[qid_str] = {"instrument_type": inst, "scale_level": scale, "is_reverse": int(rev or 0)}



            result: Dict[str, Dict[int, str]] = {}

            for qid, meta in main_scale.items():

                inst = meta.get("instrument_type")

                scale = meta.get("scale_level")

                reverse_flag = meta.get("is_reverse")

                opts = self._fetch_scale_option_labels(db, inst, scale, reverse_flag)

                normalized_map: Dict[int, str] = {}

                expected_levels = {

                    int(lvl)

                    for (lvl, _lbl) in (opts or [])

                    if lvl is not None

                }

                for lvl, lbl in (opts or []):

                    label_text = self._normalize_option_label(lbl)

                    if not label_text:

                        continue

                    normalized_map[int(lvl)] = label_text



                if not normalized_map or set(normalized_map.keys()) != expected_levels:

                    guess_sql = text(

                        """

                        SELECT option_level, option_label, COUNT(*) AS cnt

                        FROM questionnaire_question_scores

                        WHERE batch_code=:batch AND subject_name=:subject AND question_id=:qid AND option_label IS NOT NULL

                        GROUP BY option_level, option_label

                        ORDER BY option_level, cnt DESC

                        """

                    )

                    rows_guess = db.execute(guess_sql, {"batch": batch_code, "subject": subject_name, "qid": qid}).fetchall()

                    best: Dict[int, tuple] = {}

                    for lvl, label, cnt in rows_guess:

                        normalized = self._normalize_option_label(label)

                        if not normalized:

                            continue

                        lvl_int = int(lvl)

                        cnt_int = int(cnt or 0)

                        if lvl_int not in best or cnt_int > best[lvl_int][1]:

                            best[lvl_int] = (normalized, cnt_int)

                    for lvl_int, (label_text, _cnt) in best.items():

                        if lvl_int not in normalized_map:

                            normalized_map[lvl_int] = label_text



                if normalized_map:

                    result[qid] = normalized_map



        return result



    def _compute_questionnaire_question_option_distribution(self, batch_code: str, subject_name: str) -> Dict[str, List[Dict[str, Any]]]:

        subject_label_map = self._get_scale_label_map(batch_code, subject_name)

        question_label_maps = self._get_question_option_label_maps(batch_code, subject_name)

        sql = text(

            """

            SELECT agg.question_id,

                   agg.option_level,

                   ROUND(

                     agg.total_count * 100.0 /

                     NULLIF(SUM(agg.total_count) OVER (PARTITION BY agg.question_id), 0),

                     2

                   ) AS pct

            FROM (

              SELECT question_id,

                     option_level,

                     SUM(count) AS total_count

              FROM questionnaire_option_distribution

              WHERE batch_code COLLATE utf8mb4_unicode_ci = CAST(:batch AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                AND subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

              GROUP BY question_id, option_level

            ) AS agg

            ORDER BY agg.question_id, agg.option_level

            """

        )

        out: Dict[str, List[Dict[str, Any]]] = {}

        with get_db_context() as db:

            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall()

        subject_labels = subject_label_map if isinstance(subject_label_map, dict) else {}

        for qid, level_val, pct in rows:

            qid_str = str(qid)

            lvl = int(level_val)

            question_map = question_label_maps.get(qid_str, {}) if isinstance(question_label_maps, dict) else {}

            label = question_map.get(lvl) or subject_labels.get(lvl)

            if label is None:

                label = f"选项{lvl}"

            out.setdefault(qid_str, []).append({

                "option_level": lvl,

                "option_label": label,

                "pct": float(pct)

            })

        return out



    def _compute_questionnaire_question_option_distribution_school(self, batch_code: str, subject_name: str, school_code: str) -> Dict[str, List[Dict[str, Any]]]:

        """学校级问卷题目选项分布，直接从 questionnaire_question_scores 聚合"""

        subject_label_map = self._get_scale_label_map(batch_code, subject_name)

        question_label_maps = self._get_question_option_label_maps(batch_code, subject_name)

        sql = text(

            """

            SELECT agg.question_id,

                   agg.option_level,

                   ROUND(

                     agg.cnt * 100.0 /

                     NULLIF(SUM(agg.cnt) OVER (PARTITION BY agg.question_id), 0),

                     2

                   ) AS pct

            FROM (

              SELECT qqs.question_id,

                     GREATEST(

                         1,

                         LEAST(

                             qqs.scale_level,

                             ROUND(COALESCE(qqs.original_score,0) / NULLIF(qqs.max_score,0) * qqs.scale_level, 0)

                         )

                     ) AS option_level,

                     COUNT(*) AS cnt

              FROM questionnaire_question_scores qqs

              WHERE qqs.batch_code = :batch

                AND qqs.subject_name = :subject

                AND qqs.school_id = :school

              GROUP BY qqs.question_id,

                       GREATEST(

                         1,

                         LEAST(

                             qqs.scale_level,

                             ROUND(COALESCE(qqs.original_score,0) / NULLIF(qqs.max_score,0) * qqs.scale_level, 0)

                         )

                       )

            ) AS agg

            ORDER BY agg.question_id, agg.option_level

            """

        )

        out: Dict[str, List[Dict[str, Any]]] = {}

        with get_db_context() as db:

            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name, "school": school_code}).fetchall()

        subject_labels = subject_label_map if isinstance(subject_label_map, dict) else {}

        for qid, level_val, pct in rows:

            qid_str = str(qid)

            lvl = int(level_val)

            question_map = question_label_maps.get(qid_str, {}) if isinstance(question_label_maps, dict) else {}

            label = question_map.get(lvl) or subject_labels.get(lvl)

            if label is None:

                label = f"选项{lvl}"

            out.setdefault(qid_str, []).append({

                "option_level": lvl,

                "option_label": label,

                "pct": float(pct) if pct is not None else None

            })



        if not out:

            question_ids = list(question_label_maps.keys()) if isinstance(question_label_maps, dict) else []

            if not question_ids:

                with get_db_context() as db:

                    rows_q = db.execute(

                        text(

                            """

                            SELECT DISTINCT question_id

                            FROM question_dimension_mapping

                            WHERE batch_code=:batch AND subject_name=:subject

                            ORDER BY question_id

                            """

                        ),

                        {"batch": batch_code, "subject": subject_name},

                    ).fetchall()

                question_ids = [str(row[0]) for row in rows_q if row and row[0] is not None]



            level_candidates: List[int] = []

            if subject_labels:

                level_candidates = sorted(int(lvl) for lvl in subject_labels.keys())

            if not level_candidates and isinstance(question_label_maps, dict):

                for level_map in question_label_maps.values():

                    if isinstance(level_map, dict):

                        level_candidates.extend(int(k) for k in level_map.keys())

                level_candidates = sorted(set(level_candidates))

            if not level_candidates:

                level_candidates = [1, 2, 3, 4]



            for qid in question_ids:

                entries: List[Dict[str, Any]] = []

                level_map = (

                    question_label_maps.get(qid, {})

                    if isinstance(question_label_maps, dict)

                    else {}

                )

                for lvl in level_candidates:

                    label = None

                    if isinstance(level_map, dict):

                        label = level_map.get(lvl)

                    if label is None:

                        label = subject_labels.get(lvl)

                    if label is None:

                        label = f"选项{lvl}"

                    entries.append({

                        "option_level": lvl,

                        "option_label": label,

                        "pct": 0.0,

                    })

                if entries:

                    out[qid] = entries



        return out



    def _compute_regional_dimension_metrics(self, batch_code: str, subject_name: str) -> Dict[str, Dict[str, Any]]:



        """计算区域层各维度的平均分、得分率等指标."""







        dim_codes = self._discover_dimension_codes(batch_code, subject_name)



        metrics: Dict[str, Dict[str, Any]] = {}



        if not dim_codes:



            return metrics







        with get_db_context() as db:



            for dim in dim_codes:



                dim_str = str(dim)



                dim_avg_val: Optional[float] = None



                student_count = 0







                try:



                    sql = text(



                        f"""



                        SELECT COUNT(*) AS student_cnt,



                               ROUND(AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(CAST(scs.dimension_scores AS JSON), '$."{dim_str}".score')) AS DECIMAL(10,4))), 2) AS dim_avg



                        FROM student_cleaned_scores scs



                        JOIN school_master_data smd 



                          ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci



                         AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci



                         AND smd.status = 'ACTIVE'



                        WHERE scs.batch_code=:batch AND scs.subject_name=:subject



                          AND LOWER(scs.subject_type) IN ('exam','questionnaire')



                          AND JSON_EXTRACT(CAST(scs.dimension_scores AS JSON), '$."{dim_str}".score') IS NOT NULL



                        """



                    )



                    row = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchone()



                except Exception:



                    row = None







                if row:



                    try:



                        student_count = int(row[0] or 0)



                    except (TypeError, ValueError):



                        student_count = 0



                    try:



                        if row[1] is not None:



                            dim_avg_val = float(row[1])



                    except (TypeError, ValueError):



                        dim_avg_val = None







                if dim_avg_val is None:



                    try:



                        sql2 = text(



                            """



                            SELECT COUNT(*) AS resp_cnt,



                                   ROUND(AVG(qqs.original_score), 2) AS dim_avg



                            FROM questionnaire_question_scores qqs



                            JOIN question_dimension_mapping qdm



                              ON qdm.batch_code COLLATE utf8mb4_unicode_ci = qqs.batch_code COLLATE utf8mb4_unicode_ci



                             AND qdm.subject_name COLLATE utf8mb4_unicode_ci = qqs.subject_name COLLATE utf8mb4_unicode_ci



                             AND qdm.question_id COLLATE utf8mb4_unicode_ci = qqs.question_id COLLATE utf8mb4_unicode_ci



                            JOIN school_master_data smd 



                              ON smd.batch_code COLLATE utf8mb4_unicode_ci = qqs.batch_code COLLATE utf8mb4_unicode_ci



                             AND smd.school_id COLLATE utf8mb4_unicode_ci = qqs.school_id COLLATE utf8mb4_unicode_ci



                             AND smd.status = 'ACTIVE'



                            WHERE qqs.batch_code COLLATE utf8mb4_unicode_ci = CAST(:batch AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci



                              AND qqs.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci



                              AND qdm.subject_name COLLATE utf8mb4_unicode_ci = CAST(:subject AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci



                              AND qdm.dimension_code COLLATE utf8mb4_unicode_ci = CAST(:dim AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci



                            """



                        )



                        row2 = db.execute(sql2, {"batch": batch_code, "subject": subject_name, "dim": dim_str}).fetchone()



                    except Exception:



                        row2 = None







                    if row2:



                        try:



                            if row2[1] is not None:



                                dim_avg_val = float(row2[1])



                        except (TypeError, ValueError):



                            dim_avg_val = None



                        if student_count == 0:



                            try:



                                student_count = int(row2[0] or 0)



                            except (TypeError, ValueError):



                                student_count = 0







                if dim_avg_val is None:



                    continue







                max_score = None



                try:



                    sql_max = text(



                        f"""



                        SELECT ROUND(AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(CAST(scs.dimension_max_scores AS JSON), '$."{dim_str}".max_score')) AS DECIMAL(10,4))), 2) AS max_score



                        FROM student_cleaned_scores scs



                        JOIN school_master_data smd 



                          ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci



                         AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci



                         AND smd.status = 'ACTIVE'



                        WHERE scs.batch_code=:batch AND scs.subject_name=:subject



                          AND LOWER(scs.subject_type) IN ('exam','questionnaire')



                          AND JSON_EXTRACT(CAST(scs.dimension_max_scores AS JSON), '$."{dim_str}".max_score') IS NOT NULL



                        """



                    )



                    max_row = db.execute(sql_max, {"batch": batch_code, "subject": subject_name}).fetchone()



                except Exception:



                    max_row = None







                if max_row and max_row[0] is not None:



                    try:



                        max_score = float(max_row[0])



                    except (TypeError, ValueError):



                        max_score = None







                if max_score is None or max_score == 0.0:



                    try:



                        sql_max2 = text(



                            """



                            SELECT SUM(sqc.max_score) AS dim_max



                            FROM subject_question_config sqc



                            JOIN question_dimension_mapping qdm



                              ON qdm.batch_code = sqc.batch_code



                             AND qdm.subject_name = sqc.subject_name



                             AND qdm.question_id = sqc.question_id



                            WHERE sqc.batch_code=:batch AND sqc.subject_name=:subject AND qdm.dimension_code=:dim



                            """



                        )



                        max_row2 = db.execute(sql_max2, {"batch": batch_code, "subject": subject_name, "dim": dim_str}).fetchone()



                        if max_row2 and max_row2[0] is not None:



                            try:



                                max_score = float(max_row2[0])



                            except (TypeError, ValueError):



                                max_score = None



                    except Exception:



                        pass







                score_rate = None



                if max_score and max_score != 0:



                    try:



                        score_rate = round2((dim_avg_val / max_score) * 100.0)



                    except Exception:



                        score_rate = None







                metrics[dim_str] = {



                    'avg': round2(dim_avg_val),



                    'student_count': student_count,



                    'max_score': round2(max_score) if max_score is not None else None,



                    'score_rate': score_rate,



                }







        return metrics



    def _compute_regional_dimension_school_rankings(



        self,



        batch_code: str,



        subject_name: str,



    ) -> Dict[str, List[Dict[str, Any]]]:



        """生成维度层面的学校排名列表."""







        rank_cache = self.build_dimension_rank_cache(batch_code)



        school_names = self._get_school_name_map(batch_code)



        if not isinstance(rank_cache, dict):



            return {}







        rankings: Dict[str, List[Dict[str, Any]]] = defaultdict(list)



        for school_id, subject_map in rank_cache.items():



            if not isinstance(subject_map, dict):



                continue



            dim_map = subject_map.get(subject_name)



            if not isinstance(dim_map, dict):



                continue



            school_id_str = str(school_id)



            school_name = school_names.get(school_id_str, school_id_str)



            for dim_code, payload in dim_map.items():



                if not isinstance(payload, dict):



                    continue



                avg_val = payload.get('avg')



                rank_val = payload.get('rank')



                entry: Dict[str, Any] = {



                    'school_id': school_id_str,



                    'school_name': school_name,



                    'avg': avg_val,



                    'score_rate': payload.get('score_rate'),



                    'rank': rank_val,



                }



                rankings[str(dim_code)].append(entry)







        for dim_code, entries in rankings.items():



            entries.sort(key=lambda item: (item.get('rank') or float('inf'), item.get('school_id')))



            total = len(entries)



            for entry in entries:



                entry['total_schools'] = total







        return rankings







    def _fetch_scale_option_labels(

        self,

        db,

        instrument_type: Optional[Any],

        scale_level: Optional[Any],

        is_reverse: Optional[Any] = None,

    ) -> List[Tuple[Any, Any]]:

        inst = self._normalize_option_label(instrument_type)

        if not inst:

            return []



        rows: List[Any] = []

        base_params = {"inst": inst}

        scale_text = self._normalize_option_label(scale_level)



        reverse_flag: Optional[int] = None

        if is_reverse not in (None, "", "None"):

            try:

                reverse_flag = 1 if int(is_reverse) else 0

            except (TypeError, ValueError):

                reverse_flag = None



        if scale_text:

            params_with_scale = dict(base_params)

            params_with_scale["scale"] = scale_text

            if reverse_flag is not None:

                params_rev = dict(params_with_scale)

                params_rev["rev"] = reverse_flag

                sql_scale_rev = text(

                    """

                    SELECT option_level, option_label

                    FROM questionnaire_scale_options

                    WHERE instrument_type COLLATE utf8mb4_unicode_ci = CAST(:inst AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                      AND scale_level   COLLATE utf8mb4_unicode_ci = CAST(:scale AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                      AND is_reverse = :rev

                    ORDER BY option_level

                    """

                )

                rows = db.execute(sql_scale_rev, params_rev).fetchall()

            if not rows:

                sql_with_scale = text(

                    """

                    SELECT option_level, option_label

                    FROM questionnaire_scale_options

                    WHERE instrument_type COLLATE utf8mb4_unicode_ci = CAST(:inst AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                      AND scale_level   COLLATE utf8mb4_unicode_ci = CAST(:scale AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                    ORDER BY option_level

                    """

                )

                rows = db.execute(sql_with_scale, params_with_scale).fetchall()



        if not rows and reverse_flag is not None:

            params_rev_only = dict(base_params)

            params_rev_only["rev"] = reverse_flag

            sql_plain_rev = text(

                """

                SELECT option_level, option_label

                FROM questionnaire_scale_options

                WHERE instrument_type COLLATE utf8mb4_unicode_ci = CAST(:inst AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                  AND is_reverse = :rev

                ORDER BY option_level

                """

            )

            rows = db.execute(sql_plain_rev, params_rev_only).fetchall()



        if not rows:

            sql_plain = text(

                """

                SELECT option_level, option_label

                FROM questionnaire_scale_options

                WHERE instrument_type COLLATE utf8mb4_unicode_ci = CAST(:inst AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                ORDER BY option_level

                """

            )

            rows = db.execute(sql_plain, base_params).fetchall()



        return rows





    def _get_scale_label_map(self, batch_code: str, subject_name: str) -> Dict[int, str]:

        """Return subject-level option label map, preferring questionnaire_scale_options."""

        try:

            levels = 0

            with get_db_context() as db:

                pick_sql = text(

                    """

                    SELECT instrument_type, scale_level, COUNT(*) AS cnt

                    FROM questionnaire_question_scores

                    WHERE batch_code=:batch AND subject_name=:subject

                      AND instrument_type IS NOT NULL AND scale_level IS NOT NULL

                    GROUP BY instrument_type, scale_level

                    ORDER BY cnt DESC

                    LIMIT 1

                    """

                )

                row = db.execute(pick_sql, {"batch": batch_code, "subject": subject_name}).fetchone()

                label_map: Dict[int, str] = {}

                expected_levels = set()

                if row:

                    opts = self._fetch_scale_option_labels(db, row[0], row[1])

                    expected_levels = {int(lvl) for (lvl, _lbl) in opts}

                    for lvl, lbl in opts:

                        normalized = self._normalize_option_label(lbl)

                        if not normalized:

                            continue

                        label_map[int(lvl)] = normalized

                if not expected_levels or set(label_map.keys()) != expected_levels:

                    guess_sql = text(

                        """

                        SELECT option_level, option_label, COUNT(*) AS cnt

                        FROM questionnaire_question_scores

                        WHERE batch_code=:batch AND subject_name=:subject AND option_label IS NOT NULL

                        GROUP BY option_level, option_label

                        ORDER BY option_level, cnt DESC

                        """

                    )

                    rows_guess = db.execute(guess_sql, {"batch": batch_code, "subject": subject_name}).fetchall()

                    best: Dict[int, tuple] = {}

                    for lvl, label, cnt in rows_guess:

                        normalized = self._normalize_option_label(label)

                        if not normalized:

                            continue

                        lvl_int = int(lvl)

                        cnt_int = int(cnt or 0)

                        if lvl_int not in best or cnt_int > best[lvl_int][1]:

                            best[lvl_int] = (normalized, cnt_int)

                    for lvl_int, (label_text, _cnt) in best.items():

                        if lvl_int not in label_map:

                            label_map[lvl_int] = label_text

                if label_map:

                    return label_map

                levels_sql = text(

                    """

                    SELECT COUNT(DISTINCT option_level) AS levels

                    FROM questionnaire_question_scores

                    WHERE batch_code=:batch AND subject_name=:subject

                    """

                )

                lv_row = db.execute(levels_sql, {"batch": batch_code, "subject": subject_name}).fetchone()

                levels = int(lv_row[0] or 0) if lv_row else 0

                if levels == 0:

                    levels_sql_qod = text(

                        """

                        SELECT COUNT(DISTINCT option_level) AS levels

                        FROM questionnaire_option_distribution

                        WHERE batch_code=:batch AND subject_name=:subject

                        """

                    )

                    lv_row2 = db.execute(levels_sql_qod, {"batch": batch_code, "subject": subject_name}).fetchone()

                    levels = int(lv_row2[0] or 0) if lv_row2 else 0

                if levels > 0:

                    try:

                        opt_sql_scale = text(

                            """

                            SELECT option_level, option_label, COUNT(*) AS cnt

                            FROM questionnaire_scale_options

                            WHERE scale_level   COLLATE utf8mb4_unicode_ci = CAST(:scale AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci

                            GROUP BY option_level, option_label

                            ORDER BY option_level, cnt DESC

                            """

                        )

                        rows_scale = db.execute(opt_sql_scale, {"scale": str(levels)}).fetchall()

                        if rows_scale:

                            best_scale: Dict[int, tuple] = {}

                            for lvl, label, cnt in rows_scale:

                                lvl_int = int(lvl)

                                cnt_int = int(cnt or 0)

                                normalized = self._normalize_option_label(label) or f"选项{lvl_int}"

                                if lvl_int not in best_scale or cnt_int > best_scale[lvl_int][1]:

                                    best_scale[lvl_int] = (normalized, cnt_int)

                            if best_scale:

                                picked_levels = sorted(best_scale.keys())

                                if picked_levels and picked_levels[0] == 0:

                                    return {lvl + 1: best_scale[lvl][0] for lvl in best_scale}

                                return {lvl: best_scale[lvl][0] for lvl in best_scale}

                    except Exception:

                        pass

            if levels == 5:

                return {1: "非常不同意", 2: "不同意", 3: "同意", 4: "非常同意", 5: "完全同意"}

            if levels == 4:

                return {1: "非常不同意", 2: "不同意", 3: "同意", 4: "非常同意"}

            if levels == 3:

                return {1: "不同意", 2: "一般", 3: "同意"}

            return {i: f"选项{i}" for i in range(1, max(levels, 5) + 1)}

        except Exception:

            return {1: "选项1", 2: "选项2", 3: "选项3", 4: "选项4", 5: "选项5"}













