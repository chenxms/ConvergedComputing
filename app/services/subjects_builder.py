#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SubjectsBuilder

依据《汇聚模块修复实施方案 v1.2》，从数据库计算并生成统一的 subjects 结构，
包含考试与问卷的：
- 科目层指标（avg/stddev/max/min，可扩展 p10/p50/p90）
- 区域层学校排名 school_rankings（考试/问卷均参与）
- 学校层我校名次 region_rank/total_schools
- 学校层维度排名 dimensions[].rank（按学校维度均分）
- 问卷维度/题目选项占比 option_distribution

输出已做两位小数精度统一（值与百分比字段）。
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal
from dataclasses import dataclass

from sqlalchemy import text

from app.database.connection import get_db
from app.utils.precision import round2, round2_json


@dataclass
class SubjectInfo:
    name: str
    type: str  # 'exam' | 'questionnaire' | 'interaction'


class SubjectsBuilder:
    def __init__(self) -> None:
        pass

    def list_subjects(self, batch_code: str) -> List[SubjectInfo]:
        with next(get_db()) as db:
            rows = db.execute(
                text(
                    """
                    SELECT DISTINCT subject_name, subject_type
                    FROM student_cleaned_scores
                    WHERE batch_code=:batch
                    ORDER BY subject_name
                    """
                ),
                {"batch": batch_code},
            ).fetchall()
        return [SubjectInfo(name=r[0], type=(r[1] or 'exam')) for r in rows]

    def build_regional_subjects(self, batch_code: str) -> List[Dict[str, Any]]:
        subjects: List[Dict[str, Any]] = []
        for s in self.list_subjects(batch_code):
            subj: Dict[str, Any] = {
                "subject_name": s.name,
                "type": s.type,
                "metrics": self._compute_subject_metrics(batch_code, s.name),
                "school_rankings": self._compute_school_rankings(batch_code, s.name),
            }
            if s.type == 'questionnaire':
                # 问卷维度/题目选项占比
                dims_od = self._compute_questionnaire_dimension_option_distribution(batch_code, s.name)
                qs_od = self._compute_questionnaire_question_option_distribution(batch_code, s.name)
                if dims_od:
                    subj.setdefault("dimensions", [])
                    # 将按维度聚合的分布填入 dimensions 列表项
                    for dim_code, dist in dims_od.items():
                        subj["dimensions"].append({
                            "code": dim_code,
                            "name": dim_code,
                            "option_distribution": dist,
                        })
                if qs_od:
                    subj["questions"] = [
                        {"question_id": qid, "option_distribution": dist} for qid, dist in qs_od.items()
                    ]
            subjects.append(round2_json(subj))
        return subjects

    def build_school_subjects(self, batch_code: str, school_code: str) -> List[Dict[str, Any]]:
        subjects: List[Dict[str, Any]] = []
        for s in self.list_subjects(batch_code):
            metrics = self._compute_subject_metrics(batch_code, s.name, school_code)
            region_rank = self._compute_school_region_rank(batch_code, s.name, school_code)
            dims = self._compute_school_dimensions_with_rank(batch_code, s.name, school_code)
            subj: Dict[str, Any] = {
                "subject_name": s.name,
                "type": s.type,
                "metrics": metrics,
                **region_rank,
            }
            if dims:
                subj["dimensions"] = dims
            subjects.append(round2_json(subj))
        return subjects

    # --- Internals ---

    def _compute_subject_metrics(self, batch_code: str, subject_name: str, school_code: Optional[str] = None) -> Dict[str, Any]:
        where = ["batch_code = :batch", "subject_name = :subject", "subject_type IN ('exam','questionnaire')"]
        params: Dict[str, Any] = {"batch": batch_code, "subject": subject_name}
        if school_code:
            where.append("school_code = :school")
            params["school"] = school_code
        where_clause = " AND ".join(where)
        sql = text(
            f"""
            SELECT 
                ROUND(AVG(total_score), 2) AS avg,
                ROUND(STDDEV_POP(total_score), 2) AS stddev,
                ROUND(MAX(total_score), 2) AS max,
                ROUND(MIN(total_score), 2) AS min,
                ROUND(MAX(max_score), 2) AS max_score
            FROM student_cleaned_scores
            WHERE {where_clause}
            """
        )
        with next(get_db()) as db:
            row = db.execute(sql, params).fetchone()
        avg = float(row[0] or 0)
        stddev = float(row[1] or 0)
        max_v = float(row[2] or 0)
        min_v = float(row[3] or 0)
        max_score = float(row[4] or 0)
        difficulty = round2((avg / max_score) if max_score else 0)
        return {
            "avg": round2(avg),
            "stddev": round2(stddev),
            "max": round2(max_v),
            "min": round2(min_v),
            "difficulty": difficulty,
        }

    def _compute_school_rankings(self, batch_code: str, subject_name: str) -> List[Dict[str, Any]]:
        sql = text(
            """
            SELECT school_code,
                   MAX(school_name) AS school_name,
                   ROUND(AVG(total_score), 2) AS avg,
                   DENSE_RANK() OVER (ORDER BY AVG(total_score) DESC, school_code ASC) AS rnk
            FROM student_cleaned_scores
            WHERE batch_code = :batch AND subject_name = :subject
              AND subject_type IN ('exam','questionnaire')
            GROUP BY school_code
            ORDER BY avg DESC, school_code ASC
            """
        )
        with next(get_db()) as db:
            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall()
        return [
            {"school_code": r[0], "school_name": r[1], "avg": float(r[2] or 0), "rank": int(r[3])}
            for r in rows
        ]

    def _compute_school_region_rank(self, batch_code: str, subject_name: str, school_code: str) -> Dict[str, Any]:
        sql = text(
            """
            WITH ranks AS (
              SELECT school_code,
                     DENSE_RANK() OVER (ORDER BY AVG(total_score) DESC, school_code ASC) AS r
              FROM student_cleaned_scores
              WHERE batch_code = :batch AND subject_name = :subject
                AND subject_type IN ('exam','questionnaire')
              GROUP BY school_code
            )
            SELECT r AS region_rank,
                   (SELECT COUNT(DISTINCT school_code)
                      FROM student_cleaned_scores
                      WHERE batch_code = :batch AND subject_name = :subject
                        AND subject_type IN ('exam','questionnaire')) AS total_schools
            FROM ranks WHERE school_code = :school
            """
        )
        with next(get_db()) as db:
            row = db.execute(sql, {"batch": batch_code, "subject": subject_name, "school": school_code}).fetchone()
        if not row:
            return {"region_rank": None, "total_schools": 0}
        return {"region_rank": int(row[0] or 0), "total_schools": int(row[1] or 0)}

    def _discover_dimension_codes(self, batch_code: str, subject_name: str) -> List[str]:
        # 探测维度编码（从学生维度JSON中抽取）
        sql = text(
            """
            SELECT dimension_scores
            FROM student_cleaned_scores
            WHERE batch_code=:batch AND subject_name=:subject
              AND subject_type IN ('exam','questionnaire')
              AND dimension_scores IS NOT NULL AND dimension_scores != ''
            LIMIT 200
            """
        )
        codes: Dict[str, int] = {}
        import json
        with next(get_db()) as db:
            for (ds_json,) in db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall():
                try:
                    ds = json.loads(ds_json) if isinstance(ds_json, str) else (ds_json or {})
                except Exception:
                    ds = {}
                if isinstance(ds, dict):
                    for code in ds.keys():
                        codes[code] = 1
        return list(codes.keys())

    def _compute_school_dimensions_with_rank(self, batch_code: str, subject_name: str, school_code: str) -> List[Dict[str, Any]]:
        dims_out: List[Dict[str, Any]] = []
        dim_codes = self._discover_dimension_codes(batch_code, subject_name)
        if not dim_codes:
            return dims_out
        with next(get_db()) as db:
            for dim in dim_codes:
                sql_rank = text(
                    f"""
                    WITH per_school AS (
                      SELECT school_code,
                             AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(CAST(dimension_scores AS JSON), '$."{dim}".score')) AS DECIMAL(10,4))) AS dim_avg
                      FROM student_cleaned_scores
                      WHERE batch_code=:batch AND subject_name=:subject
                        AND subject_type IN ('exam','questionnaire')
                        AND JSON_EXTRACT(CAST(dimension_scores AS JSON), '$."{dim}".score') IS NOT NULL
                      GROUP BY school_code
                    )
                    SELECT 
                      (SELECT ROUND(dim_avg, 2) FROM per_school WHERE school_code=:school) AS my_avg,
                      (SELECT DENSE_RANK() OVER (ORDER BY dim_avg DESC, school_code ASC) FROM per_school WHERE school_code=:school) AS my_rank
                    """
                )
                row = db.execute(sql_rank, {"batch": batch_code, "subject": subject_name, "school": school_code}).fetchone()
                if not row:
                    continue
                dim_avg = float(row[0]) if row[0] is not None else None
                dim_rank = int(row[1]) if row[1] is not None else None
                # 估算维度满分（可选）：以该维度 max_score 的平均值为准
                sql_max = text(
                    f"""
                    SELECT ROUND(AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(CAST(dimension_max_scores AS JSON), '$."{dim}"')) AS DECIMAL(10,4))), 2) AS max_score
                    FROM student_cleaned_scores
                    WHERE batch_code=:batch AND subject_name=:subject
                      AND subject_type IN ('exam','questionnaire')
                      AND JSON_EXTRACT(CAST(dimension_max_scores AS JSON), '$."{dim}"') IS NOT NULL
                    """
                )
                max_row = db.execute(sql_max, {"batch": batch_code, "subject": subject_name}).fetchone()
                max_score = float(max_row[0]) if max_row and max_row[0] is not None else None
                score_rate = round2((dim_avg / max_score * 100.0) if (dim_avg is not None and max_score) else None)
                dims_out.append({
                    "code": dim,
                    "name": dim,
                    "avg": round2(dim_avg) if dim_avg is not None else None,
                    "score_rate": score_rate,
                    "rank": dim_rank,
                })
        return dims_out

    def _compute_questionnaire_dimension_option_distribution(self, batch_code: str, subject_name: str) -> Dict[str, List[Dict[str, Any]]]:
        # 需要 question_dimension_mapping 提供维度映射
        # 获取量表标签映射（option_level -> option_label）
        label_map = self._get_scale_label_map(batch_code, subject_name)
        sql = text(
            """
            SELECT qdm.dimension_code,
                   qqd.option_level,
                   ROUND(SUM(qqd.count) * 100.0 /
                         SUM(SUM(qqd.count)) OVER (PARTITION BY qdm.dimension_code), 2) AS pct
            FROM questionnaire_option_distribution qqd
            JOIN question_dimension_mapping qdm
              ON qdm.batch_code=qqd.batch_code
             AND qdm.subject_name=qqd.subject_name
             AND qdm.question_id=qqd.question_id
            WHERE qqd.batch_code=:batch AND qqd.subject_name=:subject
            GROUP BY qdm.dimension_code, qqd.option_level
            ORDER BY qdm.dimension_code, qqd.option_level
            """
        )
        out: Dict[str, List[Dict[str, Any]]] = {}
        with next(get_db()) as db:
            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall()
        for r in rows:
            dim = r[0]
            lvl = int(r[1])
            out.setdefault(dim, []).append({
                "option_level": lvl,
                "option_label": label_map.get(lvl),
                "pct": float(r[2])
            })
        return out

    def _compute_questionnaire_question_option_distribution(self, batch_code: str, subject_name: str) -> Dict[str, List[Dict[str, Any]]]:
        # 获取量表标签映射（option_level -> option_label）
        label_map = self._get_scale_label_map(batch_code, subject_name)
        sql = text(
            """
            SELECT question_id,
                   option_level,
                   ROUND(count * 100.0 / SUM(count) OVER (PARTITION BY question_id), 2) AS pct
            FROM questionnaire_option_distribution
            WHERE batch_code=:batch AND subject_name=:subject
            ORDER BY question_id, option_level
            """
        )
        out: Dict[str, List[Dict[str, Any]]] = {}
        with next(get_db()) as db:
            rows = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall()
        for r in rows:
            qid = r[0]
            lvl = int(r[1])
            out.setdefault(qid, []).append({
                "option_level": lvl,
                "option_label": label_map.get(lvl),
                "pct": float(r[2])
            })
        return out

    def _get_scale_label_map(self, batch_code: str, subject_name: str) -> Dict[int, str]:
        """根据问卷明细表推断 instrument_type/scale_level，构建选项等级到标签的映射"""
        # 先找出现频次最高的 instrument_type/scale_level 组合作为本科目的量表
        sql_pick = text(
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
        with next(get_db()) as db:
            row = db.execute(sql_pick, {"batch": batch_code, "subject": subject_name}).fetchone()
            opts = []
            if row:
                inst, scale = row[0], row[1]
                # 读取该量表的等级标签（优先）
                sql_opts = text(
                    """
                    SELECT option_level, option_label
                    FROM questionnaire_scale_options
                    WHERE instrument_type=:inst AND scale_level=:scale
                    ORDER BY option_level
                    """
                )
                opts = db.execute(sql_opts, {"inst": inst, "scale": scale}).fetchall()
            if not opts:
                # 回退：从问卷明细推断每个等级最常见的标签
                sql_guess = text(
                    """
                    SELECT option_level, option_label, COUNT(*) AS cnt
                    FROM questionnaire_question_scores
                    WHERE batch_code=:batch AND subject_name=:subject AND option_label IS NOT NULL
                    GROUP BY option_level, option_label
                    ORDER BY option_level, cnt DESC
                    """
                )
                rows = db.execute(sql_guess, {"batch": batch_code, "subject": subject_name}).fetchall()
                best: Dict[int, Tuple[str,int]] = {}
                for lvl, label, cnt in rows:
                    lvl = int(lvl)
                    if lvl not in best or cnt > best[lvl][1]:
                        best[lvl] = (label, cnt)
                derived = {lvl: pair[0] for lvl, pair in best.items()}
                if derived:
                    return derived
                # 仍无标签：按量表等级数给出通用标签（满意度）
                sql_levels = text(
                    """
                    SELECT COUNT(DISTINCT option_level) AS levels,
                           MIN(option_level) AS min_level,
                           MAX(option_level) AS max_level
                    FROM questionnaire_question_scores
                    WHERE batch_code=:batch AND subject_name=:subject
                    """
                )
                lv_row = db.execute(sql_levels, {"batch": batch_code, "subject": subject_name}).fetchone()
                levels = int(lv_row[0] or 0) if lv_row else 0
                min_level = int(lv_row[1] or 1) if lv_row else 1
                max_level = int(lv_row[2] or levels) if lv_row else levels
                generic = {
                    3: {1: '不满意', 2: '一般', 3: '满意'},
                    4: {1: '不满意', 2: '一般', 3: '满意', 4: '非常满意'},
                    5: {1: '非常不满意', 2: '不满意', 3: '一般', 4: '满意', 5: '非常满意'},
                    7: {1: '非常不满意', 2: '不满意', 3: '较不满意', 4: '一般', 5: '较满意', 6: '满意', 7: '非常满意'},
                }
                base = generic.get(levels, {})
                if not base:
                    return {}
                # 若等级从0开始，扩展映射以覆盖 0..N-1
                out = {}
                rng = list(range(min_level, max_level+1)) if levels>0 else []
                if rng and len(rng)==len(base):
                    for label_idx, lvl in enumerate(rng, start=1):
                        out[lvl] = base.get(label_idx)
                else:
                    out = base
                return out
        return {int(r[0]): r[1] for r in opts} if opts else {}
