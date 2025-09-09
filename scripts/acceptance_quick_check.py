#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
快速验收交叉检查脚本（只读）：
- 覆盖范围统计（statistical_aggregations）
- 结构与命名检查（subjects、school_rankings、region_rank、dimensions[].rank、问卷结构）
- 精度与百分比检查（抽查关键字段是否超过两位小数，pct 是否在 0–100 且分组合计≈100）
- 排名结果抽样对比（按 SQL 计算 vs JSON：科目层学校排名 Top 10）

注意：脚本仅读取数据库，不做任何写入。
"""

from __future__ import annotations
import json
import math
import sys
from collections import defaultdict
from decimal import Decimal
from typing import Any, Dict, List, Tuple

from sqlalchemy import create_engine, text

# 默认连接（来源于仓库脚本），如需变更可通过命令行参数传入
DEFAULT_DB_URL = (
    "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/"
    "appraisal_test?charset=utf8mb4"
)

TARGET_BATCHES = ["G4-2025", "G7-2025", "G8-2025"]


def more_than_2dp(x: Any) -> bool:
    try:
        d = Decimal(str(x)).normalize()
        # exponent < -2 表示小数位数超过 2 位
        return d.as_tuple().exponent < -2
    except Exception:
        return False


def nearly_100(sum_pct: float, tol: float = 0.05) -> bool:
    return abs(sum_pct - 100.0) <= tol


def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)


def fetch_coverage(conn, batches: List[str]) -> List[Tuple[str, str, int]]:
    rows = conn.execute(
        text(
            """
            SELECT batch_code, aggregation_level, COUNT(*) AS cnt
            FROM statistical_aggregations
            WHERE batch_code IN :batches
            GROUP BY batch_code, aggregation_level
            ORDER BY batch_code, aggregation_level
            """
        ),
        {"batches": tuple(batches)},
    ).fetchall()
    return [(r[0], str(r[1]), int(r[2])) for r in rows]


def load_agg(conn, batch: str, level: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            SELECT id, school_id, school_name, statistics_data
            FROM statistical_aggregations
            WHERE batch_code=:batch AND aggregation_level=:level
            ORDER BY school_name
            """
        ),
        {"batch": batch, "level": level},
    ).fetchall()
    out = []
    for r in rows:
        try:
            data = r[3]
            # 部分驱动返回字符串 JSON
            if isinstance(data, str):
                data = json.loads(data)
            out.append({
                "id": int(r[0]),
                "school_id": r[1],
                "school_name": r[2],
                "data": data or {},
            })
        except Exception:
            out.append({
                "id": int(r[0]),
                "school_id": r[1],
                "school_name": r[2],
                "data": {},
            })
    return out


def _extract_subjects_like(container: Dict[str, Any]) -> List[Dict[str, Any]]:
    """将现有 JSON 结构（subjects 或 academic/non_academic_subjects）统一抽取为 subjects-like 列表"""
    if not isinstance(container, dict):
        return []
    # 新契约
    subs = container.get("subjects")
    if isinstance(subs, list) and subs:
        return subs
    extracted: List[Dict[str, Any]] = []
    # 旧结构：学科
    ac = container.get("academic_subjects")
    if isinstance(ac, dict):
        for name, info in ac.items():
            item = {"subject_name": name}
            if isinstance(info, dict):
                # 尝试映射 metrics（仅供精度检测用，不做强依赖）
                bs = info.get("basic_stats") or {}
                em = info.get("educational_metrics") or {}
                metrics = {}
                if isinstance(bs, dict):
                    metrics.update({
                        "avg": bs.get("avg_score"),
                        "min": bs.get("min_score"),
                        "stddev": bs.get("std_score"),
                    })
                if isinstance(info, dict):
                    metrics.update({
                        "max": info.get("max_score"),
                    })
                if isinstance(em, dict):
                    # 将得分率映射为 score_rate（0-1）
                    metrics.update({
                        "difficulty": em.get("difficulty_coefficient"),
                        "discrimination": em.get("discrimination_index"),
                        "score_rate": em.get("average_score_rate"),
                    })
                if metrics:
                    item["metrics"] = metrics
                if isinstance(info.get("dimensions"), dict):
                    # 维度数组化
                    dims = []
                    for dcode, dinfo in info["dimensions"].items():
                        if isinstance(dinfo, dict):
                            dims.append({
                                "code": dcode,
                                "name": dinfo.get("dimension_name") or dcode,
                                "avg": (dinfo.get("basic_stats") or {}).get("avg_score"),
                                "score_rate": (dinfo.get("basic_stats") or {}).get("score_rate"),
                            })
                    item["dimensions"] = dims
            extracted.append(item)
    # 旧结构：非学科（问卷）
    nac = container.get("non_academic_subjects")
    if isinstance(nac, dict):
        for name, info in nac.items():
            item = {"subject_name": name, "type": "questionnaire"}
            if isinstance(info, dict):
                if isinstance(info.get("dimensions"), list):
                    item["dimensions"] = info["dimensions"]
                if isinstance(info.get("questions"), list):
                    item["questions"] = info["questions"]
            extracted.append(item)
    return extracted


def summarize_structure(region: Dict[str, Any], schools: List[Dict[str, Any]]) -> Dict[str, Any]:
    report: Dict[str, Any] = {}

    # 区域层
    rdata = region.get("data", {}) if region else {}
    subjects = _extract_subjects_like(rdata)
    has_non_academic = isinstance(rdata, dict) and ("non_academic_subjects" in rdata)
    subj_names = [s.get("subject_name") or s.get("name") for s in subjects]

    missing_school_rankings = sum(1 for s in subjects if isinstance(s, dict) and not s.get("school_rankings"))

    # 问卷识别
    questionnaire_subjects = [
        s for s in subjects
        if (s.get("type") == "questionnaire") or (s.get("subject_name") == "问卷")
    ]

    # 学校层（汇总统计）
    total_school_rows = len(schools)
    school_subject_count_total = 0
    missing_region_rank = 0
    missing_total_schools = 0
    missing_dim_rank = 0

    for row in schools:
        data = row.get("data", {})
        subs = _extract_subjects_like(data)
        school_subject_count_total += len(subs)
        for s in subs:
            if "region_rank" not in s:
                missing_region_rank += 1
            if "total_schools" not in s:
                missing_total_schools += 1
            dims = s.get("dimensions", []) or []
            for d in dims:
                if isinstance(d, dict) and ("rank" not in d):
                    missing_dim_rank += 1

    # 问卷分布（仅区域层检查是否产出）
    questionnaire_dist_missing = {
        "dimensions_option_distribution_missing": 0,
        "questions_option_distribution_missing": 0,
    }
    for s in questionnaire_subjects:
        dims = s.get("dimensions", []) or []
        if not dims or any("option_distribution" not in d for d in dims):
            questionnaire_dist_missing["dimensions_option_distribution_missing"] += 1
        qs = s.get("questions", []) or []
        if not qs or any("option_distribution" not in q for q in qs):
            questionnaire_dist_missing["questions_option_distribution_missing"] += 1

    report.update({
        "region_subject_count": len(subjects),
        "region_subject_names": [n for n in subj_names if n],
        "has_non_academic_subjects": has_non_academic,
        "missing_school_rankings": missing_school_rankings,
        "questionnaire_count_in_subjects": len(questionnaire_subjects),
        **questionnaire_dist_missing,
        "school_row_count": total_school_rows,
        "school_subject_count_total": school_subject_count_total,
        "missing_region_rank": missing_region_rank,
        "missing_total_schools": missing_total_schools,
        "missing_dimension_rank": missing_dim_rank,
    })
    return report


def collect_precision_issues(region: Dict[str, Any], schools: List[Dict[str, Any]]) -> Dict[str, Any]:
    issues = defaultdict(int)

    def check_metrics(s: Dict[str, Any]):
        m = s.get("metrics") or {}
        for k in ("avg","stddev","max","min","difficulty","discrimination","p10","p50","p90"):
            v = m.get(k)
            if isinstance(v, (int, float, Decimal)) and more_than_2dp(v):
                issues[f"metrics_{k}_>2dp"] += 1

    def check_rankings(s: Dict[str, Any]):
        ranks = s.get("school_rankings") or []
        for r in ranks:
            v = r.get("avg")
            if isinstance(v, (int, float, Decimal)) and more_than_2dp(v):
                issues["rankings_avg_>2dp"] += 1

    def check_option_dist(s: Dict[str, Any]):
        # 维度分布
        dims = s.get("dimensions") or []
        # 精度检查：维度层 avg/score_rate 两位小数
        for d in dims:
            if isinstance(d, dict):
                v_avg = d.get("avg")
                v_sr = d.get("score_rate")
                if isinstance(v_avg, (int, float, Decimal)) and more_than_2dp(v_avg):
                    issues["dimension_avg_>2dp"] += 1
                if isinstance(v_sr, (int, float, Decimal)) and more_than_2dp(v_sr):
                    issues["dimension_score_rate_>2dp"] += 1
        for d in dims:
            od = d.get("option_distribution") or []
            if od:
                sum_pct = 0.0
                for it in od:
                    pct = it.get("pct")
                    if pct is None:
                        issues["dim_option_pct_missing"] += 1
                        continue
                    if not isinstance(pct, (int, float, Decimal)):
                        issues["dim_option_pct_not_number"] += 1
                        continue
                    if float(pct) < 0 or float(pct) > 100:
                        issues["dim_option_pct_out_of_range"] += 1
                    if more_than_2dp(pct):
                        issues["dim_option_pct_>2dp"] += 1
                    sum_pct += float(pct)
                if not nearly_100(sum_pct):
                    issues["dim_option_pct_sum_!=100"] += 1
        # 题目分布
        qs = s.get("questions") or []
        for q in qs:
            od = q.get("option_distribution") or []
            if od:
                sum_pct = 0.0
                for it in od:
                    pct = it.get("pct")
                    if pct is None:
                        issues["q_option_pct_missing"] += 1
                        continue
                    if not isinstance(pct, (int, float, Decimal)):
                        issues["q_option_pct_not_number"] += 1
                        continue
                    if float(pct) < 0 or float(pct) > 100:
                        issues["q_option_pct_out_of_range"] += 1
                    if more_than_2dp(pct):
                        issues["q_option_pct_>2dp"] += 1
                    sum_pct += float(pct)
                if not nearly_100(sum_pct):
                    issues["q_option_pct_sum_!=100"] += 1

    # 区域层检查
    rdata = region.get("data", {}) if region else {}
    for s in _extract_subjects_like(rdata):
        check_metrics(s)
        check_rankings(s)
        check_option_dist(s)

    # 学校层检查（只检查 metrics 与 dimensions.rank）
    for row in schools:
        for s in (row.get("data", {}).get("subjects") or []):
            check_metrics(s)
            # 对学校层，不强制检查问卷分布（按设计由区域层承载）
    return dict(issues)


def fetch_subjects_from_region(region: Dict[str, Any]) -> List[str]:
    data = region.get("data", {}) if region else {}
    subjects = _extract_subjects_like(data)
    names = []
    for s in subjects:
        n = s.get("subject_name") or s.get("name")
        if n:
            names.append(n)
    return names


def compare_school_rankings(conn, batch: str, subject_name: str, region: Dict[str, Any]) -> Dict[str, Any]:
    """抽样对比：区域层 JSON 的 school_rankings 与 SQL 计算 Top10 对比"""
    data = region.get("data", {}) if region else {}
    subjects = data.get("subjects", []) if isinstance(data, dict) else []
    target = None
    for s in subjects:
        if (s.get("subject_name") or s.get("name")) == subject_name:
            target = s
            break
    if not target:
        return {"error": f"region JSON 未找到科目: {subject_name}"}

    json_ranks = target.get("school_rankings") or []
    json_top10 = [(r.get("school_code"), r.get("avg")) for r in json_ranks[:10]]

    sql = text(
        """
        SELECT school_code,
               ROUND(AVG(total_score), 2) AS avg
        FROM student_cleaned_scores
        WHERE batch_code = :batch AND subject_name = :subject
          AND subject_type IN ('exam','questionnaire')
        GROUP BY school_code
        ORDER BY avg DESC, school_code ASC
        LIMIT 10
        """
    )
    rows = conn.execute(sql, {"batch": batch, "subject": subject_name}).fetchall()
    sql_top10 = [(r[0], float(r[1]) if r[1] is not None else None) for r in rows]

    diffs = []
    for i, (js, ss) in enumerate(zip(json_top10, sql_top10), start=1):
        if js[0] != ss[0] or (js[1] is None or ss[1] is None or abs(js[1] - ss[1]) > 0.01):
            diffs.append({
                "pos": i, "json": {"school_code": js[0], "avg": js[1]},
                "sql": {"school_code": ss[0], "avg": ss[1]}
            })
    return {
        "subject": subject_name,
        "json_top10": json_top10,
        "sql_top10": sql_top10,
        "mismatch_count": len(diffs),
        "mismatches": diffs,
    }


def main(db_url: str) -> int:
    engine = get_engine(db_url)
    with engine.connect() as conn:
        coverage = fetch_coverage(conn, TARGET_BATCHES)

        # 仅对 G4-2025 做深入校验（其余批次可能未入库）
        g4_region = load_agg(conn, "G4-2025", "REGIONAL")
        g4_schools = load_agg(conn, "G4-2025", "SCHOOL")
        region_row = g4_region[0] if g4_region else {}

        structure_summary = summarize_structure(region_row, g4_schools)
        precision_issues = collect_precision_issues(region_row, g4_schools)

        # 抽样排名对比：选取区域 JSON 中第一个科目
        subjects = fetch_subjects_from_region(region_row)
        ranking_compare = {}
        if subjects:
            ranking_compare = compare_school_rankings(conn, "G4-2025", subjects[0], region_row)

        # 预览首个科目的 metrics，便于诊断精度问题
        metrics_preview = {}
        if region_row:
            subs_like = _extract_subjects_like(region_row.get("data", {}))
            if subs_like:
                mp = subs_like[0].get("metrics") or {}
                metrics_preview = {k: (str(v) if isinstance(v, Decimal) else v) for k, v in mp.items()}

        result = {
            "coverage": coverage,
            "g4_structure": structure_summary,
            "g4_precision_issues": precision_issues,
            "g4_ranking_compare_sample": ranking_compare,
            "g4_metrics_preview": metrics_preview,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_URL
    raise SystemExit(main(url))
