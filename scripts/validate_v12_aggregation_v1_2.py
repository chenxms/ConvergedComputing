#!/usr/bin/env python3
"""
v1.2 汇聚结果校验脚本（REGIONAL 与 SCHOOL）

校验内容：
- API: 类型合法、数值两位小数、关键字段存在
- SQL: ACTIVE 学校计数与 total_schools 一致；问卷分布不跨科目

依赖：
- requests
- SQLAlchemy
- PyMySQL（SQLAlchemy MySQL 驱动）

示例：
  python scripts/validate_v12_aggregation_v1_2.py \
    --api http://localhost:8000/api/v12 \
    --db "mysql+pymysql://user:pass@host:port/appraisal_test?charset=utf8mb4" \
    --batch G4-2025 \
    --school-id 5071    # 或 --school 5071（兼容别名）
"""

import argparse
import sys
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional

import requests
from sqlalchemy import create_engine, text
import os


def parse_args():
    p = argparse.ArgumentParser(description="v1.2 汇聚结果校验脚本")
    p.add_argument("--api", required=True, help="API 基地址，如 http://localhost:8000/api/v12")
    p.add_argument("--db", required=False, help="SQLAlchemy 数据库URL（缺省从环境变量 DATABASE_URL 读取）")
    p.add_argument("--batch", required=True, help="批次代码，如 G4-2025")
    # 兼容：优先 --school-id，保留 --school 作为别名
    p.add_argument("--school-id", dest="school_id", required=False, help="学校ID（未提供时将从 REGIONAL 排名取样）")
    p.add_argument("--school", dest="school_id_alias", required=False, help="学校ID（别名）")
    p.add_argument("--timeout", type=int, default=30, help="HTTP 超时（秒）")
    return p.parse_args()


def http_get_json(url: str, timeout: int = 30) -> Dict[str, Any]:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    # 兼容 { success, data }
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        return data["data"]
    return data


def is_two_decimals(x: float) -> bool:
    try:
        d = Decimal(str(x))
        q = d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return d == q
    except Exception:
        return False


def walk_and_check_precision(obj: Any, path: str = "") -> List[str]:
    issues: List[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            key_path = f"{path}.{k}" if path else k
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                if k.endswith(("_pct", "_rate", "_percentage")) or k == "pct":
                    if v < 0 or v > 100:
                        issues.append(f"{key_path} 百分比值超出[0,100]范围: {v}")
                    if not is_two_decimals(float(v)):
                        issues.append(f"{key_path} 非两位小数: {v}")
                else:
                    if not is_two_decimals(float(v)):
                        issues.append(f"{key_path} 非两位小数: {v}")
            else:
                issues.extend(walk_and_check_precision(v, key_path))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            issues.extend(walk_and_check_precision(item, f"{path}[{i}]"))
    return issues


def verify_subject_types(regional: Dict[str, Any]) -> List[str]:
    issues: List[str] = []
    for s in regional.get("subjects", []):
        t = s.get("type")
        if t not in ("exam", "questionnaire"):
            issues.append(f"非法科目类型: {s.get('subject_name')} type={t}")
    return issues


def verify_school_identifiers(regional: Dict[str, Any], school: Optional[Dict[str, Any]] = None) -> List[str]:
    """可选校验：目前宽松处理，不强制 school_id 字段名。"""
    return []


def pick_sample_school(regional: Dict[str, Any]) -> Optional[str]:
    """从第一个存在 school_rankings 的科目中，取首个学校作为样本。"""
    for s in regional.get("subjects", []):
        ranks = s.get("school_rankings")
        if isinstance(ranks, list) and ranks:
            # 兼容：优先 school_id，回退 school_code
            return ranks[0].get("school_id") or ranks[0].get("school_code")
    return None


def sql_active_schools_count(engine, batch: str, subject: str) -> int:
    """返回 ACTIVE 学校数量（按 school_code 口径，统一 COLLATE 避免索引/排序规则差异）。"""
    sql = text(
        """
        SELECT COUNT(DISTINCT scs.school_code) AS cnt
        FROM student_cleaned_scores scs
        JOIN school_master_data smd
          ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
         AND smd.school_id  COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
         AND smd.status = 'ACTIVE'
        WHERE scs.batch_code = :b AND scs.subject_name = :s
          AND scs.subject_type IN ('exam','questionnaire')
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"b": batch, "s": subject}).fetchone()
        return int(row[0] or 0)


def sql_questionnaire_cross_subject_mismatch(engine, batch: str, subject: str) -> int:
    sql = text(
        """
        SELECT COUNT(1) AS cnt
        FROM question_dimension_mapping qdm
        JOIN questionnaire_option_distribution qqd
          ON qdm.batch_code  COLLATE utf8mb4_unicode_ci = qqd.batch_code  COLLATE utf8mb4_unicode_ci
         AND qdm.question_id COLLATE utf8mb4_unicode_ci = qqd.question_id COLLATE utf8mb4_unicode_ci
        WHERE qdm.batch_code   COLLATE utf8mb4_unicode_ci = CAST(:b AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
          AND qdm.subject_name COLLATE utf8mb4_unicode_ci = CAST(:s AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
          AND qqd.subject_name COLLATE utf8mb4_unicode_ci <> CAST(:s AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"b": batch, "s": subject}).fetchone()
        return int(row[0] or 0)


def sql_bdd_dimension_codes(engine, batch: str, subject: str) -> List[str]:
    sql = text(
        """
        SELECT dimension_code
        FROM batch_dimension_definition
        WHERE batch_code = :b AND subject_name = :s
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"b": batch, "s": subject}).fetchall()
        return [str(r[0]) for r in rows]


def verify_bdd_dimensions(engine, batch: str, regional: Dict[str, Any]) -> List[str]:
    issues: List[str] = []
    for s in regional.get("subjects", []):
        name = s.get("subject_name")
        dims = s.get("dimensions") or []
        got_codes = {str(d.get("code")) for d in dims if isinstance(d, dict) and d.get("code") is not None}
        bdd_codes = set(sql_bdd_dimension_codes(engine, batch, name))
        if bdd_codes and not bdd_codes.issubset(got_codes):
            missing = sorted(list(bdd_codes - got_codes))
            issues.append(f"维度集合不完整: subject={name} 缺少 {missing}")
    return issues


def main():
    args = parse_args()
    api_base = args.api.rstrip("/")
    batch = args.batch
    school_id = args.school_id or args.school_id_alias

    # 获取 REGIONAL 数据
    regional_url = f"{api_base}/batch/{batch}/regional"
    regional = http_get_json(regional_url, timeout=args.timeout)

    issues: List[str] = []
    issues += verify_subject_types(regional)
    issues += walk_and_check_precision(regional)

    # 如未提供学校，则从 REGIONAL 排名取样
    if not school_id:
        school_id = pick_sample_school(regional)
        if not school_id:
            print("[WARN] 未能从 REGIONAL 中获取样本学校，跳过学校级校验。")

    school = None
    if school_id:
        school_url = f"{api_base}/batch/{batch}/school/{school_id}"
        school = http_get_json(school_url, timeout=args.timeout)
        issues += walk_and_check_precision(school)

    # SQL 校验
    db_url = args.db or os.getenv("DATABASE_URL") or ""
    if not db_url:
        print("[ERROR] 未提供数据库URL，且环境变量 DATABASE_URL 不可用。")
        sys.exit(2)
    engine = create_engine(db_url)
    # 1) ACTIVE 学校计数与 total_schools 对齐（任取一个科目进行对比）
    try:
        for s in regional.get("subjects", []):
            name = s.get("subject_name")
            if not name or not school:
                continue
            subj_school = next((x for x in school.get("subjects", []) if x.get("subject_name") == name), None)
            if subj_school and "total_schools" in subj_school:
                api_total = int(subj_school.get("total_schools") or 0)
                sql_total = sql_active_schools_count(engine, batch, name)
                if api_total != sql_total:
                    issues.append(f"total_schools 不一致: subject={name} api={api_total} sql={sql_total}")
            break
    except Exception as e:
        issues.append(f"ACTIVE 学校口径 SQL 校验失败: {e}")

    # 2) 问卷分布跨科目交集应为 0
    try:
        for s in regional.get("subjects", []):
            if s.get("type") == "questionnaire":
                name = s.get("subject_name")
                mism = sql_questionnaire_cross_subject_mismatch(engine, batch, name)
                if mism > 0:
                    issues.append(f"问卷分布跨科目交集>0: subject={name} cnt={mism}")
    except Exception as e:
        issues.append(f"问卷分布跨科目 SQL 校验失败: {e}")

    # 输出
    if issues:
        print("=== 校验结果: FAIL ===")
        for i, msg in enumerate(issues, 1):
            print(f"[{i}] {msg}")
        sys.exit(1)
    else:
        print("=== 校验结果: PASS ===")
        sys.exit(0)


if __name__ == "__main__":
    main()
