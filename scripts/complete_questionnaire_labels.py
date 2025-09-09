#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
补全问卷标签字典脚本

目标：为指定批次下的问卷科目，在 questionnaire_scale_options 中补齐缺失的
      (instrument_type, scale_level, option_level) → option_label 记录。

策略：
1) 优先使用 questionnaire_question_scores 中出现频次最高的 instrument_type/scale_level 组合；
2) 若 questionnaire_scale_options 已存在该组合的标签，跳过已存在的等级；
3) 对缺失的等级：
   - 优先从问卷明细(questionnaire_question_scores)中“每个等级最常见标签”推断；
   - 若仍缺失，则按等级数(3/4/5/7)填充通用满意度标签；
4) 不覆盖已有字典，仅新增缺失项。

用法：
  python scripts/complete_questionnaire_labels.py <BATCH_CODE> [<SUBJECT_NAME>]
示例：
  python scripts/complete_questionnaire_labels.py G7-2025
  python scripts/complete_questionnaire_labels.py G7-2025 问卷
"""

from __future__ import annotations
import sys
from typing import Dict, Any, List, Tuple, Optional
from sqlalchemy import text

from app.database.connection import get_db


GENERIC_LABELS = {
    3: {1: '不满意', 2: '一般', 3: '满意'},
    4: {1: '不满意', 2: '一般', 3: '满意', 4: '非常满意'},
    5: {1: '非常不满意', 2: '不满意', 3: '一般', 4: '满意', 5: '非常满意'},
    7: {1: '非常不满意', 2: '不满意', 3: '较不满意', 4: '一般', 5: '较满意', 6: '满意', 7: '非常满意'},
}


def list_questionnaire_subjects(batch_code: str, subject_name: Optional[str] = None) -> List[str]:
    sql = text(
        """
        SELECT DISTINCT subject_name
        FROM student_cleaned_scores
        WHERE batch_code=:batch AND subject_type='questionnaire'
        """
    )
    with next(get_db()) as db:
        rows = db.execute(sql, {"batch": batch_code}).fetchall()
    subs = [r[0] for r in rows]
    if subject_name:
        return [s for s in subs if s == subject_name]
    return subs


def pick_scale(batch_code: str, subject_name: str) -> Optional[Tuple[str, str]]:
    sql = text(
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
        row = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchone()
    if not row:
        return None
    return str(row[0]), str(row[1])


def existing_option_levels(inst: str, scale: str) -> Dict[int, str]:
    sql = text(
        """
        SELECT option_level, option_label
        FROM questionnaire_scale_options
        WHERE instrument_type=:inst AND scale_level=:scale
        ORDER BY option_level
        """
    )
    with next(get_db()) as db:
        rows = db.execute(sql, {"inst": inst, "scale": scale}).fetchall()
    return {int(r[0]): r[1] for r in rows}


def infer_labels_from_details(batch_code: str, subject_name: str) -> Dict[int, str]:
    sql = text(
        """
        SELECT option_level, option_label, COUNT(*) AS cnt
        FROM questionnaire_question_scores
        WHERE batch_code=:batch AND subject_name=:subject AND option_label IS NOT NULL
        GROUP BY option_level, option_label
        ORDER BY option_level, cnt DESC
        """
    )
    best: Dict[int, Tuple[str, int]] = {}
    with next(get_db()) as db:
        rows = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchall()
    for lvl, label, cnt in rows:
        lvl = int(lvl)
        if lvl not in best or cnt > best[lvl][1]:
            best[lvl] = (label, cnt)
    return {lvl: pair[0] for lvl, pair in best.items()}


def detect_level_count(batch_code: str, subject_name: str) -> Tuple[int, int, int]:
    sql = text(
        """
        SELECT COUNT(DISTINCT option_level) AS levels,
               MIN(option_level) AS min_level,
               MAX(option_level) AS max_level
        FROM questionnaire_question_scores
        WHERE batch_code=:batch AND subject_name=:subject
        """
    )
    with next(get_db()) as db:
        row = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchone()
    if not row:
        return 0, 0, 0
    return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)


def detect_level_count_via_distribution(batch_code: str, subject_name: str) -> Tuple[int, int, int]:
    sql = text(
        """
        SELECT COUNT(DISTINCT option_level) AS levels,
               MIN(option_level) AS min_level,
               MAX(option_level) AS max_level
        FROM questionnaire_option_distribution
        WHERE batch_code=:batch AND subject_name=:subject
        """
    )
    with next(get_db()) as db:
        row = db.execute(sql, {"batch": batch_code, "subject": subject_name}).fetchone()
    if not row:
        return 0, 0, 0
    return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)


def insert_labels(inst: str, scale: str, labels: Dict[int, str]) -> int:
    if not labels:
        return 0
    inserted = 0
    with next(get_db()) as db:
        for lvl, label in labels.items():
            db.execute(
                text(
                    """
                    INSERT INTO questionnaire_scale_options (instrument_type, scale_level, option_level, option_label)
                    VALUES (:inst, :scale, :lvl, :label)
                    ON DUPLICATE KEY UPDATE option_label=VALUES(option_label)
                    """
                ),
                {"inst": inst, "scale": scale, "lvl": int(lvl), "label": str(label)},
            )
        db.commit()
        inserted = len(labels)
    return inserted


def complete_for_subject(batch_code: str, subject_name: str) -> Dict[str, Any]:
    picked = pick_scale(batch_code, subject_name)
    if not picked:
        return {"subject": subject_name, "status": "skipped", "reason": "no scale info in details"}
    inst, scale = picked
    existing = existing_option_levels(inst, scale)
    # 推断标签
    inferred = infer_labels_from_details(batch_code, subject_name)
    # 若仍缺失则使用通用标签
    levels, min_lvl, max_lvl = detect_level_count(batch_code, subject_name)
    if levels == 0:
        # 从分布表尝试识别等级数量
        levels, min_lvl, max_lvl = detect_level_count_via_distribution(batch_code, subject_name)
    generic = GENERIC_LABELS.get(levels, {}) if levels else {}
    # 构造应写入的集合（仅缺失项）
    to_write: Dict[int, str] = {}
    # 先根据明细推断
    for lvl, label in inferred.items():
        if lvl not in existing:
            to_write[lvl] = label
    # 再用通用补齐剩余等级
    if levels and generic:
        # 若等级从0开始，对齐 generic 键值
        if min_lvl == 0 and max_lvl == levels - 1:
            # generic 使用 1..levels，将其映射到 0..levels-1
            for idx in range(1, levels + 1):
                lvl = idx - 1
                if lvl not in existing and lvl not in to_write and idx in generic:
                    to_write[lvl] = generic[idx]
        else:
            for lvl, label in generic.items():
                if lvl not in existing and lvl not in to_write:
                    to_write[lvl] = label
    # 写入
    written = insert_labels(inst, scale, to_write)
    return {
        "subject": subject_name,
        "instrument_type": inst,
        "scale_level": scale,
        "existing": len(existing),
        "added": written,
        "levels": levels,
        "min_level": min_lvl,
        "max_level": max_lvl,
    }


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: python scripts/complete_questionnaire_labels.py <BATCH_CODE> [<SUBJECT_NAME>]")
        return 1
    batch = argv[1]
    target_subject = argv[2] if len(argv) >= 3 else None
    subjects = list_questionnaire_subjects(batch, target_subject)
    if not subjects:
        print(f"No questionnaire subjects found for batch {batch}")
        return 0
    out: List[Dict[str, Any]] = []
    for s in subjects:
        out.append(complete_for_subject(batch, s))
    # 打印结果（JSON-like）
    import json
    print(json.dumps({"batch": batch, "results": out}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
