#!/usr/bin/env python3
"""
快速修复 G7-2025 区域汇聚中的问卷数据
直接更新JSON中的问卷科目部分，无需重新计算考试科目
"""

import json
from sqlalchemy import text

import os, sys
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db
from app.services.calculation_service import CalculationService


def main():
    batch_code = "G7-2025"
    print(f"[quick-fix] 快速修复 {batch_code} 问卷数据")

    with next(get_db()) as db:
        # 设置更长的锁等待时间
        db.execute(text("SET SESSION innodb_lock_wait_timeout=300"))

        # 获取现有数据
        result = db.execute(
            text("""
                SELECT statistics_data, total_students, total_schools
                FROM statistical_aggregations
                WHERE batch_code=:batch AND aggregation_level='REGIONAL'
                AND (school_id='REGIONAL' OR school_id IS NULL)
                LIMIT 1
            """),
            {"batch": batch_code}
        ).fetchone()

        if not result:
            print("[quick-fix] 未找到现有区域数据")
            return

        existing_data = json.loads(result[0])
        total_students = result[1]
        total_schools = result[2]

        print(f"[quick-fix] 现有科目数: {len(existing_data.get('subjects', []))}")

        # 只处理问卷科目
        calc_service = CalculationService()

        # 获取问卷科目的统计数据
        questionnaire_stats = calc_service.calculate_regional_statistics(
            batch_code=batch_code,
            subject_types=['questionnaire']  # 只计算问卷
        )

        # 保留考试科目，更新问卷科目
        existing_subjects = existing_data.get('subjects', [])
        exam_subjects = [s for s in existing_subjects if s.get('type') == 'exam']
        questionnaire_subjects = questionnaire_stats.get('subjects', [])

        # 合并科目
        updated_subjects = exam_subjects + questionnaire_subjects

        # 更新数据
        existing_data['subjects'] = updated_subjects
        existing_data['updated_at'] = "2025-09-16T14:30:00.000000+00:00"

        print(f"[quick-fix] 更新后科目数: {len(updated_subjects)} (考试:{len(exam_subjects)}, 问卷:{len(questionnaire_subjects)})")

        # 直接UPDATE，避免UPSERT的锁竞争
        db.execute(
            text("""
                UPDATE statistical_aggregations
                SET statistics_data = :data, updated_at = NOW()
                WHERE batch_code = :batch AND aggregation_level = 'REGIONAL'
                AND (school_id = 'REGIONAL' OR school_id IS NULL)
            """),
            {
                "data": json.dumps(existing_data, ensure_ascii=False),
                "batch": batch_code
            }
        )

        db.commit()
        print("[quick-fix] 问卷数据修复完成")


if __name__ == "__main__":
    main()