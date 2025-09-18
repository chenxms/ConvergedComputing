#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
仅重构区域汇聚中的问卷科目数据
用于解决锁竞争时的针对性修复
"""

from __future__ import annotations
import argparse
import json
from datetime import datetime, timezone
from sqlalchemy import text

import os, sys
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db
from app.database.repositories import StatisticalAggregationRepository
from app.services.subjects_builder import SubjectsBuilder


def main():
    parser = argparse.ArgumentParser(description="仅重构区域汇聚问卷科目数据")
    parser.add_argument("--batch", "-b", default="G7-2025", help="批次编码")
    args = parser.parse_args()

    batch_code = args.batch
    print(f"[questionnaire-only] 开始重构批次 {batch_code} 的问卷科目数据")

    with next(get_db()) as db:
        # 提高锁等待上限
        db.execute(text("SET SESSION innodb_lock_wait_timeout=120"))

        # 获取现有区域数据
        repo = StatisticalAggregationRepository(db)
        existing_data = None

        try:
            result = db.execute(
                text("""
                    SELECT statistics_data
                    FROM statistical_aggregations
                    WHERE batch_code=:batch AND aggregation_level='REGIONAL'
                    AND (school_id='REGIONAL' OR school_id IS NULL)
                """),
                {"batch": batch_code}
            ).fetchone()

            if result:
                existing_data = json.loads(result[0])
                print(f"[questionnaire-only] 找到现有区域数据，科目数: {len(existing_data.get('subjects', []))}")
            else:
                print("[questionnaire-only] 未找到现有区域数据，将创建新记录")
        except Exception as e:
            print(f"[questionnaire-only] 读取现有数据失败: {e}")

        # 构建仅问卷科目的数据
        sb = SubjectsBuilder()
        questionnaire_subjects = []

        # 直接构建问卷科目（跳过考试科目）
        try:
            all_subjects = sb.build_regional_subjects_v12(
                batch_code,
                enhanced_stats=None,
                include_detail=False
            )

            questionnaire_subjects = [
                s for s in all_subjects
                if s.get("type") == "questionnaire"
            ]

            print(f"[questionnaire-only] 构建的问卷科目数: {len(questionnaire_subjects)}")

        except Exception as e:
            print(f"[questionnaire-only] 构建问卷科目失败: {e}")
            return

        # 如果有现有数据，合并考试科目
        if existing_data and existing_data.get('subjects'):
            exam_subjects = [
                s for s in existing_data['subjects']
                if s.get("type") == "exam"
            ]

            # 合并：保留考试科目，更新问卷科目
            combined_subjects = exam_subjects + questionnaire_subjects
            print(f"[questionnaire-only] 合并后科目数: {len(combined_subjects)} (考试:{len(exam_subjects)}, 问卷:{len(questionnaire_subjects)})")

        else:
            combined_subjects = questionnaire_subjects

        # 构建更新数据
        updated_data = {
            "schema_version": "v1.2",
            "data_version": "v1.2",
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "subjects": combined_subjects,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        # 直接更新问卷相关数据
        try:
            db.execute(
                text("""
                    UPDATE statistical_aggregations
                    SET statistics_data = :data, updated_at = NOW()
                    WHERE batch_code = :batch AND aggregation_level = 'REGIONAL'
                    AND (school_id = 'REGIONAL' OR school_id IS NULL)
                """),
                {
                    "data": json.dumps(updated_data, ensure_ascii=False),
                    "batch": batch_code
                }
            )

            db.commit()
            print(f"[questionnaire-only] 问卷科目数据更新完成")

        except Exception as e:
            print(f"[questionnaire-only] 更新失败: {e}")
            raise


if __name__ == "__main__":
    main()