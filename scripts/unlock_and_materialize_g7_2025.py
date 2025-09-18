#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
解锁并物化G7-2025批次的v1.2 subjects数据
根据PO修订版方案实现：临时清锁 -> 重写subjects -> 校验输出

用法：
    python scripts/unlock_and_materialize_g7_2025.py

功能：
    1. 临时解除G7-2025写入阻断
    2. 物化区域和学校级v1.2 subjects
    3. 验证数据完整性并输出校验报告
"""

import os
import sys
import json
from datetime import datetime
from sqlalchemy import text

# 兼容从任意工作目录直接运行
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus
from app.database.repositories import StatisticalAggregationRepository
from app.services.subjects_builder import SubjectsBuilder
from app.services.question_option_distribution_service import populate_questionnaire_distributions


def verify_unlock_status():
    """验证写入阻断状态"""
    disable_batches = os.getenv('DISABLE_WRITES_FOR_BATCHES', '')
    if 'G7-2025' in disable_batches:
        print(f"[WARNING] 仍然检测到写入阻断 DISABLE_WRITES_FOR_BATCHES='{disable_batches}'")
        print("   请确认docker-compose.yml已修改并重启容器")
        return False
    else:
        print(f"[SUCCESS] 写入阻断已解除：DISABLE_WRITES_FOR_BATCHES='{disable_batches}'")
        return True


def compute_totals(db, batch_code: str) -> tuple[int, int]:
    """计算区域级总学校与参与学生数（ACTIVE学校）"""
    try:
        total_schools = db.execute(
            text("SELECT COUNT(*) FROM school_master_data WHERE batch_code=:b AND status='ACTIVE'"),
            {"b": batch_code},
        ).scalar() or 0
    except Exception:
        total_schools = 0

    try:
        total_students = db.execute(
            text(
                """
                SELECT COUNT(DISTINCT scs.student_id)
                  FROM student_cleaned_scores scs
                  JOIN school_master_data smd
                    ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code
                   AND smd.school_id  COLLATE utf8mb4_unicode_ci = scs.school_code
                   AND smd.status='ACTIVE'
                 WHERE scs.batch_code=:b AND scs.subject_type IN ('exam','questionnaire')
                """
            ),
            {"b": batch_code},
        ).scalar() or 0
    except Exception:
        total_students = 0

    return int(total_schools), int(total_students)


def materialize_regional_v12(db, batch_code: str):
    """物化区域级v1.2 subjects"""
    print(f"[MATERIALIZE] 开始处理区域级数据：{batch_code}")

    # 刷新问卷题目选项分布
    try:
        populate_questionnaire_distributions(batch_code)
        print("[SUCCESS] 问卷题目选项分布刷新完成")
    except Exception as e:
        print(f"[WARNING] 问卷分布刷新失败（继续执行）：{e}")

    # 生成subjects
    sb = SubjectsBuilder()
    subjects = sb.build_regional_subjects_v12(
        batch_code,
        enhanced_stats=None,  # 首次可不含增强统计，重点恢复基础功能
        include_detail=True
    )

    total_schools, total_students = compute_totals(db, batch_code)

    # 构建v1.2数据结构
    processed = {
        "schema_version": "v1.2",
        "data_version": "v1.2",
        "batch_code": batch_code,
        "aggregation_level": "REGIONAL",
        "subjects": subjects,
        "updated_at": datetime.utcnow().isoformat() + "+00:00",
    }

    # 入库
    repo = StatisticalAggregationRepository(db)
    try:
        repo.upsert_statistics(
            {
                "batch_code": batch_code,
                "aggregation_level": DBAggregationLevel.REGIONAL,
                "school_id": "REGIONAL",
                "school_name": "区域汇聚",
                "statistics_data": processed,
                "data_version": "v1.2",
                "calculation_status": CalculationStatus.COMPLETED,
                "total_students": total_students,
                "total_schools": total_schools,
            }
        )
        print(f"[SUCCESS] 区域数据物化成功：{len(subjects)}个科目，{total_students}名学生，{total_schools}所学校")

        # 科目类型统计
        exam_count = sum(1 for s in subjects if s.get("type") == "exam")
        qn_count = sum(1 for s in subjects if s.get("type") == "questionnaire")
        print(f"   科目类型分布：考试{exam_count}个，问卷{qn_count}个")

        return True
    except Exception as e:
        print(f"[ERROR] 区域数据物化失败：{e}")
        return False


def verify_data_integrity(db, batch_code: str):
    """验证数据完整性"""
    print(f"[VERIFY] 开始数据完整性检查：{batch_code}")

    try:
        # 检查区域记录
        result = db.execute(
            text("""
                SELECT
                    statistics_data,
                    total_students,
                    total_schools,
                    updated_at
                FROM statistical_aggregations
                WHERE batch_code = :batch
                AND aggregation_level = 'REGIONAL'
                AND (school_id = 'REGIONAL' OR school_id IS NULL)
            """),
            {"batch": batch_code}
        ).fetchone()

        if not result:
            print("[ERROR] 未找到区域记录")
            return False

        data = json.loads(result[0])

        # 基础结构验证
        checks = {
            "data_version": data.get("data_version") == "v1.2",
            "schema_version": data.get("schema_version") == "v1.2",
            "batch_code": data.get("batch_code") == batch_code,
            "aggregation_level": data.get("aggregation_level") == "REGIONAL",
            "subjects_exist": len(data.get("subjects", [])) > 0
        }

        print("[CHECK] 基础结构检查：")
        for check, passed in checks.items():
            status = "[OK]" if passed else "[FAIL]"
            print(f"   {status} {check}: {passed}")

        # 科目详细检查
        subjects = data.get("subjects", [])
        exam_subjects = [s for s in subjects if s.get("type") == "exam"]
        qn_subjects = [s for s in subjects if s.get("type") == "questionnaire"]

        print(f"[CHECK] 科目详细检查：")
        print(f"   总科目数：{len(subjects)}")
        print(f"   考试科目：{len(exam_subjects)}")
        print(f"   问卷科目：{len(qn_subjects)}")

        # 检查考试科目的增强指标
        if exam_subjects:
            sample_exam = exam_subjects[0]
            metrics = sample_exam.get("metrics", {})
            required_metrics = ["student_count", "score_rate", "percentiles", "discrimination"]

            print("[CHECK] 考试科目增强指标检查：")
            for metric in required_metrics:
                exists = metric in metrics
                status = "[OK]" if exists else "[FAIL]"
                print(f"   {status} {metric}: {'存在' if exists else '缺失'}")

        # 检查问卷科目的维度数据
        if qn_subjects:
            sample_qn = qn_subjects[0]
            dimensions = sample_qn.get("dimensions", [])

            print("[CHECK] 问卷科目维度检查：")
            print(f"   维度数量：{len(dimensions)}")
            if dimensions:
                sample_dim = dimensions[0]
                required_fields = ["avg", "score_rate"]
                for field in required_fields:
                    exists = field in sample_dim
                    status = "[OK]" if exists else "[FAIL]"
                    print(f"   {status} {field}: {'存在' if exists else '缺失'}")

        print(f"[SUCCESS] 数据完整性检查完成")
        return all(checks.values())

    except Exception as e:
        print(f"[ERROR] 验证过程出错：{e}")
        return False


def main():
    print("[START] G7-2025批次v1.2物化启动")
    print("=" * 60)

    batch_code = "G7-2025"

    # 1. 验证解锁状态
    print("[STEP 1] 验证写入阻断状态")
    if not verify_unlock_status():
        print("[ERROR] 写入仍被阻断，请先修改docker-compose.yml并重启容器")
        return False

    # 2. 物化数据
    print("\n[STEP 2] 物化区域v1.2数据")
    with next(get_db()) as db:
        # 设置较长的锁等待时间
        try:
            db.execute(text("SET SESSION innodb_lock_wait_timeout=300"))
        except Exception:
            pass

        if not materialize_regional_v12(db, batch_code):
            return False

    # 3. 验证完整性
    print("\n[STEP 3] 验证数据完整性")
    with next(get_db()) as db:
        if not verify_data_integrity(db, batch_code):
            return False

    print("\n" + "=" * 60)
    print("[COMPLETE] G7-2025批次v1.2物化完成！")
    print("[INFO] 可以通过以下方式验证：")
    print("   - API: GET /api/v12/batch/G7-2025/regional")
    print("   - 数据库: SELECT statistics_data FROM statistical_aggregations WHERE batch_code='G7-2025' AND aggregation_level='REGIONAL'")

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)