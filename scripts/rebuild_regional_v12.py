#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
重建指定批次 v1.2 区域汇聚数据（含考试科目维度）
特点
- 不通过 API，直接使用应用内服务与数据库连接重建并入库；生成 subjects 列表时：
  - 问卷科目：输出维度均分 + 维度下题目选项分布（若可得）
  - 考试科目：输出维度均分（依次回退：dimension_scores → 问卷明细(仅问卷) → 学校级维度均分加权回推）
用法
  python scripts/rebuild_regional_v12.py --batch G4-2025 [--include-detail] [--skip-enhanced]

参数
- --batch/-b           批次编码（默认 G4-2025）
- --include-detail     输出区域顶层 detail 字段（p10/p50/p90/grade_distribution 等）
- --skip-enhanced      跳过增强统计（更快；不影响维度均分输出）

运行前提
- 已配置数据库连接（app/database/connection.py）
- 如需问卷维度选项分布，建议提前填充 questionnaire_option_distribution 表
验证
- 成功后，可直接请求：GET /api/v12/batch/{batch}/regional 查看最新内容
"""

from __future__ import annotations
import argparse
import asyncio
import json
from datetime import datetime, timezone

from sqlalchemy import text

# 兼容从任意工作目录直接运行：将项目根目录加入 sys.path
import os, sys
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus
from app.database.repositories import StatisticalAggregationRepository
from app.database.repositories import RepositoryError
from app.services.subjects_builder import SubjectsBuilder
from app.services.question_option_distribution_service import populate_questionnaire_distributions

# 可选：增强统计（区分度/百分位/等级分布等）
# 优先导入标准名；若失败则自动回退到 fixed 版，最后才允许缺省。
get_enhanced_stats_for_regional = None  # type: ignore
_enhanced_provider_name = None
try:
    from scripts.rewrite_subjects_v12_enhanced import (  # type: ignore
        get_enhanced_stats_for_regional,  # noqa: F401
    )
    _enhanced_provider_name = "rewrite_subjects_v12_enhanced"
except Exception:
    try:
        from scripts.rewrite_subjects_v12_enhanced_fixed import (  # type: ignore
            get_enhanced_stats_for_regional,  # noqa: F401
        )
        _enhanced_provider_name = "rewrite_subjects_v12_enhanced_fixed"
    except Exception:
        get_enhanced_stats_for_regional = None  # type: ignore
        _enhanced_provider_name = None


def _compute_totals(db, batch_code: str) -> tuple[int, int]:
    """计算区域级总学校与参与学生数（ACTIVE 学校）"""
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


async def _build_subjects_v12(db, batch_code: str, include_detail: bool, skip_enhanced: bool):
    enhanced_stats = None
    if not skip_enhanced and get_enhanced_stats_for_regional is not None:
        try:
            print(f"[rebuild] 加载增强统计提供者: provider={_enhanced_provider_name}")
            enhanced_stats = await get_enhanced_stats_for_regional(batch_code, db)  # type: ignore
            if enhanced_stats is None:
                print("[rebuild] 增强统计返回空结果，将仅输出基础与维度信息")
        except Exception as e:
            print(f"[rebuild] 增强统计计算失败（继续无增强）：{e}")
            enhanced_stats = None

    sb = SubjectsBuilder()
    subjects = sb.build_regional_subjects_v12(
        batch_code,
        enhanced_stats=enhanced_stats,
        include_detail=include_detail,
    )
    return subjects


def main():
    parser = argparse.ArgumentParser(description="重建 v1.2 区域汇聚（含考试科目维度）")
    parser.add_argument("--batch", "-b", default="G4-2025", help="批次编码，例如 G4-2025")
    parser.add_argument("--include-detail", action="store_true", help="输出区域顶层 detail 字段")
    parser.add_argument("--skip-enhanced", action="store_true", help="跳过增强统计计算")
    args = parser.parse_args()

    batch_code = args.batch
    include_detail = args.include_detail
    skip_enhanced = args.skip_enhanced

    print(
        f"[rebuild] 开始重建区域汇聚 v1.2：batch={batch_code}, include_detail={include_detail}, skip_enhanced={skip_enhanced}"
    )
    if include_detail and skip_enhanced:
        print(
            "[warn] 指定了 include-detail 但同时跳过增强统计，顶层仅输出维度细节，不含 p10/p50/p90、discrimination、grade_distribution。"
        )
    if include_detail and (not skip_enhanced) and _enhanced_provider_name is None:
        print(
            "[warn] include-detail 已启用，但未找到增强统计提供者；将无百分位/区分度/等级占比，仅输出维度细节。"
        )

    with next(get_db()) as db:
        # 提高本会话的锁等待上限，减少大JSON更新时的1205概率
        try:
            db.execute(text("SET SESSION innodb_lock_wait_timeout=120"))
        except Exception:
            pass
        # 先刷新问卷题目选项分布物化表，保证题目级量表映射（含正/反向）已落入 option_label
        try:
            _res = populate_questionnaire_distributions(batch_code)
            _qn_ok = all(
                (
                    v.get("success", True) if isinstance(v, dict) else True
                    for v in (_res.values() if isinstance(_res, dict) else [_res])
                )
            )
            print(f"[rebuild] 问卷题目选项分布刷新完成: success={_qn_ok}")
        except Exception as e:
            print(f"[rebuild] 刷新问卷题目选项分布失败（继续）：{e}")

        # 生成 subjects（异步）
        subjects = asyncio.run(
            _build_subjects_v12(db, batch_code, include_detail, skip_enhanced)
        )

        processed = {
            "schema_version": "v1.2",
            "data_version": "v1.2",
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "subjects": subjects,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        total_schools, total_students = _compute_totals(db, batch_code)

        # 入库（优先UPSERT；如遇持续1205，回退为“删除后插入”）
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
        except RepositoryError as e:
            print(f"[rebuild] UPSERT 遇到锁等待，尝试回退为删除后插入：{e}")
            try:
                # 删除同批次区域记录（REGIONAL 常量 & 兼容历史 NULL）
                db.execute(
                    text(
                        "DELETE FROM statistical_aggregations "
                        "WHERE batch_code=:b AND aggregation_level='REGIONAL' AND (school_id='REGIONAL' OR school_id IS NULL)"
                    ),
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
                        "b": batch_code,
                        "name": "区域汇聚",
                        "data": json.dumps(processed, ensure_ascii=False),
                        "ts": total_students,
                        "tc": total_schools,
                    },
                )
                db.commit()
                print("[rebuild] 已通过删除后插入完成区域数据写入。")
            except Exception as e2:
                print(f"[rebuild] 删除后插入仍失败：{e2}")
                raise

        # 简要输出
        try:
            exam_count = sum(1 for s in subjects if s.get("type") == "exam")
            qn_count = sum(1 for s in subjects if s.get("type") == "questionnaire")
            print(
                f"[rebuild] 完成：subjects={len(subjects)} (exam={exam_count}, questionnaire={qn_count}), total_schools={total_schools}, total_students={total_students}"
            )
            if not skip_enhanced:
                print(
                    f"[rebuild] 增强统计提供者: {(_enhanced_provider_name or '未启用/未找到')}；include_detail={include_detail}"
                )
            # 抽样显示一个考试科目维度情况
            for s in subjects:
                if s.get("type") == "exam" and s.get("dimensions"):
                    dims = s.get("dimensions")
                    print(
                        f"[sample] 考试科目: {s.get('subject_name')} 维度数={len(dims)} 示例维度={dims[0]}"
                    )
                    break
        except Exception:
            pass


if __name__ == "__main__":
    main()
