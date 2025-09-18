#!/usr/bin/env python3
"""
修复G7-2025预聚合表创建脚本
直接执行修正的SQL，添加必需的时间戳字段
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any

# 添加项目路径到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database.connection import get_db_context
from sqlalchemy import text

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fix_g7_precomputed_tables.log')
    ]
)
logger = logging.getLogger(__name__)


def fix_g7_precomputed_tables(batch_code: str = "G7-2025") -> Dict[str, Any]:
    """修复G7-2025预聚合表创建"""

    result = {
        "batch_code": batch_code,
        "success": False,
        "tables_created": [],
        "error": None,
        "metrics": {}
    }

    try:
        logger.info(f"开始修复批次 {batch_code} 的预聚合表...")

        with get_db_context() as db:
            # 1. 首先确保表存在
            logger.info("创建预聚合表结构...")

            create_core_metrics_table = text("""
                CREATE TABLE IF NOT EXISTS subject_core_metrics (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    batch_code VARCHAR(50) NOT NULL COMMENT '批次代码',
                    subject_name VARCHAR(100) NOT NULL COMMENT '科目名称',
                    subject_type VARCHAR(32) NOT NULL COMMENT '科目类型',
                    student_count INT NOT NULL COMMENT '学生数量',
                    avg_score DECIMAL(10, 4) DEFAULT NULL COMMENT '平均分',
                    std_score DECIMAL(10, 4) DEFAULT NULL COMMENT '标准差',
                    max_score_achieved DECIMAL(10, 4) DEFAULT NULL COMMENT '最高得分',
                    min_score DECIMAL(10, 4) DEFAULT NULL COMMENT '最低得分',
                    max_score DECIMAL(10, 4) DEFAULT NULL COMMENT '满分',
                    score_rate DECIMAL(10, 4) DEFAULT NULL COMMENT '得分率(%)',
                    difficulty_coefficient DECIMAL(10, 4) DEFAULT NULL COMMENT '难度系数',
                    pass_rate DECIMAL(10, 4) DEFAULT NULL COMMENT '及格率',
                    excellent_rate DECIMAL(10, 4) DEFAULT NULL COMMENT '优秀率',
                    good_rate DECIMAL(10, 4) DEFAULT NULL COMMENT '良好率',
                    fail_rate DECIMAL(10, 4) DEFAULT NULL COMMENT '不及格率',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    PRIMARY KEY (id),
                    UNIQUE KEY uk_subject_core_metrics_batch_subject (batch_code, subject_name),
                    KEY idx_subject_core_metrics_lookup (batch_code, subject_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='科目核心统计指标缓存表'
            """)

            create_school_rankings_table = text("""
                CREATE TABLE IF NOT EXISTS subject_school_rankings (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    batch_code VARCHAR(50) NOT NULL COMMENT '批次代码',
                    subject_name VARCHAR(100) NOT NULL COMMENT '科目名称',
                    subject_type VARCHAR(32) NOT NULL COMMENT '科目类型',
                    school_code VARCHAR(50) NOT NULL COMMENT '学校代码',
                    school_name VARCHAR(200) DEFAULT NULL COMMENT '学校名称',
                    student_count INT NOT NULL COMMENT '学生数量',
                    avg_score DECIMAL(10, 4) DEFAULT NULL COMMENT '平均分',
                    std_score DECIMAL(10, 4) DEFAULT NULL COMMENT '标准差',
                    max_score_achieved DECIMAL(10, 4) DEFAULT NULL COMMENT '最高得分',
                    min_score DECIMAL(10, 4) DEFAULT NULL COMMENT '最低得分',
                    max_score DECIMAL(10, 4) DEFAULT NULL COMMENT '满分',
                    score_rate DECIMAL(10, 4) DEFAULT NULL COMMENT '得分率(%)',
                    difficulty_coefficient DECIMAL(10, 4) DEFAULT NULL COMMENT '难度系数',
                    `rank` INT DEFAULT NULL COMMENT '排名',
                    total_schools INT DEFAULT NULL COMMENT '总学校数',
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    PRIMARY KEY (id),
                    UNIQUE KEY uk_subject_school_rankings (batch_code, subject_name, school_code),
                    KEY idx_subject_school_rankings_lookup (batch_code, subject_name, school_code)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='科目学校排名缓存表'
            """)

            db.execute(create_core_metrics_table)
            db.execute(create_school_rankings_table)
            logger.info("表结构创建完成")

            # 2. 清理现有数据
            logger.info(f"清理批次 {batch_code} 的历史数据...")
            delete_core = text("DELETE FROM subject_core_metrics WHERE batch_code = :batch")
            delete_school = text("DELETE FROM subject_school_rankings WHERE batch_code = :batch")

            db.execute(delete_core, {"batch": batch_code})
            db.execute(delete_school, {"batch": batch_code})

            # 3. 生成核心指标数据（修正版，包含created_at和updated_at）
            logger.info("生成科目核心指标数据...")
            start_time = datetime.now()

            core_insert_sql = text("""
                INSERT INTO subject_core_metrics (
                    batch_code, subject_name, subject_type, student_count,
                    avg_score, std_score, max_score_achieved, min_score,
                    max_score, score_rate, difficulty_coefficient,
                    pass_rate, excellent_rate, good_rate, fail_rate,
                    created_at, updated_at
                )
                SELECT
                    scs.batch_code,
                    scs.subject_name,
                    scs.subject_type,
                    COUNT(*) AS student_count,
                    ROUND(AVG(scs.total_score), 4) AS avg_score,
                    ROUND(STDDEV_POP(scs.total_score), 4) AS std_score,
                    ROUND(MAX(scs.total_score), 4) AS max_score_achieved,
                    ROUND(MIN(scs.total_score), 4) AS min_score,
                    ROUND(MAX(scs.max_score), 4) AS max_score,
                    ROUND(AVG(CASE WHEN scs.max_score > 0 THEN scs.total_score / scs.max_score ELSE 0 END) * 100, 4) AS score_rate,
                    ROUND(AVG(CASE WHEN scs.max_score > 0 THEN scs.total_score / scs.max_score ELSE 0 END), 4) AS difficulty_coefficient,
                    NULL AS pass_rate,
                    NULL AS excellent_rate,
                    NULL AS good_rate,
                    NULL AS fail_rate,
                    NOW() AS created_at,
                    NOW() AS updated_at
                FROM student_cleaned_scores scs
                JOIN school_master_data smd
                  ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
                 AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
                 AND smd.status = 'ACTIVE'
                WHERE scs.batch_code = :batch
                GROUP BY scs.batch_code, scs.subject_name, scs.subject_type
            """)

            core_result = db.execute(core_insert_sql, {"batch": batch_code})
            core_count = core_result.rowcount

            # 4. 生成学校排名数据（修正版，包含created_at和updated_at）
            logger.info("生成学校排名数据...")

            school_insert_sql = text("""
                INSERT INTO subject_school_rankings (
                    batch_code, subject_name, subject_type,
                    school_code, school_name, student_count,
                    avg_score, std_score, max_score_achieved, min_score, max_score,
                    score_rate, difficulty_coefficient,
                    `rank`, total_schools,
                    created_at, updated_at
                )
                SELECT
                    base.batch_code,
                    base.subject_name,
                    base.subject_type,
                    base.school_code,
                    base.school_name,
                    base.student_count,
                    ROUND(base.avg_score, 4) AS avg_score,
                    ROUND(base.std_score, 4) AS std_score,
                    ROUND(base.max_score_achieved, 4) AS max_score_achieved,
                    ROUND(base.min_score, 4) AS min_score,
                    ROUND(base.max_score, 4) AS max_score,
                    ROUND(base.score_ratio * 100, 4) AS score_rate,
                    ROUND(base.score_ratio, 4) AS difficulty_coefficient,
                    DENSE_RANK() OVER (PARTITION BY base.subject_name ORDER BY base.avg_score DESC, base.school_code ASC) AS ranking,
                    COUNT(*) OVER (PARTITION BY base.subject_name) AS total_schools,
                    NOW() AS created_at,
                    NOW() AS updated_at
                FROM (
                    SELECT
                        scs.batch_code,
                        scs.subject_name,
                        scs.subject_type,
                        scs.school_code,
                        COALESCE(smd.standard_school_name, scs.school_code) AS school_name,
                        COUNT(*) AS student_count,
                        AVG(scs.total_score) AS avg_score,
                        STDDEV_POP(scs.total_score) AS std_score,
                        MAX(scs.total_score) AS max_score_achieved,
                        MIN(scs.total_score) AS min_score,
                        MAX(scs.max_score) AS max_score,
                        AVG(CASE WHEN scs.max_score > 0 THEN scs.total_score / scs.max_score ELSE 0 END) AS score_ratio
                    FROM student_cleaned_scores scs
                    JOIN school_master_data smd
                      ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
                     AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
                     AND smd.status = 'ACTIVE'
                    WHERE scs.batch_code = :batch
                    GROUP BY
                        scs.batch_code, scs.subject_name, scs.subject_type, scs.school_code, smd.standard_school_name
                ) AS base
            """)

            school_result = db.execute(school_insert_sql, {"batch": batch_code})
            school_count = school_result.rowcount

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(f"预聚合表数据生成完成，耗时: {duration:.2f}秒")
            logger.info(f"  核心指标表: {core_count}条记录")
            logger.info(f"  学校排名表: {school_count}条记录")

            # 5. 验证生成的数据
            core_verify_query = text("""
                SELECT COUNT(*) as count,
                       COUNT(DISTINCT subject_name) as subject_count
                FROM subject_core_metrics
                WHERE batch_code = :batch_code
            """)

            core_verify = db.execute(core_verify_query, {"batch_code": batch_code}).fetchone()

            rankings_verify_query = text("""
                SELECT COUNT(*) as count,
                       COUNT(DISTINCT school_code) as school_count,
                       COUNT(DISTINCT subject_name) as subject_count
                FROM subject_school_rankings
                WHERE batch_code = :batch_code
            """)

            rankings_verify = db.execute(rankings_verify_query, {"batch_code": batch_code}).fetchone()

            result.update({
                "success": True,
                "tables_created": ["subject_core_metrics", "subject_school_rankings"],
                "metrics": {
                    "duration_seconds": duration,
                    "core_metrics": {
                        "insert_count": core_count,
                        "verify_count": core_verify.count if core_verify else 0,
                        "subject_count": core_verify.subject_count if core_verify else 0
                    },
                    "school_rankings": {
                        "insert_count": school_count,
                        "verify_count": rankings_verify.count if rankings_verify else 0,
                        "school_count": rankings_verify.school_count if rankings_verify else 0,
                        "subject_count": rankings_verify.subject_count if rankings_verify else 0
                    }
                }
            })

            # 6. 展示样本数据
            if core_verify and core_verify.count > 0:
                sample_core_query = text("""
                    SELECT subject_name, subject_type, student_count,
                           ROUND(avg_score, 2) as avg_score,
                           ROUND(score_rate, 4) as score_rate
                    FROM subject_core_metrics
                    WHERE batch_code = :batch_code
                    ORDER BY subject_name
                    LIMIT 5
                """)

                sample_core = db.execute(sample_core_query, {"batch_code": batch_code}).fetchall()

                logger.info("核心指标样本数据:")
                for row in sample_core:
                    logger.info(f"  {row.subject_name} ({row.subject_type}): {row.student_count}学生, 平均分{row.avg_score}, 得分率{row.score_rate}%")

            if rankings_verify and rankings_verify.count > 0:
                sample_rankings_query = text("""
                    SELECT subject_name, school_code, student_count,
                           ROUND(avg_score, 2) as avg_score,
                           `rank`, total_schools
                    FROM subject_school_rankings
                    WHERE batch_code = :batch_code
                    ORDER BY subject_name, `rank`
                    LIMIT 5
                """)

                sample_rankings = db.execute(sample_rankings_query, {"batch_code": batch_code}).fetchall()

                logger.info("学校排名样本数据:")
                for row in sample_rankings:
                    logger.info(f"  {row.subject_name} - {row.school_code}: 第{row.rank}名/{row.total_schools}, {row.student_count}学生, 平均分{row.avg_score}")

    except Exception as e:
        error_msg = f"预聚合表修复失败: {str(e)}"
        logger.error(error_msg)
        result.update({
            "success": False,
            "error": error_msg
        })

        import traceback
        logger.error(f"详细错误信息:\n{traceback.format_exc()}")

    return result


def generate_summary_report(result: Dict[str, Any]) -> str:
    """生成总结报告"""

    report = [
        "=" * 80,
        f"G7-2025预聚合表修复报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 80,
        ""
    ]

    if result["success"]:
        report.extend([
            f"[成功] 成功为批次 {result['batch_code']} 修复预聚合表",
            "",
            "已修复的表:",
        ])

        for table in result["tables_created"]:
            report.append(f"  - {table}")

        metrics = result["metrics"]
        report.extend([
            "",
            "生成统计:",
            f"  执行时间: {metrics['duration_seconds']:.2f}秒",
            "",
            "数据验证:",
            f"  subject_core_metrics表:",
            f"    - 插入记录: {metrics['core_metrics']['insert_count']}",
            f"    - 验证记录: {metrics['core_metrics']['verify_count']}",
            f"    - 科目数量: {metrics['core_metrics']['subject_count']}",
            f"  subject_school_rankings表:",
            f"    - 插入记录: {metrics['school_rankings']['insert_count']}",
            f"    - 验证记录: {metrics['school_rankings']['verify_count']}",
            f"    - 学校数量: {metrics['school_rankings']['school_count']}",
            f"    - 科目数量: {metrics['school_rankings']['subject_count']}",
            ""
        ])

        report.extend([
            "[下一步] 操作建议:",
            "  1. 重新运行影子库状态检查脚本验证结果:",
            "     python check_shadow_db_status.py",
            "  2. 测试API接口是否能正常使用预聚合数据:",
            "     python test_v12_api.py",
            "  3. 如果一切正常，可以开始使用影子库进行测试"
        ])

    else:
        report.extend([
            f"[失败] 批次 {result['batch_code']} 预聚合表修复失败",
            "",
            f"错误信息: {result['error']}",
            "",
            "[故障排除] 建议:",
            "  1. 检查数据库连接是否正常",
            "  2. 确认G7-2025基础数据是否存在",
            "  3. 检查student_cleaned_scores表是否有数据",
            "  4. 验证school_master_data表是否正确配置",
            "  5. 查看详细错误日志: fix_g7_precomputed_tables.log"
        ])

    report.extend([
        "",
        "=" * 80
    ])

    return "\n".join(report)


def main():
    """主函数"""
    try:
        logger.info("开始修复G7-2025预聚合表...")

        # 修复预聚合表
        result = fix_g7_precomputed_tables("G7-2025")

        # 生成并保存报告
        report = generate_summary_report(result)

        report_file = f"fix_g7_precomputed_tables_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)

        logger.info(f"报告已保存到: {report_file}")
        print(report)

        # 返回适当的退出码
        if result["success"]:
            logger.info("预聚合表修复成功完成")
            sys.exit(0)
        else:
            logger.error("预聚合表修复失败")
            sys.exit(1)

    except Exception as e:
        logger.error(f"脚本执行失败: {str(e)}")
        sys.exit(2)


if __name__ == "__main__":
    main()