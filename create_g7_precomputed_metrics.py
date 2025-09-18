#!/usr/bin/env python3
"""
创建G7-2025预聚合表脚本
用于生成subject_core_metrics和subject_school_rankings表
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any
import asyncio

# 添加项目路径到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database.connection import get_db_context
from data_cleaning_service import DataCleaningService

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('g7_precomputed_metrics.log')
    ]
)
logger = logging.getLogger(__name__)


async def create_g7_precomputed_metrics(batch_code: str = "G7-2025") -> Dict[str, Any]:
    """为G7-2025批次创建预聚合表"""

    result = {
        "batch_code": batch_code,
        "success": False,
        "tables_created": [],
        "error": None,
        "metrics": {}
    }

    try:
        logger.info(f"开始为批次 {batch_code} 创建预聚合表...")

        # 使用数据库上下文创建数据清洗服务实例
        with get_db_context() as db:
            cleaning_service = DataCleaningService(db)

            logger.info("正在生成预聚合指标...")
            start_time = datetime.now()

            # 调用数据清洗服务的预聚合方法
            await cleaning_service._materialize_precomputed_metrics(batch_code)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(f"预聚合指标生成完成，耗时: {duration:.2f}秒")

            # 验证生成的数据（使用同一个数据库会话）
            # 检查subject_core_metrics表
            core_metrics_query = """
                SELECT COUNT(*) as count,
                       COUNT(DISTINCT subject_name) as subject_count
                FROM subject_core_metrics
                WHERE batch_code = :batch_code
            """

            core_result = db.execute(core_metrics_query, {"batch_code": batch_code}).fetchone()
            core_count = core_result.count if core_result else 0
            core_subjects = core_result.subject_count if core_result else 0

            # 检查subject_school_rankings表
            rankings_query = """
                SELECT COUNT(*) as count,
                       COUNT(DISTINCT school_code) as school_count,
                       COUNT(DISTINCT subject_name) as subject_count
                FROM subject_school_rankings
                WHERE batch_code = :batch_code
            """

            rankings_result = db.execute(rankings_query, {"batch_code": batch_code}).fetchone()
            rankings_count = rankings_result.count if rankings_result else 0
            rankings_schools = rankings_result.school_count if rankings_result else 0
            rankings_subjects = rankings_result.subject_count if rankings_result else 0

            result.update({
                "success": True,
                "tables_created": ["subject_core_metrics", "subject_school_rankings"],
                "metrics": {
                    "duration_seconds": duration,
                    "core_metrics": {
                        "total_records": core_count,
                        "subject_count": core_subjects
                    },
                    "school_rankings": {
                        "total_records": rankings_count,
                        "school_count": rankings_schools,
                        "subject_count": rankings_subjects
                    }
                }
            })

            logger.info(f"验证结果:")
            logger.info(f"  subject_core_metrics: {core_count}条记录, {core_subjects}个科目")
            logger.info(f"  subject_school_rankings: {rankings_count}条记录, {rankings_schools}个学校, {rankings_subjects}个科目")

            # 获取一些样本数据
            if core_count > 0:
                sample_core_query = """
                    SELECT subject_name, subject_type, student_count,
                           ROUND(avg_score, 2) as avg_score,
                           ROUND(score_rate, 4) as score_rate
                    FROM subject_core_metrics
                    WHERE batch_code = :batch_code
                    ORDER BY subject_name
                    LIMIT 5
                """

                sample_core = db.execute(sample_core_query, {"batch_code": batch_code}).fetchall()

                logger.info("核心指标样本数据:")
                for row in sample_core:
                    logger.info(f"  {row.subject_name} ({row.subject_type}): {row.student_count}学生, 平均分{row.avg_score}, 得分率{row.score_rate}")

            if rankings_count > 0:
                sample_rankings_query = """
                    SELECT subject_name, school_code, student_count,
                           ROUND(avg_score, 2) as avg_score,
                           rank, total_schools
                    FROM subject_school_rankings
                    WHERE batch_code = :batch_code
                    ORDER BY subject_name, rank
                    LIMIT 5
                """

                sample_rankings = db.execute(sample_rankings_query, {"batch_code": batch_code}).fetchall()

                logger.info("学校排名样本数据:")
                for row in sample_rankings:
                    logger.info(f"  {row.subject_name} - {row.school_code}: 第{row.rank}名/{row.total_schools}, {row.student_count}学生, 平均分{row.avg_score}")

    except Exception as e:
        error_msg = f"预聚合表创建失败: {str(e)}"
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
        f"G7-2025预聚合表创建报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 80,
        ""
    ]

    if result["success"]:
        report.extend([
            f"[成功] 成功为批次 {result['batch_code']} 创建预聚合表",
            "",
            "已创建的表:",
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
            f"    - 记录数: {metrics['core_metrics']['total_records']}",
            f"    - 科目数: {metrics['core_metrics']['subject_count']}",
            f"  subject_school_rankings表:",
            f"    - 记录数: {metrics['school_rankings']['total_records']}",
            f"    - 学校数: {metrics['school_rankings']['school_count']}",
            f"    - 科目数: {metrics['school_rankings']['subject_count']}",
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
            f"[失败] 批次 {result['batch_code']} 预聚合表创建失败",
            "",
            f"错误信息: {result['error']}",
            "",
            "[故障排除] 建议:"
            "  1. 检查数据库连接是否正常",
            "  2. 确认G7-2025基础数据是否存在",
            "  3. 检查student_cleaned_scores表是否有数据",
            "  4. 验证school_master_data表是否正确配置",
            "  5. 查看详细错误日志: g7_precomputed_metrics.log"
        ])

    report.extend([
        "",
        "=" * 80
    ])

    return "\n".join(report)


async def main():
    """主函数"""
    try:
        logger.info("开始创建G7-2025预聚合表...")

        # 创建预聚合表
        result = await create_g7_precomputed_metrics("G7-2025")

        # 生成并保存报告
        report = generate_summary_report(result)

        report_file = f"g7_precomputed_metrics_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)

        logger.info(f"报告已保存到: {report_file}")
        print(report)

        # 返回适当的退出码
        if result["success"]:
            logger.info("预聚合表创建成功完成")
            sys.exit(0)
        else:
            logger.error("预聚合表创建失败")
            sys.exit(1)

    except Exception as e:
        logger.error(f"脚本执行失败: {str(e)}")
        sys.exit(2)


if __name__ == "__main__":
    asyncio.run(main())