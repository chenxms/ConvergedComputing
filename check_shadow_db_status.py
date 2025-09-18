#!/usr/bin/env python3
"""
影子库环境状态检查脚本
用于验证G7-2025数据和预聚合表的存在情况
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# 添加项目路径到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
from app.database.connection import get_db_context, test_connection, check_database_health


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('shadow_db_check.log')
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class DatabaseCheckResult:
    """数据库检查结果"""
    check_name: str
    status: str  # "PASS", "FAIL", "WARNING"
    message: str
    data: Optional[Dict[str, Any]] = None


class ShadowDatabaseChecker:
    """影子库状态检查器"""

    def __init__(self):
        self.results: List[DatabaseCheckResult] = []
        self.batch_code = "G7-2025"

    def add_result(self, check_name: str, status: str, message: str, data: Optional[Dict] = None):
        """添加检查结果"""
        result = DatabaseCheckResult(check_name, status, message, data)
        self.results.append(result)

        if status == "PASS":
            logger.info(f"[PASS] {check_name}: {message}")
        elif status == "WARNING":
            logger.warning(f"[WARNING] {check_name}: {message}")
        else:
            logger.error(f"[FAIL] {check_name}: {message}")

    def check_env_configuration(self) -> bool:
        """检查环境配置"""
        try:
            env_file = ".env"
            env_example = ".env.example"

            if not os.path.exists(env_file):
                if os.path.exists(env_example):
                    # 复制示例配置文件
                    with open(env_example, 'r', encoding='utf-8') as f:
                        content = f.read()
                    with open(env_file, 'w', encoding='utf-8') as f:
                        f.write(content)
                    self.add_result(
                        "环境配置检查",
                        "WARNING",
                        f"已从{env_example}创建{env_file}，请检查并修改数据库连接配置"
                    )
                    return False
                else:
                    self.add_result(
                        "环境配置检查",
                        "FAIL",
                        f"缺少{env_file}和{env_example}文件"
                    )
                    return False

            # 检查关键环境变量
            database_url = os.getenv("DATABASE_URL")
            if database_url:
                # 检查是否是影子库
                if "appraisal_test" in database_url or "shadow" in database_url.lower():
                    self.add_result(
                        "环境配置检查",
                        "PASS",
                        f"检测到影子库配置: {database_url.split('@')[1] if '@' in database_url else database_url}"
                    )
                else:
                    self.add_result(
                        "环境配置检查",
                        "WARNING",
                        f"数据库配置可能不是影子库: {database_url.split('@')[1] if '@' in database_url else database_url}"
                    )
            else:
                # 检查各组件配置
                db_host = os.getenv("DATABASE_HOST", "117.72.14.166")
                db_name = os.getenv("DATABASE_NAME", "appraisal_test")
                self.add_result(
                    "环境配置检查",
                    "PASS",
                    f"使用组件配置: {db_host}/{db_name}"
                )

            return True

        except Exception as e:
            self.add_result(
                "环境配置检查",
                "FAIL",
                f"环境配置检查失败: {str(e)}"
            )
            return False

    def check_database_connection(self) -> bool:
        """检查数据库连接"""
        try:
            # 测试基本连接
            connection_ok = test_connection()
            if not connection_ok:
                self.add_result(
                    "数据库连接检查",
                    "FAIL",
                    "数据库连接失败"
                )
                return False

            # 获取详细健康状态
            health_info = check_database_health()
            if health_info.get("status") == "healthy":
                response_time = health_info.get("response_time_ms", 0)
                pool_info = health_info.get("pool_info", {})

                self.add_result(
                    "数据库连接检查",
                    "PASS",
                    f"连接正常 (响应时间: {response_time:.2f}ms)",
                    {
                        "response_time_ms": response_time,
                        "pool_info": pool_info
                    }
                )
                return True
            else:
                self.add_result(
                    "数据库连接检查",
                    "FAIL",
                    f"数据库健康检查失败: {health_info.get('error', '未知错误')}"
                )
                return False

        except Exception as e:
            self.add_result(
                "数据库连接检查",
                "FAIL",
                f"连接检查异常: {str(e)}"
            )
            return False

    def check_g7_basic_data(self) -> Dict[str, Any]:
        """检查G7-2025基础数据"""
        g7_data = {
            "student_count": 0,
            "school_count": 0,
            "subject_count": 0,
            "question_count": 0,
            "has_data": False
        }

        try:
            with get_db_context() as db:
                # 检查学生答题数据
                student_query = text("""
                    SELECT COUNT(DISTINCT student_id) as student_count,
                           COUNT(DISTINCT school_id) as school_count,
                           COUNT(DISTINCT subject_id) as subject_count,
                           COUNT(*) as total_records
                    FROM student_score_detail
                    WHERE batch_code = :batch_code
                """)

                result = db.execute(student_query, {"batch_code": self.batch_code}).fetchone()
                if result:
                    g7_data.update({
                        "student_count": result.student_count,
                        "school_count": result.school_count,
                        "subject_count": result.subject_count,
                        "total_records": result.total_records,
                        "has_data": result.total_records > 0
                    })

                # 检查题目配置数据
                question_query = text("""
                    SELECT COUNT(DISTINCT question_id) as question_count,
                           COUNT(DISTINCT subject_id) as config_subject_count
                    FROM subject_question_config
                    WHERE batch_code = :batch_code
                """)

                result = db.execute(question_query, {"batch_code": self.batch_code}).fetchone()
                if result:
                    g7_data["question_count"] = result.question_count
                    g7_data["config_subject_count"] = result.config_subject_count

                # 检查具体的批次信息
                batch_info_query = text("""
                    SELECT subject_id, COUNT(DISTINCT student_id) as students,
                           COUNT(DISTINCT school_id) as schools,
                           COUNT(*) as records
                    FROM student_score_detail
                    WHERE batch_code = :batch_code
                    GROUP BY subject_id
                    ORDER BY subject_id
                """)

                batch_results = db.execute(batch_info_query, {"batch_code": self.batch_code}).fetchall()
                g7_data["subject_details"] = [
                    {
                        "subject_id": row.subject_id,
                        "student_count": row.students,
                        "school_count": row.schools,
                        "record_count": row.records
                    }
                    for row in batch_results
                ]

        except Exception as e:
            self.add_result(
                "G7-2025基础数据检查",
                "FAIL",
                f"数据查询失败: {str(e)}"
            )
            return g7_data

        # 分析结果
        if g7_data["has_data"]:
            self.add_result(
                "G7-2025基础数据检查",
                "PASS",
                f"发现G7-2025数据 - 学生:{g7_data['student_count']}, 学校:{g7_data['school_count']}, 科目:{g7_data['subject_count']}",
                g7_data
            )
        else:
            self.add_result(
                "G7-2025基础数据检查",
                "FAIL",
                "未发现G7-2025批次数据"
            )

        return g7_data

    def check_precomputed_tables(self) -> Dict[str, Any]:
        """检查预聚合表状态"""
        tables_info = {}

        try:
            with get_db_context() as db:
                # 检查表是否存在
                tables_to_check = [
                    "subject_core_metrics",
                    "subject_school_rankings"
                ]

                for table_name in tables_to_check:
                    table_info = {"exists": False, "row_count": 0, "g7_row_count": 0}

                    # 检查表是否存在
                    check_table_query = text("""
                        SELECT COUNT(*) as count
                        FROM information_schema.tables
                        WHERE table_schema = DATABASE()
                        AND table_name = :table_name
                    """)

                    result = db.execute(check_table_query, {"table_name": table_name}).fetchone()
                    table_exists = result.count > 0 if result else False
                    table_info["exists"] = table_exists

                    if table_exists:
                        # 检查总行数
                        count_query = text(f"SELECT COUNT(*) as count FROM {table_name}")
                        result = db.execute(count_query).fetchone()
                        table_info["row_count"] = result.count if result else 0

                        # 检查G7-2025数据行数
                        g7_count_query = text(f"""
                            SELECT COUNT(*) as count
                            FROM {table_name}
                            WHERE batch_code = :batch_code
                        """)
                        result = db.execute(g7_count_query, {"batch_code": self.batch_code}).fetchone()
                        table_info["g7_row_count"] = result.count if result else 0

                        # 获取样本数据
                        if table_info["g7_row_count"] > 0:
                            sample_query = text(f"""
                                SELECT * FROM {table_name}
                                WHERE batch_code = :batch_code
                                LIMIT 3
                            """)
                            sample_results = db.execute(sample_query, {"batch_code": self.batch_code}).fetchall()
                            table_info["sample_data"] = [dict(row._mapping) for row in sample_results]

                    tables_info[table_name] = table_info

                    # 添加检查结果
                    if table_exists:
                        if table_info["g7_row_count"] > 0:
                            self.add_result(
                                f"{table_name}表检查",
                                "PASS",
                                f"表存在且包含G7-2025数据 (总行数:{table_info['row_count']}, G7行数:{table_info['g7_row_count']})"
                            )
                        else:
                            self.add_result(
                                f"{table_name}表检查",
                                "WARNING",
                                f"表存在但无G7-2025数据 (总行数:{table_info['row_count']})"
                            )
                    else:
                        self.add_result(
                            f"{table_name}表检查",
                            "FAIL",
                            "表不存在"
                        )

        except Exception as e:
            self.add_result(
                "预聚合表检查",
                "FAIL",
                f"预聚合表检查失败: {str(e)}"
            )

        return tables_info

    def check_table_structures(self) -> Dict[str, Any]:
        """检查关键表结构"""
        structure_info = {}

        try:
            with get_db_context() as db:
                tables_to_check = [
                    "student_score_detail",
                    "subject_question_config",
                    "question_dimension_mapping",
                    "grade_aggregation_main"
                ]

                for table_name in tables_to_check:
                    try:
                        # 获取表结构
                        structure_query = text(f"DESCRIBE {table_name}")
                        columns = db.execute(structure_query).fetchall()

                        structure_info[table_name] = {
                            "exists": True,
                            "columns": [
                                {
                                    "name": col.Field,
                                    "type": col.Type,
                                    "null": col.Null,
                                    "key": col.Key,
                                    "default": col.Default
                                }
                                for col in columns
                            ]
                        }

                        self.add_result(
                            f"{table_name}结构检查",
                            "PASS",
                            f"表结构正常 ({len(columns)}列)"
                        )

                    except Exception as e:
                        structure_info[table_name] = {
                            "exists": False,
                            "error": str(e)
                        }
                        self.add_result(
                            f"{table_name}结构检查",
                            "FAIL",
                            f"表结构检查失败: {str(e)}"
                        )

        except Exception as e:
            self.add_result(
                "表结构检查",
                "FAIL",
                f"表结构检查失败: {str(e)}"
            )

        return structure_info

    def generate_report(self) -> str:
        """生成检查报告"""
        report = [
            "=" * 80,
            f"影子库环境状态检查报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 80,
            ""
        ]

        # 汇总统计
        pass_count = sum(1 for r in self.results if r.status == "PASS")
        warning_count = sum(1 for r in self.results if r.status == "WARNING")
        fail_count = sum(1 for r in self.results if r.status == "FAIL")

        report.extend([
            "检查结果汇总:",
            f"  [PASS] 通过: {pass_count}",
            f"  [WARNING] 警告: {warning_count}",
            f"  [FAIL] 失败: {fail_count}",
            f"  总计: {len(self.results)}",
            ""
        ])

        # 详细结果
        report.append("详细检查结果:")
        report.append("-" * 40)

        for result in self.results:
            status_symbol = {
                "PASS": "[PASS]",
                "WARNING": "[WARNING]",
                "FAIL": "[FAIL]"
            }.get(result.status, "[UNKNOWN]")

            report.append(f"{status_symbol} {result.check_name}: {result.message}")

            if result.data:
                # 添加关键数据
                if "student_count" in result.data:
                    report.append(f"  详细信息: 学生数={result.data['student_count']}, 学校数={result.data['school_count']}")
                    if "subject_details" in result.data:
                        report.append("  各科目统计:")
                        for detail in result.data["subject_details"]:
                            report.append(f"    科目{detail['subject_id']}: {detail['student_count']}学生, {detail['school_count']}学校")

        report.extend([
            "",
            "=" * 80,
            "建议操作:",
        ])

        # 生成建议
        if fail_count > 0:
            report.append("[警告] 发现严重问题，需要修复:")
            for result in self.results:
                if result.status == "FAIL":
                    report.append(f"  - {result.check_name}: {result.message}")

        if warning_count > 0:
            report.append("[注意] 需要注意的问题:")
            for result in self.results:
                if result.status == "WARNING":
                    report.append(f"  - {result.check_name}: {result.message}")

        # 检查是否需要执行预聚合
        precompute_needed = False
        for result in self.results:
            if "表检查" in result.check_name and result.status in ["FAIL", "WARNING"]:
                precompute_needed = True
                break

        if precompute_needed:
            report.extend([
                "",
                "[推荐] 下一步操作:",
                "  1. 如果基础数据检查通过，执行预聚合脚本:",
                "     python scripts/materialize_g7_2025.py",
                "  2. 或者运行快速物化脚本:",
                "     python fast_materialize_subjects_v12.py",
                "  3. 执行完成后重新运行此检查脚本验证结果"
            ])
        else:
            report.extend([
                "",
                "[成功] 所有关键检查通过，系统状态良好！"
            ])

        return "\n".join(report)

    def run_all_checks(self) -> Dict[str, Any]:
        """运行所有检查"""
        logger.info("开始影子库环境状态检查...")

        # 1. 环境配置检查
        env_ok = self.check_env_configuration()

        # 2. 数据库连接检查
        if env_ok:
            db_ok = self.check_database_connection()
        else:
            db_ok = False

        # 3. 基础数据检查
        g7_data = {}
        if db_ok:
            g7_data = self.check_g7_basic_data()

        # 4. 预聚合表检查
        tables_info = {}
        if db_ok:
            tables_info = self.check_precomputed_tables()

        # 5. 表结构检查
        structure_info = {}
        if db_ok:
            structure_info = self.check_table_structures()

        # 生成并保存报告
        report = self.generate_report()

        report_file = f"shadow_db_check_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)

        logger.info(f"检查完成，报告已保存到: {report_file}")
        print(report)

        return {
            "summary": {
                "total_checks": len(self.results),
                "passed": sum(1 for r in self.results if r.status == "PASS"),
                "warnings": sum(1 for r in self.results if r.status == "WARNING"),
                "failed": sum(1 for r in self.results if r.status == "FAIL")
            },
            "g7_data": g7_data,
            "tables_info": tables_info,
            "structure_info": structure_info,
            "report_file": report_file
        }


def main():
    """主函数"""
    try:
        checker = ShadowDatabaseChecker()
        results = checker.run_all_checks()

        # 返回适当的退出码
        if results["summary"]["failed"] > 0:
            sys.exit(1)
        elif results["summary"]["warnings"] > 0:
            sys.exit(2)
        else:
            sys.exit(0)

    except Exception as e:
        logger.error(f"检查脚本执行失败: {str(e)}")
        sys.exit(3)


if __name__ == "__main__":
    main()