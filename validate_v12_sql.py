# v1.2规范SQL验证脚本
import sys
import os
from typing import Dict, Any, List
import logging
from datetime import datetime

# 添加app路径到sys.path  
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.database.connection import get_db_context
from sqlalchemy import text

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class V12SqlValidator:
    """v1.2规范SQL验证器
    
    验证数据库层面的修改是否符合v1.2规范要求
    """
    
    def __init__(self):
        self.validation_results = []
    
    def run_all_validations(self, batch_code: str = "G4_2024") -> Dict[str, Any]:
        """运行所有SQL验证"""
        logger.info(f"开始v1.2规范SQL验证 - 批次: {batch_code}")
        
        start_time = datetime.now()
        
        # 验证表结构
        self.validate_table_structure()
        
        # 验证数据过滤
        self.validate_data_filtering(batch_code)
        
        # 验证指标计算
        self.validate_metrics_calculation(batch_code)
        
        # 验证题目分布数据
        self.validate_option_distribution_data(batch_code)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # 汇总结果
        passed_validations = [v for v in self.validation_results if v['status'] == 'PASSED']
        failed_validations = [v for v in self.validation_results if v['status'] == 'FAILED']
        
        summary = {
            'batch_code': batch_code,
            'validation_execution_time': f"{duration:.2f}s",
            'total_validations': len(self.validation_results),
            'passed': len(passed_validations),
            'failed': len(failed_validations),
            'success_rate': f"{len(passed_validations) / len(self.validation_results) * 100:.1f}%",
            'validation_details': self.validation_results,
            'overall_status': 'PASSED' if len(failed_validations) == 0 else 'FAILED'
        }
        
        logger.info(f"v1.2规范SQL验证完成: {summary['success_rate']} 通过率")
        return summary
    
    def validate_table_structure(self):
        """验证表结构"""
        logger.info("验证表结构")
        
        # 验证questionnaire_option_distribution表存在且结构正确
        try:
            with get_db_context() as db:
                # 检查表是否存在
                table_exists_sql = "SHOW TABLES LIKE 'questionnaire_option_distribution'"
                table_result = db.execute(text(table_exists_sql)).fetchone()
                
                if not table_result:
                    self._record_validation_result(
                        "SQL.1.1", "questionnaire_option_distribution表存在性",
                        False, "表不存在"
                    )
                    return
                
                # 检查表结构
                describe_sql = "DESCRIBE questionnaire_option_distribution"
                columns = db.execute(text(describe_sql)).fetchall()
                
                expected_columns = {
                    'id': 'bigint',
                    'batch_code': 'varchar(50)',
                    'school_id': 'varchar(50)',
                    'subject_name': 'varchar(100)',
                    'question_id': 'varchar(100)',
                    'option_level': 'bigint',
                    'option_label': 'varchar(100)',
                    'count': 'bigint',
                    'n_total': 'bigint',
                    'pct': 'decimal(7,4)'
                }
                
                actual_columns = {col[0]: col[1].lower() for col in columns}
                missing_columns = []
                type_mismatches = []
                
                for col_name, expected_type in expected_columns.items():
                    if col_name not in actual_columns:
                        missing_columns.append(col_name)
                    elif not actual_columns[col_name].startswith(expected_type.split('(')[0]):
                        type_mismatches.append(f"{col_name}: 期望{expected_type}, 实际{actual_columns[col_name]}")
                
                issues = missing_columns + type_mismatches
                self._record_validation_result(
                    "SQL.1.1", "questionnaire_option_distribution表结构",
                    len(issues) == 0,
                    f"结构正确" if len(issues) == 0 else f"问题: {issues}"
                )
                
                # 检查索引
                indexes_sql = "SHOW INDEX FROM questionnaire_option_distribution"
                indexes = db.execute(text(indexes_sql)).fetchall()
                index_names = {idx[2] for idx in indexes}  # 索引名称
                
                expected_indexes = {
                    'PRIMARY', 'idx_batch_school_subject', 'idx_question_option', 
                    'uk_questionnaire_option_distribution'
                }
                missing_indexes = expected_indexes - index_names
                
                self._record_validation_result(
                    "SQL.1.2", "questionnaire_option_distribution索引",
                    len(missing_indexes) == 0,
                    f"索引完整" if len(missing_indexes) == 0 else f"缺少索引: {missing_indexes}"
                )
                
        except Exception as e:
            self._record_validation_result("SQL.1.1", "表结构验证", False, f"验证异常: {e}")
    
    def validate_data_filtering(self, batch_code: str):
        """验证数据过滤逻辑"""
        logger.info("验证数据过滤逻辑")
        
        try:
            with get_db_context() as db:
                # 验证只查询exam和questionnaire类型的科目
                type_filter_sql = """
                SELECT DISTINCT subject_type, COUNT(*) as count
                FROM student_cleaned_scores 
                WHERE batch_code = :batch
                GROUP BY subject_type
                """
                
                type_results = db.execute(text(type_filter_sql), {'batch': batch_code}).fetchall()
                subject_types = {row[0] for row in type_results}
                
                # 检查是否只有exam和questionnaire类型
                valid_types = {'exam', 'questionnaire'}
                invalid_types = subject_types - valid_types
                
                self._record_validation_result(
                    "SQL.2.1", "科目类型过滤",
                    len(invalid_types) == 0,
                    f"仅包含exam/questionnaire类型" if len(invalid_types) == 0 else f"发现无效类型: {invalid_types}"
                )
                
                # 验证只查询ACTIVE状态的学校
                school_status_sql = """
                SELECT smd.status, COUNT(DISTINCT scs.school_code) as school_count
                FROM student_cleaned_scores scs
                JOIN school_master_data smd ON smd.batch_code = scs.batch_code 
                    AND smd.school_id = scs.school_code
                WHERE scs.batch_code = :batch
                GROUP BY smd.status
                """
                
                status_results = db.execute(text(school_status_sql), {'batch': batch_code}).fetchall()
                
                # 检查是否所有学校都是ACTIVE状态
                active_schools = 0
                inactive_schools = 0
                
                for row in status_results:
                    if row[0] == 'ACTIVE':
                        active_schools = int(row[1])
                    else:
                        inactive_schools += int(row[1])
                
                self._record_validation_result(
                    "SQL.2.2", "学校状态过滤",
                    inactive_schools == 0,
                    f"ACTIVE学校: {active_schools}" if inactive_schools == 0 else f"发现非ACTIVE学校: {inactive_schools}"
                )
                
        except Exception as e:
            self._record_validation_result("SQL.2.1", "数据过滤验证", False, f"验证异常: {e}")
    
    def validate_metrics_calculation(self, batch_code: str):
        """验证指标计算逻辑"""
        logger.info("验证指标计算逻辑")
        
        try:
            with get_db_context() as db:
                # 验证问卷科目的score_rate计算（0-100%）
                questionnaire_metrics_sql = """
                SELECT 
                    scs.subject_name,
                    AVG(scs.total_score) as avg_score,
                    MAX(sqc.max_score) as max_score,
                    ROUND(AVG(scs.total_score) / MAX(sqc.max_score) * 100, 2) as calculated_score_rate
                FROM student_cleaned_scores scs
                JOIN subject_question_config sqc ON sqc.batch_code = scs.batch_code 
                    AND sqc.subject_name = scs.subject_name
                WHERE scs.batch_code = :batch 
                  AND scs.subject_type = 'questionnaire'
                GROUP BY scs.subject_name
                LIMIT 1
                """
                
                questionnaire_result = db.execute(text(questionnaire_metrics_sql), {'batch': batch_code}).fetchone()
                
                if questionnaire_result:
                    calculated_score_rate = float(questionnaire_result[3])
                    # 验证score_rate在0-100范围内
                    score_rate_valid = 0 <= calculated_score_rate <= 100
                    
                    self._record_validation_result(
                        "SQL.3.1", "问卷score_rate计算",
                        score_rate_valid,
                        f"score_rate={calculated_score_rate}%" if score_rate_valid else f"score_rate超出范围: {calculated_score_rate}%"
                    )
                else:
                    self._record_validation_result(
                        "SQL.3.1", "问卷score_rate计算",
                        False, "未找到问卷科目数据"
                    )
                
                # 验证考试科目的difficulty计算（0-1）
                exam_metrics_sql = """
                SELECT 
                    scs.subject_name,
                    AVG(scs.total_score) as avg_score,
                    MAX(sqc.max_score) as max_score,
                    ROUND(AVG(scs.total_score) / MAX(sqc.max_score), 4) as calculated_difficulty
                FROM student_cleaned_scores scs
                JOIN subject_question_config sqc ON sqc.batch_code = scs.batch_code 
                    AND sqc.subject_name = scs.subject_name
                WHERE scs.batch_code = :batch 
                  AND scs.subject_type = 'exam'
                GROUP BY scs.subject_name
                LIMIT 1
                """
                
                exam_result = db.execute(text(exam_metrics_sql), {'batch': batch_code}).fetchone()
                
                if exam_result:
                    calculated_difficulty = float(exam_result[3])
                    # 验证difficulty在0-1范围内
                    difficulty_valid = 0 <= calculated_difficulty <= 1
                    
                    self._record_validation_result(
                        "SQL.3.2", "考试difficulty计算",
                        difficulty_valid,
                        f"difficulty={calculated_difficulty}" if difficulty_valid else f"difficulty超出范围: {calculated_difficulty}"
                    )
                else:
                    self._record_validation_result(
                        "SQL.3.2", "考试difficulty计算",
                        False, "未找到考试科目数据"
                    )
                
                # 验证排名计算为整数
                ranking_sql = """
                WITH school_avgs AS (
                    SELECT 
                        scs.school_code,
                        AVG(scs.total_score) as avg_score
                    FROM student_cleaned_scores scs
                    JOIN school_master_data smd ON smd.batch_code = scs.batch_code 
                        AND smd.school_id = scs.school_code
                    WHERE scs.batch_code = :batch 
                      AND smd.status = 'ACTIVE'
                    GROUP BY scs.school_code
                )
                SELECT 
                    school_code,
                    DENSE_RANK() OVER (ORDER BY avg_score DESC) as rank_value
                FROM school_avgs
                LIMIT 3
                """
                
                rank_results = db.execute(text(ranking_sql), {'batch': batch_code}).fetchall()
                
                rank_issues = []
                for row in rank_results:
                    rank_val = row[1]
                    if not isinstance(rank_val, int):
                        rank_issues.append(f"学校{row[0]}: rank={rank_val} (类型:{type(rank_val)})")
                
                self._record_validation_result(
                    "SQL.3.3", "排名计算整数类型",
                    len(rank_issues) == 0,
                    "排名计算正确" if len(rank_issues) == 0 else f"排名类型问题: {rank_issues}"
                )
                
        except Exception as e:
            self._record_validation_result("SQL.3.1", "指标计算验证", False, f"验证异常: {e}")
    
    def validate_option_distribution_data(self, batch_code: str):
        """验证题目选项分布数据"""
        logger.info("验证题目选项分布数据")
        
        try:
            with get_db_context() as db:
                # 检查questionnaire_option_distribution表是否有数据
                data_count_sql = """
                SELECT COUNT(*) as total_records
                FROM questionnaire_option_distribution
                WHERE batch_code = :batch
                """
                
                count_result = db.execute(text(data_count_sql), {'batch': batch_code}).fetchone()
                total_records = int(count_result[0]) if count_result else 0
                
                self._record_validation_result(
                    "SQL.4.1", "题目分布数据存在",
                    total_records > 0,
                    f"共 {total_records} 条记录" if total_records > 0 else "暂无题目分布数据"
                )
                
                if total_records > 0:
                    # 验证百分比数据的有效性
                    pct_validation_sql = """
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN pct < 0 OR pct > 100 THEN 1 ELSE 0 END) as invalid_pct,
                        MIN(pct) as min_pct,
                        MAX(pct) as max_pct
                    FROM questionnaire_option_distribution
                    WHERE batch_code = :batch
                    """
                    
                    pct_result = db.execute(text(pct_validation_sql), {'batch': batch_code}).fetchone()
                    
                    if pct_result:
                        invalid_pct_count = int(pct_result[1])
                        min_pct = float(pct_result[2])
                        max_pct = float(pct_result[3])
                        
                        pct_valid = invalid_pct_count == 0
                        
                        self._record_validation_result(
                            "SQL.4.2", "百分比数据有效性",
                            pct_valid,
                            f"百分比范围: {min_pct}%-{max_pct}%" if pct_valid else f"发现 {invalid_pct_count} 个无效百分比"
                        )
                    
                    # 验证每个题目的百分比总和约为100%
                    sum_validation_sql = """
                    SELECT 
                        question_id,
                        SUM(pct) as total_pct
                    FROM questionnaire_option_distribution
                    WHERE batch_code = :batch
                    GROUP BY batch_code, school_id, subject_name, question_id
                    HAVING ABS(SUM(pct) - 100) > 1  -- 允许1%的误差
                    LIMIT 5
                    """
                    
                    sum_issues = db.execute(text(sum_validation_sql), {'batch': batch_code}).fetchall()
                    
                    self._record_validation_result(
                        "SQL.4.3", "题目百分比求和",
                        len(sum_issues) == 0,
                        "百分比求和正确" if len(sum_issues) == 0 else f"发现 {len(sum_issues)} 个题目百分比求和异常"
                    )
                
        except Exception as e:
            self._record_validation_result("SQL.4.1", "题目分布数据验证", False, f"验证异常: {e}")
    
    def _record_validation_result(self, validation_id: str, validation_name: str, passed: bool, message: str):
        """记录验证结果"""
        result = {
            'validation_id': validation_id,
            'validation_name': validation_name,
            'status': 'PASSED' if passed else 'FAILED',
            'message': message,
            'timestamp': datetime.now().isoformat()
        }
        
        self.validation_results.append(result)
        logger.info(f"{validation_id} - {validation_name}: {'✓' if passed else '✗'} {message}")
    
    def generate_sql_report(self, results: Dict[str, Any]) -> str:
        """生成SQL验证报告"""
        report_lines = [
            "# v1.2汇聚指标修复SQL验证报告",
            f"## 验证概览",
            f"- 批次代码: {results['batch_code']}",
            f"- 执行时间: {results['validation_execution_time']}",
            f"- 总验证数: {results['total_validations']}",
            f"- 通过数: {results['passed']}",
            f"- 失败数: {results['failed']}",
            f"- 成功率: {results['success_rate']}",
            f"- 总体状态: {results['overall_status']}",
            "",
            "## 详细验证结果"
        ]
        
        # 按验证类别分组显示结果
        categories = {
            'SQL.1': 'SQL.1: 表结构验证',
            'SQL.2': 'SQL.2: 数据过滤验证',
            'SQL.3': 'SQL.3: 指标计算验证',
            'SQL.4': 'SQL.4: 题目分布数据验证'
        }
        
        for category_id, category_name in categories.items():
            report_lines.append(f"### {category_name}")
            category_validations = [v for v in results['validation_details'] if v['validation_id'].startswith(category_id)]
            
            for validation in category_validations:
                status_icon = '✓' if validation['status'] == 'PASSED' else '✗'
                report_lines.append(f"- {status_icon} **{validation['validation_id']}** {validation['validation_name']}: {validation['message']}")
            
            report_lines.append("")
        
        return "\n".join(report_lines)


def main():
    """主函数 - 运行v1.2规范SQL验证"""
    batch_code = "G4_2024"  # 可以根据需要修改批次代码
    
    validator = V12SqlValidator()
    results = validator.run_all_validations(batch_code)
    
    # 生成并输出报告
    report = validator.generate_sql_report(results)
    
    # 保存报告文件
    report_filename = f"v12_sql_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n=== v1.2规范SQL验证完成 ===")
    print(f"总体状态: {results['overall_status']}")
    print(f"成功率: {results['success_rate']}")
    print(f"详细报告已保存到: {report_filename}")
    
    # 如果有失败验证，返回非0退出码
    return 0 if results['overall_status'] == 'PASSED' else 1


if __name__ == "__main__":
    exit(main())