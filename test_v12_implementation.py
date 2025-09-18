# v1.2汇聚指标修复实施验证测试
import sys
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List

# 添加app路径到sys.path  
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.services.subjects_builder import SubjectsBuilder
from app.services.question_option_distribution_service import QuestionOptionDistributionService
from app.database.connection import get_db_context
from sqlalchemy import text

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class V12ImplementationValidator:
    """v1.2规范实施验证器
    
    验证汇聚指标修复用户故事_v1.2.md中的所有要求是否正确实施
    """
    
    def __init__(self):
        self.subjects_builder = SubjectsBuilder()
        self.distribution_service = QuestionOptionDistributionService()
        self.test_results = []
    
    def run_all_tests(self, batch_code: str = "G4_2024") -> Dict[str, Any]:
        """运行所有v1.2规范验证测试"""
        logger.info(f"开始v1.2规范验证测试 - 批次: {batch_code}")
        
        start_time = datetime.now()
        
        # T1: 结构收敛与清退验证
        self.test_structure_cleanup(batch_code)
        
        # T2: Metrics注入与字段转换验证
        self.test_metrics_injection(batch_code)
        
        # T3: 问卷指标与题目分布隔离验证
        self.test_questionnaire_separation(batch_code)
        
        # T4: 数据质量检查
        self.test_data_quality(batch_code)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # 汇总结果
        passed_tests = [t for t in self.test_results if t['status'] == 'PASSED']
        failed_tests = [t for t in self.test_results if t['status'] == 'FAILED']
        
        summary = {
            'batch_code': batch_code,
            'test_execution_time': f"{duration:.2f}s",
            'total_tests': len(self.test_results),
            'passed': len(passed_tests),
            'failed': len(failed_tests),
            'success_rate': f"{len(passed_tests) / len(self.test_results) * 100:.1f}%",
            'test_details': self.test_results,
            'overall_status': 'PASSED' if len(failed_tests) == 0 else 'FAILED'
        }
        
        logger.info(f"v1.2规范验证完成: {summary['success_rate']} 通过率")
        return summary
    
    def test_structure_cleanup(self, batch_code: str):
        """T1: 验证结构收敛与清退"""
        logger.info("T1: 验证结构收敛与清退")
        
        # 测试1: 验证subjects过滤仅包含exam和questionnaire类型
        try:
            subjects = self.subjects_builder.list_subjects(batch_code)
            valid_types = {'exam', 'questionnaire'}
            invalid_subjects = [s for s in subjects if s.type not in valid_types]
            
            self._record_test_result(
                "T1.1", 
                "科目类型过滤",
                len(invalid_subjects) == 0,
                f"发现 {len(invalid_subjects)} 个非exam/questionnaire科目" if invalid_subjects else "所有科目类型正确"
            )
        except Exception as e:
            self._record_test_result("T1.1", "科目类型过滤", False, f"测试异常: {e}")
        
        # 测试2: 验证rank字段为整数类型
        try:
            regional_subjects = self.subjects_builder.build_regional_subjects_v12(batch_code)
            rank_issues = []
            
            for subj in regional_subjects:
                if 'metrics' in subj and 'rank' in subj['metrics']:
                    rank_val = subj['metrics']['rank']
                    if not isinstance(rank_val, int):
                        rank_issues.append(f"{subj['subject_name']}: rank={rank_val} (类型:{type(rank_val)})")
            
            self._record_test_result(
                "T1.2",
                "rank字段整数类型",
                len(rank_issues) == 0,
                f"发现 {len(rank_issues)} 个rank类型错误" if rank_issues else "所有rank字段为整数类型"
            )
        except Exception as e:
            self._record_test_result("T1.2", "rank字段整数类型", False, f"测试异常: {e}")
    
    def test_metrics_injection(self, batch_code: str):
        """T2: 验证Metrics注入与字段转换"""
        logger.info("T2: 验证Metrics注入与字段转换")
        
        # 测试3: 验证问卷科目使用score_rate而非difficulty
        try:
            regional_subjects = self.subjects_builder.build_regional_subjects_v12(batch_code)
            questionnaire_metrics_issues = []
            
            for subj in regional_subjects:
                if subj.get('type') == 'questionnaire' and 'metrics' in subj:
                    metrics = subj['metrics']
                    if 'difficulty' in metrics:
                        questionnaire_metrics_issues.append(f"{subj['subject_name']}: 问卷仍使用difficulty字段")
                    if 'score_rate' not in metrics:
                        questionnaire_metrics_issues.append(f"{subj['subject_name']}: 问卷缺少score_rate字段")
            
            self._record_test_result(
                "T2.1",
                "问卷score_rate格式",
                len(questionnaire_metrics_issues) == 0,
                "; ".join(questionnaire_metrics_issues) if questionnaire_metrics_issues else "问卷正确使用score_rate格式"
            )
        except Exception as e:
            self._record_test_result("T2.1", "问卷score_rate格式", False, f"测试异常: {e}")
        
        # 测试4: 验证考试科目保留difficulty字段
        try:
            exam_metrics_issues = []
            
            for subj in regional_subjects:
                if subj.get('type') == 'exam' and 'metrics' in subj:
                    metrics = subj['metrics']
                    if 'difficulty' not in metrics:
                        exam_metrics_issues.append(f"{subj['subject_name']}: 考试科目缺少difficulty字段")
            
            self._record_test_result(
                "T2.2",
                "考试difficulty保留",
                len(exam_metrics_issues) == 0,
                "; ".join(exam_metrics_issues) if exam_metrics_issues else "考试科目正确保留difficulty字段"
            )
        except Exception as e:
            self._record_test_result("T2.2", "考试difficulty保留", False, f"测试异常: {e}")
        
        # 测试5: 验证等级阈值修正
        try:
            # 通过计算器验证等级阈值
            from app.calculation.calculators.grade_calculator import GradeLevelConfig
            
            elementary_thresholds = GradeLevelConfig.ELEMENTARY_THRESHOLDS
            middle_school_thresholds = GradeLevelConfig.MIDDLE_SCHOOL_THRESHOLDS
            
            threshold_correct = (
                elementary_thresholds['excellent'] == 0.85 and
                middle_school_thresholds['excellent'] == 0.80
            )
            
            self._record_test_result(
                "T2.3",
                "等级阈值修正",
                threshold_correct,
                f"小学优秀≥{elementary_thresholds['excellent']*100}%, 初中优秀≥{middle_school_thresholds['excellent']*100}%" if threshold_correct else "等级阈值未按规范修正"
            )
        except Exception as e:
            self._record_test_result("T2.3", "等级阈值修正", False, f"测试异常: {e}")
    
    def test_questionnaire_separation(self, batch_code: str):
        """T3: 验证问卷指标与题目分布隔离"""
        logger.info("T3: 验证问卷指标与题目分布隔离")
        
        # 测试6: 验证问卷科目不再嵌入questions[]结构
        try:
            regional_subjects = self.subjects_builder.build_regional_subjects_v12(batch_code)
            embedded_questions_issues = []
            
            for subj in regional_subjects:
                if subj.get('type') == 'questionnaire':
                    if 'questions' in subj:
                        embedded_questions_issues.append(f"{subj['subject_name']}: 仍包含嵌入questions[]结构")
            
            self._record_test_result(
                "T3.1",
                "问卷questions[]移除",
                len(embedded_questions_issues) == 0,
                "; ".join(embedded_questions_issues) if embedded_questions_issues else "问卷科目成功移除嵌入questions[]结构"
            )
        except Exception as e:
            self._record_test_result("T3.1", "问卷questions[]移除", False, f"测试异常: {e}")
        
        # 测试7: 验证questionnaire_option_distribution表结构
        try:
            with get_db_context() as db:
                # 检查表是否存在
                table_check = db.execute(text("SHOW TABLES LIKE 'questionnaire_option_distribution'")).fetchone()
                table_exists = table_check is not None
                
                if table_exists:
                    # 检查表结构
                    columns = db.execute(text("DESCRIBE questionnaire_option_distribution")).fetchall()
                    required_columns = {'batch_code', 'school_id', 'subject_name', 'question_id', 'option_level', 'pct'}
                    actual_columns = {col[0] for col in columns}
                    missing_columns = required_columns - actual_columns
                    
                    structure_valid = len(missing_columns) == 0
                    message = "表结构完整" if structure_valid else f"缺少字段: {missing_columns}"
                else:
                    structure_valid = False
                    message = "questionnaire_option_distribution表不存在"
            
            self._record_test_result(
                "T3.2",
                "独立分布表结构",
                table_exists and structure_valid,
                message
            )
        except Exception as e:
            self._record_test_result("T3.2", "独立分布表结构", False, f"测试异常: {e}")
        
        # 测试8: 验证学校级问卷数据也移除questions[]
        try:
            school_subjects = self.subjects_builder.build_school_subjects_v12(batch_code, "test_school")
            school_embedded_issues = []
            
            for subj in school_subjects:
                if subj.get('type') == 'questionnaire':
                    if 'questions' in subj:
                        school_embedded_issues.append(f"{subj['subject_name']}: 学校级仍包含嵌入questions[]结构")
            
            self._record_test_result(
                "T3.3",
                "学校级questions[]移除",
                len(school_embedded_issues) == 0,
                "; ".join(school_embedded_issues) if school_embedded_issues else "学校级问卷科目成功移除嵌入questions[]结构"
            )
        except Exception as e:
            self._record_test_result("T3.3", "学校级questions[]移除", False, f"测试异常: {e}")
    
    def test_data_quality(self, batch_code: str):
        """T4: 数据质量检查"""
        logger.info("T4: 数据质量检查")
        
        # 测试9: 验证subjects输出数据完整性
        try:
            regional_subjects = self.subjects_builder.build_regional_subjects_v12(batch_code)
            data_issues = []
            
            for subj in regional_subjects:
                # 必需字段检查
                required_fields = {'subject_name', 'type', 'metrics'}
                missing_fields = required_fields - set(subj.keys())
                if missing_fields:
                    data_issues.append(f"{subj.get('subject_name', 'UNKNOWN')}: 缺少字段 {missing_fields}")
                
                # metrics字段检查
                if 'metrics' in subj:
                    metrics = subj['metrics']
                    required_metrics = {'avg', 'rank'}
                    missing_metrics = required_metrics - set(metrics.keys())
                    if missing_metrics:
                        data_issues.append(f"{subj['subject_name']}: metrics缺少 {missing_metrics}")
            
            self._record_test_result(
                "T4.1",
                "数据完整性检查",
                len(data_issues) == 0,
                f"发现 {len(data_issues)} 个数据完整性问题" if data_issues else "数据完整性检查通过"
            )
        except Exception as e:
            self._record_test_result("T4.1", "数据完整性检查", False, f"测试异常: {e}")
        
        # 测试10: 验证API接口可用性
        try:
            # 模拟API调用验证
            api_tests = []
            
            # 检查问卷科目列表获取
            with get_db_context() as db:
                questionnaire_check = db.execute(text("""
                    SELECT COUNT(DISTINCT subject_name) as questionnaire_count
                    FROM student_cleaned_scores 
                    WHERE batch_code = :batch AND subject_type = 'questionnaire'
                """), {'batch': batch_code}).fetchone()
                
                questionnaire_count = int(questionnaire_check[0]) if questionnaire_check else 0
                api_tests.append(f"发现 {questionnaire_count} 个问卷科目")
            
            self._record_test_result(
                "T4.2",
                "API接口验证",
                questionnaire_count > 0,
                "; ".join(api_tests)
            )
        except Exception as e:
            self._record_test_result("T4.2", "API接口验证", False, f"测试异常: {e}")
    
    def _record_test_result(self, test_id: str, test_name: str, passed: bool, message: str):
        """记录测试结果"""
        result = {
            'test_id': test_id,
            'test_name': test_name,
            'status': 'PASSED' if passed else 'FAILED',
            'message': message,
            'timestamp': datetime.now().isoformat()
        }
        
        self.test_results.append(result)
        logger.info(f"{test_id} - {test_name}: {'✓' if passed else '✗'} {message}")
    
    def generate_test_report(self, results: Dict[str, Any]) -> str:
        """生成测试报告"""
        report_lines = [
            "# v1.2汇聚指标修复实施验证报告",
            f"## 测试概览",
            f"- 批次代码: {results['batch_code']}",
            f"- 执行时间: {results['test_execution_time']}",
            f"- 总测试数: {results['total_tests']}",
            f"- 通过数: {results['passed']}",
            f"- 失败数: {results['failed']}",
            f"- 成功率: {results['success_rate']}",
            f"- 总体状态: {results['overall_status']}",
            "",
            "## 详细测试结果"
        ]
        
        # 按任务分组显示结果
        tasks = {
            'T1': 'T1: 结构收敛与清退',
            'T2': 'T2: Metrics注入与字段转换',
            'T3': 'T3: 问卷指标与题目分布隔离',
            'T4': 'T4: 数据质量检查'
        }
        
        for task_id, task_name in tasks.items():
            report_lines.append(f"### {task_name}")
            task_tests = [t for t in results['test_details'] if t['test_id'].startswith(task_id)]
            
            for test in task_tests:
                status_icon = '✓' if test['status'] == 'PASSED' else '✗'
                report_lines.append(f"- {status_icon} **{test['test_id']}** {test['test_name']}: {test['message']}")
            
            report_lines.append("")
        
        # 如果有失败的测试，添加改进建议
        failed_tests = [t for t in results['test_details'] if t['status'] == 'FAILED']
        if failed_tests:
            report_lines.extend([
                "## 改进建议",
                "以下测试未通过，需要进一步修正："
            ])
            
            for test in failed_tests:
                report_lines.append(f"- **{test['test_id']}**: {test['message']}")
        
        return "\n".join(report_lines)


def main():
    """主函数 - 运行v1.2规范验证"""
    batch_code = "G4_2024"  # 可以根据需要修改批次代码
    
    validator = V12ImplementationValidator()
    results = validator.run_all_tests(batch_code)
    
    # 生成并输出报告
    report = validator.generate_test_report(results)
    
    # 保存报告文件
    report_filename = f"v12_implementation_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n=== v1.2规范验证完成 ===")
    print(f"总体状态: {results['overall_status']}")
    print(f"成功率: {results['success_rate']}")
    print(f"详细报告已保存到: {report_filename}")
    
    # 如果有失败测试，返回非0退出码
    return 0 if results['overall_status'] == 'PASSED' else 1


if __name__ == "__main__":
    exit(main())