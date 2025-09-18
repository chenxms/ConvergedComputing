# v1.2规范Given-When-Then业务场景测试
import sys
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List

# 添加app路径到sys.path  
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.services.subjects_builder import SubjectsBuilder
from app.services.question_option_distribution_service import QuestionOptionDistributionService, populate_questionnaire_distributions
from app.database.connection import get_db_context
from sqlalchemy import text

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class V12ScenarioTest:
    """v1.2规范Given-When-Then业务场景测试"""
    
    def __init__(self):
        self.subjects_builder = SubjectsBuilder()
        self.distribution_service = QuestionOptionDistributionService()
        self.scenario_results = []
    
    def run_all_scenarios(self, batch_code: str = "G4_2024") -> Dict[str, Any]:
        """运行所有业务场景测试"""
        logger.info(f"开始v1.2规范业务场景测试 - 批次: {batch_code}")
        
        start_time = datetime.now()
        
        # 场景1: 区域级数据汇聚与清退
        self.scenario_regional_data_aggregation(batch_code)
        
        # 场景2: 学校级数据输出与对标
        self.scenario_school_data_comparison(batch_code)
        
        # 场景3: 问卷题目分布独立查询
        self.scenario_questionnaire_distribution_query(batch_code)
        
        # 场景4: 指标计算差异化处理
        self.scenario_differentiated_metrics(batch_code)
        
        # 场景5: 等级分布计算准确性
        self.scenario_grade_distribution_accuracy(batch_code)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # 汇总结果
        passed_scenarios = [s for s in self.scenario_results if s['status'] == 'PASSED']
        failed_scenarios = [s for s in self.scenario_results if s['status'] == 'FAILED']
        
        summary = {
            'batch_code': batch_code,
            'scenario_execution_time': f"{duration:.2f}s",
            'total_scenarios': len(self.scenario_results),
            'passed': len(passed_scenarios),
            'failed': len(failed_scenarios),
            'success_rate': f"{len(passed_scenarios) / len(self.scenario_results) * 100:.1f}%",
            'scenario_details': self.scenario_results,
            'overall_status': 'PASSED' if len(failed_scenarios) == 0 else 'FAILED'
        }
        
        logger.info(f"v1.2规范业务场景测试完成: {summary['success_rate']} 通过率")
        return summary
    
    def scenario_regional_data_aggregation(self, batch_code: str):
        """场景1: 区域级数据汇聚与清退
        
        Given: 系统有G4批次的多学校数据
        When: 调用区域级subjects汇聚接口
        Then: 返回清退后的v1.2格式数据，不包含p10/p50/p90等顶层字段
        """
        logger.info("场景1: 区域级数据汇聚与清退")
        
        try:
            # Given: 验证系统有批次数据
            with get_db_context() as db:
                data_check_sql = """
                SELECT COUNT(DISTINCT school_code) as school_count,
                       COUNT(DISTINCT subject_name) as subject_count
                FROM student_cleaned_scores 
                WHERE batch_code = :batch
                """
                data_result = db.execute(text(data_check_sql), {'batch': batch_code}).fetchone()
                
                if not data_result or int(data_result[0]) == 0:
                    self._record_scenario_result(
                        "场景1", "区域级数据汇聚与清退",
                        False, "Given条件未满足：系统无批次数据"
                    )
                    return
            
            # When: 调用区域级subjects汇聚接口
            regional_subjects = self.subjects_builder.build_regional_subjects_v12(batch_code)
            
            # Then: 验证返回数据格式
            validation_issues = []
            
            for subj in regional_subjects:
                # 验证基本结构
                required_fields = {'subject_name', 'type', 'metrics'}
                missing_fields = required_fields - set(subj.keys())
                if missing_fields:
                    validation_issues.append(f"{subj.get('subject_name', 'UNKNOWN')}: 缺少字段 {missing_fields}")
                
                # 验证清退的顶层字段不存在
                retired_fields = {'p10', 'p50', 'p90', 'discrimination'}
                found_retired_fields = retired_fields & set(subj.keys())
                if found_retired_fields:
                    validation_issues.append(f"{subj['subject_name']}: 发现已清退字段 {found_retired_fields}")
                
                # 验证metrics字段包含必需内容
                if 'metrics' in subj:
                    required_metrics = {'avg', 'rank'}
                    missing_metrics = required_metrics - set(subj['metrics'].keys())
                    if missing_metrics:
                        validation_issues.append(f"{subj['subject_name']}: metrics缺少 {missing_metrics}")
                    
                    # 验证rank是整数类型
                    if 'rank' in subj['metrics'] and not isinstance(subj['metrics']['rank'], int):
                        validation_issues.append(f"{subj['subject_name']}: rank不是整数类型")
            
            self._record_scenario_result(
                "场景1", "区域级数据汇聚与清退",
                len(validation_issues) == 0,
                f"成功汇聚 {len(regional_subjects)} 个科目" if len(validation_issues) == 0 else f"验证失败: {validation_issues[:3]}"  # 只显示前3个问题
            )
            
        except Exception as e:
            self._record_scenario_result("场景1", "区域级数据汇聚与清退", False, f"场景执行异常: {e}")
    
    def scenario_school_data_comparison(self, batch_code: str):
        """场景2: 学校级数据输出与对标
        
        Given: 系统有特定学校的数据
        When: 调用学校级subjects接口  
        Then: 返回该学校数据，包含区域排名，不包含regional_avg对标
        """
        logger.info("场景2: 学校级数据输出与对标")
        
        try:
            # Given: 获取一个有数据的学校
            with get_db_context() as db:
                school_query_sql = """
                SELECT DISTINCT school_code 
                FROM student_cleaned_scores 
                WHERE batch_code = :batch 
                LIMIT 1
                """
                school_result = db.execute(text(school_query_sql), {'batch': batch_code}).fetchone()
                
                if not school_result:
                    self._record_scenario_result(
                        "场景2", "学校级数据输出与对标",
                        False, "Given条件未满足：无学校数据"
                    )
                    return
                
                test_school_id = school_result[0]
            
            # When: 调用学校级subjects接口
            school_subjects = self.subjects_builder.build_school_subjects_v12(batch_code, test_school_id)
            
            # Then: 验证学校级数据格式
            validation_issues = []
            
            for subj in school_subjects:
                # 验证基本结构
                if 'metrics' not in subj:
                    validation_issues.append(f"{subj.get('subject_name', 'UNKNOWN')}: 缺少metrics字段")
                    continue
                
                # 验证包含区域排名
                if 'rank' not in subj['metrics']:
                    validation_issues.append(f"{subj['subject_name']}: 缺少区域排名")
                elif not isinstance(subj['metrics']['rank'], int):
                    validation_issues.append(f"{subj['subject_name']}: 排名不是整数类型")
                
                # 验证dimensions中不包含regional_avg对标
                if 'dimensions' in subj:
                    for dim in subj['dimensions']:
                        if 'regional_avg' in dim:
                            validation_issues.append(f"{subj['subject_name']}: 维度包含已清退的regional_avg字段")
                
                # 验证问卷科目不包含questions[]
                if subj.get('type') == 'questionnaire' and 'questions' in subj:
                    validation_issues.append(f"{subj['subject_name']}: 问卷包含已清退的questions[]字段")
            
            self._record_scenario_result(
                "场景2", "学校级数据输出与对标",
                len(validation_issues) == 0,
                f"学校 {test_school_id} 数据正确，共 {len(school_subjects)} 个科目" if len(validation_issues) == 0 else f"验证失败: {validation_issues[:3]}"
            )
            
        except Exception as e:
            self._record_scenario_result("场景2", "学校级数据输出与对标", False, f"场景执行异常: {e}")
    
    def scenario_questionnaire_distribution_query(self, batch_code: str):
        """场景3: 问卷题目分布独立查询
        
        Given: 系统有问卷科目数据
        When: 先填充题目分布数据，然后调用独立查询接口
        Then: 能正确查询到学校级和区域级的题目选项分布
        """
        logger.info("场景3: 问卷题目分布独立查询")
        
        try:
            # Given: 查找问卷科目
            with get_db_context() as db:
                questionnaire_query_sql = """
                SELECT DISTINCT subject_name, school_code
                FROM student_cleaned_scores 
                WHERE batch_code = :batch 
                  AND subject_type = 'questionnaire'
                LIMIT 1
                """
                questionnaire_result = db.execute(text(questionnaire_query_sql), {'batch': batch_code}).fetchone()
                
                if not questionnaire_result:
                    self._record_scenario_result(
                        "场景3", "问卷题目分布独立查询",
                        False, "Given条件未满足：无问卷科目数据"
                    )
                    return
                
                test_subject = questionnaire_result[0]
                test_school = questionnaire_result[1]
            
            # When: 填充题目分布数据
            populate_result = populate_questionnaire_distributions(batch_code, test_subject)
            
            if not populate_result.get('success', False):
                self._record_scenario_result(
                    "场景3", "问卷题目分布独立查询",
                    False, f"数据填充失败: {populate_result.get('errors', [])}"
                )
                return
            
            # When: 调用学校级查询接口
            school_distribution = self.distribution_service.get_school_option_distributions(
                batch_code, test_subject, test_school
            )
            
            # When: 调用区域级查询接口
            regional_distribution = self.distribution_service.get_regional_option_distributions(
                batch_code, test_subject
            )
            
            # Then: 验证查询结果
            validation_issues = []
            
            # 验证学校级数据结构
            if not school_distribution.get('questions'):
                validation_issues.append("学校级查询无题目数据")
            else:
                for question in school_distribution['questions']:
                    required_fields = {'question_id', 'total_responses', 'options'}
                    missing_fields = required_fields - set(question.keys())
                    if missing_fields:
                        validation_issues.append(f"学校级题目缺少字段: {missing_fields}")
            
            # 验证区域级数据结构  
            if not regional_distribution.get('questions'):
                validation_issues.append("区域级查询无题目数据")
            else:
                for question in regional_distribution['questions']:
                    required_fields = {'question_id', 'total_responses', 'options'}
                    missing_fields = required_fields - set(question.keys())
                    if missing_fields:
                        validation_issues.append(f"区域级题目缺少字段: {missing_fields}")
            
            # 验证选项数据结构
            if school_distribution.get('questions'):
                first_question = school_distribution['questions'][0]
                if first_question.get('options'):
                    first_option = first_question['options'][0]
                    required_option_fields = {'option_level', 'option_label', 'count', 'pct'}
                    missing_option_fields = required_option_fields - set(first_option.keys())
                    if missing_option_fields:
                        validation_issues.append(f"选项缺少字段: {missing_option_fields}")
            
            self._record_scenario_result(
                "场景3", "问卷题目分布独立查询",
                len(validation_issues) == 0,
                f"成功查询 {test_subject} 题目分布，学校级{len(school_distribution.get('questions', []))}题，区域级{len(regional_distribution.get('questions', []))}题" if len(validation_issues) == 0 else f"验证失败: {validation_issues[:3]}"
            )
            
        except Exception as e:
            self._record_scenario_result("场景3", "问卷题目分布独立查询", False, f"场景执行异常: {e}")
    
    def scenario_differentiated_metrics(self, batch_code: str):
        """场景4: 指标计算差异化处理
        
        Given: 系统同时有考试和问卷科目数据
        When: 调用区域级subjects接口
        Then: 考试科目使用difficulty(0-1)，问卷科目使用score_rate(0-100)
        """
        logger.info("场景4: 指标计算差异化处理")
        
        try:
            # Given: 检查是否同时有考试和问卷科目
            with get_db_context() as db:
                subject_types_sql = """
                SELECT subject_type, COUNT(DISTINCT subject_name) as count
                FROM student_cleaned_scores 
                WHERE batch_code = :batch
                GROUP BY subject_type
                """
                type_results = db.execute(text(subject_types_sql), {'batch': batch_code}).fetchall()
                type_counts = {row[0]: int(row[1]) for row in type_results}
                
                if 'exam' not in type_counts or 'questionnaire' not in type_counts:
                    self._record_scenario_result(
                        "场景4", "指标计算差异化处理",
                        False, f"Given条件未满足：科目类型不全，当前有 {list(type_counts.keys())}"
                    )
                    return
            
            # When: 调用区域级subjects接口
            regional_subjects = self.subjects_builder.build_regional_subjects_v12(batch_code)
            
            # Then: 验证差异化指标计算
            validation_issues = []
            exam_subjects = []
            questionnaire_subjects = []
            
            for subj in regional_subjects:
                if not subj.get('metrics'):
                    continue
                    
                metrics = subj['metrics']
                subject_type = subj.get('type')
                
                if subject_type == 'exam':
                    exam_subjects.append(subj['subject_name'])
                    # 考试科目应该有difficulty而非score_rate
                    if 'difficulty' not in metrics:
                        validation_issues.append(f"考试科目 {subj['subject_name']} 缺少difficulty字段")
                    elif 'score_rate' in metrics:
                        validation_issues.append(f"考试科目 {subj['subject_name']} 不应有score_rate字段")
                    else:
                        # 验证difficulty范围0-1
                        difficulty = metrics['difficulty']
                        if not (0 <= difficulty <= 1):
                            validation_issues.append(f"考试科目 {subj['subject_name']} difficulty={difficulty}超出范围[0,1]")
                
                elif subject_type == 'questionnaire':
                    questionnaire_subjects.append(subj['subject_name'])
                    # 问卷科目应该有score_rate而非difficulty
                    if 'score_rate' not in metrics:
                        validation_issues.append(f"问卷科目 {subj['subject_name']} 缺少score_rate字段")
                    elif 'difficulty' in metrics:
                        validation_issues.append(f"问卷科目 {subj['subject_name']} 不应有difficulty字段")
                    else:
                        # 验证score_rate范围0-100
                        score_rate = metrics['score_rate']
                        if not (0 <= score_rate <= 100):
                            validation_issues.append(f"问卷科目 {subj['subject_name']} score_rate={score_rate}超出范围[0,100]")
            
            result_message = f"考试科目{len(exam_subjects)}个，问卷科目{len(questionnaire_subjects)}个"
            if len(validation_issues) > 0:
                result_message += f"，验证失败: {validation_issues[:3]}"
            
            self._record_scenario_result(
                "场景4", "指标计算差异化处理",
                len(validation_issues) == 0,
                result_message
            )
            
        except Exception as e:
            self._record_scenario_result("场景4", "指标计算差异化处理", False, f"场景执行异常: {e}")
    
    def scenario_grade_distribution_accuracy(self, batch_code: str):
        """场景5: 等级分布计算准确性
        
        Given: 系统配置了正确的等级阈值（小学优秀≥90%，初中A≥85%）
        When: 计算等级分布
        Then: 等级划分准确，阈值应用正确
        """
        logger.info("场景5: 等级分布计算准确性")
        
        try:
            # Given: 验证等级阈值配置
            from app.calculation.calculators.grade_calculator import GradeLevelConfig
            
            elementary_threshold = GradeLevelConfig.ELEMENTARY_THRESHOLDS['excellent']
            middle_school_threshold = GradeLevelConfig.MIDDLE_SCHOOL_THRESHOLDS['excellent']
            
            # Then: 验证阈值正确性
            thresholds_correct = (elementary_threshold == 0.85 and middle_school_threshold == 0.80)
            
            if not thresholds_correct:
                self._record_scenario_result(
                    "场景5", "等级分布计算准确性",
                    False, f"Given条件未满足：等级阈值错误，小学优秀={elementary_threshold}, 初中优秀={middle_school_threshold}"
                )
                return
            
            # When & Then: 使用计算器验证等级计算逻辑
            from app.calculation.calculators.grade_calculator import calculate_individual_grade
            
            # 测试小学等级计算
            elementary_test_cases = [
                (85, '3rd_grade', 100, 'excellent'),  # 85分/100分 -> 优秀
                (75, '3rd_grade', 100, 'good'),      # 75分/100分 -> 良好
                (65, '3rd_grade', 100, 'pass'),      # 65分/100分 -> 及格
                (50, '3rd_grade', 100, 'fail'),      # 50分/100分 -> 不及格
            ]
            
            elementary_issues = []
            for score, grade_level, max_score, expected_grade in elementary_test_cases:
                result = calculate_individual_grade(score, grade_level, max_score)
                if result['grade'] != expected_grade:
                    elementary_issues.append(f"小学{score}分期望{expected_grade}实际{result['grade']}")
            
            # 测试初中等级计算
            middle_school_test_cases = [
                (80, '7th_grade', 100, 'excellent'),  # 80分/100分 -> 优秀
                (75, '7th_grade', 100, 'good'),      # 75分/100分 -> 良好
                (65, '7th_grade', 100, 'pass'),      # 65分/100分 -> 及格
                (50, '7th_grade', 100, 'fail'),      # 50分/100分 -> 不及格
            ]
            
            middle_school_issues = []
            for score, grade_level, max_score, expected_grade in middle_school_test_cases:
                result = calculate_individual_grade(score, grade_level, max_score)
                if result['grade'] != expected_grade:
                    middle_school_issues.append(f"初中{score}分期望{expected_grade}实际{result['grade']}")
            
            all_issues = elementary_issues + middle_school_issues
            
            self._record_scenario_result(
                "场景5", "等级分布计算准确性",
                len(all_issues) == 0,
                f"等级计算准确，小学优秀≥85%，初中优秀≥80%" if len(all_issues) == 0 else f"等级计算错误: {all_issues}"
            )
            
        except Exception as e:
            self._record_scenario_result("场景5", "等级分布计算准确性", False, f"场景执行异常: {e}")
    
    def _record_scenario_result(self, scenario_id: str, scenario_name: str, passed: bool, message: str):
        """记录场景测试结果"""
        result = {
            'scenario_id': scenario_id,
            'scenario_name': scenario_name,
            'status': 'PASSED' if passed else 'FAILED',
            'message': message,
            'timestamp': datetime.now().isoformat()
        }
        
        self.scenario_results.append(result)
        logger.info(f"{scenario_id} - {scenario_name}: {'✓' if passed else '✗'} {message}")
    
    def generate_scenario_report(self, results: Dict[str, Any]) -> str:
        """生成场景测试报告"""
        report_lines = [
            "# v1.2汇聚指标修复Given-When-Then场景测试报告",
            f"## 场景测试概览",
            f"- 批次代码: {results['batch_code']}",
            f"- 执行时间: {results['scenario_execution_time']}",
            f"- 总场景数: {results['total_scenarios']}",
            f"- 通过数: {results['passed']}",
            f"- 失败数: {results['failed']}",
            f"- 成功率: {results['success_rate']}",
            f"- 总体状态: {results['overall_status']}",
            "",
            "## 详细场景测试结果"
        ]
        
        for scenario in results['scenario_details']:
            status_icon = '✅' if scenario['status'] == 'PASSED' else '❌'
            report_lines.extend([
                f"### {status_icon} {scenario['scenario_name']}",
                f"**测试结果**: {scenario['message']}",
                f"**执行时间**: {scenario['timestamp']}",
                ""
            ])
        
        # 如果有失败的场景，添加改进建议
        failed_scenarios = [s for s in results['scenario_details'] if s['status'] == 'FAILED']
        if failed_scenarios:
            report_lines.extend([
                "## 改进建议",
                "以下场景测试失败，需要进一步检查和修正："
            ])
            
            for scenario in failed_scenarios:
                report_lines.append(f"- **{scenario['scenario_name']}**: {scenario['message']}")
        
        return "\n".join(report_lines)


def main():
    """主函数 - 运行v1.2规范业务场景测试"""
    batch_code = "G4_2024"  # 可以根据需要修改批次代码
    
    tester = V12ScenarioTest()
    results = tester.run_all_scenarios(batch_code)
    
    # 生成并输出报告
    report = tester.generate_scenario_report(results)
    
    # 保存报告文件
    report_filename = f"v12_scenario_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n=== v1.2规范业务场景测试完成 ===")
    print(f"总体状态: {results['overall_status']}")
    print(f"成功率: {results['success_rate']}")
    print(f"详细报告已保存到: {report_filename}")
    
    # 如果有失败场景，返回非0退出码
    return 0 if results['overall_status'] == 'PASSED' else 1


if __name__ == "__main__":
    exit(main())