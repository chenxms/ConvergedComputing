# v1.2汇聚指标修复完整测试套件
import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, Any

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 导入测试模块
try:
    from test_v12_implementation import V12ImplementationValidator
    from validate_v12_sql import V12SqlValidator
    from test_v12_scenarios import V12ScenarioTest
except ImportError as e:
    logger.error(f"导入测试模块失败: {e}")
    sys.exit(1)


class V12TestSuite:
    """v1.2规范完整测试套件"""
    
    def __init__(self):
        self.test_results = {}
        self.overall_start_time = None
        self.overall_end_time = None
    
    def run_complete_test_suite(self, batch_code: str = "G4_2024") -> Dict[str, Any]:
        """运行完整的v1.2规范测试套件"""
        logger.info("=" * 60)
        logger.info("开始v1.2汇聚指标修复完整测试套件")
        logger.info("=" * 60)
        
        self.overall_start_time = datetime.now()
        
        # 1. 实施验证测试
        logger.info("\n1. 开始实施验证测试...")
        implementation_validator = V12ImplementationValidator()
        self.test_results['implementation'] = implementation_validator.run_all_tests(batch_code)
        
        # 2. SQL验证测试
        logger.info("\n2. 开始SQL验证测试...")
        sql_validator = V12SqlValidator()
        self.test_results['sql_validation'] = sql_validator.run_all_validations(batch_code)
        
        # 3. 业务场景测试
        logger.info("\n3. 开始业务场景测试...")
        scenario_tester = V12ScenarioTest()
        self.test_results['scenarios'] = scenario_tester.run_all_scenarios(batch_code)
        
        self.overall_end_time = datetime.now()
        
        # 汇总所有测试结果
        overall_summary = self._generate_overall_summary(batch_code)
        
        # 生成综合报告
        self._generate_comprehensive_report(overall_summary)
        
        logger.info("=" * 60)
        logger.info("v1.2汇聚指标修复完整测试套件执行完成")
        logger.info("=" * 60)
        
        return overall_summary
    
    def _generate_overall_summary(self, batch_code: str) -> Dict[str, Any]:
        """生成总体测试摘要"""
        total_duration = (self.overall_end_time - self.overall_start_time).total_seconds()
        
        # 统计所有测试结果
        total_tests = 0
        total_passed = 0
        total_failed = 0
        
        test_categories = []
        
        for category, results in self.test_results.items():
            category_total = results.get('total_tests', 0) or results.get('total_validations', 0) or results.get('total_scenarios', 0)
            category_passed = results.get('passed', 0)
            category_failed = results.get('failed', 0)
            category_status = results.get('overall_status', 'UNKNOWN')
            
            total_tests += category_total
            total_passed += category_passed
            total_failed += category_failed
            
            test_categories.append({
                'category': category,
                'total': category_total,
                'passed': category_passed,
                'failed': category_failed,
                'status': category_status,
                'success_rate': results.get('success_rate', '0%')
            })
        
        # 计算总体成功率
        overall_success_rate = f"{total_passed / total_tests * 100:.1f}%" if total_tests > 0 else "0%"
        overall_status = 'PASSED' if total_failed == 0 else 'FAILED'
        
        summary = {
            'batch_code': batch_code,
            'execution_time': f"{total_duration:.2f}s",
            'start_time': self.overall_start_time.isoformat(),
            'end_time': self.overall_end_time.isoformat(),
            'overall_status': overall_status,
            'overall_success_rate': overall_success_rate,
            'total_tests': total_tests,
            'total_passed': total_passed,
            'total_failed': total_failed,
            'test_categories': test_categories,
            'detailed_results': self.test_results
        }
        
        return summary
    
    def _generate_comprehensive_report(self, summary: Dict[str, Any]):
        """生成综合测试报告"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_filename = f"v12_comprehensive_test_report_{timestamp}.md"
        
        report_lines = [
            "# v1.2汇聚指标修复完整测试报告",
            "",
            "## 测试概览",
            f"- 批次代码: {summary['batch_code']}",
            f"- 执行时间: {summary['execution_time']}",
            f"- 开始时间: {summary['start_time']}",
            f"- 结束时间: {summary['end_time']}",
            f"- 总体状态: **{summary['overall_status']}**",
            f"- 总体成功率: **{summary['overall_success_rate']}**",
            f"- 总测试数: {summary['total_tests']}",
            f"- 通过数: {summary['total_passed']}",
            f"- 失败数: {summary['total_failed']}",
            "",
            "## 测试分类结果",
            ""
        ]
        
        # 测试分类表格
        report_lines.extend([
            "| 测试类别 | 总数 | 通过 | 失败 | 成功率 | 状态 |",
            "|---------|------|------|------|--------|------|"
        ])
        
        for category in summary['test_categories']:
            status_icon = "✅" if category['status'] == 'PASSED' else "❌"
            category_name_map = {
                'implementation': '实施验证',
                'sql_validation': 'SQL验证',
                'scenarios': '业务场景'
            }
            category_display = category_name_map.get(category['category'], category['category'])
            
            report_lines.append(
                f"| {category_display} | {category['total']} | {category['passed']} | {category['failed']} | {category['success_rate']} | {status_icon} {category['status']} |"
            )
        
        report_lines.extend([
            "",
            "## 详细测试结果",
            ""
        ])
        
        # 实施验证测试详细结果
        if 'implementation' in summary['detailed_results']:
            impl_results = summary['detailed_results']['implementation']
            report_lines.extend([
                "### 1. 实施验证测试",
                f"**状态**: {impl_results['overall_status']} | **成功率**: {impl_results['success_rate']}",
                ""
            ])
            
            # 按任务分组显示
            tasks = {
                'T1': 'T1: 结构收敛与清退',
                'T2': 'T2: Metrics注入与字段转换', 
                'T3': 'T3: 问卷指标与题目分布隔离',
                'T4': 'T4: 数据质量检查'
            }
            
            for task_id, task_name in tasks.items():
                task_tests = [t for t in impl_results['test_details'] if t['test_id'].startswith(task_id)]
                if task_tests:
                    report_lines.append(f"#### {task_name}")
                    for test in task_tests:
                        status_icon = '✅' if test['status'] == 'PASSED' else '❌'
                        report_lines.append(f"- {status_icon} **{test['test_id']}** {test['test_name']}: {test['message']}")
                    report_lines.append("")
        
        # SQL验证测试详细结果
        if 'sql_validation' in summary['detailed_results']:
            sql_results = summary['detailed_results']['sql_validation']
            report_lines.extend([
                "### 2. SQL验证测试",
                f"**状态**: {sql_results['overall_status']} | **成功率**: {sql_results['success_rate']}",
                ""
            ])
            
            # 按验证类别分组显示
            categories = {
                'SQL.1': 'SQL.1: 表结构验证',
                'SQL.2': 'SQL.2: 数据过滤验证',
                'SQL.3': 'SQL.3: 指标计算验证', 
                'SQL.4': 'SQL.4: 题目分布数据验证'
            }
            
            for category_id, category_name in categories.items():
                category_validations = [v for v in sql_results['validation_details'] if v['validation_id'].startswith(category_id)]
                if category_validations:
                    report_lines.append(f"#### {category_name}")
                    for validation in category_validations:
                        status_icon = '✅' if validation['status'] == 'PASSED' else '❌'
                        report_lines.append(f"- {status_icon} **{validation['validation_id']}** {validation['validation_name']}: {validation['message']}")
                    report_lines.append("")
        
        # 业务场景测试详细结果
        if 'scenarios' in summary['detailed_results']:
            scenario_results = summary['detailed_results']['scenarios']
            report_lines.extend([
                "### 3. 业务场景测试",
                f"**状态**: {scenario_results['overall_status']} | **成功率**: {scenario_results['success_rate']}",
                ""
            ])
            
            for scenario in scenario_results['scenario_details']:
                status_icon = '✅' if scenario['status'] == 'PASSED' else '❌'
                report_lines.extend([
                    f"#### {status_icon} {scenario['scenario_name']}",
                    f"**测试结果**: {scenario['message']}",
                    ""
                ])
        
        # 失败测试汇总
        all_failed_items = []
        
        # 收集实施验证失败项
        if 'implementation' in summary['detailed_results']:
            failed_tests = [t for t in summary['detailed_results']['implementation']['test_details'] if t['status'] == 'FAILED']
            all_failed_items.extend([(f"实施验证 - {t['test_id']}", t['message']) for t in failed_tests])
        
        # 收集SQL验证失败项
        if 'sql_validation' in summary['detailed_results']:
            failed_validations = [v for v in summary['detailed_results']['sql_validation']['validation_details'] if v['status'] == 'FAILED']
            all_failed_items.extend([(f"SQL验证 - {v['validation_id']}", v['message']) for v in failed_validations])
        
        # 收集场景测试失败项
        if 'scenarios' in summary['detailed_results']:
            failed_scenarios = [s for s in summary['detailed_results']['scenarios']['scenario_details'] if s['status'] == 'FAILED']
            all_failed_items.extend([(f"场景测试 - {s['scenario_id']}", s['message']) for s in failed_scenarios])
        
        if all_failed_items:
            report_lines.extend([
                "## ⚠️ 失败项汇总",
                "以下测试项未通过，需要进一步修正:",
                ""
            ])
            
            for failed_item, message in all_failed_items:
                report_lines.append(f"- **{failed_item}**: {message}")
            
            report_lines.append("")
        
        # 总结和建议
        report_lines.extend([
            "## 总结",
            f"本次v1.2汇聚指标修复测试共执行了 **{summary['total_tests']}** 项测试，",
            f"其中 **{summary['total_passed']}** 项通过，**{summary['total_failed']}** 项失败，",
            f"总体成功率为 **{summary['overall_success_rate']}**。",
            ""
        ])
        
        if summary['overall_status'] == 'PASSED':
            report_lines.extend([
                "🎉 **恭喜！** v1.2规范实施完全通过所有测试验证。",
                "",
                "### 已完成的改进:",
                "- ✅ T1: 结构收敛与清退 - 移除未定义字段，rank强制为整数",
                "- ✅ T2: Metrics注入与字段转换 - 补充discrimination/百分位/等级比例，修复排名计算逻辑", 
                "- ✅ T3: 问卷指标与题目分布隔离 - 改用score_rate格式，创建独立题目分布表接口",
                "- ✅ T4: 回归测试与接口联调 - 数据质量检查，Given-When-Then测试，SQL校验",
                ""
            ])
        else:
            report_lines.extend([
                "⚠️ **注意！** v1.2规范实施存在问题，需要进一步修正。",
                "",
                "### 建议修正步骤:",
                "1. 重点关注失败的测试项目",
                "2. 检查相关代码实现",
                "3. 修正后重新运行测试",
                "4. 确保所有测试通过后再部署",
                ""
            ])
        
        # 保存报告
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"综合测试报告已生成: {report_filename}")
        
        # 在控制台输出简要结果
        print(f"\n" + "="*60)
        print(f"v1.2汇聚指标修复测试结果摘要")
        print(f"="*60)
        print(f"总体状态: {summary['overall_status']}")
        print(f"成功率: {summary['overall_success_rate']}")
        print(f"总测试: {summary['total_tests']}, 通过: {summary['total_passed']}, 失败: {summary['total_failed']}")
        print(f"执行时间: {summary['execution_time']}")
        print(f"详细报告: {report_filename}")
        print(f"="*60)


def main():
    """主函数"""
    # 可以通过命令行参数指定批次代码
    batch_code = sys.argv[1] if len(sys.argv) > 1 else "G4_2024"
    
    test_suite = V12TestSuite()
    summary = test_suite.run_complete_test_suite(batch_code)
    
    # 根据测试结果返回适当的退出码
    return 0 if summary['overall_status'] == 'PASSED' else 1


if __name__ == "__main__":
    exit(main())