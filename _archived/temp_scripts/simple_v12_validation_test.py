#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化的v1.2验证测试 - 直接使用内部服务调用
专门验证G7-2025批次数据的v1.2格式合规性
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

# 添加app目录到系统路径
sys.path.insert(0, 'app')

from app.database.connection import get_db_context
from app.services.subjects_builder import SubjectsBuilder
from sqlalchemy import text

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class V12ValidationTester:
    """v1.2格式验证测试器"""
    
    def __init__(self, batch_code: str = "G7-2025"):
        self.batch_code = batch_code
        self.validation_results = []
        
    def validate_json_structure(self, data: Dict[str, Any], test_type: str) -> Dict[str, Any]:
        """验证JSON结构是否符合v1.2规范"""
        result = {
            "test_type": test_type,
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "issues": [],
            "details": {}
        }
        
        logger.info(f"🔍 开始验证{test_type}数据结构...")
        
        # 1. 检查顶层结构
        if "batch_code" not in data:
            result["issues"].append("缺少batch_code字段")
            result["passed"] = False
        
        if "statistics_data" not in data:
            result["issues"].append("缺少statistics_data字段")
            result["passed"] = False
            return result
            
        stats_data = data["statistics_data"]
        
        # 2. 验证schema_version
        if "schema_version" not in stats_data:
            result["issues"].append("❌ 缺少schema_version字段")
            result["passed"] = False
        elif stats_data["schema_version"] != "v1.2":
            result["issues"].append(f"❌ schema_version错误: {stats_data['schema_version']}, 期望: v1.2")
            result["passed"] = False
        else:
            result["details"]["schema_version"] = "✅ v1.2"
            logger.info("✅ schema_version验证通过: v1.2")
        
        # 3. 验证subjects数组
        if "subjects" not in stats_data:
            result["issues"].append("❌ 缺少subjects字段")
            result["passed"] = False
            return result
            
        subjects = stats_data["subjects"]
        if not isinstance(subjects, list) or len(subjects) == 0:
            result["issues"].append("❌ subjects字段应为非空数组")
            result["passed"] = False
            return result
            
        result["details"]["subjects_count"] = len(subjects)
        logger.info(f"📊 发现 {len(subjects)} 个科目数据")
        
        # 4. 验证每个subject的结构
        for i, subject in enumerate(subjects):
            subject_name = subject.get("subject_name", f"科目{i}")
            logger.info(f"🔍 验证科目: {subject_name}")
            
            # 检查基础字段
            required_fields = ["subject_code", "subject_name", "metrics", "dimensions"]
            for field in required_fields:
                if field not in subject:
                    result["issues"].append(f"❌ {subject_name}缺少字段: {field}")
                    result["passed"] = False
            
            # 验证metrics字段
            if "metrics" in subject:
                metrics = subject["metrics"]
                
                # 验证百分位数
                if "percentiles" not in metrics:
                    result["issues"].append(f"❌ {subject_name}缺少percentiles字段")
                    result["passed"] = False
                else:
                    percentiles = metrics["percentiles"]
                    required_percentiles = ["p10", "p50", "p90"]
                    missing_percentiles = []
                    
                    for p in required_percentiles:
                        if p not in percentiles:
                            missing_percentiles.append(p)
                        elif not isinstance(percentiles[p], (int, float)):
                            result["issues"].append(f"❌ {subject_name}百分位数{p}数据类型错误")
                            result["passed"] = False
                    
                    if missing_percentiles:
                        result["issues"].append(f"❌ {subject_name}缺少百分位数: {', '.join(missing_percentiles)}")
                        result["passed"] = False
                    else:
                        logger.info(f"  ✅ {subject_name}百分位数字段完整: p10={percentiles.get('p10')}, p50={percentiles.get('p50')}, p90={percentiles.get('p90')}")
                
                # 验证区分度
                if "discrimination" not in metrics:
                    result["issues"].append(f"❌ {subject_name}缺少discrimination字段")
                    result["passed"] = False
                elif not isinstance(metrics["discrimination"], (int, float)):
                    result["issues"].append(f"❌ {subject_name}区分度数据类型错误")
                    result["passed"] = False
                else:
                    logger.info(f"  ✅ {subject_name}区分度字段存在: {metrics['discrimination']}")
                
                # 检查是否存在旧版字段
                old_fields = ["questions", "regional_avg"]
                found_old_fields = []
                for old_field in old_fields:
                    if old_field in metrics:
                        found_old_fields.append(old_field)
                
                if found_old_fields:
                    result["issues"].append(f"❌ {subject_name}包含旧版字段: {', '.join(found_old_fields)}")
                    result["passed"] = False
                else:
                    logger.info(f"  ✅ {subject_name}无旧版fallback字段")
            
            # 验证dimensions字段
            if "dimensions" in subject:
                dimensions = subject["dimensions"]
                if isinstance(dimensions, list):
                    missing_rankings = []
                    for j, dimension in enumerate(dimensions):
                        dim_name = dimension.get("dimension_name", f"维度{j}")
                        if "ranking" not in dimension:
                            missing_rankings.append(dim_name)
                        elif not isinstance(dimension["ranking"], int):
                            result["issues"].append(f"❌ {subject_name}维度{dim_name}排名数据类型错误")
                            result["passed"] = False
                    
                    if missing_rankings:
                        result["issues"].append(f"❌ {subject_name}维度缺少ranking字段: {', '.join(missing_rankings)}")
                        result["passed"] = False
                    else:
                        logger.info(f"  ✅ {subject_name}所有维度都有ranking字段 ({len(dimensions)}个维度)")
        
        # 总结验证结果
        if result["passed"]:
            logger.info(f"✅ {test_type}数据结构验证通过")
        else:
            logger.error(f"❌ {test_type}数据结构验证失败，发现 {len(result['issues'])} 个问题")
        
        return result
    
    def get_sample_school_id(self) -> Optional[str]:
        """获取样本学校ID"""
        try:
            with get_db_context() as session:
                result = session.execute(
                    text("""
                    SELECT DISTINCT school_id
                    FROM student_score_detail
                    WHERE batch_code = :batch_code
                    LIMIT 1
                    """),
                    {"batch_code": self.batch_code}
                )

                row = result.fetchone()
                if row:
                    return row[0]
                else:
                    logger.warning("未找到样本学校ID")
                    return None

        except Exception as e:
            logger.error(f"获取样本学校ID失败: {e}")
            return None
    
    async def test_regional_subjects(self) -> Dict[str, Any]:
        """测试区域级subjects数据"""
        logger.info("🌍 开始测试区域级subjects数据构建...")
        
        try:
            subjects_builder = SubjectsBuilder()
            regional_data = await subjects_builder.build_regional_subjects(self.batch_code)
            
            # 保存原始数据
            with open("regional_subjects_v12_test.json", "w", encoding="utf-8") as f:
                json.dump(regional_data, f, ensure_ascii=False, indent=2)
            
            # 验证数据结构
            validation_result = self.validate_json_structure(regional_data, "区域级")
            validation_result["success"] = True
            validation_result["data_file"] = "regional_subjects_v12_test.json"
            
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ 区域级数据测试失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "test_type": "区域级"
            }
    
    async def test_school_subjects(self, school_id: str) -> Dict[str, Any]:
        """测试学校级subjects数据"""
        logger.info(f"🏫 开始测试学校级subjects数据构建 (学校ID: {school_id})...")
        
        try:
            subjects_builder = SubjectsBuilder()
            school_data = await subjects_builder.build_school_subjects(self.batch_code, school_id)
            
            # 保存原始数据
            filename = f"school_subjects_v12_test_{school_id}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(school_data, f, ensure_ascii=False, indent=2)
            
            # 验证数据结构
            validation_result = self.validate_json_structure(school_data, "学校级")
            validation_result["success"] = True
            validation_result["data_file"] = filename
            validation_result["school_id"] = school_id
            
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ 学校级数据测试失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "test_type": "学校级",
                "school_id": school_id
            }
    
    def generate_report(self, results: List[Dict[str, Any]]) -> str:
        """生成验证报告"""
        report_lines = [
            "=" * 80,
            f"v1.2格式验证测试报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 80,
            "",
            f"测试批次: {self.batch_code}",
            f"测试时间: {datetime.now().isoformat()}",
            "",
            "📋 测试概要:",
            "-" * 40
        ]
        
        total_tests = len(results)
        passed_tests = sum(1 for r in results if r.get("success", False) and r.get("passed", True))
        
        report_lines.extend([
            f"总测试数: {total_tests}",
            f"通过测试: {passed_tests}",
            f"失败测试: {total_tests - passed_tests}",
            f"通过率: {(passed_tests/total_tests)*100:.1f}%" if total_tests > 0 else "通过率: 0%",
            ""
        ])
        
        # 详细测试结果
        for i, result in enumerate(results, 1):
            test_type = result.get("test_type", f"测试{i}")
            success = result.get("success", False)
            passed = result.get("passed", True)
            
            status = "✅ PASS" if success and passed else "❌ FAIL"
            
            report_lines.extend([
                f"{i}. {test_type} - {status}",
                "-" * 40
            ])
            
            if "details" in result:
                for key, value in result["details"].items():
                    report_lines.append(f"   {key}: {value}")
            
            if "data_file" in result:
                report_lines.append(f"   数据文件: {result['data_file']}")
            
            if "school_id" in result:
                report_lines.append(f"   学校ID: {result['school_id']}")
            
            if "issues" in result and result["issues"]:
                report_lines.append("   ⚠️ 问题列表:")
                for issue in result["issues"]:
                    report_lines.append(f"     - {issue}")
            
            if "error" in result:
                report_lines.append(f"   ❌ 错误: {result['error']}")
            
            report_lines.append("")
        
        # 关键字段验证汇总
        report_lines.extend([
            "🔍 关键字段验证汇总:",
            "-" * 40
        ])
        
        key_validations = {
            "schema_version": "所有响应都包含v1.2版本标识",
            "percentiles": "百分位数字段(p10, p50, p90)完整性",
            "discrimination": "区分度字段存在性", 
            "ranking": "维度ranking字段完整性",
            "no_old_fields": "无旧版兼容字段残留"
        }
        
        for field, description in key_validations.items():
            field_issues = [
                issue for result in results 
                for issue in result.get("issues", [])
                if field.replace("_", "").lower() in issue.lower()
            ]
            
            status = "✅ PASS" if not field_issues else "❌ FAIL"
            report_lines.append(f"   {field}: {status} - {description}")
            
            if field_issues:
                for issue in field_issues[:3]:  # 显示前3个问题
                    report_lines.append(f"     - {issue}")
        
        # 结论
        report_lines.extend([
            "",
            "🎯 测试结论:",
            "-" * 40
        ])
        
        if passed_tests == total_tests and total_tests > 0:
            report_lines.append("🎉 所有测试通过！v1.2格式规范完全合规。")
        else:
            report_lines.append("⚠️ 部分测试失败，需要检查API实现。")
        
        report_lines.extend([
            "",
            "📁 生成的测试文件:",
            "-" * 40
        ])
        
        for result in results:
            if "data_file" in result:
                report_lines.append(f"   - {result['data_file']}")
        
        report_lines.append("")
        
        return "\n".join(report_lines)
    
    async def run_all_tests(self) -> str:
        """运行所有验证测试"""
        logger.info("🚀 开始v1.2格式验证测试...")
        
        results = []
        
        # 1. 测试区域级数据
        regional_result = await self.test_regional_subjects()
        results.append(regional_result)
        
        # 2. 测试学校级数据
        sample_school_id = self.get_sample_school_id()
        if sample_school_id:
            school_result = await self.test_school_subjects(sample_school_id)
            results.append(school_result)
        else:
            logger.warning("⚠️ 无法获取样本学校ID，跳过学校级测试")
        
        # 生成报告
        report = self.generate_report(results)
        
        # 保存报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"v12_validation_report_{timestamp}.md"
        
        with open(report_filename, "w", encoding="utf-8") as f:
            f.write(report)
        
        logger.info(f"📋 验证报告已保存到: {report_filename}")
        
        return report

async def main():
    """主函数"""
    print("v1.2格式验证测试开始...")

    tester = V12ValidationTester()
    report = await tester.run_all_tests()

    try:
        print("\n" + report)
        print("\n测试完成！")
    except UnicodeEncodeError:
        print("\n[报告已保存到文件，由于编码问题无法直接显示]")
        print("测试完成！请查看生成的markdown报告文件。")

if __name__ == "__main__":
    asyncio.run(main())
