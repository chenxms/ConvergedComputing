#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API v1.2 验证测试脚本

根据PO测试方案要求，验证 /api/v12/subjects 区域与学校接口的JSON返回格式：
- 检查返回JSON中百分位、区分度、维度排名字段齐全
- 无旧版fallback字段（如questions、regional_avg）
- 验证schema_version=v1.2与增强字段
"""

import asyncio
import json
import logging
import sys
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

import requests
from sqlalchemy import text

# 添加app目录到系统路径
sys.path.insert(0, 'app')

from app.database.connection import get_db_context
from app.services.subjects_builder import SubjectsBuilder

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class APIValidationTester:
    """API v1.2 验证测试器"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.test_results = []
        self.batch_code = "G7-2025"
        
    def test_api_availability(self) -> bool:
        """测试API服务可用性"""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ API服务运行正常: {response.json()}")
                return True
            else:
                logger.error(f"❌ API健康检查失败: {response.status_code}")
                return False
        except requests.exceptions.ConnectionError:
            logger.error("❌ 无法连接到API服务，请确保服务已启动")
            return False
        except Exception as e:
            logger.error(f"❌ API可用性测试失败: {e}")
            return False
    
    def validate_json_structure(self, data: Dict[str, Any], test_type: str) -> Dict[str, Any]:
        """验证JSON结构"""
        validation_result = {
            "test_type": test_type,
            "timestamp": datetime.now().isoformat(),
            "passed": True,
            "issues": [],
            "details": {}
        }
        
        # 1. 检查顶层结构
        required_top_keys = ["batch_code", "statistics_data"]
        for key in required_top_keys:
            if key not in data:
                validation_result["issues"].append(f"缺少顶层字段: {key}")
                validation_result["passed"] = False
        
        if "statistics_data" not in data:
            return validation_result
            
        stats_data = data["statistics_data"]
        
        # 2. 检查schema_version
        if "schema_version" not in stats_data:
            validation_result["issues"].append("缺少schema_version字段")
            validation_result["passed"] = False
        elif stats_data["schema_version"] != "v1.2":
            validation_result["issues"].append(f"schema_version不正确: {stats_data['schema_version']}，期望: v1.2")
            validation_result["passed"] = False
        else:
            validation_result["details"]["schema_version"] = "✅ v1.2"
        
        # 3. 检查subjects数组
        if "subjects" not in stats_data:
            validation_result["issues"].append("缺少subjects字段")
            validation_result["passed"] = False
            return validation_result
            
        subjects = stats_data["subjects"]
        if not isinstance(subjects, list) or len(subjects) == 0:
            validation_result["issues"].append("subjects字段应为非空数组")
            validation_result["passed"] = False
            return validation_result
            
        validation_result["details"]["subjects_count"] = len(subjects)
        
        # 4. 检查每个subject的结构
        for i, subject in enumerate(subjects):
            subject_issues = []
            
            # 检查基础字段
            required_subject_keys = ["subject_code", "subject_name", "metrics", "dimensions"]
            for key in required_subject_keys:
                if key not in subject:
                    subject_issues.append(f"科目{i}缺少字段: {key}")
            
            # 检查metrics字段
            if "metrics" in subject:
                metrics = subject["metrics"]
                
                # 检查百分位数
                if "percentiles" not in metrics:
                    subject_issues.append(f"科目{i}缺少percentiles字段")
                else:
                    percentiles = metrics["percentiles"]
                    required_percentiles = ["p10", "p50", "p90"]
                    for p in required_percentiles:
                        if p not in percentiles:
                            subject_issues.append(f"科目{i}缺少百分位数: {p}")
                        elif not isinstance(percentiles[p], (int, float)):
                            subject_issues.append(f"科目{i}百分位数{p}数据类型错误")
                
                # 检查区分度
                if "discrimination" not in metrics:
                    subject_issues.append(f"科目{i}缺少discrimination字段")
                elif not isinstance(metrics["discrimination"], (int, float)):
                    subject_issues.append(f"科目{i}区分度数据类型错误")
                
                # 检查是否存在旧版字段
                old_fields = ["questions", "regional_avg"]
                for old_field in old_fields:
                    if old_field in metrics:
                        subject_issues.append(f"科目{i}包含旧版字段: {old_field}")
            
            # 检查dimensions字段
            if "dimensions" in subject:
                dimensions = subject["dimensions"]
                if isinstance(dimensions, list):
                    for j, dimension in enumerate(dimensions):
                        if "ranking" not in dimension:
                            subject_issues.append(f"科目{i}维度{j}缺少ranking字段")
                        elif not isinstance(dimension["ranking"], int):
                            subject_issues.append(f"科目{i}维度{j}排名数据类型错误")
            
            if subject_issues:
                validation_result["issues"].extend(subject_issues)
                validation_result["passed"] = False
        
        return validation_result
    
    def test_regional_api(self) -> Dict[str, Any]:
        """测试区域级API"""
        logger.info("🔍 开始测试区域级subjects接口...")
        
        try:
            url = f"{self.base_url}/api/v12/subjects/regional"
            params = {"batch_code": self.batch_code}
            
            logger.info(f"请求URL: {url}")
            logger.info(f"请求参数: {params}")
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}",
                    "url": url,
                    "params": params
                }
            
            data = response.json()
            validation_result = self.validate_json_structure(data, "区域级")
            
            # 记录响应样本
            validation_result["response_sample"] = {
                "url": url,
                "params": params,
                "response_size": len(response.content),
                "response_time": response.elapsed.total_seconds()
            }
            
            # 保存完整响应到文件
            with open("regional_api_response.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            validation_result["success"] = True
            logger.info(f"✅ 区域级API测试完成，结果保存到 regional_api_response.json")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ 区域级API测试失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "test_type": "区域级"
            }
    
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
                    """)
                    .bindparam(batch_code=self.batch_code)
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
    
    def test_school_api(self, school_id: str) -> Dict[str, Any]:
        """测试学校级API"""
        logger.info(f"🔍 开始测试学校级subjects接口 (学校ID: {school_id})...")
        
        try:
            url = f"{self.base_url}/api/v12/subjects/school"
            params = {
                "batch_code": self.batch_code,
                "school_id": school_id
            }
            
            logger.info(f"请求URL: {url}")
            logger.info(f"请求参数: {params}")
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}",
                    "url": url,
                    "params": params
                }
            
            data = response.json()
            validation_result = self.validate_json_structure(data, "学校级")
            
            # 记录响应样本
            validation_result["response_sample"] = {
                "url": url,
                "params": params,
                "school_id": school_id,
                "response_size": len(response.content),
                "response_time": response.elapsed.total_seconds()
            }
            
            # 保存完整响应到文件
            with open(f"school_api_response_{school_id}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            validation_result["success"] = True
            logger.info(f"✅ 学校级API测试完成，结果保存到 school_api_response_{school_id}.json")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ 学校级API测试失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "test_type": "学校级",
                "school_id": school_id
            }
    
    async def test_internal_service(self) -> Dict[str, Any]:
        """测试内部服务调用"""
        logger.info("🔍 开始测试内部服务调用...")

        try:
            subjects_builder = SubjectsBuilder()

            # 测试区域级数据构建
            logger.info("测试区域级数据构建...")
            regional_data = await subjects_builder.build_regional_subjects(self.batch_code)

            validation_result = self.validate_json_structure(regional_data, "内部服务-区域级")

            # 保存内部服务响应
            with open("internal_service_regional_response.json", "w", encoding="utf-8") as f:
                json.dump(regional_data, f, ensure_ascii=False, indent=2)

            validation_result["success"] = True
            logger.info("✅ 内部服务测试完成，结果保存到 internal_service_regional_response.json")

            return validation_result

        except Exception as e:
            logger.error(f"❌ 内部服务测试失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "test_type": "内部服务"
            }
    
    def generate_test_report(self, results: List[Dict[str, Any]]) -> str:
        """生成测试报告"""
        report_lines = [
            "=" * 80,
            f"API v1.2 验证测试报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 80,
            "",
            f"测试批次: {self.batch_code}",
            f"API基础URL: {self.base_url}",
            "",
            "测试概要:",
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
            
            if "response_sample" in result:
                sample = result["response_sample"]
                report_lines.append(f"   URL: {sample.get('url', 'N/A')}")
                report_lines.append(f"   参数: {sample.get('params', 'N/A')}")
                report_lines.append(f"   响应大小: {sample.get('response_size', 0)} bytes")
                report_lines.append(f"   响应时间: {sample.get('response_time', 0):.3f}s")
            
            if "details" in result:
                for key, value in result["details"].items():
                    report_lines.append(f"   {key}: {value}")
            
            if "issues" in result and result["issues"]:
                report_lines.append("   问题:")
                for issue in result["issues"]:
                    report_lines.append(f"     - {issue}")
            
            if "error" in result:
                report_lines.append(f"   错误: {result['error']}")
            
            report_lines.append("")
        
        # 关键字段验证结果
        report_lines.extend([
            "关键字段验证结果:",
            "-" * 40
        ])
        
        key_validations = {
            "schema_version": "所有响应都包含v1.2版本标识",
            "percentiles": "百分位数字段(p10, p50, p90)完整性",
            "discrimination": "区分度字段存在性",
            "ranking": "维度排名字段完整性",
            "no_old_fields": "无旧版兼容字段残留"
        }
        
        for field, description in key_validations.items():
            field_passed = all(
                field not in str(r.get("issues", [])) 
                for r in results 
                if r.get("success", False)
            )
            status = "✅ PASS" if field_passed else "❌ FAIL"
            report_lines.append(f"   {field}: {status} - {description}")
        
        report_lines.extend([
            "",
            "=" * 80,
            "测试结论:",
            "-" * 40
        ])
        
        if passed_tests == total_tests and total_tests > 0:
            report_lines.append("🎉 所有测试通过！API符合v1.2规范要求。")
        else:
            report_lines.append("⚠️  部分测试失败，需要检查API实现。")
        
        report_lines.append("")
        
        return "\n".join(report_lines)
    
    async def run_all_tests(self) -> str:
        """运行所有验证测试"""
        logger.info("🚀 开始API v1.2 验证测试...")
        
        results = []
        
        # 1. 测试API可用性
        if not self.test_api_availability():
            logger.error("❌ API服务不可用，无法继续测试")
            return "API服务不可用，请确保服务已启动"
        
        # 2. 测试区域级API
        regional_result = self.test_regional_api()
        results.append(regional_result)
        
        # 3. 测试学校级API
        sample_school_id = self.get_sample_school_id()
        if sample_school_id:
            school_result = self.test_school_api(sample_school_id)
            results.append(school_result)
        else:
            logger.warning("⚠️  无法获取样本学校ID，跳过学校级API测试")
        
        # 4. 测试内部服务
        internal_result = await self.test_internal_service()
        results.append(internal_result)
        
        # 生成测试报告
        report = self.generate_test_report(results)
        
        # 保存报告到文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"api_v12_validation_report_{timestamp}.md"
        
        with open(report_filename, "w", encoding="utf-8") as f:
            f.write(report)
        
        logger.info(f"📋 测试报告已保存到: {report_filename}")
        
        return report

async def main():
    """主函数"""
    print("API v1.2 验证测试开始...")
    
    tester = APIValidationTester()
    report = await tester.run_all_tests()
    
    print("\n" + report)
    print("\n测试完成！")

if __name__ == "__main__":
    asyncio.run(main())
