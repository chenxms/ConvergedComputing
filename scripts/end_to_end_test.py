#!/usr/bin/env python3
"""
端到端测试脚本
测试完整的数据处理流程：数据输入 -> 统计计算 -> 结果输出
"""

import sys
import os
import requests
import json
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.calculation import initialize_calculation_system

# 配置
API_BASE_URL = "http://localhost:8000"
TIMEOUT = 30


class EndToEndTester:
    def __init__(self):
        self.api_base = API_BASE_URL
        self.session = requests.Session()
        self.test_batch_code = f"E2E_TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
    def step1_verify_api_service(self):
        """步骤1: 验证API服务可用性"""
        print("📋 步骤1: 验证API服务...")
        
        try:
            response = self.session.get(f"{self.api_base}/health", timeout=10)
            if response.status_code == 200:
                print("✅ API服务运行正常")
                return True
            else:
                print(f"❌ API服务异常: HTTP {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ API服务不可用: {str(e)}")
            return False
    
    def step2_test_calculation_engine(self):
        """步骤2: 测试统计计算引擎"""
        print("\n📋 步骤2: 测试统计计算引擎...")
        
        try:
            # 初始化计算引擎
            engine = initialize_calculation_system()
            print("✅ 计算引擎初始化成功")
            
            # 准备测试数据
            test_scores = [95, 88, 92, 78, 85, 90, 82, 87, 93, 89, 
                          76, 91, 84, 88, 86, 79, 94, 81, 87, 83]
            
            # 测试基础统计
            basic_result = engine.calculate('basic_statistics', test_scores, {'data_type': 'scores'})
            if not basic_result or 'mean' not in basic_result:
                print("❌ 基础统计计算失败")
                return False
            
            print(f"✅ 基础统计: 平均分={basic_result['mean']:.2f}, 标准差={basic_result.get('std_dev', 0):.2f}")
            
            # 测试百分位数计算
            percentile_result = engine.calculate('percentiles', test_scores, {'data_type': 'scores'})
            if not percentile_result:
                print("❌ 百分位数计算失败")
                return False
                
            print(f"✅ 百分位数: P25={percentile_result.get('P25', 0):.1f}, P50={percentile_result.get('P50', 0):.1f}, P75={percentile_result.get('P75', 0):.1f}")
            
            # 测试等级分布
            grade_result = engine.calculate('grade_distribution', test_scores, {
                'grade_level': '4th_grade',
                'max_score': 100
            })
            if not grade_result:
                print("❌ 等级分布计算失败")
                return False
                
            distribution = grade_result.get('grade_distribution', {})
            print(f"✅ 等级分布: 优秀={distribution.get('excellent', {}).get('percentage', 0):.1f}%, 良好={distribution.get('good', {}).get('percentage', 0):.1f}%")
            
            # 测试问卷数据处理
            from app.calculation.survey import SurveyCalculator
            
            survey_calc = SurveyCalculator()
            survey_responses = [
                {"student_id": "S001", "Q1": 4, "Q2": 3, "Q3": 5},
                {"student_id": "S002", "Q1": 3, "Q2": 4, "Q3": 4},
                {"student_id": "S003", "Q1": 5, "Q2": 2, "Q3": 4}
            ]
            
            survey_config = {
                "dimensions": {
                    "好奇心": {
                        "questions": ["Q1", "Q3"],
                        "forward_questions": ["Q1", "Q3"],
                        "reverse_questions": []
                    }
                }
            }
            
            survey_result = survey_calc.process_survey_data(survey_responses, survey_config)
            if survey_result:
                print("✅ 问卷数据处理正常")
            else:
                print("❌ 问卷数据处理失败")
                return False
            
            print("✅ 统计计算引擎测试全部通过")
            return True
            
        except Exception as e:
            print(f"❌ 统计计算引擎测试失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def step3_test_batch_management(self):
        """步骤3: 测试批次管理功能"""
        print("\n📋 步骤3: 测试批次管理...")
        
        try:
            # 创建测试批次
            batch_data = {
                "batch_code": self.test_batch_code,
                "description": "端到端测试批次",
                "status": "pending"
            }
            
            response = self.session.post(
                f"{self.api_base}/api/v1/statistics/batches",
                json=batch_data,
                timeout=TIMEOUT
            )
            
            if response.status_code not in [200, 201]:
                print(f"❌ 批次创建失败: {response.status_code}")
                print(f"   响应: {response.text}")
                return False
                
            print(f"✅ 测试批次创建成功: {self.test_batch_code}")
            
            # 查询批次
            response = self.session.get(
                f"{self.api_base}/api/v1/statistics/batches/{self.test_batch_code}",
                timeout=TIMEOUT
            )
            
            if response.status_code == 200:
                batch_info = response.json()
                print(f"✅ 批次查询成功: {batch_info.get('batch_code', 'N/A')}")
            else:
                print(f"❌ 批次查询失败: {response.status_code}")
                return False
            
            return True
            
        except Exception as e:
            print(f"❌ 批次管理测试失败: {str(e)}")
            return False
    
    def step4_test_json_serialization(self):
        """步骤4: 测试JSON序列化功能"""
        print("\n📋 步骤4: 测试JSON序列化...")
        
        try:
            from app.services.serialization.statistics_json_serializer import StatisticsJsonSerializer
            
            serializer = StatisticsJsonSerializer()
            
            # 准备测试统计数据
            test_stats = {
                "batch_code": self.test_batch_code,
                "academic_subjects": {
                    "数学": {
                        "avg_score": 85.5,
                        "max_score": 100,
                        "score_rate": 0.855,
                        "grade_distribution": {
                            "excellent": {"count": 25, "percentage": 25.0},
                            "good": {"count": 45, "percentage": 45.0},
                            "pass": {"count": 25, "percentage": 25.0},
                            "fail": {"count": 5, "percentage": 5.0}
                        }
                    }
                },
                "non_academic_dimensions": {
                    "好奇心": {"avg_score": 4.2, "max_score": 5.0}
                }
            }
            
            # 测试区域级数据序列化
            regional_data = serializer.serialize_regional_data(
                batch_code=self.test_batch_code,
                region_info={"region_code": "TEST_REGION", "region_name": "测试区域"},
                statistics=test_stats
            )
            
            if not regional_data or 'data_version' not in regional_data:
                print("❌ 区域数据序列化失败")
                return False
                
            print("✅ 区域数据序列化成功")
            print(f"   数据版本: {regional_data.get('data_version', 'N/A')}")
            print(f"   Schema版本: {regional_data.get('schema_version', 'N/A')}")
            
            # 验证雷达图数据格式
            radar_data = regional_data.get('radar_chart_data', {})
            if 'academic_dimensions' in radar_data and 'non_academic_dimensions' in radar_data:
                print("✅ 雷达图数据格式正确")
            else:
                print("❌ 雷达图数据格式错误")
                return False
            
            return True
            
        except Exception as e:
            print(f"❌ JSON序列化测试失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def step5_test_api_reports(self):
        """步骤5: 测试报告API"""
        print("\n📋 步骤5: 测试报告API...")
        
        try:
            # 测试API端点（即使没有真实数据，也要验证端点结构正确）
            endpoints_to_test = [
                f"/api/v1/reports/regional/{self.test_batch_code}",
                f"/api/v1/reports/school/{self.test_batch_code}/TEST_SCHOOL_001",
                f"/api/v1/reports/radar-chart/{self.test_batch_code}"
            ]
            
            for endpoint in endpoints_to_test:
                response = self.session.get(f"{self.api_base}{endpoint}", timeout=TIMEOUT)
                
                # 404是正常的（没有真实数据），但不应该有500错误
                if response.status_code in [200, 404]:
                    print(f"✅ {endpoint}: 端点结构正常")
                elif response.status_code == 500:
                    print(f"❌ {endpoint}: 服务器内部错误")
                    return False
                else:
                    print(f"⚠️ {endpoint}: HTTP {response.status_code}")
            
            print("✅ 报告API端点测试通过")
            return True
            
        except Exception as e:
            print(f"❌ 报告API测试失败: {str(e)}")
            return False
    
    def step6_performance_check(self):
        """步骤6: 基本性能检查"""
        print("\n📋 步骤6: 基本性能检查...")
        
        try:
            # 测试API响应时间
            start_time = time.time()
            response = self.session.get(f"{self.api_base}/health", timeout=TIMEOUT)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                print(f"✅ API响应时间: {response_time:.2f}ms")
                
                if response_time < 100:
                    print("   性能等级: 优秀")
                elif response_time < 500:
                    print("   性能等级: 良好")
                else:
                    print("   性能等级: 需要优化")
            
            # 测试批量计算性能
            engine = initialize_calculation_system()
            large_dataset = list(range(1000))  # 1000个数据点
            
            start_time = time.time()
            result = engine.calculate('basic_statistics', large_dataset, {'data_type': 'scores'})
            calc_time = (time.time() - start_time) * 1000
            
            if result:
                print(f"✅ 批量计算时间(1000条): {calc_time:.2f}ms")
            else:
                print("❌ 批量计算失败")
                return False
            
            return True
            
        except Exception as e:
            print(f"❌ 性能检查失败: {str(e)}")
            return False
    
    def cleanup(self):
        """清理测试数据"""
        print("\n📋 清理测试数据...")
        
        try:
            # 删除测试批次
            response = self.session.delete(
                f"{self.api_base}/api/v1/statistics/batches/{self.test_batch_code}",
                timeout=TIMEOUT
            )
            
            if response.status_code in [200, 204, 404]:
                print("✅ 测试数据清理完成")
            else:
                print(f"⚠️ 测试数据清理警告: HTTP {response.status_code}")
                
        except Exception as e:
            print(f"⚠️ 清理过程异常: {str(e)}")


def main():
    print("=" * 70)
    print("🚀 Data-Calculation 端到端测试")
    print("=" * 70)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"API地址: {API_BASE_URL}")
    
    tester = EndToEndTester()
    
    # 定义测试步骤
    test_steps = [
        ("验证API服务", tester.step1_verify_api_service),
        ("测试统计计算引擎", tester.step2_test_calculation_engine),
        ("测试批次管理", tester.step3_test_batch_management),
        ("测试JSON序列化", tester.step4_test_json_serialization),
        ("测试报告API", tester.step5_test_api_reports),
        ("基本性能检查", tester.step6_performance_check)
    ]
    
    # 执行测试
    results = []
    for step_name, step_func in test_steps:
        print(f"\n{'='*50}")
        result = step_func()
        results.append((step_name, result))
        
        if not result:
            print(f"\n❌ 测试在 '{step_name}' 步骤失败，停止后续测试")
            break
    
    # 清理
    tester.cleanup()
    
    # 总结结果
    print("\n" + "=" * 70)
    print("📊 端到端测试结果总结")
    print("=" * 70)
    
    passed_count = 0
    total_count = len(results)
    
    for step_name, passed in results:
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"   {step_name}: {status}")
        if passed:
            passed_count += 1
    
    # 总体结果
    success_rate = (passed_count / total_count) * 100 if total_count > 0 else 0
    print(f"\n📈 总体通过率: {passed_count}/{total_count} ({success_rate:.1f}%)")
    
    if success_rate == 100:
        print("\n🎉 恭喜！所有端到端测试都通过了！")
        print("🚀 系统已准备好用于生产环境或进一步开发。")
        print("\n📋 建议后续操作：")
        print("   1. 运行负载测试验证高并发性能")
        print("   2. 导入真实数据进行业务验证")
        print("   3. 集成前端进行用户界面测试")
        print("   4. 配置监控和告警系统")
        sys.exit(0)
    elif success_rate >= 80:
        print("\n✅ 大部分测试通过，系统基本正常！")
        print("⚠️ 建议修复失败的测试项后再部署生产环境。")
        sys.exit(1)
    else:
        print("\n❌ 多个关键测试失败，需要排查和修复！")
        print("🔧 请查看上述错误信息并逐项解决。")
        sys.exit(1)


if __name__ == "__main__":
    main()