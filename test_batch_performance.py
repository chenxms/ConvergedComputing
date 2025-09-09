#!/usr/bin/env python3
"""
批次创建API性能和验证测试
测试场景：
1. 性能测试 - 区域级批次创建（应该 <2秒）
2. 性能测试 - 学校级批次创建（有school_id，应该 <2秒）
3. 验证测试 - 学校级批次创建（无school_id，应该返回422错误）
"""

import requests
import time
import json
from datetime import datetime
from typing import Dict, Any


class BatchAPITester:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
    
    def generate_test_statistics_data(self) -> Dict[str, Any]:
        """生成测试用的统计数据"""
        return {
            "batch_info": {
                "batch_code": f"TEST_{int(time.time())}",
                "created_at": datetime.now().isoformat(),
                "data_version": "v1.0"
            },
            "academic_subjects": {
                "chinese": {
                    "total_score": 450,
                    "average_score": 85.5,
                    "pass_rate": 0.92
                },
                "math": {
                    "total_score": 500,
                    "average_score": 88.2,
                    "pass_rate": 0.95
                }
            }
        }
    
    def test_regional_batch_performance(self) -> Dict[str, Any]:
        """测试区域级批次创建性能"""
        print("\n=== 测试1：区域级批次创建性能 ===")
        
        batch_code = f"REGIONAL_PERF_{int(time.time())}"
        payload = {
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "region_name": "测试区域",
            "statistics_data": self.generate_test_statistics_data(),
            "total_schools": 15
        }
        
        start_time = time.time()
        response = self.session.post(
            f"{self.base_url}/api/v1/batches",
            json=payload
        )
        end_time = time.time()
        
        response_time = end_time - start_time
        
        result = {
            "test_name": "区域级批次创建性能测试",
            "response_time": response_time,
            "status_code": response.status_code,
            "success": response.status_code == 200 and response_time < 2.0,
            "expected_time": "<2秒",
            "actual_time": f"{response_time:.3f}秒",
            "payload": payload,
            "response": response.json() if response.status_code == 200 else response.text
        }
        
        self._print_test_result(result)
        return result
    
    def test_school_batch_with_id_performance(self) -> Dict[str, Any]:
        """测试学校级批次创建性能（包含school_id）"""
        print("\n=== 测试2：学校级批次创建性能（有school_id） ===")
        
        batch_code = f"SCHOOL_PERF_{int(time.time())}"
        payload = {
            "batch_code": batch_code,
            "aggregation_level": "SCHOOL",
            "region_name": "测试区域",
            "school_id": "TEST_SCHOOL_001",
            "school_name": "测试学校",
            "statistics_data": self.generate_test_statistics_data()
        }
        
        start_time = time.time()
        response = self.session.post(
            f"{self.base_url}/api/v1/batches",
            json=payload
        )
        end_time = time.time()
        
        response_time = end_time - start_time
        
        result = {
            "test_name": "学校级批次创建性能测试（有school_id）",
            "response_time": response_time,
            "status_code": response.status_code,
            "success": response.status_code == 200 and response_time < 2.0,
            "expected_time": "<2秒",
            "actual_time": f"{response_time:.3f}秒",
            "payload": payload,
            "response": response.json() if response.status_code == 200 else response.text
        }
        
        self._print_test_result(result)
        return result
    
    def test_school_batch_validation_error(self) -> Dict[str, Any]:
        """测试学校级批次验证错误（缺少school_id）"""
        print("\n=== 测试3：学校级批次验证测试（无school_id，应该失败） ===")
        
        batch_code = f"SCHOOL_VALIDATION_{int(time.time())}"
        payload = {
            "batch_code": batch_code,
            "aggregation_level": "SCHOOL",
            "region_name": "测试区域",
            # 故意不包含 school_id
            "school_name": "测试学校",
            "statistics_data": self.generate_test_statistics_data()
        }
        
        start_time = time.time()
        response = self.session.post(
            f"{self.base_url}/api/v1/batches",
            json=payload
        )
        end_time = time.time()
        
        response_time = end_time - start_time
        
        result = {
            "test_name": "学校级批次验证测试（无school_id）",
            "response_time": response_time,
            "status_code": response.status_code,
            "success": response.status_code == 422,  # 期望返回422验证错误
            "expected_status": "422 (验证错误)",
            "actual_status": f"{response.status_code}",
            "payload": payload,
            "response": response.json() if hasattr(response, 'json') and response.headers.get('content-type', '').startswith('application/json') else response.text
        }
        
        self._print_test_result(result)
        return result
    
    def _print_test_result(self, result: Dict[str, Any]):
        """打印测试结果"""
        print(f"测试名称: {result['test_name']}")
        print(f"状态: {'✓ 通过' if result['success'] else '✗ 失败'}")
        print(f"响应时间: {result['response_time']:.3f}秒")
        print(f"HTTP状态码: {result['status_code']}")
        
        if 'expected_time' in result:
            print(f"期望时间: {result['expected_time']}")
            print(f"实际时间: {result['actual_time']}")
        
        if 'expected_status' in result:
            print(f"期望状态: {result['expected_status']}")
            print(f"实际状态: {result['actual_status']}")
        
        # 只打印响应的关键部分，避免过长输出
        if isinstance(result['response'], dict):
            if 'message' in result['response']:
                print(f"响应消息: {result['response']['message']}")
            if 'detail' in result['response']:
                print(f"错误详情: {result['response']['detail']}")
        else:
            # 如果响应过长，只显示前200字符
            response_str = str(result['response'])
            if len(response_str) > 200:
                response_str = response_str[:200] + "..."
            print(f"响应内容: {response_str}")
    
    def run_all_tests(self) -> Dict[str, Any]:
        """运行所有测试"""
        print("开始批次创建API性能和验证测试")
        print("=" * 60)
        
        # 检查服务是否可用
        try:
            health_response = self.session.get(f"{self.base_url}/health")
            if health_response.status_code != 200:
                print(f"警告: 服务健康检查失败 (状态码: {health_response.status_code})")
        except Exception as e:
            print(f"警告: 无法连接到服务 {self.base_url}: {e}")
        
        results = {
            "test_timestamp": datetime.now().isoformat(),
            "base_url": self.base_url,
            "tests": []
        }
        
        # 执行三个测试场景
        test_methods = [
            self.test_regional_batch_performance,
            self.test_school_batch_with_id_performance,
            self.test_school_batch_validation_error
        ]
        
        for test_method in test_methods:
            try:
                test_result = test_method()
                results["tests"].append(test_result)
                time.sleep(0.5)  # 测试间隔
            except Exception as e:
                error_result = {
                    "test_name": test_method.__name__,
                    "success": False,
                    "error": str(e),
                    "response_time": None
                }
                results["tests"].append(error_result)
                print(f"测试 {test_method.__name__} 执行失败: {e}")
        
        self._print_summary(results)
        return results
    
    def _print_summary(self, results: Dict[str, Any]):
        """打印测试总结"""
        print("\n" + "=" * 60)
        print("测试总结")
        print("=" * 60)
        
        total_tests = len(results["tests"])
        passed_tests = sum(1 for test in results["tests"] if test.get("success", False))
        
        print(f"总测试数: {total_tests}")
        print(f"通过数: {passed_tests}")
        print(f"失败数: {total_tests - passed_tests}")
        print(f"通过率: {(passed_tests/total_tests)*100:.1f}%")
        
        # 性能统计
        performance_tests = [test for test in results["tests"] if test.get("response_time") is not None]
        if performance_tests:
            avg_response_time = sum(test["response_time"] for test in performance_tests) / len(performance_tests)
            max_response_time = max(test["response_time"] for test in performance_tests)
            print(f"平均响应时间: {avg_response_time:.3f}秒")
            print(f"最大响应时间: {max_response_time:.3f}秒")
        
        # 失败的测试详情
        failed_tests = [test for test in results["tests"] if not test.get("success", False)]
        if failed_tests:
            print("\n失败的测试:")
            for test in failed_tests:
                print(f"- {test['test_name']}")
                if 'error' in test:
                    print(f"  错误: {test['error']}")


def main():
    """主函数"""
    tester = BatchAPITester()
    results = tester.run_all_tests()
    
    # 保存测试结果到文件
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_file = f"batch_test_results_{timestamp}.json"
    
    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n详细测试结果已保存到: {result_file}")


if __name__ == "__main__":
    main()