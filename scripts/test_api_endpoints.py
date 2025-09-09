#!/usr/bin/env python3
"""
API接口功能测试脚本
验证所有API端点是否正常工作
"""

import sys
import os
import requests
import json
import time
from datetime import datetime

# 配置
BASE_URL = "http://localhost:8000"
TIMEOUT = 10


class APITester:
    def __init__(self, base_url=BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()
        
    def test_health_check(self):
        """测试健康检查端点"""
        print("[TEST] 测试健康检查端点...")
        
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=TIMEOUT)
            
            if response.status_code == 200:
                print("[PASS] 健康检查端点正常")
                return True
            else:
                print(f"[FAIL] 健康检查失败: HTTP {response.status_code}")
                return False
                
        except requests.exceptions.ConnectionError:
            print("[FAIL] 无法连接到API服务")
            print("   请确保FastAPI服务已启动：")
            print("   poetry run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000")
            return False
        except Exception as e:
            print(f"[FAIL] 健康检查异常: {str(e)}")
            return False
    
    def test_api_docs(self):
        """测试API文档端点"""
        print("\n[TEST] 测试API文档端点...")
        
        try:
            response = self.session.get(f"{self.base_url}/docs", timeout=TIMEOUT)
            
            if response.status_code == 200:
                print("[PASS] API文档端点正常")
                return True
            else:
                print(f"[FAIL] API文档访问失败: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            print(f"[FAIL] API文档测试异常: {str(e)}")
            return False
    
    def test_batch_management_api(self):
        """测试批次管理API"""
        print("\n[TEST] 测试批次管理API...")
        
        test_batch_code = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # 1. 创建批次 - 使用正确的schema格式
            create_data = {
                "batch_code": test_batch_code,
                "aggregation_level": "regional",
                "statistics_data": {
                    "batch_info": {
                        "batch_code": test_batch_code,
                        "total_students": 1000,
                        "total_schools": 50
                    },
                    "academic_subjects": [
                        {
                            "subject_id": 1,
                            "subject_name": "语文",
                            "statistics": {
                                "average_score": 85.5,
                                "difficulty_coefficient": 0.71,
                                "discrimination_coefficient": 0.45
                            }
                        }
                    ]
                },
                "data_version": "1.0",
                "total_students": 1000,
                "total_schools": 50,
                "triggered_by": "api_test"
            }
            
            response = self.session.post(
                f"{self.base_url}/api/v1/management/batches",
                json=create_data,
                timeout=TIMEOUT
            )
            
            if response.status_code not in [200, 201]:
                print(f"[FAIL] 创建批次失败: HTTP {response.status_code}")
                print(f"   响应内容: {response.text}")
                return False
            
            # 获取创建的批次代码
            batch_response = response.json()
            if batch_response.get('success'):
                created_batch_code = batch_response.get('data', {}).get('batch_code') or test_batch_code
                print(f"[PASS] 批次创建成功, 批次代码: {created_batch_code}")
            else:
                print("[FAIL] 批次创建响应格式错误")
                return False
            
            # 2. 查询批次列表
            response = self.session.get(
                f"{self.base_url}/api/v1/management/batches",
                timeout=TIMEOUT
            )
            
            if response.status_code == 200:
                batches = response.json()
                print(f"[PASS] 批次查询成功，共 {len(batches)} 个批次")
            else:
                print(f"[FAIL] 批次查询失败: HTTP {response.status_code}")
                return False
            
            # 3. 查询单个批次
            response = self.session.get(
                f"{self.base_url}/api/v1/management/batches/{created_batch_code}",
                timeout=TIMEOUT
            )
            
            if response.status_code == 200:
                batch_info = response.json()
                print("[PASS] 单个批次查询成功")
                print(f"   批次代码: {batch_info.get('batch_code', 'N/A')}")
                print(f"   汇聚级别: {batch_info.get('aggregation_level', 'N/A')}")
            else:
                print(f"[FAIL] 单个批次查询失败: HTTP {response.status_code}")
                return False
            
            # 4. 更新批次
            update_data = {
                "statistics_data": {
                    "batch_info": {
                        "batch_code": created_batch_code,
                        "total_students": 1100,
                        "total_schools": 55
                    },
                    "academic_subjects": [
                        {
                            "subject_id": 1,
                            "subject_name": "语文",
                            "statistics": {
                                "average_score": 88.0,
                                "difficulty_coefficient": 0.75,
                                "discrimination_coefficient": 0.50
                            }
                        }
                    ]
                },
                "change_reason": "API测试更新"
            }
            
            response = self.session.put(
                f"{self.base_url}/api/v1/management/batches/{created_batch_code}",
                json=update_data,
                timeout=TIMEOUT
            )
            
            if response.status_code == 200:
                print("[PASS] 批次更新成功")
            else:
                print(f"[FAIL] 批次更新失败: HTTP {response.status_code}")
                return False
            
            # 5. 删除批次
            response = self.session.delete(
                f"{self.base_url}/api/v1/management/batches/{created_batch_code}",
                timeout=TIMEOUT
            )
            
            if response.status_code in [200, 204]:
                print("[PASS] 批次删除成功")
            else:
                print(f"[FAIL] 批次删除失败: HTTP {response.status_code}")
                return False
            
            print("[PASS] 批次管理API测试全部通过")
            return True
            
        except Exception as e:
            print(f"[FAIL] 批次管理API测试异常: {str(e)}")
            return False
    
    def test_system_status_api(self):
        """测试系统状态API"""
        print("\n[TEST] 测试系统状态API...")
        
        try:
            response = self.session.get(
                f"{self.base_url}/api/v1/statistics/system/status",
                timeout=TIMEOUT
            )
            
            if response.status_code == 200:
                status_info = response.json()
                print("[PASS] 系统状态API正常")
                print(f"   状态: {status_info.get('status', 'N/A')}")
                print(f"   版本: {status_info.get('version', 'N/A')}")
                print(f"   数据库连接: {status_info.get('database_status', 'N/A')}")
                return True
            else:
                print(f"[FAIL] 系统状态查询失败: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            print(f"[FAIL] 系统状态API测试异常: {str(e)}")
            return False
    
    def test_task_management_api(self):
        """测试任务管理API"""
        print("\n[TEST] 测试任务管理API...")
        
        try:
            # 查询任务列表
            response = self.session.get(
                f"{self.base_url}/api/v1/statistics/tasks",
                timeout=TIMEOUT
            )
            
            if response.status_code == 200:
                tasks = response.json()
                print(f"[PASS] 任务列表查询成功，共 {len(tasks)} 个任务")
            else:
                print(f"[FAIL] 任务列表查询失败: HTTP {response.status_code}")
                return False
            
            print("[PASS] 任务管理API测试通过")
            return True
            
        except Exception as e:
            print(f"[FAIL] 任务管理API测试异常: {str(e)}")
            return False
    
    def test_reporting_api_structure(self):
        """测试报告API结构（不需要真实数据）"""
        print("\n[TEST] 测试报告API结构...")
        
        test_batch_code = "TEST_BATCH_001"
        test_school_id = "TEST_SCHOOL_001"
        
        try:
            # 测试区域报告API结构
            response = self.session.get(
                f"{self.base_url}/api/v1/reports/regional/{test_batch_code}",
                timeout=TIMEOUT
            )
            
            if response.status_code in [200, 404]:  # 404表示没有数据但API结构正常
                print("[PASS] 区域报告API结构正常")
            else:
                print(f"[FAIL] 区域报告API异常: HTTP {response.status_code}")
                return False
            
            # 测试学校报告API结构
            response = self.session.get(
                f"{self.base_url}/api/v1/reports/school/{test_batch_code}/{test_school_id}",
                timeout=TIMEOUT
            )
            
            if response.status_code in [200, 404]:
                print("[PASS] 学校报告API结构正常")
            else:
                print(f"[FAIL] 学校报告API异常: HTTP {response.status_code}")
                return False
            
            # 测试雷达图数据API结构
            response = self.session.get(
                f"{self.base_url}/api/v1/reports/radar-chart/{test_batch_code}",
                timeout=TIMEOUT
            )
            
            if response.status_code in [200, 404]:
                print("[PASS] 雷达图数据API结构正常")
            else:
                print(f"[FAIL] 雷达图数据API异常: HTTP {response.status_code}")
                return False
            
            print("[PASS] 报告API结构测试通过")
            return True
            
        except Exception as e:
            print(f"[FAIL] 报告API测试异常: {str(e)}")
            return False
    
    def test_performance_basic(self):
        """基本性能测试"""
        print("\n[TEST] 基本性能测试...")
        
        try:
            # 测试健康检查响应时间
            start_time = time.time()
            response = self.session.get(f"{self.base_url}/health", timeout=TIMEOUT)
            response_time = (time.time() - start_time) * 1000  # 转换为毫秒
            
            if response.status_code == 200:
                print(f"[PASS] 健康检查响应时间: {response_time:.2f}ms")
                
                if response_time < 100:
                    print("   性能：优秀 (<100ms)")
                elif response_time < 500:
                    print("   性能：良好 (<500ms)")
                else:
                    print("   性能：需要优化 (>500ms)")
                
                return True
            else:
                print("[FAIL] 性能测试失败")
                return False
                
        except Exception as e:
            print(f"[FAIL] 性能测试异常: {str(e)}")
            return False


def main():
    print("=" * 60)
    print("[START] Data-Calculation API接口功能测试")
    print("=" * 60)
    
    tester = APITester()
    
    # 运行所有测试
    tests = [
        ("健康检查端点", tester.test_health_check),
        ("API文档端点", tester.test_api_docs),
        ("批次管理API", tester.test_batch_management_api),
        ("系统状态API", tester.test_system_status_api),
        ("任务管理API", tester.test_task_management_api),
        ("报告API结构", tester.test_reporting_api_structure),
        ("基本性能测试", tester.test_performance_basic)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n[STEP] {test_name}")
        print("-" * 40)
        result = test_func()
        results.append((test_name, result))
    
    # 总结结果
    print("\n" + "=" * 60)
    print("[SUMMARY] 测试结果总结")
    print("=" * 60)
    
    all_passed = True
    for test_name, passed in results:
        status = "[PASS] PASS" if passed else "[FAIL] FAIL"
        print(f"   {test_name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n[SUCCESS] 所有API测试通过！")
        print("\n[STEP] 下一步建议：")
        print("   1. 创建测试数据运行完整业务流程测试")
        print("   2. 进行负载测试验证并发性能")
        print("   3. 集成前端进行端到端测试")
        sys.exit(0)
    else:
        print("\n[FAIL] 部分API测试失败，请检查：")
        print("   1. FastAPI服务是否已启动")
        print("   2. 数据库连接是否正常")
        print("   3. 相关依赖是否已安装")
        sys.exit(1)


if __name__ == "__main__":
    main()