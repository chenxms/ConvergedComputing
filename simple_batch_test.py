#!/usr/bin/env python3
"""
批次创建API性能和验证测试 - 简化版
"""

import requests
import time
import json
from datetime import datetime
from typing import Dict, Any


def generate_test_statistics_data() -> Dict[str, Any]:
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


def test_regional_batch():
    """测试区域级批次创建性能"""
    print("\n=== 测试1: 区域级批次创建性能 ===")
    
    batch_code = f"REGIONAL_PERF_{int(time.time())}"
    payload = {
        "batch_code": batch_code,
        "aggregation_level": "REGIONAL",
        "region_name": "测试区域",
        "statistics_data": generate_test_statistics_data(),
        "total_schools": 15
    }
    
    start_time = time.time()
    try:
        response = requests.post(
            "http://localhost:8000/api/v1/batches",
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        end_time = time.time()
        response_time = end_time - start_time
        
        print(f"响应时间: {response_time:.3f}秒")
        print(f"HTTP状态码: {response.status_code}")
        print(f"性能目标: <2秒")
        print(f"性能测试: {'通过' if response_time < 2.0 and response.status_code == 200 else '失败'}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"响应消息: {result.get('message', 'N/A')}")
        else:
            print(f"错误响应: {response.text[:200]}")
            
        return {
            "success": response.status_code == 200 and response_time < 2.0,
            "response_time": response_time,
            "status_code": response.status_code
        }
        
    except Exception as e:
        print(f"请求失败: {e}")
        return {"success": False, "error": str(e)}


def test_school_batch_with_id():
    """测试学校级批次创建性能（包含school_id）"""
    print("\n=== 测试2: 学校级批次创建性能（有school_id） ===")
    
    batch_code = f"SCHOOL_PERF_{int(time.time())}"
    payload = {
        "batch_code": batch_code,
        "aggregation_level": "SCHOOL",
        "region_name": "测试区域",
        "school_id": "TEST_SCHOOL_001",
        "school_name": "测试学校",
        "statistics_data": generate_test_statistics_data()
    }
    
    start_time = time.time()
    try:
        response = requests.post(
            "http://localhost:8000/api/v1/batches",
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        end_time = time.time()
        response_time = end_time - start_time
        
        print(f"响应时间: {response_time:.3f}秒")
        print(f"HTTP状态码: {response.status_code}")
        print(f"性能目标: <2秒")
        print(f"性能测试: {'通过' if response_time < 2.0 and response.status_code == 200 else '失败'}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"响应消息: {result.get('message', 'N/A')}")
        else:
            print(f"错误响应: {response.text[:200]}")
            
        return {
            "success": response.status_code == 200 and response_time < 2.0,
            "response_time": response_time,
            "status_code": response.status_code
        }
        
    except Exception as e:
        print(f"请求失败: {e}")
        return {"success": False, "error": str(e)}


def test_school_batch_validation():
    """测试学校级批次验证错误（缺少school_id）"""
    print("\n=== 测试3: 学校级批次验证测试（无school_id，应该失败） ===")
    
    batch_code = f"SCHOOL_VALIDATION_{int(time.time())}"
    payload = {
        "batch_code": batch_code,
        "aggregation_level": "SCHOOL",
        "region_name": "测试区域",
        # 故意不包含 school_id
        "school_name": "测试学校",
        "statistics_data": generate_test_statistics_data()
    }
    
    try:
        response = requests.post(
            "http://localhost:8000/api/v1/batches",
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        print(f"HTTP状态码: {response.status_code}")
        print(f"期望状态码: 422 (验证错误)")
        print(f"验证测试: {'通过' if response.status_code == 422 else '失败'}")
        
        if response.status_code == 422:
            try:
                result = response.json()
                print(f"验证错误详情: {result.get('detail', 'N/A')}")
            except:
                print(f"响应内容: {response.text[:200]}")
        else:
            print(f"意外响应: {response.text[:200]}")
            
        return {
            "success": response.status_code == 422,
            "status_code": response.status_code
        }
        
    except Exception as e:
        print(f"请求失败: {e}")
        return {"success": False, "error": str(e)}


def main():
    """主函数"""
    print("批次创建API性能和验证测试")
    print("=" * 50)
    
    # 检查服务状态
    try:
        health_response = requests.get("http://localhost:8000/health", timeout=5)
        print(f"服务状态: {health_response.status_code}")
    except Exception as e:
        print(f"服务连接失败: {e}")
        print("请确保服务在 http://localhost:8000 运行")
        return
    
    # 运行测试
    results = []
    
    # 测试1: 区域级批次创建性能
    results.append(test_regional_batch())
    time.sleep(0.5)
    
    # 测试2: 学校级批次创建性能
    results.append(test_school_batch_with_id())
    time.sleep(0.5)
    
    # 测试3: 学校级批次验证测试
    results.append(test_school_batch_validation())
    
    # 总结
    print("\n" + "=" * 50)
    print("测试总结")
    print("=" * 50)
    
    total_tests = len(results)
    passed_tests = sum(1 for result in results if result.get("success", False))
    
    print(f"总测试数: {total_tests}")
    print(f"通过数: {passed_tests}")
    print(f"失败数: {total_tests - passed_tests}")
    print(f"通过率: {(passed_tests/total_tests)*100:.1f}%")
    
    # 性能统计
    performance_times = [r.get("response_time") for r in results if r.get("response_time")]
    if performance_times:
        avg_time = sum(performance_times) / len(performance_times)
        max_time = max(performance_times)
        print(f"平均响应时间: {avg_time:.3f}秒")
        print(f"最大响应时间: {max_time:.3f}秒")


if __name__ == "__main__":
    main()