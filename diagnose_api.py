#!/usr/bin/env python3
"""
API诊断脚本 - 详细分析API问题
"""

import requests
import json
import sys
from datetime import datetime

BASE_URL = "http://localhost:8000"

def test_connection():
    """测试基础连接"""
    print("=== 连接诊断 ===")
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        print(f"健康检查: {response.status_code} - {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"连接失败: {e}")
        return False

def test_batch_creation():
    """详细测试批次创建"""
    print("\n=== 批次创建诊断 ===")
    
    test_data = {
        "batch_code": f"DIAG_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "aggregation_level": "regional",
        "statistics_data": {
            "batch_info": {
                "batch_code": f"DIAG_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
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
        "triggered_by": "diagnose_test"
    }
    
    print("发送数据:")
    print(json.dumps(test_data, indent=2, ensure_ascii=False))
    
    try:
        response = requests.post(
            f"{BASE_URL}/api/v1/management/batches",
            json=test_data,
            timeout=10
        )
        
        print(f"\n响应状态码: {response.status_code}")
        print(f"响应头: {dict(response.headers)}")
        print(f"响应内容: {response.text}")
        
        if response.status_code == 200:
            print("\n✅ 批次创建成功!")
            return True
        elif response.status_code == 422:
            print("\n❌ 数据验证错误 (422)")
            try:
                error_detail = response.json()
                print(f"验证错误详情: {json.dumps(error_detail, indent=2, ensure_ascii=False)}")
            except:
                pass
        elif response.status_code == 500:
            print("\n❌ 服务器内部错误 (500)")
        else:
            print(f"\n❌ 其他错误: {response.status_code}")
            
        return False
        
    except requests.exceptions.ConnectionError:
        print("❌ 无法连接到服务器")
        return False
    except Exception as e:
        print(f"❌ 请求异常: {e}")
        return False

def test_api_endpoints():
    """测试其他API端点"""
    print("\n=== API端点诊断 ===")
    
    endpoints = [
        ("/api/v1/management/batches", "GET", "批次列表"),
        ("/api/v1/statistics/system/status", "GET", "系统状态"),
        ("/api/v1/statistics/tasks", "GET", "任务列表")
    ]
    
    for endpoint, method, name in endpoints:
        try:
            if method == "GET":
                response = requests.get(f"{BASE_URL}{endpoint}", timeout=10)
            
            print(f"{name}: {response.status_code}")
            if response.status_code != 200:
                print(f"  错误: {response.text[:100]}")
        except Exception as e:
            print(f"{name}: 异常 - {e}")

def main():
    print("🔍 Data-Calculation API 详细诊断")
    print("=" * 50)
    
    # 1. 测试连接
    if not test_connection():
        print("❌ 基础连接失败，请检查服务是否启动")
        sys.exit(1)
    
    # 2. 测试批次创建
    batch_success = test_batch_creation()
    
    # 3. 测试其他端点
    test_api_endpoints()
    
    # 4. 总结
    print("\n" + "=" * 50)
    if batch_success:
        print("🎉 批次管理API工作正常!")
    else:
        print("❌ 批次管理API仍有问题，请查看上述诊断信息")

if __name__ == "__main__":
    main()