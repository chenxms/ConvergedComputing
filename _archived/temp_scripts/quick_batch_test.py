#!/usr/bin/env python3
"""
快速批次管理API测试脚本
验证数据格式修复是否成功
"""

import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8000"

def test_batch_api():
    """快速测试批次API"""
    print("=== 快速批次API测试 ===")
    
    # 生成测试批次代码
    test_batch_code = f"QUICK_TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # 正确的数据格式
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
        "triggered_by": "quick_test"
    }
    
    try:
        # 1. 测试创建批次
        print("\n[TEST] 创建批次...")
        response = requests.post(
            f"{BASE_URL}/api/v1/management/batches",
            json=create_data,
            timeout=10
        )
        
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        
        if response.status_code in [200, 201]:
            print("[SUCCESS] 批次创建成功！")
            
            # 2. 测试查询批次
            print("\n[TEST] 查询批次...")
            get_response = requests.get(
                f"{BASE_URL}/api/v1/management/batches/{test_batch_code}",
                timeout=10
            )
            
            print(f"查询状态码: {get_response.status_code}")
            if get_response.status_code == 200:
                print("[SUCCESS] 批次查询成功！")
                batch_info = get_response.json()
                print(f"批次信息: {batch_info.get('batch_code', 'N/A')}")
            else:
                print(f"[WARNING] 批次查询失败: {get_response.text}")
            
            return True
        else:
            print(f"[FAIL] 批次创建失败")
            print(f"错误详情: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("[ERROR] 无法连接到API服务，请确保FastAPI正在运行")
        return False
    except Exception as e:
        print(f"[ERROR] 测试异常: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_batch_api()
    if success:
        print("\n🎉 批次管理API修复成功！")
    else:
        print("\n❌ 批次管理API仍有问题")