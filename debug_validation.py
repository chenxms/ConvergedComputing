#!/usr/bin/env python3
"""
调试验证问题 - 直接使用不同的输入格式测试
"""
import requests
import json

API_BASE = "http://localhost:8000/api/v1/management"

def test_different_formats():
    print("Testing different input formats...")
    
    # 格式1：school_id完全缺失
    print("\n=== Format 1: school_id completely missing ===")
    test_data_1 = {
        "batch_code": "DEBUG_TEST_1",
        "aggregation_level": "school",
        # school_id 完全不存在
        "statistics_data": {
            "batch_info": {"test": "data"},
            "academic_subjects": {"test": "data"}
        }
    }
    
    response_1 = requests.post(f"{API_BASE}/batches", json=test_data_1, timeout=10)
    print(f"Status: {response_1.status_code}")
    print(f"Response: {response_1.text[:300]}")
    
    # 格式2：school_id设置为null
    print("\n=== Format 2: school_id set to null ===")
    test_data_2 = {
        "batch_code": "DEBUG_TEST_2", 
        "aggregation_level": "school",
        "school_id": None,  # 显式设置为None
        "statistics_data": {
            "batch_info": {"test": "data"},
            "academic_subjects": {"test": "data"}
        }
    }
    
    response_2 = requests.post(f"{API_BASE}/batches", json=test_data_2, timeout=10)
    print(f"Status: {response_2.status_code}")
    print(f"Response: {response_2.text[:300]}")
    
    # 格式3：school_id设置为空字符串
    print("\n=== Format 3: school_id set to empty string ===")
    test_data_3 = {
        "batch_code": "DEBUG_TEST_3",
        "aggregation_level": "school", 
        "school_id": "",  # 空字符串
        "statistics_data": {
            "batch_info": {"test": "data"},
            "academic_subjects": {"test": "data"}
        }
    }
    
    response_3 = requests.post(f"{API_BASE}/batches", json=test_data_3, timeout=10)
    print(f"Status: {response_3.status_code}")
    print(f"Response: {response_3.text[:300]}")

if __name__ == "__main__":
    test_different_formats()