#!/usr/bin/env python3
"""
直接测试API端点的验证功能
"""
import requests
import json
import time

API_BASE = "http://localhost:8000/api/v1/management"

def test_api_validation():
    print("Testing API validation...")
    
    # 测试1：SCHOOL级别，缺少school_id - 应该返回422
    print("\nTest 1: SCHOOL level without school_id")
    
    test_data = {
        "batch_code": "API_VALIDATION_TEST_1",
        "aggregation_level": "school",  # 注意：没有提供school_id
        "statistics_data": {
            "batch_info": {"test": "data"}, 
            "academic_subjects": {"test": "data"}
        }
    }
    
    try:
        response = requests.post(f"{API_BASE}/batches", json=test_data, timeout=30)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text[:200]}...")
        
        if response.status_code == 422:
            print("PASS - API returned 422 validation error as expected")
            return True
        else:
            print(f"FAIL - Expected 422, got {response.status_code}")
            return False
            
    except Exception as e:
        print(f"FAIL - Request error: {e}")
        return False

def test_api_with_school_id():
    print("\nTest 2: SCHOOL level with school_id")
    
    test_data = {
        "batch_code": f"API_VALIDATION_TEST_2_{int(time.time())}",
        "aggregation_level": "school",
        "school_id": "TEST_SCHOOL_123",
        "statistics_data": {
            "batch_info": {"test": "data"},
            "academic_subjects": {"test": "data"}
        }
    }
    
    try:
        start_time = time.time()
        response = requests.post(f"{API_BASE}/batches", json=test_data, timeout=30)
        duration = time.time() - start_time
        
        print(f"Status Code: {response.status_code}")
        print(f"Duration: {duration:.2f}s")
        print(f"Response: {response.text[:200]}...")
        
        if response.status_code == 200:
            print("PASS - API returned 200 success as expected")
            return True
        else:
            print(f"FAIL - Expected 200, got {response.status_code}")
            return False
            
    except Exception as e:
        print(f"FAIL - Request error: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("API Validation Test")
    print("=" * 60)
    
    results = []
    results.append(test_api_validation())
    results.append(test_api_with_school_id())
    
    print("\n" + "=" * 60)
    print(f"Test Results: {sum(results)}/{len(results)} passed")
    
    if all(results):
        print("PASS - All API validation tests passed")
    else:
        print("FAIL - Some API validation tests failed")