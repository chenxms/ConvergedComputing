#!/usr/bin/env python3
"""
Focused Batch API Test
Tests core batch creation functionality with detailed validation
"""

import requests
import json
import uuid
import time
from datetime import datetime

BASE_URL = "http://localhost:8000"
API_URL = f"{BASE_URL}/api/v1/management/batches"

def generate_batch_code(prefix="TEST"):
    return f"{prefix}_{datetime.now().strftime('%H%M%S')}_{str(uuid.uuid4())[:6].upper()}"

def test_regional_batch():
    print("\n[TEST 1] Regional Batch Creation")
    print("-" * 50)
    
    batch_code = generate_batch_code("REGIONAL")
    data = {
        "batch_code": batch_code,
        "aggregation_level": "regional",
        "school_name": "全市统计",
        "statistics_data": {
            "batch_info": {
                "batch_code": batch_code,
                "total_students": 15000,
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
                },
                {
                    "subject_id": 2,
                    "subject_name": "数学",
                    "statistics": {
                        "average_score": 82.3,
                        "difficulty_coefficient": 0.68,
                        "discrimination_coefficient": 0.52
                    }
                }
            ]
        },
        "data_version": "1.0",
        "total_students": 15000,
        "total_schools": 50,
        "change_reason": "测试数据",
        "triggered_by": "test_system"
    }
    
    start_time = time.time()
    try:
        response = requests.post(API_URL, json=data, timeout=30)
        response_time = time.time() - start_time
        
        print(f"Request time: {response_time:.3f}s")
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Success: {result.get('success')}")
            print(f"Message: {result.get('message')}")
            print(f"Batch ID: {result.get('data', {}).get('batch_id')}")
            
            # Verify retrieval
            verify_url = f"{API_URL}/{batch_code}?aggregation_level=regional"
            verify_response = requests.get(verify_url)
            
            if verify_response.status_code == 200:
                batch_data = verify_response.json()
                print(f"Verification: SUCCESS")
                print(f"  - Retrieved ID: {batch_data.get('id')}")
                print(f"  - Students: {batch_data.get('total_students')}")
                print(f"  - Schools: {batch_data.get('total_schools')}")
                return batch_code, True
            else:
                print(f"Verification: FAILED ({verify_response.status_code})")
                return batch_code, False
        else:
            print(f"Error: {response.text}")
            return None, False
            
    except Exception as e:
        print(f"Exception: {e}")
        return None, False

def test_school_batch():
    print("\n[TEST 2] School Batch Creation")
    print("-" * 50)
    
    batch_code = generate_batch_code("SCHOOL")
    school_id = f"SCHOOL_{str(uuid.uuid4())[:8]}"
    
    data = {
        "batch_code": batch_code,
        "aggregation_level": "school",
        "school_id": school_id,
        "school_name": "测试中学",
        "statistics_data": {
            "batch_info": {
                "batch_code": batch_code,
                "school_id": school_id,
                "total_students": 300
            },
            "academic_subjects": [
                {
                    "subject_id": 1,
                    "subject_name": "语文",
                    "statistics": {
                        "average_score": 87.2,
                        "difficulty_coefficient": 0.73,
                        "discrimination_coefficient": 0.42
                    }
                }
            ]
        },
        "data_version": "1.0",
        "total_students": 300,
        "total_schools": 0,
        "change_reason": "学校测试数据",
        "triggered_by": "test_system"
    }
    
    start_time = time.time()
    try:
        response = requests.post(API_URL, json=data, timeout=30)
        response_time = time.time() - start_time
        
        print(f"Request time: {response_time:.3f}s")
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Success: {result.get('success')}")
            print(f"Message: {result.get('message')}")
            print(f"Batch ID: {result.get('data', {}).get('batch_id')}")
            
            # Verify retrieval
            verify_url = f"{API_URL}/{batch_code}?aggregation_level=school&school_id={school_id}"
            verify_response = requests.get(verify_url)
            
            if verify_response.status_code == 200:
                batch_data = verify_response.json()
                print(f"Verification: SUCCESS")
                print(f"  - Retrieved ID: {batch_data.get('id')}")
                print(f"  - School ID: {batch_data.get('school_id')}")
                print(f"  - Students: {batch_data.get('total_students')}")
                return (batch_code, school_id), True
            else:
                print(f"Verification: FAILED ({verify_response.status_code})")
                return (batch_code, school_id), False
        else:
            print(f"Error: {response.text}")
            return None, False
            
    except Exception as e:
        print(f"Exception: {e}")
        return None, False

def test_validation():
    print("\n[TEST 3] Input Validation")
    print("-" * 50)
    
    test_cases = [
        {
            "name": "Missing batch_code",
            "data": {
                "aggregation_level": "regional",
                "statistics_data": {"batch_info": {}, "academic_subjects": []},
                "data_version": "1.0"
            }
        },
        {
            "name": "School without school_id",
            "data": {
                "batch_code": generate_batch_code("INVALID"),
                "aggregation_level": "school",
                "statistics_data": {"batch_info": {}, "academic_subjects": []},
                "data_version": "1.0"
            }
        },
        {
            "name": "Invalid aggregation level",
            "data": {
                "batch_code": generate_batch_code("INVALID2"),
                "aggregation_level": "invalid",
                "statistics_data": {"batch_info": {}, "academic_subjects": []},
                "data_version": "1.0"
            }
        }
    ]
    
    passed = 0
    for case in test_cases:
        try:
            response = requests.post(API_URL, json=case["data"], timeout=10)
            if response.status_code in [400, 422]:
                print(f"[PASS] {case['name']}: Correctly rejected ({response.status_code})")
                passed += 1
            else:
                print(f"[FAIL] {case['name']}: Unexpected status {response.status_code}")
        except Exception as e:
            print(f"[FAIL] {case['name']}: Exception {e}")
    
    return passed, len(test_cases)

def test_duplicate_handling(existing_batch):
    if not existing_batch:
        print("\n[TEST 4] Duplicate Handling - SKIPPED (no existing batch)")
        return True
        
    print("\n[TEST 4] Duplicate Batch Handling")
    print("-" * 50)
    
    # Try to create same batch again
    data = {
        "batch_code": existing_batch,
        "aggregation_level": "regional",
        "statistics_data": {"batch_info": {}, "academic_subjects": []},
        "data_version": "1.0"
    }
    
    try:
        response = requests.post(API_URL, json=data, timeout=10)
        if response.status_code in [400, 409, 422]:
            print(f"[PASS] Duplicate correctly rejected ({response.status_code})")
            return True
        else:
            print(f"[FAIL] Duplicate not rejected (status: {response.status_code})")
            return False
    except Exception as e:
        print(f"[FAIL] Exception: {e}")
        return False

def test_batch_listing():
    print("\n[TEST 5] Batch Listing")
    print("-" * 50)
    
    try:
        response = requests.get(API_URL, timeout=10)
        if response.status_code == 200:
            batches = response.json()
            print(f"[PASS] Retrieved {len(batches)} batches")
            
            # Show sample batches
            for i, batch in enumerate(batches[:3]):
                print(f"  {i+1}. {batch.get('batch_code')} ({batch.get('aggregation_level')})")
            
            return True
        else:
            print(f"[FAIL] Failed to list batches: {response.status_code}")
            return False
    except Exception as e:
        print(f"[FAIL] Exception: {e}")
        return False

def main():
    print("=" * 60)
    print("FOCUSED BATCH API TEST SUITE")
    print("=" * 60)
    print(f"API Endpoint: {API_URL}")
    print(f"Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check service health
    try:
        health = requests.get(f"{BASE_URL}/health", timeout=5)
        if health.status_code != 200:
            print(f"[ERROR] Service not healthy: {health.status_code}")
            return
        print("[OK] Service is healthy")
    except Exception as e:
        print(f"[ERROR] Cannot reach service: {e}")
        return
    
    results = []
    
    # Test 1: Regional batch
    regional_batch, success1 = test_regional_batch()
    results.append(("Regional Batch Creation", success1))
    
    # Test 2: School batch  
    school_result, success2 = test_school_batch()
    results.append(("School Batch Creation", success2))
    
    # Test 3: Validation
    passed_validations, total_validations = test_validation()
    success3 = passed_validations == total_validations
    results.append(("Input Validation", success3, f"{passed_validations}/{total_validations}"))
    
    # Test 4: Duplicates
    success4 = test_duplicate_handling(regional_batch)
    results.append(("Duplicate Handling", success4))
    
    # Test 5: Listing
    success5 = test_batch_listing()
    results.append(("Batch Listing", success5))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST RESULTS SUMMARY")
    print("=" * 60)
    
    total_tests = len(results)
    passed_tests = sum(1 for r in results if r[1])
    
    for result in results:
        status = "PASS" if result[1] else "FAIL"
        extra = f" ({result[2]})" if len(result) > 2 else ""
        print(f"  {result[0]}: {status}{extra}")
    
    print(f"\nOverall: {passed_tests}/{total_tests} tests passed ({passed_tests/total_tests*100:.1f}%)")
    
    # Key metrics
    if regional_batch:
        print(f"\nCreated Regional Batch: {regional_batch}")
    if school_result and isinstance(school_result, tuple):
        print(f"Created School Batch: {school_result[0]} (School: {school_result[1]})")
    
    if passed_tests == total_tests:
        print("\n[SUCCESS] ALL TESTS PASSED - API is working correctly!")
    elif passed_tests >= total_tests * 0.8:
        print("\n[WARNING]  MOSTLY WORKING - Some issues found")
    else:
        print("\n[CRITICAL] SIGNIFICANT ISSUES - API needs attention")

if __name__ == "__main__":
    main()