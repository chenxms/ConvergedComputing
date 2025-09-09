#!/usr/bin/env python3
"""
Final Comprehensive Batch API Test Report
Demonstrates all core functionality with detailed results
"""
import requests
import json
import uuid
import time
from datetime import datetime

BASE_URL = "http://localhost:8000"
API_URL = f"{BASE_URL}/api/v1/management/batches"

def print_section(title):
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")

def print_test(name):
    print(f"\n[TEST] {name}")
    print("-" * 40)

def test_regional_batch_comprehensive():
    print_test("Regional Batch Creation - Comprehensive Data")
    
    batch_code = f"COMPREHENSIVE_REGIONAL_{int(time.time())}"
    
    comprehensive_data = {
        "batch_code": batch_code,
        "aggregation_level": "regional",
        "school_name": "全市统计汇总",
        "statistics_data": {
            "batch_info": {
                "batch_code": batch_code,
                "total_students": 50000,
                "total_schools": 120,
                "calculation_date": datetime.now().isoformat(),
                "region_name": "示例教育区",
                "exam_period": "2025年第一学期期末考试"
            },
            "academic_subjects": [
                {
                    "subject_id": 1,
                    "subject_name": "语文",
                    "statistics": {
                        "average_score": 84.7,
                        "difficulty_coefficient": 0.72,
                        "discrimination_coefficient": 0.46,
                        "student_count": 50000,
                        "score_distribution": {
                            "excellent": 10000,
                            "good": 25000,
                            "pass": 12000,
                            "fail": 3000
                        },
                        "grade_analysis": {
                            "grade_1": {"avg": 82.3, "count": 16667, "ranking": "良好"},
                            "grade_2": {"avg": 84.8, "count": 16666, "ranking": "优秀"},
                            "grade_3": {"avg": 87.0, "count": 16667, "ranking": "优秀"}
                        }
                    }
                },
                {
                    "subject_id": 2,
                    "subject_name": "数学",
                    "statistics": {
                        "average_score": 81.2,
                        "difficulty_coefficient": 0.69,
                        "discrimination_coefficient": 0.52,
                        "student_count": 50000,
                        "score_distribution": {
                            "excellent": 8500,
                            "good": 22500,
                            "pass": 15000,
                            "fail": 4000
                        },
                        "grade_analysis": {
                            "grade_1": {"avg": 78.9, "count": 16667, "ranking": "良好"},
                            "grade_2": {"avg": 81.2, "count": 16666, "ranking": "良好"},
                            "grade_3": {"avg": 83.5, "count": 16667, "ranking": "优秀"}
                        }
                    }
                },
                {
                    "subject_id": 3,
                    "subject_name": "英语",
                    "statistics": {
                        "average_score": 77.8,
                        "difficulty_coefficient": 0.65,
                        "discrimination_coefficient": 0.48,
                        "student_count": 50000,
                        "score_distribution": {
                            "excellent": 7000,
                            "good": 20000,
                            "pass": 18000,
                            "fail": 5000
                        }
                    }
                }
            ],
            "regional_summary": {
                "total_participants": 50000,
                "total_schools": 120,
                "average_school_size": 417,
                "completion_rate": 98.5,
                "overall_performance": "良好",
                "top_performing_schools": 12,
                "improvement_needed_schools": 8,
                "subject_rankings": ["语文", "数学", "英语"],
                "recommendations": [
                    "加强数学基础训练",
                    "提升英语口语能力",
                    "继续保持语文优势"
                ]
            }
        },
        "data_version": "2.0",
        "total_students": 50000,
        "total_schools": 120,
        "change_reason": "2025年度综合统计分析",
        "triggered_by": "comprehensive_test_system"
    }
    
    print(f"Batch Code: {batch_code}")
    print(f"Data Size: {len(json.dumps(comprehensive_data))} bytes")
    print(f"Students: {comprehensive_data['total_students']:,}")
    print(f"Schools: {comprehensive_data['total_schools']}")
    print(f"Subjects: {len(comprehensive_data['statistics_data']['academic_subjects'])}")
    
    start_time = time.time()
    
    try:
        response = requests.post(API_URL, json=comprehensive_data, timeout=60)
        response_time = time.time() - start_time
        
        print(f"Response Time: {response_time:.2f}s")
        print(f"HTTP Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"API Response: SUCCESS")
            print(f"  - Success: {result.get('success')}")
            print(f"  - Message: {result.get('message')}")
            print(f"  - Batch ID: {result.get('data', {}).get('batch_id')}")
            print(f"  - Created At: {result.get('data', {}).get('created_at')}")
            print(f"[RESULT] PASS - Regional batch created successfully")
            return batch_code
        else:
            print(f"API Response: FAILED")
            print(f"Error: {response.text}")
            print(f"[RESULT] FAIL - HTTP {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Exception: {str(e)}")
        print(f"[RESULT] FAIL - Exception occurred")
        return None

def test_school_batch_detailed():
    print_test("School Batch Creation - Detailed Data")
    
    batch_code = f"COMPREHENSIVE_SCHOOL_{int(time.time())}"
    school_id = f"SCHOOL_{str(uuid.uuid4())[:12].upper()}"
    
    school_data = {
        "batch_code": batch_code,
        "aggregation_level": "school",
        "school_id": school_id,
        "school_name": "示例第一中学",
        "statistics_data": {
            "batch_info": {
                "batch_code": batch_code,
                "school_id": school_id,
                "school_name": "示例第一中学",
                "total_students": 1200,
                "calculation_date": datetime.now().isoformat(),
                "school_type": "市重点中学",
                "district": "中心城区",
                "principal": "张校长"
            },
            "academic_subjects": [
                {
                    "subject_id": 1,
                    "subject_name": "语文",
                    "statistics": {
                        "average_score": 89.3,
                        "difficulty_coefficient": 0.74,
                        "discrimination_coefficient": 0.43,
                        "student_count": 1200,
                        "class_statistics": [
                            {
                                "class_id": "C2025_001",
                                "class_name": "高三(1)班",
                                "average_score": 91.5,
                                "student_count": 48,
                                "teacher_name": "李语文",
                                "top_score": 98,
                                "lowest_score": 78
                            },
                            {
                                "class_id": "C2025_002",
                                "class_name": "高三(2)班",
                                "average_score": 89.8,
                                "student_count": 47,
                                "teacher_name": "王语文",
                                "top_score": 96,
                                "lowest_score": 75
                            },
                            {
                                "class_id": "C2025_003",
                                "class_name": "高三(3)班",
                                "average_score": 87.2,
                                "student_count": 46,
                                "teacher_name": "张语文",
                                "top_score": 94,
                                "lowest_score": 72
                            }
                        ],
                        "teacher_performance": {
                            "best_class": "高三(1)班",
                            "improvement_areas": ["阅读理解", "作文表达"],
                            "strengths": ["基础知识", "古诗词理解"]
                        }
                    }
                },
                {
                    "subject_id": 2,
                    "subject_name": "数学",
                    "statistics": {
                        "average_score": 86.7,
                        "difficulty_coefficient": 0.71,
                        "discrimination_coefficient": 0.49,
                        "student_count": 1200,
                        "class_statistics": [
                            {
                                "class_id": "C2025_001",
                                "class_name": "高三(1)班",
                                "average_score": 88.9,
                                "student_count": 48,
                                "teacher_name": "赵数学"
                            },
                            {
                                "class_id": "C2025_002",
                                "class_name": "高三(2)班",
                                "average_score": 85.8,
                                "student_count": 47,
                                "teacher_name": "孙数学"
                            }
                        ]
                    }
                }
            ],
            "school_summary": {
                "total_participants": 1200,
                "total_classes": 25,
                "average_class_size": 48,
                "completion_rate": 99.7,
                "school_ranking": "区域前5%",
                "special_achievements": [
                    "数学竞赛一等奖3人",
                    "语文作文大赛二等奖5人",
                    "英语演讲比赛优胜奖8人"
                ],
                "facilities": {
                    "labs": 6,
                    "library_books": 50000,
                    "computers": 200,
                    "sports_fields": 3
                },
                "faculty": {
                    "total_teachers": 85,
                    "senior_teachers": 25,
                    "master_degree": 45,
                    "teaching_experience_avg": 12.5
                }
            }
        },
        "data_version": "2.0",
        "total_students": 1200,
        "total_schools": 0,
        "change_reason": "学校详细统计数据创建及分析",
        "triggered_by": "comprehensive_test_system"
    }
    
    print(f"Batch Code: {batch_code}")
    print(f"School ID: {school_id}")
    print(f"School Name: {school_data['school_name']}")
    print(f"Data Size: {len(json.dumps(school_data))} bytes")
    print(f"Students: {school_data['total_students']:,}")
    print(f"Classes: {school_data['statistics_data']['school_summary']['total_classes']}")
    
    start_time = time.time()
    
    try:
        response = requests.post(API_URL, json=school_data, timeout=60)
        response_time = time.time() - start_time
        
        print(f"Response Time: {response_time:.2f}s")
        print(f"HTTP Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"API Response: SUCCESS")
            print(f"  - Success: {result.get('success')}")
            print(f"  - Batch ID: {result.get('data', {}).get('batch_id')}")
            print(f"  - Created At: {result.get('data', {}).get('created_at')}")
            print(f"[RESULT] PASS - School batch created successfully")
            return batch_code, school_id
        else:
            print(f"API Response: FAILED")
            print(f"Error: {response.text}")
            print(f"[RESULT] FAIL - HTTP {response.status_code}")
            return None, None
            
    except Exception as e:
        print(f"Exception: {str(e)}")
        print(f"[RESULT] FAIL - Exception occurred")
        return None, None

def test_validation_comprehensive():
    print_test("Comprehensive Input Validation")
    
    validation_tests = [
        {
            "name": "Empty batch_code",
            "data": {
                "batch_code": "",
                "aggregation_level": "regional",
                "statistics_data": {"batch_info": {}, "academic_subjects": []},
                "data_version": "1.0"
            },
            "expected_error": "Field validation"
        },
        {
            "name": "Invalid aggregation_level",
            "data": {
                "batch_code": f"INVALID_LEVEL_{int(time.time())}",
                "aggregation_level": "invalid_type",
                "statistics_data": {"batch_info": {}, "academic_subjects": []},
                "data_version": "1.0"
            },
            "expected_error": "Enum validation"
        },
        {
            "name": "School without school_id",
            "data": {
                "batch_code": f"NO_SCHOOL_ID_{int(time.time())}",
                "aggregation_level": "school",
                "statistics_data": {"batch_info": {}, "academic_subjects": []},
                "data_version": "1.0"
            },
            "expected_error": "School validation"
        },
        {
            "name": "Missing statistics_data fields",
            "data": {
                "batch_code": f"MISSING_STATS_{int(time.time())}",
                "aggregation_level": "regional",
                "statistics_data": {"incomplete": "data"},
                "data_version": "1.0"
            },
            "expected_error": "Statistics validation"
        },
        {
            "name": "Negative student count",
            "data": {
                "batch_code": f"NEG_COUNT_{int(time.time())}",
                "aggregation_level": "regional",
                "statistics_data": {"batch_info": {}, "academic_subjects": []},
                "data_version": "1.0",
                "total_students": -500
            },
            "expected_error": "Range validation"
        }
    ]
    
    passed_validations = 0
    total_validations = len(validation_tests)
    
    for i, test_case in enumerate(validation_tests, 1):
        print(f"\n{i}. Testing: {test_case['name']}")
        
        try:
            response = requests.post(API_URL, json=test_case['data'], timeout=15)
            
            if response.status_code in [400, 422]:
                print(f"   [PASS] Correctly rejected (HTTP {response.status_code})")
                
                if response.status_code == 422:
                    try:
                        error_detail = response.json().get('detail', [])
                        print(f"   Validation errors: {len(error_detail)}")
                        if error_detail:
                            print(f"   Primary error: {error_detail[0].get('msg', 'Unknown')}")
                    except:
                        pass
                        
                passed_validations += 1
            else:
                print(f"   [FAIL] Expected validation error but got HTTP {response.status_code}")
                print(f"   Response: {response.text[:100]}")
                
        except Exception as e:
            print(f"   [ERROR] Exception: {str(e)}")
    
    print(f"\nValidation Summary: {passed_validations}/{total_validations} tests passed")
    print(f"[RESULT] {'PASS' if passed_validations == total_validations else 'PARTIAL'}")
    
    return passed_validations == total_validations

def test_data_retrieval_detailed(regional_batch, school_batch_info):
    print_test("Data Retrieval and Verification")
    
    tests_passed = 0
    total_tests = 0
    
    # Test regional batch retrieval
    if regional_batch:
        total_tests += 1
        print(f"\n1. Regional Batch Retrieval: {regional_batch}")
        
        try:
            url = f"{API_URL}/{regional_batch}?aggregation_level=regional"
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                batch_data = response.json()
                
                # Detailed verification
                checks = {
                    "batch_code": batch_data.get("batch_code") == regional_batch,
                    "aggregation_level": batch_data.get("aggregation_level") == "regional",
                    "has_statistics": batch_data.get("statistics_data") is not None,
                    "has_timestamp": batch_data.get("created_at") is not None,
                    "student_count": batch_data.get("total_students", 0) > 0,
                    "school_count": batch_data.get("total_schools", 0) > 0
                }
                
                all_passed = all(checks.values())
                
                if all_passed:
                    print(f"   [PASS] All data fields verified")
                    print(f"   - ID: {batch_data.get('id')}")
                    print(f"   - Students: {batch_data.get('total_students'):,}")
                    print(f"   - Schools: {batch_data.get('total_schools')}")
                    print(f"   - Status: {batch_data.get('calculation_status')}")
                    tests_passed += 1
                else:
                    print(f"   [FAIL] Data verification failed:")
                    for check, result in checks.items():
                        print(f"     - {check}: {'PASS' if result else 'FAIL'}")
            else:
                print(f"   [FAIL] Could not retrieve batch (HTTP {response.status_code})")
                
        except Exception as e:
            print(f"   [ERROR] Exception: {str(e)}")
    
    # Test school batch retrieval
    if school_batch_info[0] and school_batch_info[1]:
        total_tests += 1
        batch_code, school_id = school_batch_info
        print(f"\n2. School Batch Retrieval: {batch_code}")
        print(f"   School ID: {school_id}")
        
        try:
            url = f"{API_URL}/{batch_code}?aggregation_level=school&school_id={school_id}"
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                batch_data = response.json()
                
                checks = {
                    "batch_code": batch_data.get("batch_code") == batch_code,
                    "aggregation_level": batch_data.get("aggregation_level") == "school",
                    "school_id": batch_data.get("school_id") == school_id,
                    "has_statistics": batch_data.get("statistics_data") is not None,
                    "student_count": batch_data.get("total_students", 0) > 0
                }
                
                all_passed = all(checks.values())
                
                if all_passed:
                    print(f"   [PASS] All data fields verified")
                    print(f"   - ID: {batch_data.get('id')}")
                    print(f"   - Students: {batch_data.get('total_students'):,}")
                    print(f"   - Status: {batch_data.get('calculation_status')}")
                    tests_passed += 1
                else:
                    print(f"   [FAIL] Data verification failed:")
                    for check, result in checks.items():
                        print(f"     - {check}: {'PASS' if result else 'FAIL'}")
            else:
                print(f"   [FAIL] Could not retrieve batch (HTTP {response.status_code})")
                
        except Exception as e:
            print(f"   [ERROR] Exception: {str(e)}")
    
    print(f"\nRetrieval Summary: {tests_passed}/{total_tests} tests passed")
    print(f"[RESULT] {'PASS' if tests_passed == total_tests else 'PARTIAL'}")
    
    return tests_passed == total_tests

def generate_final_report(results):
    print_section("FINAL TEST REPORT")
    
    total_tests = len(results)
    passed_tests = sum(1 for r in results if r['success'])
    
    print(f"Test Execution Summary:")
    print(f"  Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  API Endpoint: {API_URL}")
    print(f"  Total Tests: {total_tests}")
    print(f"  Passed: {passed_tests}")
    print(f"  Failed: {total_tests - passed_tests}")
    print(f"  Success Rate: {(passed_tests/total_tests)*100:.1f}%")
    
    print(f"\nDetailed Results:")
    for i, result in enumerate(results, 1):
        status = "PASS" if result['success'] else "FAIL"
        print(f"  {i}. {result['name']}: {status}")
        if 'details' in result:
            for key, value in result['details'].items():
                print(f"     - {key}: {value}")
    
    # Performance Analysis
    response_times = [r['details'].get('response_time', 0) for r in results if 'details' in r and 'response_time' in r['details']]
    if response_times:
        avg_time = sum(response_times) / len(response_times)
        max_time = max(response_times)
        print(f"\nPerformance Metrics:")
        print(f"  Average Response Time: {avg_time:.2f}s")
        print(f"  Maximum Response Time: {max_time:.2f}s")
        print(f"  Performance Rating: {'Good' if avg_time < 5 else 'Needs Improvement' if avg_time < 15 else 'Poor'}")
    
    # API Health Assessment
    print(f"\nAPI Health Assessment:")
    if passed_tests == total_tests:
        health_status = "EXCELLENT"
        health_desc = "All functionality working perfectly"
    elif passed_tests >= total_tests * 0.8:
        health_status = "GOOD"
        health_desc = "Core functionality working with minor issues"
    elif passed_tests >= total_tests * 0.6:
        health_status = "FAIR"
        health_desc = "Basic functionality working but needs attention"
    else:
        health_status = "POOR"
        health_desc = "Significant issues requiring immediate attention"
    
    print(f"  Overall Status: {health_status}")
    print(f"  Assessment: {health_desc}")
    
    # Recommendations
    print(f"\nRecommendations:")
    if avg_time > 10:
        print(f"  - Consider performance optimization (avg response: {avg_time:.1f}s)")
    if passed_tests < total_tests:
        print(f"  - Address failing tests for improved reliability")
    print(f"  - API is {'ready for production' if health_status in ['EXCELLENT', 'GOOD'] else 'not ready for production'}")

def main():
    print_section("COMPREHENSIVE BATCH API TEST SUITE")
    print(f"Target API: {API_URL}")
    print(f"Test Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check service health
    try:
        health_response = requests.get(f"{BASE_URL}/health", timeout=5)
        if health_response.status_code == 200:
            print(f"Service Status: HEALTHY")
        else:
            print(f"Service Status: UNHEALTHY (HTTP {health_response.status_code})")
            return
    except Exception as e:
        print(f"Service Status: UNREACHABLE ({str(e)})")
        return
    
    # Execute tests
    results = []
    
    # Test 1: Regional batch creation
    start_time = time.time()
    regional_batch = test_regional_batch_comprehensive()
    response_time = time.time() - start_time
    
    results.append({
        'name': 'Regional Batch Creation (Comprehensive)',
        'success': regional_batch is not None,
        'details': {
            'batch_code': regional_batch or 'N/A',
            'response_time': response_time
        }
    })
    
    # Test 2: School batch creation  
    start_time = time.time()
    school_batch, school_id = test_school_batch_detailed()
    response_time = time.time() - start_time
    
    results.append({
        'name': 'School Batch Creation (Detailed)',
        'success': school_batch is not None,
        'details': {
            'batch_code': school_batch or 'N/A',
            'school_id': school_id or 'N/A',
            'response_time': response_time
        }
    })
    
    # Test 3: Input validation
    validation_success = test_validation_comprehensive()
    results.append({
        'name': 'Input Validation (Comprehensive)',
        'success': validation_success
    })
    
    # Test 4: Data retrieval
    retrieval_success = test_data_retrieval_detailed(regional_batch, (school_batch, school_id))
    results.append({
        'name': 'Data Retrieval & Verification',
        'success': retrieval_success
    })
    
    # Generate final report
    generate_final_report(results)
    
    print(f"\n{'='*60}")
    print("TEST SUITE COMPLETED")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()