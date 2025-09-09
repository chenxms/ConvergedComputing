#!/usr/bin/env python3
"""
Comprehensive Batch API Test Suite
Tests POST /api/v1/management/batches endpoint with various scenarios including:
- Regional and School batch creation
- Field validation 
- Data integrity verification
- Error handling
- Performance benchmarks
- Edge cases
"""

import requests
import json
import uuid
import time
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Configuration
BASE_URL = "http://localhost:8000"
API_URL = f"{BASE_URL}/api/v1/management/batches"
TIMEOUT = 30

class BatchAPITestSuite:
    def __init__(self):
        self.test_results = []
        self.created_batches = []
        
    def log_result(self, test_name: str, success: bool, details: Dict):
        """Log test result"""
        self.test_results.append({
            "test_name": test_name,
            "success": success,
            "timestamp": datetime.now().isoformat(),
            "details": details
        })
        
    def print_header(self, test_name: str):
        """Print test header"""
        print(f"\n{'='*80}")
        print(f"TEST: {test_name}")
        print(f"{'='*80}")
    
    def generate_batch_code(self, prefix: str = "TEST") -> str:
        """Generate unique batch code"""
        return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8].upper()}"
    
    def create_comprehensive_regional_data(self, batch_code: str) -> Dict:
        """Create comprehensive regional batch data"""
        return {
            "batch_code": batch_code,
            "aggregation_level": "regional",
            "school_name": "全市统计汇总",
            "statistics_data": {
                "batch_info": {
                    "batch_code": batch_code,
                    "total_students": 25000,
                    "total_schools": 75,
                    "calculation_date": datetime.now().isoformat(),
                    "region_name": "示例市",
                    "exam_period": "2025年第一学期期末"
                },
                "academic_subjects": [
                    {
                        "subject_id": 1,
                        "subject_name": "语文",
                        "statistics": {
                            "average_score": 84.3,
                            "difficulty_coefficient": 0.72,
                            "discrimination_coefficient": 0.46,
                            "student_count": 25000,
                            "score_distribution": {
                                "excellent": 5000,
                                "good": 12500,
                                "pass": 6250,
                                "fail": 1250
                            },
                            "grade_distribution": {
                                "grade_1": {"average": 82.1, "count": 8333},
                                "grade_2": {"average": 84.5, "count": 8333},
                                "grade_3": {"average": 86.3, "count": 8334}
                            }
                        }
                    },
                    {
                        "subject_id": 2,
                        "subject_name": "数学",
                        "statistics": {
                            "average_score": 79.8,
                            "difficulty_coefficient": 0.68,
                            "discrimination_coefficient": 0.51,
                            "student_count": 25000,
                            "score_distribution": {
                                "excellent": 4250,
                                "good": 11250,
                                "pass": 7500,
                                "fail": 2000
                            },
                            "grade_distribution": {
                                "grade_1": {"average": 77.5, "count": 8333},
                                "grade_2": {"average": 80.1, "count": 8333},
                                "grade_3": {"average": 81.8, "count": 8334}
                            }
                        }
                    },
                    {
                        "subject_id": 3,
                        "subject_name": "英语", 
                        "statistics": {
                            "average_score": 76.5,
                            "difficulty_coefficient": 0.64,
                            "discrimination_coefficient": 0.48,
                            "student_count": 25000,
                            "score_distribution": {
                                "excellent": 3750,
                                "good": 10000,
                                "pass": 8750,
                                "fail": 2500
                            },
                            "grade_distribution": {
                                "grade_1": {"average": 74.2, "count": 8333},
                                "grade_2": {"average": 76.8, "count": 8333},
                                "grade_3": {"average": 78.5, "count": 8334}
                            }
                        }
                    }
                ],
                "regional_summary": {
                    "total_participants": 25000,
                    "total_schools": 75,
                    "average_school_size": 333,
                    "completion_rate": 98.8,
                    "performance_ranking": "优秀",
                    "improvement_areas": ["数学计算能力", "英语听力理解"],
                    "strengths": ["语文阅读理解", "综合分析能力"]
                }
            },
            "data_version": "2.0",
            "total_students": 25000,
            "total_schools": 75,
            "change_reason": "2025年第一学期期末统计数据",
            "triggered_by": "comprehensive_test_system"
        }
    
    def create_comprehensive_school_data(self, batch_code: str, school_id: str) -> Dict:
        """Create comprehensive school batch data"""
        return {
            "batch_code": batch_code,
            "aggregation_level": "school",
            "school_id": school_id,
            "school_name": "示例第一中学",
            "statistics_data": {
                "batch_info": {
                    "batch_code": batch_code,
                    "school_id": school_id,
                    "school_name": "示例第一中学",
                    "total_students": 450,
                    "calculation_date": datetime.now().isoformat(),
                    "school_type": "重点中学",
                    "district": "中心区"
                },
                "academic_subjects": [
                    {
                        "subject_id": 1,
                        "subject_name": "语文",
                        "statistics": {
                            "average_score": 89.2,
                            "difficulty_coefficient": 0.75,
                            "discrimination_coefficient": 0.43,
                            "student_count": 450,
                            "class_statistics": [
                                {
                                    "class_id": "CLASS_001",
                                    "class_name": "高一(1)班",
                                    "average_score": 90.5,
                                    "student_count": 45,
                                    "teacher_name": "张老师"
                                },
                                {
                                    "class_id": "CLASS_002",
                                    "class_name": "高一(2)班",
                                    "average_score": 88.8,
                                    "student_count": 44,
                                    "teacher_name": "李老师"
                                },
                                {
                                    "class_id": "CLASS_003",
                                    "class_name": "高一(3)班",
                                    "average_score": 88.3,
                                    "student_count": 46,
                                    "teacher_name": "王老师"
                                }
                            ],
                            "teacher_performance": {
                                "top_performer": "张老师",
                                "improvement_needed": "需加强阅读理解训练"
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
                            "student_count": 450,
                            "class_statistics": [
                                {
                                    "class_id": "CLASS_001",
                                    "class_name": "高一(1)班",
                                    "average_score": 88.2,
                                    "student_count": 45,
                                    "teacher_name": "赵老师"
                                },
                                {
                                    "class_id": "CLASS_002",
                                    "class_name": "高一(2)班",
                                    "average_score": 86.1,
                                    "student_count": 44,
                                    "teacher_name": "孙老师"
                                }
                            ]
                        }
                    }
                ],
                "school_summary": {
                    "total_participants": 450,
                    "total_classes": 15,
                    "average_class_size": 30,
                    "completion_rate": 99.5,
                    "school_ranking": "区域前10%",
                    "special_programs": ["数学竞赛班", "语文实验班"],
                    "teacher_count": 28,
                    "facilities": ["实验室", "图书馆", "多媒体教室"]
                }
            },
            "data_version": "2.0",
            "total_students": 450,
            "total_schools": 0,
            "change_reason": "学校详细统计数据创建",
            "triggered_by": "comprehensive_test_system"
        }

    def test_regional_batch_creation(self) -> bool:
        """Test comprehensive regional batch creation"""
        self.print_header("Regional Batch Creation with Rich Data")
        
        batch_code = self.generate_batch_code("REGIONAL")
        test_data = self.create_comprehensive_regional_data(batch_code)
        
        start_time = time.time()
        
        try:
            print(f"Creating regional batch: {batch_code}")
            print(f"Data size: {len(json.dumps(test_data))} bytes")
            
            response = requests.post(API_URL, json=test_data, timeout=TIMEOUT)
            response_time = time.time() - start_time
            
            print(f"Response time: {response_time:.3f}s")
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                response_data = response.json()
                success = (
                    response_data.get("success") is True and
                    response_data.get("data", {}).get("batch_code") == batch_code and
                    response_data.get("data", {}).get("aggregation_level") == "regional"
                )
                
                if success:
                    print(f"[PASS] Regional batch created successfully")
                    print(f"  - Batch ID: {response_data.get('data', {}).get('batch_id')}")
                    print(f"  - Created at: {response_data.get('data', {}).get('created_at')}")
                    self.created_batches.append(("regional", batch_code, None))
                else:
                    print(f"[FAIL] Response validation failed")
                    print(json.dumps(response_data, indent=2, ensure_ascii=False))
            else:
                print(f"[FAIL] HTTP error: {response.status_code}")
                print(response.text)
                success = False
                
            self.log_result("regional_batch_creation", success, {
                "batch_code": batch_code,
                "response_time": response_time,
                "status_code": response.status_code,
                "data_size": len(json.dumps(test_data))
            })
            
            return success
            
        except Exception as e:
            print(f"[ERROR] Exception: {str(e)}")
            self.log_result("regional_batch_creation", False, {"error": str(e)})
            return False

    def test_school_batch_creation(self) -> bool:
        """Test comprehensive school batch creation"""
        self.print_header("School Batch Creation with Rich Data")
        
        batch_code = self.generate_batch_code("SCHOOL")
        school_id = f"SCHOOL_{str(uuid.uuid4())[:12].upper()}"
        test_data = self.create_comprehensive_school_data(batch_code, school_id)
        
        start_time = time.time()
        
        try:
            print(f"Creating school batch: {batch_code}")
            print(f"School ID: {school_id}")
            print(f"Data size: {len(json.dumps(test_data))} bytes")
            
            response = requests.post(API_URL, json=test_data, timeout=TIMEOUT)
            response_time = time.time() - start_time
            
            print(f"Response time: {response_time:.3f}s")
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                response_data = response.json()
                success = (
                    response_data.get("success") is True and
                    response_data.get("data", {}).get("batch_code") == batch_code and
                    response_data.get("data", {}).get("aggregation_level") == "school"
                )
                
                if success:
                    print(f"[PASS] School batch created successfully")
                    print(f"  - Batch ID: {response_data.get('data', {}).get('batch_id')}")
                    print(f"  - Created at: {response_data.get('data', {}).get('created_at')}")
                    self.created_batches.append(("school", batch_code, school_id))
                else:
                    print(f"[FAIL] Response validation failed")
                    print(json.dumps(response_data, indent=2, ensure_ascii=False))
            else:
                print(f"[FAIL] HTTP error: {response.status_code}")
                print(response.text)
                success = False
                
            self.log_result("school_batch_creation", success, {
                "batch_code": batch_code,
                "school_id": school_id,
                "response_time": response_time,
                "status_code": response.status_code,
                "data_size": len(json.dumps(test_data))
            })
            
            return success
            
        except Exception as e:
            print(f"[ERROR] Exception: {str(e)}")
            self.log_result("school_batch_creation", False, {"error": str(e)})
            return False

    def test_duplicate_batch_handling(self) -> bool:
        """Test duplicate batch handling"""
        if not self.created_batches:
            print("\n[SKIP] No batches created for duplicate test")
            return True
            
        self.print_header("Duplicate Batch Handling")
        
        # Use first created batch
        level, batch_code, school_id = self.created_batches[0]
        
        if level == "regional":
            test_data = self.create_comprehensive_regional_data(batch_code)
        else:
            test_data = self.create_comprehensive_school_data(batch_code, school_id)
        
        try:
            print(f"Attempting to create duplicate batch: {batch_code}")
            
            response = requests.post(API_URL, json=test_data, timeout=TIMEOUT)
            
            print(f"Response status: {response.status_code}")
            
            # Should return error (400/409/422)
            success = response.status_code in [400, 409, 422]
            
            if success:
                print(f"[PASS] Duplicate batch correctly rejected")
                if response.status_code == 422:
                    error_detail = response.json().get("detail", [])
                    print(f"  - Validation errors: {len(error_detail)}")
                else:
                    print(f"  - Error response: {response.text[:100]}")
            else:
                print(f"[FAIL] Expected error but got {response.status_code}")
                print(response.text)
                
            self.log_result("duplicate_batch_handling", success, {
                "batch_code": batch_code,
                "status_code": response.status_code
            })
            
            return success
            
        except Exception as e:
            print(f"[ERROR] Exception: {str(e)}")
            self.log_result("duplicate_batch_handling", False, {"error": str(e)})
            return False

    def test_field_validation(self) -> bool:
        """Test field validation with various invalid inputs"""
        self.print_header("Field Validation Tests")
        
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
                "name": "Invalid aggregation_level",
                "data": {
                    "batch_code": self.generate_batch_code("INVALID_LEVEL"),
                    "aggregation_level": "invalid_level",
                    "statistics_data": {"batch_info": {}, "academic_subjects": []},
                    "data_version": "1.0"
                }
            },
            {
                "name": "School level without school_id",
                "data": {
                    "batch_code": self.generate_batch_code("NO_SCHOOL_ID"),
                    "aggregation_level": "school",
                    "statistics_data": {"batch_info": {}, "academic_subjects": []},
                    "data_version": "1.0"
                }
            },
            {
                "name": "Missing statistics_data required fields",
                "data": {
                    "batch_code": self.generate_batch_code("MISSING_STATS"),
                    "aggregation_level": "regional",
                    "statistics_data": {"incomplete": "data"},
                    "data_version": "1.0"
                }
            },
            {
                "name": "Negative total_students",
                "data": {
                    "batch_code": self.generate_batch_code("NEG_STUDENTS"),
                    "aggregation_level": "regional",
                    "statistics_data": {"batch_info": {}, "academic_subjects": []},
                    "data_version": "1.0",
                    "total_students": -100
                }
            },
            {
                "name": "Empty batch_code",
                "data": {
                    "batch_code": "",
                    "aggregation_level": "regional",
                    "statistics_data": {"batch_info": {}, "academic_subjects": []},
                    "data_version": "1.0"
                }
            }
        ]
        
        all_passed = True
        
        for test_case in test_cases:
            print(f"\n--- Testing: {test_case['name']} ---")
            
            try:
                response = requests.post(API_URL, json=test_case['data'], timeout=TIMEOUT)
                
                # Should return validation error (400/422)
                success = response.status_code in [400, 422]
                
                if success:
                    print(f"[PASS] {test_case['name']}: Correctly rejected ({response.status_code})")
                    if response.status_code == 422:
                        errors = response.json().get("detail", [])
                        print(f"  - Validation errors: {len(errors)}")
                        for error in errors[:2]:  # Show first 2 errors
                            print(f"    * {error.get('msg', 'Unknown error')}")
                else:
                    print(f"[FAIL] {test_case['name']}: Expected error but got {response.status_code}")
                    print(f"  Response: {response.text[:100]}")
                    all_passed = False
                    
                self.log_result(f"validation_{test_case['name']}", success, {
                    "status_code": response.status_code
                })
                
            except Exception as e:
                print(f"[ERROR] {test_case['name']}: {str(e)}")
                all_passed = False
                
        return all_passed

    def test_data_retrieval_verification(self) -> bool:
        """Test that created batches can be retrieved with correct data"""
        if not self.created_batches:
            print("\n[SKIP] No batches created for retrieval test")
            return True
            
        self.print_header("Data Retrieval Verification")
        
        all_passed = True
        
        for level, batch_code, school_id in self.created_batches:
            print(f"\n--- Verifying {level} batch: {batch_code} ---")
            
            try:
                url = f"{API_URL}/{batch_code}"
                params = {"aggregation_level": level}
                if school_id:
                    params["school_id"] = school_id
                
                response = requests.get(url, params=params, timeout=TIMEOUT)
                
                if response.status_code == 200:
                    batch_data = response.json()
                    
                    # Verify key fields
                    checks = [
                        ("batch_code", batch_data.get("batch_code") == batch_code),
                        ("aggregation_level", batch_data.get("aggregation_level") == level),
                        ("statistics_data", batch_data.get("statistics_data") is not None),
                        ("created_at", batch_data.get("created_at") is not None),
                        ("total_students", batch_data.get("total_students", 0) > 0)
                    ]
                    
                    if school_id:
                        checks.append(("school_id", batch_data.get("school_id") == school_id))
                    
                    success = all(check[1] for check in checks)
                    
                    if success:
                        print(f"[PASS] Batch data verified")
                        print(f"  - ID: {batch_data.get('id')}")
                        print(f"  - Students: {batch_data.get('total_students')}")
                        print(f"  - Status: {batch_data.get('calculation_status')}")
                        print(f"  - Created: {batch_data.get('created_at')}")
                    else:
                        print(f"[FAIL] Data validation failed:")
                        for check_name, check_result in checks:
                            print(f"  - {check_name}: {'PASS' if check_result else 'FAIL'}")
                        all_passed = False
                else:
                    print(f"[FAIL] Could not retrieve batch: {response.status_code}")
                    print(response.text[:100])
                    all_passed = False
                    
                self.log_result(f"retrieval_{level}_{batch_code}", success if response.status_code == 200 else False, {
                    "status_code": response.status_code,
                    "batch_code": batch_code
                })
                
            except Exception as e:
                print(f"[ERROR] Exception: {str(e)}")
                all_passed = False
                
        return all_passed

    def test_performance_benchmark(self) -> bool:
        """Test API performance with multiple concurrent requests"""
        self.print_header("Performance Benchmark")
        
        print("Testing concurrent batch creation performance...")
        
        def create_test_batch(i: int) -> Tuple[int, float, bool]:
            """Create a single test batch and measure performance"""
            batch_code = f"PERF_TEST_{i:03d}_{str(uuid.uuid4())[:6].upper()}"
            test_data = {
                "batch_code": batch_code,
                "aggregation_level": "regional",
                "statistics_data": {
                    "batch_info": {"batch_code": batch_code, "total_students": 1000, "total_schools": 10},
                    "academic_subjects": [
                        {
                            "subject_id": 1,
                            "subject_name": "测试科目",
                            "statistics": {
                                "average_score": 80.0,
                                "difficulty_coefficient": 0.7,
                                "discrimination_coefficient": 0.5
                            }
                        }
                    ]
                },
                "data_version": "1.0",
                "total_students": 1000,
                "total_schools": 10,
                "triggered_by": "performance_test"
            }
            
            start_time = time.time()
            try:
                response = requests.post(API_URL, json=test_data, timeout=TIMEOUT)
                response_time = time.time() - start_time
                success = response.status_code == 200
                return i, response_time, success
            except Exception:
                return i, time.time() - start_time, False
        
        # Test with 5 concurrent requests
        num_requests = 5
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_index = {executor.submit(create_test_batch, i): i for i in range(num_requests)}
            results = []
            
            for future in concurrent.futures.as_completed(future_to_index):
                results.append(future.result())
        
        total_time = time.time() - start_time
        
        # Analyze results
        response_times = [result[1] for result in results]
        success_count = sum(1 for result in results if result[2])
        
        avg_response_time = sum(response_times) / len(response_times)
        max_response_time = max(response_times)
        min_response_time = min(response_times)
        
        print(f"Performance Results:")
        print(f"  - Concurrent requests: {num_requests}")
        print(f"  - Successful requests: {success_count}/{num_requests}")
        print(f"  - Total time: {total_time:.3f}s")
        print(f"  - Average response time: {avg_response_time:.3f}s")
        print(f"  - Min response time: {min_response_time:.3f}s")
        print(f"  - Max response time: {max_response_time:.3f}s")
        print(f"  - Requests per second: {num_requests/total_time:.2f}")
        
        # Performance criteria
        success = (
            success_count == num_requests and  # All requests succeeded
            avg_response_time < 2.0 and  # Average response under 2s
            max_response_time < 5.0  # No request took more than 5s
        )
        
        if success:
            print(f"[PASS] Performance benchmarks met")
        else:
            print(f"[FAIL] Performance benchmarks not met")
            
        self.log_result("performance_benchmark", success, {
            "concurrent_requests": num_requests,
            "success_count": success_count,
            "avg_response_time": avg_response_time,
            "max_response_time": max_response_time,
            "total_time": total_time
        })
        
        return success

    def test_batch_listing_filtering(self) -> bool:
        """Test batch listing and filtering functionality"""
        self.print_header("Batch Listing and Filtering")
        
        try:
            # Test basic listing
            print("Testing basic batch listing...")
            response = requests.get(API_URL, timeout=TIMEOUT)
            
            if response.status_code != 200:
                print(f"[FAIL] Basic listing failed: {response.status_code}")
                return False
                
            batches = response.json()
            print(f"[PASS] Retrieved {len(batches)} total batches")
            
            # Test filtering by aggregation level if we have created batches
            if self.created_batches:
                print("\nTesting filtered listing...")
                level, batch_code, school_id = self.created_batches[0]
                
                params = {"aggregation_level": level}
                response = requests.get(API_URL, params=params, timeout=TIMEOUT)
                
                if response.status_code == 200:
                    filtered_batches = response.json()
                    # Check that all returned batches have the correct level
                    level_match = all(batch.get("aggregation_level") == level for batch in filtered_batches)
                    
                    if level_match:
                        print(f"[PASS] Level filtering works: {len(filtered_batches)} {level} batches")
                    else:
                        print(f"[FAIL] Level filtering returned incorrect results")
                        return False
                else:
                    print(f"[FAIL] Filtered listing failed: {response.status_code}")
                    return False
            
            self.log_result("batch_listing_filtering", True, {
                "total_batches": len(batches),
                "filtering_tested": len(self.created_batches) > 0
            })
            
            return True
            
        except Exception as e:
            print(f"[ERROR] Exception: {str(e)}")
            self.log_result("batch_listing_filtering", False, {"error": str(e)})
            return False

    def run_all_tests(self):
        """Run all tests in sequence"""
        print("[START] Comprehensive Batch API Test Suite")
        print("=" * 80)
        print(f"Target API: {API_URL}")
        print(f"Test started: {datetime.now().isoformat()}")
        
        # Define test sequence
        tests = [
            ("Regional Batch Creation", self.test_regional_batch_creation),
            ("School Batch Creation", self.test_school_batch_creation),
            ("Data Retrieval Verification", self.test_data_retrieval_verification),
            ("Duplicate Batch Handling", self.test_duplicate_batch_handling),
            ("Field Validation", self.test_field_validation),
            ("Batch Listing & Filtering", self.test_batch_listing_filtering),
            ("Performance Benchmark", self.test_performance_benchmark),
        ]
        
        # Run tests
        for test_name, test_func in tests:
            try:
                success = test_func()
                status = "PASS" if success else "FAIL"
                print(f"\n[{status}] {test_name}")
                
            except Exception as e:
                print(f"\n[ERROR] {test_name}: {str(e)}")
                self.log_result(f"{test_name}_exception", False, {"error": str(e)})
        
        # Generate final report
        self.generate_final_report()

    def generate_final_report(self):
        """Generate comprehensive final test report"""
        print(f"\n{'='*80}")
        print("[FINAL REPORT] Comprehensive Batch API Test Results")
        print(f"{'='*80}")
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result["success"])
        failed_tests = total_tests - passed_tests
        
        print(f"Test Summary:")
        print(f"  Total Tests: {total_tests}")
        print(f"  Passed: {passed_tests}")
        print(f"  Failed: {failed_tests}")
        print(f"  Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        # Show performance metrics
        perf_result = next((r for r in self.test_results if "performance" in r["test_name"]), None)
        if perf_result and perf_result["success"]:
            details = perf_result["details"]
            print(f"\nPerformance Metrics:")
            print(f"  Concurrent Requests: {details['concurrent_requests']}")
            print(f"  Average Response Time: {details['avg_response_time']:.3f}s")
            print(f"  Max Response Time: {details['max_response_time']:.3f}s")
        
        # Show created batches
        if self.created_batches:
            print(f"\nCreated Batches ({len(self.created_batches)}):")
            for level, batch_code, school_id in self.created_batches:
                school_info = f" (School: {school_id})" if school_id else ""
                print(f"  - {level.title()}: {batch_code}{school_info}")
        
        # Show failed tests
        if failed_tests > 0:
            print(f"\nFailed Tests:")
            for result in self.test_results:
                if not result["success"]:
                    error = result["details"].get("error", "Unknown error")
                    print(f"  - {result['test_name']}: {error}")
        
        # API health assessment
        creation_success = any("creation" in r["test_name"] and r["success"] for r in self.test_results)
        validation_success = any("validation" in r["test_name"] and r["success"] for r in self.test_results)
        
        print(f"\nAPI Health Assessment:")
        print(f"  Core Functionality: {'HEALTHY' if creation_success else 'ISSUES'}")
        print(f"  Input Validation: {'HEALTHY' if validation_success else 'ISSUES'}")
        print(f"  Overall Status: {'HEALTHY' if passed_tests/total_tests >= 0.8 else 'NEEDS ATTENTION'}")
        
        print(f"\n{'='*80}")


if __name__ == "__main__":
    # Check service health
    try:
        health_response = requests.get(f"{BASE_URL}/health", timeout=5)
        if health_response.status_code != 200:
            print(f"[ERROR] Service health check failed: {health_response.status_code}")
            exit(1)
        print("[OK] Service is healthy and accessible")
    except Exception as e:
        print(f"[ERROR] Cannot connect to service: {e}")
        exit(1)
    
    # Run comprehensive test suite
    test_suite = BatchAPITestSuite()
    test_suite.run_all_tests()