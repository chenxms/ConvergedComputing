#!/usr/bin/env python3
"""
Comprehensive API Test for Batch Creation Functionality
Tests POST /api/management/batches endpoint with various scenarios
"""

import requests
import json
import uuid
from datetime import datetime
from typing import Dict, Any
import sys

# Base URL for the API
BASE_URL = "http://localhost:8000"
API_PREFIX = "/api/v1/management"

class BatchAPITester:
    def __init__(self):
        self.base_url = BASE_URL + API_PREFIX
        self.test_results = []
        
    def log_test_result(self, test_name: str, success: bool, details: Dict[str, Any]):
        """Log test result for tracking"""
        result = {
            "test_name": test_name,
            "success": success,
            "timestamp": datetime.now().isoformat(),
            "details": details
        }
        self.test_results.append(result)
        
    def print_test_header(self, test_name: str):
        """Print formatted test header"""
        print(f"\n{'='*60}")
        print(f"TEST: {test_name}")
        print(f"{'='*60}")
        
    def print_request_details(self, method: str, url: str, data: Dict = None):
        """Print request details"""
        print(f"Request: {method} {url}")
        if data:
            print(f"Request Body:\n{json.dumps(data, indent=2, ensure_ascii=False)}")
            
    def print_response_details(self, response: requests.Response):
        """Print response details"""
        print(f"Response Status: {response.status_code}")
        try:
            response_json = response.json()
            print(f"Response Body:\n{json.dumps(response_json, indent=2, ensure_ascii=False)}")
        except:
            print(f"Response Body (text):\n{response.text}")
            
    def create_regional_batch_data(self, batch_code: str = None) -> Dict[str, Any]:
        """Create test data for regional batch"""
        if not batch_code:
            batch_code = f"BATCH_2025_{str(uuid.uuid4())[:8].upper()}"
            
        return {
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "school_name": "全市汇总数据",
            "statistics_data": {
                "batch_info": {
                    "batch_code": batch_code,
                    "total_students": 15000,
                    "total_schools": 50,
                    "calculation_date": datetime.now().isoformat()
                },
                "academic_subjects": [
                    {
                        "subject_id": 1,
                        "subject_name": "语文",
                        "statistics": {
                            "average_score": 85.5,
                            "difficulty_coefficient": 0.71,
                            "discrimination_coefficient": 0.45,
                            "student_count": 15000,
                            "score_distribution": {
                                "excellent": 3000,
                                "good": 7500,
                                "pass": 3750,
                                "fail": 750
                            }
                        }
                    },
                    {
                        "subject_id": 2,
                        "subject_name": "数学", 
                        "statistics": {
                            "average_score": 82.3,
                            "difficulty_coefficient": 0.68,
                            "discrimination_coefficient": 0.52,
                            "student_count": 15000,
                            "score_distribution": {
                                "excellent": 2700,
                                "good": 7200,
                                "pass": 4050,
                                "fail": 1050
                            }
                        }
                    },
                    {
                        "subject_id": 3,
                        "subject_name": "英语",
                        "statistics": {
                            "average_score": 78.9,
                            "difficulty_coefficient": 0.65,
                            "discrimination_coefficient": 0.48,
                            "student_count": 15000,
                            "score_distribution": {
                                "excellent": 2250,
                                "good": 6750,
                                "pass": 4500,
                                "fail": 1500
                            }
                        }
                    }
                ],
                "regional_summary": {
                    "total_participants": 15000,
                    "total_schools": 50,
                    "average_school_size": 300,
                    "completion_rate": 98.5
                }
            },
            "data_version": "1.0",
            "total_students": 15000,
            "total_schools": 50,
            "change_reason": "新批次数据初始化",
            "triggered_by": "test_system"
        }
    
    def create_school_batch_data(self, batch_code: str = None, school_id: str = None) -> Dict[str, Any]:
        """Create test data for school batch"""
        if not batch_code:
            batch_code = f"BATCH_2025_{str(uuid.uuid4())[:8].upper()}"
        if not school_id:
            school_id = f"SCHOOL_{str(uuid.uuid4())[:8].upper()}"
            
        return {
            "batch_code": batch_code,
            "aggregation_level": "SCHOOL",
            "school_id": school_id,
            "school_name": "第一中学",
            "statistics_data": {
                "batch_info": {
                    "batch_code": batch_code,
                    "school_id": school_id,
                    "school_name": "第一中学",
                    "total_students": 300,
                    "calculation_date": datetime.now().isoformat()
                },
                "academic_subjects": [
                    {
                        "subject_id": 1,
                        "subject_name": "语文",
                        "statistics": {
                            "average_score": 87.2,
                            "difficulty_coefficient": 0.73,
                            "discrimination_coefficient": 0.42,
                            "student_count": 300,
                            "class_statistics": [
                                {
                                    "class_id": "CLASS_001",
                                    "class_name": "高三(1)班",
                                    "average_score": 88.5,
                                    "student_count": 30
                                },
                                {
                                    "class_id": "CLASS_002", 
                                    "class_name": "高三(2)班",
                                    "average_score": 86.8,
                                    "student_count": 32
                                }
                            ]
                        }
                    },
                    {
                        "subject_id": 2,
                        "subject_name": "数学",
                        "statistics": {
                            "average_score": 84.1,
                            "difficulty_coefficient": 0.70,
                            "discrimination_coefficient": 0.49,
                            "student_count": 300,
                            "class_statistics": [
                                {
                                    "class_id": "CLASS_001",
                                    "class_name": "高三(1)班", 
                                    "average_score": 85.3,
                                    "student_count": 30
                                },
                                {
                                    "class_id": "CLASS_002",
                                    "class_name": "高三(2)班",
                                    "average_score": 83.7,
                                    "student_count": 32
                                }
                            ]
                        }
                    }
                ],
                "school_summary": {
                    "total_participants": 300,
                    "total_classes": 10,
                    "average_class_size": 30,
                    "completion_rate": 99.2
                }
            },
            "data_version": "1.0", 
            "total_students": 300,
            "total_schools": 0,  # School level doesn't count schools
            "change_reason": "学校级数据初始化",
            "triggered_by": "test_system"
        }

    def test_create_regional_batch(self) -> bool:
        """Test creating a regional batch"""
        self.print_test_header("Create Regional Batch")
        
        # Create test data
        test_data = self.create_regional_batch_data()
        batch_code = test_data["batch_code"]
        
        try:
            # Make request
            url = f"{self.base_url}/batches"
            self.print_request_details("POST", url, test_data)
            
            response = requests.post(url, json=test_data)
            self.print_response_details(response)
            
            # Verify response
            success = response.status_code == 200
            if success:
                response_data = response.json()
                success = (
                    response_data.get("success") is True and
                    batch_code in response_data.get("message", "") and
                    response_data.get("data", {}).get("batch_code") == batch_code and
                    response_data.get("data", {}).get("aggregation_level") == "REGIONAL"
                )
                
                if success:
                    print(f"[OK] Regional batch created successfully: {batch_code}")
                    # Store for verification
                    self.created_regional_batch = batch_code
                else:
                    print("[FAIL] Response validation failed")
            else:
                print(f"[FAIL] Request failed with status {response.status_code}")
                
            self.log_test_result("create_regional_batch", success, {
                "batch_code": batch_code,
                "status_code": response.status_code,
                "response": response.text[:500]
            })
            
            return success
            
        except Exception as e:
            print(f"[FAIL] Exception occurred: {str(e)}")
            self.log_test_result("create_regional_batch", False, {"error": str(e)})
            return False

    def test_create_school_batch(self) -> bool:
        """Test creating a school batch"""
        self.print_test_header("Create School Batch")
        
        # Create test data
        test_data = self.create_school_batch_data()
        batch_code = test_data["batch_code"]
        school_id = test_data["school_id"]
        
        try:
            # Make request
            url = f"{self.base_url}/batches"
            self.print_request_details("POST", url, test_data)
            
            response = requests.post(url, json=test_data)
            self.print_response_details(response)
            
            # Verify response
            success = response.status_code == 200
            if success:
                response_data = response.json()
                success = (
                    response_data.get("success") is True and
                    batch_code in response_data.get("message", "") and
                    response_data.get("data", {}).get("batch_code") == batch_code and
                    response_data.get("data", {}).get("aggregation_level") == "SCHOOL"
                )
                
                if success:
                    print(f"[OK] School batch created successfully: {batch_code}")
                    # Store for verification
                    self.created_school_batch = batch_code
                    self.created_school_id = school_id
                else:
                    print("[FAIL] Response validation failed")
            else:
                print(f"[FAIL] Request failed with status {response.status_code}")
                
            self.log_test_result("create_school_batch", success, {
                "batch_code": batch_code,
                "school_id": school_id,
                "status_code": response.status_code,
                "response": response.text[:500]
            })
            
            return success
            
        except Exception as e:
            print(f"[FAIL] Exception occurred: {str(e)}")
            self.log_test_result("create_school_batch", False, {"error": str(e)})
            return False

    def test_create_duplicate_batch(self) -> bool:
        """Test creating duplicate batch (should fail)"""
        self.print_test_header("Create Duplicate Batch (Should Fail)")
        
        if not hasattr(self, 'created_regional_batch'):
            print("[WARN] Skipping duplicate test - no existing batch")
            return True
            
        # Try to create batch with same code
        test_data = self.create_regional_batch_data(self.created_regional_batch)
        
        try:
            url = f"{self.base_url}/batches"
            self.print_request_details("POST", url, test_data)
            
            response = requests.post(url, json=test_data)
            self.print_response_details(response)
            
            # Should fail with 400 or 409
            success = response.status_code in [400, 409]
            if success:
                print(f"[OK] Duplicate batch correctly rejected")
            else:
                print(f"[FAIL] Expected error but got status {response.status_code}")
                
            self.log_test_result("create_duplicate_batch", success, {
                "batch_code": self.created_regional_batch,
                "status_code": response.status_code,
                "response": response.text[:500]
            })
            
            return success
            
        except Exception as e:
            print(f"[FAIL] Exception occurred: {str(e)}")
            self.log_test_result("create_duplicate_batch", False, {"error": str(e)})
            return False

    def test_invalid_data_formats(self) -> bool:
        """Test various invalid data formats"""
        self.print_test_header("Invalid Data Format Tests")
        
        test_cases = [
            {
                "name": "Missing batch_code",
                "data": {
                    "aggregation_level": "REGIONAL",
                    "statistics_data": {"batch_info": {}, "academic_subjects": []},
                    "data_version": "1.0"
                }
            },
            {
                "name": "Invalid aggregation_level", 
                "data": {
                    "batch_code": "BATCH_2025_INVALID",
                    "aggregation_level": "INVALID_LEVEL",
                    "statistics_data": {"batch_info": {}, "academic_subjects": []},
                    "data_version": "1.0"
                }
            },
            {
                "name": "School level without school_id",
                "data": {
                    "batch_code": "BATCH_2025_NO_SCHOOL_ID",
                    "aggregation_level": "SCHOOL",
                    "statistics_data": {"batch_info": {}, "academic_subjects": []},
                    "data_version": "1.0"
                }
            },
            {
                "name": "Missing required statistics_data fields",
                "data": {
                    "batch_code": "BATCH_2025_MISSING_FIELDS",
                    "aggregation_level": "REGIONAL", 
                    "statistics_data": {"incomplete": "data"},
                    "data_version": "1.0"
                }
            }
        ]
        
        all_passed = True
        
        for test_case in test_cases:
            print(f"\n--- Testing: {test_case['name']} ---")
            
            try:
                url = f"{self.base_url}/batches"
                self.print_request_details("POST", url, test_case['data'])
                
                response = requests.post(url, json=test_case['data'])
                self.print_response_details(response)
                
                # Should fail with 400 (validation error)
                success = response.status_code == 422 or response.status_code == 400
                if success:
                    print(f"[OK] {test_case['name']}: Correctly rejected")
                else:
                    print(f"[FAIL] {test_case['name']}: Expected validation error but got {response.status_code}")
                    all_passed = False
                    
                self.log_test_result(f"invalid_data_{test_case['name']}", success, {
                    "status_code": response.status_code,
                    "response": response.text[:200]
                })
                
            except Exception as e:
                print(f"[FAIL] Exception in {test_case['name']}: {str(e)}")
                all_passed = False
                
        return all_passed

    def verify_batch_in_database(self, batch_code: str, aggregation_level: str = None, school_id: str = None) -> bool:
        """Verify batch was actually created in database"""
        self.print_test_header(f"Verify Batch in Database: {batch_code}")
        
        try:
            # Get batch details
            url = f"{self.base_url}/batches/{batch_code}"
            params = {}
            if aggregation_level:
                params['aggregation_level'] = aggregation_level.lower()
            if school_id:
                params['school_id'] = school_id
                
            print(f"Request: GET {url}")
            if params:
                print(f"Query Params: {params}")
                
            response = requests.get(url, params=params)
            self.print_response_details(response)
            
            success = response.status_code == 200
            if success:
                batch_data = response.json()
                # Verify key fields
                success = (
                    batch_data.get("batch_code") == batch_code and
                    batch_data.get("statistics_data") is not None and
                    batch_data.get("created_at") is not None
                )
                
                if aggregation_level:
                    success = success and batch_data.get("aggregation_level") == aggregation_level.upper()
                if school_id:
                    success = success and batch_data.get("school_id") == school_id
                    
                if success:
                    print(f"[OK] Batch {batch_code} verified in database")
                    print(f"   - ID: {batch_data.get('id')}")
                    print(f"   - Level: {batch_data.get('aggregation_level')}")
                    print(f"   - Students: {batch_data.get('total_students')}")
                    print(f"   - Created: {batch_data.get('created_at')}")
                else:
                    print("[FAIL] Batch data validation failed")
            else:
                print(f"[FAIL] Failed to retrieve batch: {response.status_code}")
                
            self.log_test_result("verify_batch_database", success, {
                "batch_code": batch_code,
                "status_code": response.status_code,
                "found": success
            })
            
            return success
            
        except Exception as e:
            print(f"[FAIL] Exception occurred: {str(e)}")
            self.log_test_result("verify_batch_database", False, {"error": str(e)})
            return False

    def test_batch_listing(self) -> bool:
        """Test batch listing functionality"""
        self.print_test_header("Test Batch Listing")
        
        try:
            # Test basic listing
            url = f"{self.base_url}/batches"
            print(f"Request: GET {url}")
            
            response = requests.get(url)
            self.print_response_details(response)
            
            success = response.status_code == 200
            if success:
                batches = response.json()
                success = isinstance(batches, list)
                if success:
                    print(f"[OK] Retrieved {len(batches)} batches")
                    for batch in batches[:3]:  # Show first 3
                        print(f"   - {batch.get('batch_code')} ({batch.get('aggregation_level')})")
                else:
                    print("[FAIL] Response is not a list")
            else:
                print(f"[FAIL] Failed to list batches: {response.status_code}")
                
            # Test filtered listing if we have created batches
            if hasattr(self, 'created_regional_batch'):
                print(f"\n--- Testing filtered listing ---")
                params = {"batch_code": self.created_regional_batch}
                response = requests.get(url, params=params)
                print(f"Request: GET {url} with params {params}")
                self.print_response_details(response)
                
                filter_success = (
                    response.status_code == 200 and
                    len(response.json()) >= 1 and
                    response.json()[0].get("batch_code") == self.created_regional_batch
                )
                
                if filter_success:
                    print(f"[OK] Filtered listing works correctly")
                else:
                    print(f"[FAIL] Filtered listing failed")
                    
                success = success and filter_success
                
            self.log_test_result("batch_listing", success, {
                "status_code": response.status_code,
                "batch_count": len(response.json()) if response.status_code == 200 else 0
            })
            
            return success
            
        except Exception as e:
            print(f"[FAIL] Exception occurred: {str(e)}")
            self.log_test_result("batch_listing", False, {"error": str(e)})
            return False

    def run_all_tests(self):
        """Run all tests and generate report"""
        print("[TEST] Starting Comprehensive Batch API Tests")
        print(f"Target API: {self.base_url}")
        print(f"Test Time: {datetime.now().isoformat()}")
        
        tests = [
            ("Regional Batch Creation", self.test_create_regional_batch),
            ("School Batch Creation", self.test_create_school_batch), 
            ("Duplicate Batch Handling", self.test_create_duplicate_batch),
            ("Invalid Data Format Tests", self.test_invalid_data_formats),
            ("Batch Listing", self.test_batch_listing),
        ]
        
        # Run creation tests first
        for test_name, test_func in tests[:2]:
            try:
                success = test_func()
                if success:
                    print(f"[PASS] {test_name} - PASSED")
                else:
                    print(f"[FAIL] {test_name} - FAILED")
            except Exception as e:
                print(f"[ERROR] {test_name} - ERROR: {str(e)}")
        
        # Verify created batches in database
        if hasattr(self, 'created_regional_batch'):
            self.verify_batch_in_database(self.created_regional_batch, "REGIONAL")
            
        if hasattr(self, 'created_school_batch'):
            self.verify_batch_in_database(
                self.created_school_batch, 
                "SCHOOL", 
                self.created_school_id
            )
        
        # Run remaining tests
        for test_name, test_func in tests[2:]:
            try:
                success = test_func()
                if success:
                    print(f"[PASS] {test_name} - PASSED")
                else:
                    print(f"[FAIL] {test_name} - FAILED")
            except Exception as e:
                print(f"[ERROR] {test_name} - ERROR: {str(e)}")
        
        # Generate summary report
        self.generate_summary_report()

    def generate_summary_report(self):
        """Generate final test summary report"""
        print(f"\n{'='*80}")
        print("[SUMMARY] TEST SUMMARY REPORT")
        print(f"{'='*80}")
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result["success"])
        failed_tests = total_tests - passed_tests
        
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests} [PASS]")
        print(f"Failed: {failed_tests} [FAIL]")
        print(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        if failed_tests > 0:
            print(f"\n[FAIL] FAILED TESTS:")
            for result in self.test_results:
                if not result["success"]:
                    print(f"   - {result['test_name']}: {result['details'].get('error', 'Unknown error')}")
        
        print(f"\n[METRICS] KEY METRICS:")
        print(f"   - API Base URL: {self.base_url}")
        print(f"   - Test Duration: {datetime.now().isoformat()}")
        if hasattr(self, 'created_regional_batch'):
            print(f"   - Created Regional Batch: {self.created_regional_batch}")
        if hasattr(self, 'created_school_batch'):
            print(f"   - Created School Batch: {self.created_school_batch}")
        
        print(f"\n{'='*80}")


if __name__ == "__main__":
    print("[START] Batch Creation API Test Suite")
    print("=" * 60)
    
    try:
        # Check if service is accessible
        health_response = requests.get(f"{BASE_URL}/health", timeout=5)
        if health_response.status_code != 200:
            print(f"[ERROR] Service health check failed: {health_response.status_code}")
            sys.exit(1)
        else:
            print("[OK] Service is running and accessible")
            
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Cannot connect to service at {BASE_URL}: {str(e)}")
        sys.exit(1)
    
    # Run tests
    tester = BatchAPITester()
    tester.run_all_tests()