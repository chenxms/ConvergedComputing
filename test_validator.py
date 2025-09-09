#!/usr/bin/env python3
"""
测试Pydantic验证器是否正常工作
"""
import json
from app.schemas.request_schemas import StatisticalAggregationCreateRequest
from app.database.enums import AggregationLevel
from pydantic import ValidationError

def test_school_validator():
    print("Testing Pydantic validator...")
    
    # Test 1: SCHOOL level without school_id - should fail
    print("\nTest 1: SCHOOL level missing school_id")
    try:
        request_data = {
            "batch_code": "TEST_VALIDATION",
            "aggregation_level": "school",  # 使用字符串
            # "school_id": None,  # 故意不提供
            "statistics_data": {
                "batch_info": {"test": "data"},
                "academic_subjects": {"test": "data"}
            }
        }
        
        # 尝试创建请求对象
        request = StatisticalAggregationCreateRequest(**request_data)
        print(f"FAIL - Unexpected success: {request.school_id}")
        return False
        
    except ValidationError as e:
        print(f"PASS - Validation failed as expected: {e}")
        return True
    except ValueError as e:
        print(f"PASS - Business validation failed as expected: {e}")
        return True
    except Exception as e:
        print(f"FAIL - Other error: {e}")
        return False

def test_school_validator_with_id():
    print("\nTest 2: SCHOOL level with school_id")
    try:
        request_data = {
            "batch_code": "TEST_VALIDATION_WITH_ID",
            "aggregation_level": "school",
            "school_id": "SCHOOL_123",
            "statistics_data": {
                "batch_info": {"test": "data"},
                "academic_subjects": {"test": "data"}
            }
        }
        
        request = StatisticalAggregationCreateRequest(**request_data)
        print(f"PASS - Created successfully: school_id={request.school_id}")
        return True
        
    except Exception as e:
        print(f"FAIL - Unexpected failure: {e}")
        return False

def test_regional_validator():
    print("\nTest 3: REGIONAL level, no school_id needed")
    try:
        request_data = {
            "batch_code": "TEST_REGIONAL",
            "aggregation_level": "regional",
            "statistics_data": {
                "batch_info": {"test": "data"},
                "academic_subjects": {"test": "data"}
            }
        }
        
        request = StatisticalAggregationCreateRequest(**request_data)
        print(f"PASS - Created successfully: aggregation_level={request.aggregation_level}")
        return True
        
    except Exception as e:
        print(f"FAIL - Unexpected failure: {e}")
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("Pydantic Validator Test")
    print("=" * 50)
    
    results = []
    results.append(test_school_validator())
    results.append(test_school_validator_with_id())
    results.append(test_regional_validator())
    
    print("\n" + "=" * 50)
    print(f"Test Results: {sum(results)}/{len(results)} passed")
    
    if all(results):
        print("PASS - All validator tests passed")
    else:
        print("FAIL - Some validator tests failed")