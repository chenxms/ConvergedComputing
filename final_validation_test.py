#!/usr/bin/env python3
"""
最终验证测试 - 使用唯一批次代码
"""
import requests
import time

API_BASE = "http://localhost:8000/api/v1/management"

def test_validation_fixed():
    timestamp = int(time.time())
    
    print("=== Final Validation Test ===")
    
    # 测试：SCHOOL级别，完全缺失school_id
    test_data = {
        "batch_code": f"FINAL_TEST_{timestamp}",
        "aggregation_level": "school",
        # school_id 完全不存在
        "statistics_data": {
            "batch_info": {"test": "data"},
            "academic_subjects": {"test": "data"}
        }
    }
    
    print("Testing SCHOOL level batch without school_id...")
    
    try:
        response = requests.post(f"{API_BASE}/batches", json=test_data, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 422:
            print("✅ PASS - Validation working correctly (422)")
            print("🔧 Validation fix successful!")
            return True
        else:
            print(f"❌ FAIL - Expected 422, got {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ FAIL - Request error: {e}")
        return False

if __name__ == "__main__":
    if test_validation_fixed():
        print("\n🎉 验证器修复成功！")
    else:
        print("\n❌ 验证器仍有问题")