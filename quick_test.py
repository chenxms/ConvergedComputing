import requests
import time

def test_api():
    print("=== 批次创建API性能和验证测试 ===\n")
    
    base_url = "http://localhost:8000"
    
    # 检查服务状态
    try:
        health = requests.get(f"{base_url}/health", timeout=5)
        print(f"服务状态: {health.status_code} - {health.json()}")
    except Exception as e:
        print(f"服务连接失败: {e}")
        return
    
    # 测试1: 区域级批次创建性能
    print("\n测试1: 区域级批次创建性能")
    print("-" * 30)
    
    payload1 = {
        "batch_code": f"REGIONAL_PERF_{int(time.time())}",
        "aggregation_level": "REGIONAL",
        "region_name": "测试区域",
        "statistics_data": {
            "batch_info": {
                "batch_code": "TEST_001",
                "created_at": "2025-01-07T21:23:00",
                "data_version": "v1.0"
            },
            "academic_subjects": {
                "chinese": {
                    "total_score": 450,
                    "average_score": 85.5,
                    "pass_rate": 0.92
                }
            }
        },
        "total_schools": 15
    }
    
    start_time = time.time()
    try:
        response1 = requests.post(f"{base_url}/api/v1/statistics/batches", json=payload1, timeout=10)
        end_time = time.time()
        response_time1 = (end_time - start_time) * 1000
        
        print(f"响应时间: {response_time1:.0f}ms")
        print(f"HTTP状态码: {response1.status_code}")
        print(f"性能目标: <2000ms")
        
        if response1.status_code == 200:
            result = response1.json()
            print(f"状态: 成功 - {result.get('message', '')}")
        else:
            print(f"状态: 失败 - {response1.text[:200]}")
            
    except Exception as e:
        print(f"请求失败: {e}")
    
    time.sleep(1)
    
    # 测试2: 学校级批次创建（有school_id）
    print("\n测试2: 学校级批次创建（有school_id）")
    print("-" * 35)
    
    payload2 = {
        "batch_code": f"SCHOOL_PERF_{int(time.time())}",
        "aggregation_level": "SCHOOL",
        "region_name": "测试区域",
        "school_id": "TEST_SCHOOL_001",
        "school_name": "测试学校",
        "statistics_data": {
            "batch_info": {
                "batch_code": "TEST_002",
                "created_at": "2025-01-07T21:23:00",
                "data_version": "v1.0"
            },
            "academic_subjects": {
                "math": {
                    "total_score": 500,
                    "average_score": 88.2,
                    "pass_rate": 0.95
                }
            }
        }
    }
    
    start_time = time.time()
    try:
        response2 = requests.post(f"{base_url}/api/v1/statistics/batches", json=payload2, timeout=10)
        end_time = time.time()
        response_time2 = (end_time - start_time) * 1000
        
        print(f"响应时间: {response_time2:.0f}ms")
        print(f"HTTP状态码: {response2.status_code}")
        print(f"性能目标: <2000ms")
        
        if response2.status_code == 200:
            result = response2.json()
            print(f"状态: 成功 - {result.get('message', '')}")
        else:
            print(f"状态: 失败 - {response2.text[:200]}")
            
    except Exception as e:
        print(f"请求失败: {e}")
    
    time.sleep(1)
    
    # 测试3: 学校级批次验证（无school_id，应该失败）
    print("\n测试3: 学校级批次验证（无school_id）")
    print("-" * 35)
    
    payload3 = {
        "batch_code": f"SCHOOL_VAL_{int(time.time())}",
        "aggregation_level": "SCHOOL",
        "region_name": "测试区域",
        # 故意不包含 school_id
        "school_name": "测试学校",
        "statistics_data": {
            "batch_info": {
                "batch_code": "TEST_003",
                "created_at": "2025-01-07T21:23:00",
                "data_version": "v1.0"
            },
            "academic_subjects": {
                "english": {
                    "total_score": 400,
                    "average_score": 82.3,
                    "pass_rate": 0.89
                }
            }
        }
    }
    
    try:
        response3 = requests.post(f"{base_url}/api/v1/statistics/batches", json=payload3, timeout=10)
        
        print(f"HTTP状态码: {response3.status_code}")
        print(f"期望状态码: 422 (验证错误)")
        
        if response3.status_code == 422:
            try:
                result = response3.json()
                print(f"状态: 验证成功 - 正确拦截了无效请求")
                print(f"错误详情: {result.get('detail', '')}")
            except:
                print(f"状态: 验证成功 - 正确返回422错误")
        else:
            print(f"状态: 验证失败 - 应该返回422错误")
            print(f"实际响应: {response3.text[:200]}")
            
    except Exception as e:
        print(f"请求失败: {e}")
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    test_api()