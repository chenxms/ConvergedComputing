#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用真实数据测试API
"""
import requests
import json

def test_api_status():
    """测试API状态"""
    print("测试API状态...")
    try:
        response = requests.get("http://127.0.0.1:8001/api/v1/statistics/system/status", timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            print(f"API状态: {data.get('status', '未知')}")
            return True
        else:
            print(f"API响应错误: {response.status_code}")
            return False
    except Exception as e:
        print(f"连接失败: {e}")
        return False

def test_aggregation_task():
    """测试数据汇聚任务"""
    print("\n测试数据汇聚任务...")
    
    # 使用真实批次G7-2025进行测试
    batch_code = "G7-2025"
    
    try:
        response = requests.post(
            f"http://127.0.0.1:8001/api/v1/statistics/tasks/{batch_code}/start",
            params={
                "aggregation_level": "regional",
                "priority": 3
            },
            timeout=30
        )
        
        if response.status_code == 200:
            task_data = response.json()
            print("任务启动成功!")
            print(f"  任务ID: {task_data.get('id', 'N/A')}")
            print(f"  状态: {task_data.get('status', 'N/A')}")
            print(f"  批次: {batch_code}")
            return True
        else:
            print(f"任务启动失败: {response.status_code}")
            print(f"响应: {response.text}")
            return False
            
    except Exception as e:
        print(f"任务测试失败: {e}")
        return False

def test_batch_creation():
    """测试批次创建"""
    print("\n测试批次管理API...")
    
    test_batch = {
        "batch_code": "TEST-API-" + str(int(__import__('time').time())),
        "aggregation_level": "regional",
        "statistics_data": {
            "batch_info": {
                "total_students": 100,
                "total_schools": 5,
                "calculation_time": "2025-09-05T14:30:00"
            },
            "academic_subjects": [
                {
                    "subject_id": "TEST_SUBJECT",
                    "subject_name": "测试科目",
                    "statistics": {
                        "average_score": 85.5,
                        "participant_count": 100
                    }
                }
            ]
        },
        "data_version": "1.0",
        "total_students": 100,
        "total_schools": 5,
        "triggered_by": "api_test"
    }
    
    try:
        response = requests.post(
            "http://127.0.0.1:8001/api/v1/management/batches",
            json=test_batch,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print("批次创建成功!")
            print(f"  批次代码: {test_batch['batch_code']}")
            return True
        else:
            print(f"批次创建失败: {response.status_code}")
            print(f"响应: {response.text}")
            return False
            
    except Exception as e:
        print(f"批次创建测试失败: {e}")
        return False

def main():
    print("=" * 60)
    print("FastAPI数据汇聚计算测试")
    print("=" * 60)
    print("服务器地址: http://127.0.0.1:8001")
    
    # 1. 测试API状态
    if not test_api_status():
        print("API不可用，请检查服务器")
        return False
    
    # 2. 测试批次管理
    batch_success = test_batch_creation()
    
    # 3. 测试数据汇聚计算
    task_success = test_aggregation_task()
    
    print("\n" + "=" * 60)
    print("测试结果总结")
    print("=" * 60)
    print(f"批次管理API: {'成功' if batch_success else '失败'}")
    print(f"数据汇聚任务: {'成功' if task_success else '失败'}")
    
    if batch_success and task_success:
        print("\n恭喜！数据汇聚计算系统运行正常！")
        print("可以使用真实学生数据进行统计分析了。")
    elif batch_success:
        print("\n批次管理正常，但汇聚计算需要检查")
    else:
        print("\n系统需要进一步调试")
    
    return batch_success or task_success

if __name__ == "__main__":
    success = main()