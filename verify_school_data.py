#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证G7-2025学校级数据是否已生成
"""
import requests
import json

def verify_school_data():
    print("=== 验证G7-2025学校级数据生成 ===")
    print()

    # 测试几个学校的API端点
    school_ids = ['SCH_001', 'SCH_002', 'SCH_003', 'SCH_004', 'SCH_005']
    
    # 首先检查是否需要重新启动一个增强的区域任务
    print("1. 启动增强版G7-2025区域汇聚任务...")
    try:
        response = requests.post(
            'http://127.0.0.1:8002/api/v1/statistics/tasks/G7-2025/start',
            params={'aggregation_level': 'regional', 'priority': 3},
            timeout=30
        )
        if response.status_code == 200:
            task_data = response.json()
            print(f"   任务启动成功: {task_data.get('id')}")
            
            # 等待任务完成
            import time
            print("   等待任务完成...")
            for i in range(10):
                time.sleep(3)
                progress_response = requests.get(f'http://127.0.0.1:8002/api/v1/statistics/tasks/{task_data.get("id")}/progress')
                if progress_response.status_code == 200:
                    progress = progress_response.json()
                    overall = progress.get('overall_progress', 0)
                    print(f"   进度: {overall:.1f}%")
                    if overall >= 100:
                        print("   任务完成!")
                        break
                
        else:
            print(f"   任务启动失败: {response.status_code}")
    except Exception as e:
        print(f"   任务启动错误: {e}")

    print()
    print("2. 验证学校级数据...")
    
    success_count = 0
    for school_id in school_ids:
        try:
            response = requests.get(f'http://127.0.0.1:8002/api/v1/reporting/reports/school/G7-2025/{school_id}', timeout=10)
            if response.status_code == 200:
                data = response.json()
                school_info = data.get('data', {}).get('school_info', {})
                school_name = school_info.get('school_name', 'N/A')
                students = school_info.get('total_students', 0)
                print(f"   SUCCESS: {school_id} ({school_name}): {students}名学生")
                success_count += 1
            else:
                print(f"   ERROR: {school_id}: HTTP {response.status_code}")
        except Exception as e:
            print(f"   ERROR: {school_id}: {e}")
    
    print()
    print("3. 验证区域级数据...")
    try:
        response = requests.get('http://127.0.0.1:8002/api/v1/reporting/reports/regional/G7-2025', timeout=10)
        if response.status_code == 200:
            data = response.json()
            batch_info = data.get('data', {}).get('batch_info', {})
            total_students = batch_info.get('total_students', 0)
            total_schools = batch_info.get('total_schools', 0)
            print(f"   SUCCESS: 区域数据包含 {total_students}名学生, {total_schools}所学校")
        else:
            print(f"   ERROR: 区域数据 HTTP {response.status_code}")
    except Exception as e:
        print(f"   ERROR: 区域数据 {e}")
    
    print()
    print("=== 验证结果总结 ===")
    print(f"学校级数据可用: {success_count}/{len(school_ids)}")
    
    if success_count == len(school_ids):
        print("SUCCESS: 增强区域级计算成功! 所有学校数据都已生成!")
    elif success_count > 0:
        print("PARTIAL: 部分学校数据已生成, 需要进一步检查")
    else:
        print("FAILED: 学校级数据未生成, 需要调试增强计算逻辑")

if __name__ == "__main__":
    verify_school_data()