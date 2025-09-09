#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
G7-2025批次汇聚结果最终验证
"""
import requests
import json

def test_g7_2025_apis():
    """验证G7-2025相关API端点"""
    
    print("验证G7-2025批次汇聚结果API访问")
    print("=" * 60)

    # 测试区域级报告
    try:
        response = requests.get('http://127.0.0.1:8002/api/v1/reporting/reports/regional/G7-2025', timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("SUCCESS: 区域级报告获取成功!")
            batch_info = data.get("data", {}).get("batch_info", {})
            print(f"   批次代码: {batch_info.get('batch_code', 'N/A')}")
            print(f"   年级水平: {batch_info.get('grade_level', 'N/A')}")
            print(f"   参与学生: {batch_info.get('total_students', 0):,}人")
            print(f"   参与学校: {batch_info.get('total_schools', 0)}所")
            
            subjects = data.get('data', {}).get('academic_subjects', [])
            print(f"   学科数量: {len(subjects)}个")
            if subjects:
                math_stats = subjects[0].get('statistics', {})
                print(f"   数学平均分: {math_stats.get('average_score', 0):.1f}")
                print(f"   难度系数: {math_stats.get('difficulty_coefficient', 0):.3f}")
                print(f"   区分度: {math_stats.get('discrimination_coefficient', 0):.3f}")
                
        else:
            print(f"ERROR: 区域级报告获取失败: {response.status_code}")
            print(f"   错误: {response.text[:200]}")
    except Exception as e:
        print(f"ERROR: 区域级报告请求失败: {e}")

    print()

    # 测试学校级报告
    try:
        response = requests.get('http://127.0.0.1:8002/api/v1/reporting/reports/school/G7-2025/SCH_001', timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("SUCCESS: 学校级报告获取成功!")
            school_info = data.get('data', {}).get('school_info', {})
            print(f"   学校名称: {school_info.get('school_name', 'N/A')}")
            print(f"   学校ID: {school_info.get('school_id', 'N/A')}")
            print(f"   参与学生: {school_info.get('total_students', 0):,}人")
            
            subjects = data.get('data', {}).get('academic_subjects', [])
            if subjects:
                math_stats = subjects[0].get('statistics', {})
                print(f"   数学平均分: {math_stats.get('average_score', 0):.1f}")
                print(f"   区域排名: {math_stats.get('regional_ranking', 0)}")
                
        else:
            print(f"ERROR: 学校级报告获取失败: {response.status_code}")
            print(f"   错误: {response.text[:200]}")
    except Exception as e:
        print(f"ERROR: 学校级报告请求失败: {e}")

    print()

    # 测试雷达图数据
    try:
        response = requests.get('http://127.0.0.1:8002/api/v1/reporting/reports/radar-chart/G7-2025?school_id=SCH_001', timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("SUCCESS: 雷达图数据获取成功!")
            radar_data = data.get('data', {})
            dimensions = radar_data.get('dimensions', [])
            print(f"   维度数量: {len(dimensions)}个")
            if dimensions:
                first_dim = dimensions[0]
                print(f"   第一维度: {first_dim.get('name', 'N/A')} (分数: {first_dim.get('score', 0):.1f})")
                
        else:
            print(f"ERROR: 雷达图数据获取失败: {response.status_code}")
            print(f"   错误: {response.text[:200]}")
    except Exception as e:
        print(f"ERROR: 雷达图数据请求失败: {e}")

    print()
    print("=" * 60)
    print("G7-2025批次汇聚结果验证完成!")

if __name__ == "__main__":
    test_g7_2025_apis()