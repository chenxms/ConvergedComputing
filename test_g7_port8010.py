#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试端口8010的G7-2025 API端点
"""
import requests
import json

def test_g7_2025_on_port_8010():
    """测试端口8010的G7-2025 API"""
    
    print("测试端口8010的G7-2025批次汇聚结果")
    print("=" * 60)

    # 测试区域级报告 
    try:
        response = requests.get('http://127.0.0.1:8010/api/v1/reporting/reports/regional/G7-2025', timeout=10)
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
                for subject in subjects:
                    name = subject.get('subject_name', 'N/A')
                    stats = subject.get('statistics', {})
                    avg_score = stats.get('average_score', 0)
                    diff_coeff = stats.get('difficulty_coefficient', 0)
                    print(f"   {name}: 平均分{avg_score:.1f}, 难度{diff_coeff:.3f}")
                
        else:
            print(f"ERROR: 区域级报告获取失败: {response.status_code}")
            print(f"   错误: {response.text[:200]}")
    except Exception as e:
        print(f"ERROR: 区域级报告请求失败: {e}")

    print()

    # 测试学校级报告
    try:
        response = requests.get('http://127.0.0.1:8010/api/v1/reporting/reports/school/G7-2025/SCH_001', timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("SUCCESS: 学校级报告获取成功!")
            school_info = data.get('data', {}).get('school_info', {})
            print(f"   学校名称: {school_info.get('school_name', 'N/A')}")
            print(f"   学校ID: {school_info.get('school_id', 'N/A')}")
            print(f"   参与学生: {school_info.get('total_students', 0):,}人")
            
            subjects = data.get('data', {}).get('academic_subjects', [])
            if subjects:
                for subject in subjects:
                    name = subject.get('subject_name', 'N/A')
                    stats = subject.get('statistics', {})
                    avg_score = stats.get('average_score', 0)
                    ranking = stats.get('regional_ranking', 0)
                    print(f"   {name}: 平均分{avg_score:.1f}, 排名{ranking}")
                
        else:
            print(f"ERROR: 学校级报告获取失败: {response.status_code}")
            print(f"   错误: {response.text[:200]}")
    except Exception as e:
        print(f"ERROR: 学校级报告请求失败: {e}")

    print()

    # 测试雷达图数据
    try:
        response = requests.get('http://127.0.0.1:8010/api/v1/reporting/reports/radar-chart/G7-2025?school_id=SCH_001', timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("SUCCESS: 雷达图数据获取成功!")
            radar_data = data.get('data', {})
            dimensions = radar_data.get('dimensions', [])
            print(f"   维度数量: {len(dimensions)}个")
            for dim in dimensions:
                name = dim.get('name', 'N/A')
                score = dim.get('score', 0)
                print(f"   {name}: {score:.1f}分")
                
        else:
            print(f"ERROR: 雷达图数据获取失败: {response.status_code}")
            print(f"   错误: {response.text[:200]}")
    except Exception as e:
        print(f"ERROR: 雷达图数据请求失败: {e}")

    print()
    print("=" * 60)
    print("G7-2025批次API测试完成!")

if __name__ == "__main__":
    test_g7_2025_on_port_8010()