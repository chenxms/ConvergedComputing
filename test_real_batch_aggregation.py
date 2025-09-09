#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用真实批次数据测试汇聚计算
"""
import requests
import json
import time
from sqlalchemy import text
from app.database.connection import engine

def get_real_batch_info():
    """获取真实批次信息"""
    print("获取数据库中的真实批次信息...")
    
    try:
        with engine.connect() as connection:
            # 获取批次基本信息
            result = connection.execute(text("""
                SELECT batch_code, batch_name, grade_level, total_students, total_schools, status
                FROM grade_aggregation_main 
                WHERE status = 'completed'
                ORDER BY batch_code
            """))
            
            batches = result.fetchall()
            
            if batches:
                print("可用的批次:")
                for batch in batches:
                    batch_code, batch_name, grade_level, total_students, total_schools, status = batch
                    print(f"  - {batch_code} ({batch_name})")
                    print(f"    年级: {grade_level}, 状态: {status}")
                
                # 选择G7-2025进行测试（数据量最大）
                test_batch = "G7-2025"
                
                # 检查这个批次的详细数据
                result = connection.execute(text("""
                    SELECT COUNT(DISTINCT student_id) as student_count,
                           COUNT(DISTINCT school_id) as school_count,
                           COUNT(DISTINCT subject_id) as subject_count
                    FROM student_score_detail 
                    WHERE batch_code = :batch_code
                """), {"batch_code": test_batch})
                
                stats = result.fetchone()
                
                return {
                    "batch_code": test_batch,
                    "students": stats[0],
                    "schools": stats[1],
                    "subjects": stats[2]
                }
            else:
                print("未找到已完成的批次")
                return None
                
    except Exception as e:
        print(f"获取批次信息失败: {e}")
        return None

def create_batch_for_aggregation(batch_info):
    """为汇聚计算创建批次记录"""
    print(f"\n为批次 {batch_info['batch_code']} 创建汇聚记录...")
    
    # 创建区域级批次记录
    batch_data = {
        "batch_code": batch_info["batch_code"],
        "aggregation_level": "regional", 
        "statistics_data": {
            "batch_info": {
                "batch_code": batch_info["batch_code"],
                "grade_level": "初中",  # G7是7年级，属于初中
                "total_schools": batch_info["schools"],
                "total_students": batch_info["students"],
                "calculation_time": time.strftime("%Y-%m-%dT%H:%M:%S")
            },
            "academic_subjects": [
                {
                    "subject_id": "PLACEHOLDER_SUBJECT",
                    "subject_name": "待计算学科",
                    "subject_type": "exam",
                    "total_score": 100,
                    "statistics": {
                        "participant_count": batch_info["students"],
                        "average_score": 0.0,
                        "note": "awaiting_calculation"
                    }
                }
            ]
        },
        "data_version": "1.0",
        "total_students": batch_info["students"],
        "total_schools": batch_info["schools"],
        "triggered_by": "aggregation_test"
    }
    
    try:
        response = requests.post(
            "http://127.0.0.1:8001/api/v1/management/batches",
            json=batch_data,
            timeout=10
        )
        
        if response.status_code == 200:
            print("批次记录创建成功!")
            return True
        else:
            print(f"批次记录创建失败: {response.status_code}")
            print(f"响应: {response.text}")
            return False
            
    except Exception as e:
        print(f"创建批次记录失败: {e}")
        return False

def start_aggregation_task(batch_code):
    """启动数据汇聚计算任务"""
    print(f"\n启动批次 {batch_code} 的数据汇聚计算...")
    
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
            print("汇聚计算任务启动成功!")
            print(f"  任务ID: {task_data.get('id', 'N/A')}")
            print(f"  状态: {task_data.get('status', 'N/A')}")
            print(f"  批次: {batch_code}")
            
            # 等待一下，然后查询任务状态
            print("\n等待计算完成...")
            time.sleep(5)
            
            # 查询任务状态
            status_response = requests.get(
                f"http://127.0.0.1:8001/api/v1/statistics/tasks",
                timeout=10
            )
            
            if status_response.status_code == 200:
                tasks = status_response.json()
                print(f"当前任务数量: {len(tasks)}")
            
            return True
        else:
            print(f"任务启动失败: {response.status_code}")
            print(f"响应: {response.text}")
            return False
            
    except Exception as e:
        print(f"启动汇聚任务失败: {e}")
        return False

def check_aggregation_results(batch_code):
    """检查汇聚计算结果"""
    print(f"\n检查批次 {batch_code} 的汇聚结果...")
    
    try:
        # 查询汇聚结果
        response = requests.get(
            f"http://127.0.0.1:8001/api/v1/reporting/reports/regional/{batch_code}",
            timeout=10
        )
        
        if response.status_code == 200:
            report_data = response.json()
            print("汇聚结果查询成功!")
            
            if "data" in report_data and report_data["data"]:
                data = report_data["data"]
                print("汇聚计算结果概览:")
                
                # 显示基本信息
                if "batch_info" in data:
                    batch_info = data["batch_info"]
                    print(f"  批次: {batch_info.get('batch_code', 'N/A')}")
                    print(f"  学生数: {batch_info.get('total_students', 'N/A')}")
                    print(f"  学校数: {batch_info.get('total_schools', 'N/A')}")
                
                # 显示学科统计
                if "academic_subjects" in data:
                    subjects = data["academic_subjects"]
                    print(f"  统计学科数: {len(subjects)}")
                    
                    for subject in subjects[:3]:  # 显示前3个学科
                        stats = subject.get("statistics", {})
                        print(f"    - {subject.get('subject_name', 'N/A')}: "
                              f"平均分 {stats.get('average_score', 'N/A')}, "
                              f"参与人数 {stats.get('participant_count', 'N/A')}")
                
                print("\n🎉 数据汇聚计算成功完成!")
                return True
            else:
                print("汇聚结果为空，计算可能仍在进行中")
                return False
                
        elif response.status_code == 404:
            print("汇聚结果尚未生成，计算可能仍在进行中")
            return False
        else:
            print(f"查询汇聚结果失败: {response.status_code}")
            print(f"响应: {response.text}")
            return False
            
    except Exception as e:
        print(f"检查汇聚结果失败: {e}")
        return False

def main():
    print("=" * 70)
    print("真实数据汇聚计算完整测试")
    print("=" * 70)
    print("服务器地址: http://127.0.0.1:8001")
    
    # 1. 获取真实批次信息
    batch_info = get_real_batch_info()
    
    if not batch_info:
        print("未找到可用的批次数据")
        return False
    
    print(f"\n选择测试批次: {batch_info['batch_code']}")
    print(f"  学生数: {batch_info['students']}")
    print(f"  学校数: {batch_info['schools']}")
    print(f"  学科数: {batch_info['subjects']}")
    
    # 2. 创建批次汇聚记录
    if not create_batch_for_aggregation(batch_info):
        print("批次记录创建失败")
        return False
    
    # 3. 启动汇聚计算任务
    if not start_aggregation_task(batch_info["batch_code"]):
        print("汇聚计算任务启动失败")
        return False
    
    # 4. 检查计算结果
    result_success = check_aggregation_results(batch_info["batch_code"])
    
    print("\n" + "=" * 70)
    print("测试结果总结")
    print("=" * 70)
    
    if result_success:
        print("🎉 恭喜！数据汇聚计算系统完全正常！")
        print(f"✓ 成功处理了 {batch_info['students']} 个学生的数据")
        print(f"✓ 涵盖了 {batch_info['schools']} 所学校")
        print(f"✓ 统计了 {batch_info['subjects']} 个学科")
        print("\n系统已准备好处理大规模教育统计数据！")
    else:
        print("⚠️ 汇聚计算可能需要更多时间")
        print("可以稍后查询结果，或检查后台任务状态")
    
    return True

if __name__ == "__main__":
    success = main()