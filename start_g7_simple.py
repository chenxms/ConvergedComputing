#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import time

def start_aggregation():
    print("启动G7-2025批次数据汇聚任务")
    print("批次信息: G7-2025, 15,200个学生, 43所学校, 11个学科")
    
    try:
        response = requests.post(
            "http://127.0.0.1:8002/api/v1/statistics/tasks/G7-2025/start",
            params={
                "aggregation_level": "regional", 
                "priority": 3
            },
            timeout=30
        )
        
        if response.status_code == 200:
            task_data = response.json()
            print("SUCCESS: 数据汇聚任务启动成功!")
            print(f"任务ID: {task_data.get('id')}")
            print(f"批次代码: {task_data.get('batch_code')}")
            print(f"当前状态: {task_data.get('status')}")
            print(f"启动时间: {task_data.get('started_at')}")
            
            if 'stage_details' in task_data:
                print("\n计算阶段:")
                for i, stage in enumerate(task_data['stage_details'], 1):
                    status = "等待中" if stage['status'] == 'pending' else "完成"
                    print(f"  {i}. [{status}] {stage['stage']}")
            
            return task_data.get('id')
        else:
            print(f"任务启动失败: HTTP {response.status_code}")
            print(f"响应: {response.text}")
            return None
    except Exception as e:
        print(f"启动错误: {e}")
        return None

def monitor_progress(task_id):
    print(f"\n开始监控任务进度 (任务ID: {task_id})")
    print("=" * 60)
    
    for i in range(10):  # 监控10次
        try:
            response = requests.get(
                f"http://127.0.0.1:8002/api/v1/statistics/tasks/{task_id}/progress",
                timeout=10
            )
            
            if response.status_code == 200:
                progress = response.json()
                overall = progress.get('overall_progress', 0)
                print(f"[{i+1:2d}/10] 总体进度: {overall:6.2f}% | {time.strftime('%H:%M:%S')}")
                
                # 显示当前阶段
                if 'stage_details' in progress:
                    for stage in progress['stage_details']:
                        if stage['status'] == 'in_progress':
                            print(f"         当前: {stage['stage']} ({stage['progress']:.1f}%)")
                
                if overall >= 100:
                    print("\n计算完成!")
                    get_results()
                    return True
            else:
                print(f"[{i+1:2d}/10] 获取进度失败: HTTP {response.status_code}")
            
            time.sleep(10)  # 等待10秒
        except Exception as e:
            print(f"[{i+1:2d}/10] 监控错误: {e}")
            time.sleep(10)
    
    print("监控结束")
    return False

def get_results():
    print("\n获取汇聚计算结果...")
    
    try:
        response = requests.get(
            "http://127.0.0.1:8002/api/v1/reporting/reports/regional/G7-2025",
            timeout=15
        )
        
        if response.status_code == 200:
            data = response.json().get('data', {})
            
            if 'batch_info' in data:
                batch = data['batch_info']
                print(f"批次: {batch.get('batch_code')}")
                print(f"学生: {batch.get('total_students', 0):,}人")
                print(f"学校: {batch.get('total_schools')}所")
            
            if 'academic_subjects' in data:
                subjects = data['academic_subjects']
                print(f"\n学科统计 (共{len(subjects)}个):")
                print("学科名称     | 人数   | 平均分 | 难度")
                print("-" * 40)
                
                for subject in subjects[:5]:  # 显示前5个
                    name = subject.get('subject_name', 'N/A')[:10]
                    stats = subject.get('statistics', {})
                    count = stats.get('participant_count', 0)
                    avg = stats.get('average_score', 0)
                    diff = stats.get('difficulty_coefficient', 0)
                    print(f"{name:<10} | {count:>6,} | {avg:>6.2f} | {diff:>4.3f}")
            
            print("\nG7-2025数据汇聚计算完成!")
            return True
        else:
            print(f"获取结果失败: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"获取结果错误: {e}")
        return False

def main():
    print("=" * 60)
    print("G7-2025批次数据汇聚任务")
    print("=" * 60)
    
    task_id = start_aggregation()
    
    if task_id:
        print(f"\n任务已启动，ID: {task_id}")
        monitor_progress(task_id)
    else:
        print("任务启动失败")

if __name__ == "__main__":
    main()