#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
启动G7-2025批次的数据汇聚任务
"""
import requests
import time
import json

def start_g7_aggregation():
    """启动G7-2025批次的数据汇聚任务"""
    print("=" * 60)
    print("启动G7-2025批次数据汇聚任务")
    print("=" * 60)
    print("批次信息:")
    print("  - 批次代码: G7-2025")
    print("  - 学生数量: 15,200人")
    print("  - 学校数量: 43所")
    print("  - 学科数量: 11个")
    print("  - 汇聚级别: 区域级")
    print()
    
    # 启动区域级汇聚任务
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
            print("🎉 数据汇聚任务启动成功!")
            print("=" * 40)
            print(f"任务ID: {task_data.get('id')}")
            print(f"批次代码: {task_data.get('batch_code')}")
            print(f"当前状态: {task_data.get('status')}")
            print(f"汇聚级别: {task_data.get('aggregation_level')}")
            print(f"优先级: {task_data.get('priority')}")
            print(f"启动时间: {task_data.get('started_at')}")
            print("=" * 40)
            
            # 显示任务阶段
            if 'stage_details' in task_data:
                print("\n计算阶段:")
                for i, stage in enumerate(task_data['stage_details'], 1):
                    status_icon = "⏳" if stage['status'] == 'pending' else "✅"
                    print(f"  {i}. {status_icon} {stage['stage']}")
            
            return task_data.get('id')
            
        else:
            print(f"❌ 任务启动失败: HTTP {response.status_code}")
            print(f"错误信息: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ 启动任务时发生错误: {e}")
        return None

def monitor_task_progress(task_id):
    """监控任务进度"""
    print(f"\n开始监控任务进度 (任务ID: {task_id})")
    print("=" * 60)
    
    for i in range(20):  # 监控20次，每次间隔15秒
        try:
            # 获取任务进度
            response = requests.get(
                f"http://127.0.0.1:8002/api/v1/statistics/tasks/{task_id}/progress",
                timeout=10
            )
            
            if response.status_code == 200:
                progress_data = response.json()
                overall_progress = progress_data.get('overall_progress', 0)
                
                print(f"[{i+1:2d}/20] 总体进度: {overall_progress:6.2f}% | {time.strftime('%H:%M:%S')}")
                
                # 显示阶段详情
                if 'stage_details' in progress_data:
                    for stage in progress_data['stage_details']:
                        if stage['status'] == 'in_progress':
                            print(f"         当前阶段: {stage['stage']} ({stage['progress']:.1f}%)")
                
                # 如果完成了，获取结果
                if overall_progress >= 100:
                    print("\n🎉 汇聚计算完成!")
                    get_aggregation_results()
                    return True
                    
            else:
                print(f"[{i+1:2d}/20] 获取进度失败: HTTP {response.status_code}")
            
            # 等待15秒
            if i < 19:  # 最后一次不用等待
                time.sleep(15)
                
        except Exception as e:
            print(f"[{i+1:2d}/20] 监控错误: {e}")
            time.sleep(15)
    
    print("\n监控结束，任务可能仍在进行中")
    print("可以稍后手动查询结果")
    return False

def get_aggregation_results():
    """获取汇聚计算结果"""
    print("\n🔍 获取汇聚计算结果...")
    print("=" * 60)
    
    try:
        response = requests.get(
            "http://127.0.0.1:8002/api/v1/reporting/reports/regional/G7-2025",
            timeout=15
        )
        
        if response.status_code == 200:
            report_data = response.json()
            
            if report_data.get('success') and report_data.get('data'):
                data = report_data['data']
                
                print("✅ 汇聚计算结果:")
                print("=" * 40)
                
                # 批次信息
                if 'batch_info' in data:
                    batch_info = data['batch_info']
                    print(f"批次代码: {batch_info.get('batch_code')}")
                    print(f"年级水平: {batch_info.get('grade_level')}")
                    print(f"参与学生: {batch_info.get('total_students'):,}人")
                    print(f"参与学校: {batch_info.get('total_schools')}所")
                    print(f"计算时间: {batch_info.get('calculation_time')}")
                
                # 学科统计
                if 'academic_subjects' in data:
                    subjects = data['academic_subjects']
                    print(f"\n📚 学科统计结果 (共{len(subjects)}个学科):")
                    print("-" * 80)
                    print("学科名称         | 参与人数 | 平均分 | 难度系数 | 区分度")
                    print("-" * 80)
                    
                    for subject in subjects[:10]:  # 显示前10个学科
                        name = subject.get('subject_name', subject.get('subject_id', 'N/A'))[:15]
                        stats = subject.get('statistics', {})
                        participants = stats.get('participant_count', 0)
                        avg_score = stats.get('average_score', 0)
                        difficulty = stats.get('difficulty_coefficient', 0)
                        discrimination = stats.get('discrimination_coefficient', 0)
                        
                        print(f"{name:<15} | {participants:>8,} | {avg_score:>6.2f} | {difficulty:>8.3f} | {discrimination:>6.3f}")
                
                print("\n✅ G7-2025批次数据汇聚任务圆满完成!")
                return True
            else:
                print("⚠️  计算结果为空，可能仍在处理中")
                return False
                
        elif response.status_code == 404:
            print("⚠️  结果尚未生成，计算可能仍在进行中")
            return False
        else:
            print(f"❌ 获取结果失败: HTTP {response.status_code}")
            print(f"响应: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ 获取结果时发生错误: {e}")
        return False

def main():
    print("G7-2025批次数据汇聚任务启动器")
    print("处理15,200个学生、43所学校、11个学科的真实数据")
    
    # 1. 启动汇聚任务
    task_id = start_g7_aggregation()
    
    if not task_id:
        print("\n❌ 任务启动失败，程序退出")
        return False
    
    # 2. 监控任务进度
    completed = monitor_task_progress(task_id)
    
    if not completed:
        # 3. 尝试获取结果（如果监控期间未完成）
        print("\n尝试获取可能已完成的结果...")
        get_aggregation_results()
    
    print(f"\n任务ID: {task_id}")
    print("可以使用此ID查询任务状态和结果")
    
    return True

if __name__ == "__main__":
    success = main()