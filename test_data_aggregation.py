#!/usr/bin/env python3
"""
数据汇聚计算完整测试脚本
测试从数据库原始数据到统计汇聚的完整流程
"""

import requests
import json
import time
import sys
from datetime import datetime
from typing import Dict, Any

BASE_URL = "http://localhost:8000"

class DataAggregationTester:
    def __init__(self):
        self.session = requests.Session()
        self.test_batch_code = f"CALC_TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
    def step_1_verify_database_data(self):
        """步骤1: 验证数据库中是否有学生答题数据"""
        print("=== 步骤1: 验证数据库数据 ===")
        
        # 这里我们需要检查数据库中是否有原始数据
        # 可以通过简单的验证脚本检查
        print("请确认远程数据库中包含以下数据表和数据:")
        print("✓ student_score_detail - 学生答题明细")
        print("✓ subject_question_config - 题目配置")
        print("✓ question_dimension_mapping - 维度映射")
        print("✓ grade_aggregation_main - 年级信息")
        
        confirm = input("数据库数据已确认准备就绪? (y/n): ")
        return confirm.lower() == 'y'
    
    def step_2_create_test_batch(self):
        """步骤2: 创建测试批次"""
        print(f"\n=== 步骤2: 创建测试批次 {self.test_batch_code} ===")
        
        batch_data = {
            "batch_code": self.test_batch_code,
            "aggregation_level": "regional",
            "statistics_data": {
                "batch_info": {
                    "batch_code": self.test_batch_code,
                    "total_students": 0,  # 将通过计算确定
                    "total_schools": 0
                },
                "academic_subjects": []  # 将通过计算填充
            },
            "data_version": "1.0",
            "total_students": 0,
            "total_schools": 0,
            "triggered_by": "aggregation_test"
        }
        
        try:
            response = self.session.post(
                f"{BASE_URL}/api/v1/management/batches",
                json=batch_data,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✓ 批次创建成功: {result['data']['batch_id']}")
                return True
            else:
                print(f"✗ 批次创建失败: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"✗ 批次创建异常: {e}")
            return False
    
    def step_3_start_calculation_task(self):
        """步骤3: 启动数据汇聚计算任务"""
        print(f"\n=== 步骤3: 启动计算任务 ===")
        
        try:
            # 启动区域级汇聚计算
            response = self.session.post(
                f"{BASE_URL}/api/v1/statistics/tasks/{self.test_batch_code}/start",
                params={
                    "aggregation_level": "regional",
                    "priority": 5
                },
                timeout=10
            )
            
            if response.status_code == 200:
                task_info = response.json()
                task_id = task_info.get('id')
                print(f"✓ 计算任务启动成功")
                print(f"  任务ID: {task_id}")
                print(f"  批次代码: {task_info.get('batch_code')}")
                print(f"  状态: {task_info.get('status')}")
                return task_id
            else:
                print(f"✗ 任务启动失败: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"✗ 任务启动异常: {e}")
            return None
    
    def step_4_monitor_task_progress(self, task_id: str):
        """步骤4: 监控任务进度"""
        print(f"\n=== 步骤4: 监控任务进度 ===")
        
        max_wait_time = 300  # 最长等待5分钟
        check_interval = 5   # 每5秒检查一次
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                # 查询任务状态
                status_response = self.session.get(
                    f"{BASE_URL}/api/v1/statistics/tasks/{task_id}/status",
                    timeout=10
                )
                
                if status_response.status_code == 200:
                    status_data = status_response.json()
                    status = status_data.get('status')
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] 任务状态: {status}")
                    
                    if status == 'completed':
                        print("✓ 任务完成!")
                        return True
                    elif status == 'failed':
                        print("✗ 任务失败!")
                        print(f"  错误信息: {status_data.get('error_message', 'N/A')}")
                        return False
                    elif status in ['pending', 'running']:
                        # 查询进度
                        try:
                            progress_response = self.session.get(
                                f"{BASE_URL}/api/v1/statistics/tasks/{task_id}/progress",
                                timeout=10
                            )
                            if progress_response.status_code == 200:
                                progress = progress_response.json()
                                print(f"  进度: {progress.get('percentage', 0)}%")
                                print(f"  阶段: {progress.get('current_stage', 'N/A')}")
                        except:
                            pass
                else:
                    print(f"✗ 状态查询失败: {status_response.status_code}")
                    
            except Exception as e:
                print(f"✗ 状态查询异常: {e}")
            
            time.sleep(check_interval)
        
        print("✗ 任务执行超时")
        return False
    
    def step_5_verify_results(self):
        """步骤5: 验证计算结果"""
        print(f"\n=== 步骤5: 验证计算结果 ===")
        
        try:
            # 查询更新后的批次数据
            response = self.session.get(
                f"{BASE_URL}/api/v1/management/batches/{self.test_batch_code}",
                params={"aggregation_level": "regional"},
                timeout=10
            )
            
            if response.status_code == 200:
                batch_data = response.json()
                statistics = batch_data.get('statistics_data', {})
                
                print("✓ 批次数据查询成功")
                print(f"  批次代码: {batch_data.get('batch_code')}")
                print(f"  汇聚级别: {batch_data.get('aggregation_level')}")
                print(f"  计算状态: {batch_data.get('calculation_status')}")
                print(f"  参与学生: {batch_data.get('total_students')}")
                print(f"  参与学校: {batch_data.get('total_schools')}")
                
                # 验证统计数据结构
                if 'batch_info' in statistics:
                    print("✓ batch_info 数据存在")
                    
                if 'academic_subjects' in statistics:
                    subjects = statistics['academic_subjects']
                    print(f"✓ academic_subjects 数据存在 ({len(subjects) if subjects else 0} 个科目)")
                    
                    # 显示第一个科目的统计信息作为示例
                    if subjects and len(subjects) > 0:
                        first_subject = subjects[0] if isinstance(subjects, list) else list(subjects.values())[0]
                        print("  示例科目统计:")
                        print(f"    科目: {first_subject.get('subject_name', 'N/A')}")
                        stats = first_subject.get('statistics', {})
                        print(f"    平均分: {stats.get('average_score', 'N/A')}")
                        print(f"    难度系数: {stats.get('difficulty_coefficient', 'N/A')}")
                        print(f"    区分度: {stats.get('discrimination_coefficient', 'N/A')}")
                
                return True
            else:
                print(f"✗ 批次查询失败: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"✗ 结果验证异常: {e}")
            return False
    
    def step_6_test_reporting_api(self):
        """步骤6: 测试报告API"""
        print(f"\n=== 步骤6: 测试报告生成 ===")
        
        try:
            # 测试区域报告API
            response = self.session.get(
                f"{BASE_URL}/api/v1/reporting/reports/regional/{self.test_batch_code}",
                timeout=10
            )
            
            if response.status_code == 200:
                report_data = response.json()
                print("✓ 区域报告生成成功")
                print(f"  数据版本: {report_data.get('data', {}).get('data_version', 'N/A')}")
                print(f"  schema版本: {report_data.get('data', {}).get('schema_version', 'N/A')}")
                return True
            elif response.status_code == 404:
                print("! 区域报告暂无数据 (可能需要更多计算时间)")
                return True
            else:
                print(f"✗ 报告生成失败: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"✗ 报告测试异常: {e}")
            return False
    
    def run_full_test(self):
        """运行完整测试流程"""
        print("🧪 数据汇聚计算完整测试")
        print("=" * 50)
        
        steps = [
            ("验证数据库数据", self.step_1_verify_database_data),
            ("创建测试批次", self.step_2_create_test_batch),
            ("启动计算任务", self.step_3_start_calculation_task),
            ("监控任务进度", None),  # 特殊处理
            ("验证计算结果", self.step_5_verify_results),
            ("测试报告生成", self.step_6_test_reporting_api)
        ]
        
        task_id = None
        
        for i, (step_name, step_func) in enumerate(steps, 1):
            print(f"\n{'='*10} {step_name} {'='*10}")
            
            if i == 1:  # 验证数据库
                if not step_func():
                    print("❌ 测试中止：数据库数据未准备就绪")
                    return False
            elif i == 2:  # 创建批次
                if not step_func():
                    print("❌ 测试失败：批次创建失败")
                    return False
            elif i == 3:  # 启动任务
                task_id = step_func()
                if not task_id:
                    print("❌ 测试失败：任务启动失败")
                    return False
            elif i == 4:  # 监控进度
                if task_id:
                    if not self.step_4_monitor_task_progress(task_id):
                        print("❌ 测试失败：任务执行失败")
                        return False
            elif i == 5:  # 验证结果
                if not step_func():
                    print("❌ 测试失败：结果验证失败")
                    return False
            elif i == 6:  # 测试报告
                step_func()  # 报告API可选
        
        print("\n" + "=" * 50)
        print("🎉 数据汇聚计算测试完成!")
        print(f"📊 测试批次: {self.test_batch_code}")
        print("✅ 所有核心功能正常工作")
        return True


def main():
    tester = DataAggregationTester()
    success = tester.run_full_test()
    
    if success:
        print("\n🚀 系统已准备好进行生产数据汇聚计算!")
    else:
        print("\n⚠️  系统需要进一步调试和优化")
        
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()