#!/usr/bin/env python3
"""
启动G4-2025批次的数据汇聚计算
"""

import asyncio
import requests
import json
from datetime import datetime

async def run_aggregation():
    """运行G4-2025的数据汇聚"""
    
    # API服务地址
    base_url = "http://localhost:8000"
    batch_code = "G4-2025"
    
    print(f"=== 启动批次{batch_code}的数据汇聚 ===")
    print(f"时间: {datetime.now()}")
    
    try:
        # 1. 首先重置批次状态为pending，准备重新汇聚
        print("\n步骤1: 重置批次状态...")
        reset_url = f"{base_url}/api/management/reset-batch-status/{batch_code}"
        
        # 构建重置请求（如果有这个接口的话）
        # 如果没有重置接口，我们直接调用汇聚接口
        
        # 2. 启动汇聚计算
        print("\n步骤2: 启动数据汇聚计算...")
        aggregation_url = f"{base_url}/api/management/aggregate-batch"
        
        # 构建请求数据
        request_data = {
            "batch_code": batch_code,
            "recalculate": True  # 强制重新计算
        }
        
        print(f"请求URL: {aggregation_url}")
        print(f"请求数据: {json.dumps(request_data, indent=2, ensure_ascii=False)}")
        
        # 发送汇聚请求
        response = requests.post(
            aggregation_url,
            json=request_data,
            headers={"Content-Type": "application/json"},
            timeout=300  # 5分钟超时
        )
        
        print(f"\n响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("汇聚计算启动成功！")
            print(f"响应数据: {json.dumps(result, indent=2, ensure_ascii=False)}")
            
            # 如果是异步处理，获取任务ID
            task_id = result.get('task_id')
            if task_id:
                print(f"任务ID: {task_id}")
                await monitor_task_progress(base_url, task_id)
            else:
                print("同步处理已完成")
                
        else:
            print(f"汇聚计算启动失败!")
            print(f"错误信息: {response.text}")
            
            # 尝试直接调用计算引擎
            print("\n尝试直接调用计算引擎...")
            await call_calculation_engine_directly(batch_code)
    
    except Exception as e:
        print(f"汇聚过程中出现错误: {e}")
        # 如果API调用失败，尝试直接调用计算引擎
        print("\nAPI调用失败，尝试直接调用计算引擎...")
        await call_calculation_engine_directly(batch_code)


async def monitor_task_progress(base_url, task_id):
    """监控任务进度"""
    print(f"\n监控任务进度: {task_id}")
    
    for i in range(30):  # 最多监控30次，每次间隔10秒
        try:
            status_url = f"{base_url}/api/management/task-status/{task_id}"
            response = requests.get(status_url, timeout=10)
            
            if response.status_code == 200:
                status_data = response.json()
                task_status = status_data.get('status', 'unknown')
                progress = status_data.get('progress', 0)
                
                print(f"任务状态: {task_status}, 进度: {progress}%")
                
                if task_status in ['completed', 'failed', 'error']:
                    print(f"任务已完成，最终状态: {task_status}")
                    if task_status == 'completed':
                        print("数据汇聚成功完成！")
                    else:
                        print(f"任务失败: {status_data.get('error', '未知错误')}")
                    break
            else:
                print(f"获取任务状态失败: {response.status_code}")
                
        except Exception as e:
            print(f"监控任务时出错: {e}")
            
        # 等待10秒后再次检查
        if i < 29:  # 不是最后一次
            await asyncio.sleep(10)
    
    print("任务监控结束")


async def call_calculation_engine_directly(batch_code):
    """直接调用计算引擎进行汇聚"""
    print(f"\n=== 直接调用计算服务 ===")
    
    try:
        # 导入必要的模块
        from app.database.connection import get_db_context
        from app.services.calculation_service import CalculationService
        
        print("初始化计算服务...")
        
        with get_db_context() as session:
            # 创建计算服务
            calculation_service = CalculationService(session)
            
            print(f"开始计算批次: {batch_code}")
            
            # 定义进度回调函数
            def progress_callback(progress, message):
                print(f"进度 {progress}%: {message}")
            
            # 调用批次统计计算
            result = await calculation_service.calculate_batch_statistics(
                batch_code=batch_code,
                progress_callback=progress_callback
            )
            
            print("\n计算结果:")
            print(f"  状态: {result.get('status', 'unknown')}")
            print(f"  处理时间: {result.get('execution_time', 0)} 秒")
            print(f"  区域级数据: {result.get('regional_results', {}).get('total_records', 0)} 条记录")
            print(f"  学校级数据: {result.get('school_results', {}).get('total_records', 0)} 条记录")
            
            if result.get('status') == 'completed':
                print("数据汇聚计算成功完成！")
            else:
                error_msg = result.get('error', '未知错误')
                print(f"数据汇聚计算失败: {error_msg}")
                
    except Exception as e:
        print(f"直接调用计算服务时出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(run_aggregation())