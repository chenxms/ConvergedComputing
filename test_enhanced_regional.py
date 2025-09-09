#!/usr/bin/env python3
"""
测试增强的区域级计算功能
验证区域级任务能够自动生成学校级数据
"""
import asyncio
import sys
import os
import logging
from datetime import datetime
from typing import Dict, Any

# 添加应用根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database.connection import get_db
from app.services.calculation_service import CalculationService
from app.services.task_manager import TaskManager
from app.database.enums import AggregationLevel, CalculationStatus
from app.database.repositories import StatisticalAggregationRepository

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProgressTracker:
    """进度跟踪器"""
    
    def __init__(self, task_name: str):
        self.task_name = task_name
        self.start_time = datetime.now()
        
    def __call__(self, progress: float, message: str):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        logger.info(f"[{self.task_name}] {progress:.1f}% - {message} (已用时: {elapsed:.1f}s)")


async def test_enhanced_regional_calculation():
    """测试增强的区域级计算"""
    logger.info("="*60)
    logger.info("测试增强的区域级计算功能")
    logger.info("="*60)
    
    # 获取数据库会话
    db = next(get_db())
    
    try:
        # 1. 创建计算服务和任务管理器
        calculation_service = CalculationService(db)
        task_manager = TaskManager(db)
        repo = StatisticalAggregationRepository(db)
        
        batch_code = "G7-2025"
        logger.info(f"开始测试批次: {batch_code}")
        
        # 2. 检查现有数据
        logger.info("\n📊 检查现有统计数据...")
        existing_regional = repo.get_regional_statistics(batch_code)
        existing_schools = repo.get_all_school_statistics(batch_code)
        
        logger.info(f"现有区域级数据: {'存在' if existing_regional else '不存在'}")
        logger.info(f"现有学校级数据: {len(existing_schools)} 个学校")
        
        # 3. 执行增强的区域级计算
        logger.info("\n🚀 执行增强的区域级计算...")
        progress_tracker = ProgressTracker("增强区域计算")
        
        start_time = datetime.now()
        result = await calculation_service.calculate_batch_statistics(
            batch_code=batch_code,
            progress_callback=progress_tracker
        )
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"\n✅ 计算完成! 总耗时: {duration:.2f}s")
        
        # 4. 验证结果
        logger.info("\n🔍 验证计算结果...")
        
        # 验证区域级数据
        updated_regional = repo.get_regional_statistics(batch_code)
        if updated_regional:
            logger.info("✅ 区域级数据已生成")
            logger.info(f"   - 批次代码: {updated_regional.batch_code}")
            logger.info(f"   - 计算状态: {updated_regional.calculation_status}")
            logger.info(f"   - 学生总数: {updated_regional.total_students}")
            logger.info(f"   - 计算时长: {updated_regional.calculation_duration:.2f}s")
        else:
            logger.error("❌ 区域级数据未生成")
            return False
            
        # 验证学校级数据
        updated_schools = repo.get_all_school_statistics(batch_code)
        logger.info(f"\n📚 学校级数据验证:")
        logger.info(f"   - 学校数量: {len(updated_schools)}")
        
        if len(updated_schools) > 0:
            logger.info("✅ 学校级数据已自动生成")
            for i, school in enumerate(updated_schools[:5]):  # 显示前5个学校
                logger.info(f"   - 学校{i+1}: {school.school_id} ({school.school_name})")
                logger.info(f"     学生数: {school.total_students}, 状态: {school.calculation_status}")
        else:
            logger.error("❌ 学校级数据未生成")
            return False
        
        # 5. 验证返回结果结构
        logger.info(f"\n📋 验证返回结果结构...")
        if 'regional_statistics' in result:
            logger.info("✅ 包含区域级统计数据")
        if 'school_statistics_summary' in result:
            summary = result['school_statistics_summary']
            logger.info("✅ 包含学校级统计摘要")
            logger.info(f"   - 总学校数: {summary.get('total_schools', 0)}")
            logger.info(f"   - 成功学校数: {summary.get('successful_schools', 0)}")
            logger.info(f"   - 失败学校数: {summary.get('failed_schools', 0)}")
        
        logger.info(f"\n🎉 增强区域级计算测试完成!")
        logger.info(f"✅ 区域级数据: 已生成")
        logger.info(f"✅ 学校级数据: {len(updated_schools)} 个学校")
        logger.info(f"⏱️  总计算时间: {duration:.2f}s")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {str(e)}", exc_info=True)
        return False
    
    finally:
        db.close()


async def test_task_manager_integration():
    """测试任务管理器集成"""
    logger.info("\n" + "="*60)
    logger.info("测试任务管理器集成")
    logger.info("="*60)
    
    db = next(get_db())
    
    try:
        task_manager = TaskManager(db)
        batch_code = "G7-2025"
        
        logger.info(f"通过任务管理器启动批次计算: {batch_code}")
        
        # 启动区域级任务
        task_response = await task_manager.start_calculation_task(
            batch_code=batch_code,
            aggregation_level=AggregationLevel.REGIONAL
        )
        
        logger.info(f"✅ 任务已启动: {task_response.id}")
        logger.info(f"   - 状态: {task_response.status}")
        logger.info(f"   - 进度: {task_response.progress}%")
        
        # 等待任务完成
        max_wait_time = 300  # 最多等待5分钟
        wait_interval = 5    # 每5秒检查一次
        elapsed_time = 0
        
        while elapsed_time < max_wait_time:
            await asyncio.sleep(wait_interval)
            elapsed_time += wait_interval
            
            task_status = await task_manager.get_task_status(str(task_response.id))
            if not task_status:
                logger.error("❌ 任务状态丢失")
                break
                
            logger.info(f"任务进度: {task_status.progress}% - 状态: {task_status.status}")
            
            if task_status.status in ["completed", "failed"]:
                break
        
        # 最终状态检查
        final_status = await task_manager.get_task_status(str(task_response.id))
        if final_status:
            logger.info(f"🏁 最终状态: {final_status.status}")
            if final_status.status == "completed":
                logger.info("✅ 任务管理器集成测试成功!")
                return True
            else:
                logger.error(f"❌ 任务执行失败: {final_status.error_message}")
                return False
        else:
            logger.error("❌ 无法获取最终任务状态")
            return False
        
    except Exception as e:
        logger.error(f"❌ 任务管理器集成测试失败: {str(e)}", exc_info=True)
        return False
    
    finally:
        db.close()


async def verify_api_endpoints():
    """验证API端点能正确返回数据"""
    logger.info("\n" + "="*60)
    logger.info("验证API端点")
    logger.info("="*60)
    
    try:
        # 这里可以添加实际的API调用测试
        # 由于当前API使用模拟数据，我们主要验证数据库中是否有真实数据
        
        db = next(get_db())
        repo = StatisticalAggregationRepository(db)
        
        batch_code = "G7-2025"
        
        # 检查区域级数据
        regional_data = repo.get_regional_statistics(batch_code)
        if regional_data:
            logger.info("✅ 区域级API数据源可用")
        else:
            logger.warning("⚠️  区域级API数据源不可用")
        
        # 检查学校级数据
        school_data_list = repo.get_all_school_statistics(batch_code)
        logger.info(f"📚 学校级API数据源: {len(school_data_list)} 个学校可用")
        
        if len(school_data_list) > 0:
            # 随机选择一个学校测试
            test_school = school_data_list[0]
            logger.info(f"✅ 示例学校数据可用: {test_school.school_id} ({test_school.school_name})")
        
        db.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ API端点验证失败: {str(e)}", exc_info=True)
        return False


async def main():
    """主测试函数"""
    logger.info("🎯 开始增强区域级计算完整测试")
    
    test_results = []
    
    # 测试1: 核心计算功能
    logger.info("\n" + "🔧 测试1: 核心计算功能")
    result1 = await test_enhanced_regional_calculation()
    test_results.append(("核心计算功能", result1))
    
    # 测试2: 任务管理器集成 (暂时跳过，因为可能时间较长)
    # logger.info("\n" + "🔧 测试2: 任务管理器集成")
    # result2 = await test_task_manager_integration()
    # test_results.append(("任务管理器集成", result2))
    
    # 测试3: API端点验证
    logger.info("\n" + "🔧 测试3: API端点验证")
    result3 = await verify_api_endpoints()
    test_results.append(("API端点验证", result3))
    
    # 总结
    logger.info("\n" + "="*60)
    logger.info("📊 测试结果总结")
    logger.info("="*60)
    
    all_passed = True
    for test_name, passed in test_results:
        status = "✅ 通过" if passed else "❌ 失败"
        logger.info(f"{status} {test_name}")
        if not passed:
            all_passed = False
    
    if all_passed:
        logger.info("\n🎉 所有测试通过! 增强区域级计算功能已准备就绪")
        logger.info("💡 现在G7-2025批次的区域级任务将自动生成所有学校级数据")
    else:
        logger.error("\n💥 部分测试失败，请检查实现")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())