#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试多科目统计计算功能
验证G7-2025批次所有科目的统计数据生成
"""
import asyncio
import sys
import os
import json
import logging
from datetime import datetime

# 添加应用根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database.connection import get_db
from app.services.calculation_service import CalculationService

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


async def test_multi_subject_calculation():
    """测试多科目统计计算"""
    logger.info("=" * 60)
    logger.info("测试G7-2025批次多科目统计计算")
    logger.info("=" * 60)
    
    # 获取数据库会话
    db = next(get_db())
    
    try:
        calculation_service = CalculationService(db)
        batch_code = "G7-2025"
        
        logger.info(f"开始测试批次: {batch_code}")
        
        # 1. 查询可用科目
        logger.info("\n📚 查询批次可用科目...")
        subjects = await calculation_service._get_batch_subjects(batch_code)
        logger.info(f"找到 {len(subjects)} 个科目:")
        for i, subject in enumerate(subjects, 1):
            logger.info(f"  {i}. {subject['subject_name']} (满分: {subject['max_score']})")
        
        # 2. 查询学生数据概况
        logger.info(f"\n📊 查询学生数据概况...")
        student_data = await calculation_service._fetch_student_scores(batch_code)
        if not student_data.empty:
            total_records = len(student_data)
            unique_students = student_data['student_id'].nunique()
            unique_schools = student_data['school_code'].nunique()
            unique_subjects = student_data['subject_name'].nunique()
            
            logger.info(f"数据概况:")
            logger.info(f"  - 总记录数: {total_records:,}")
            logger.info(f"  - 学生数: {unique_students:,}")
            logger.info(f"  - 学校数: {unique_schools}")
            logger.info(f"  - 科目数: {unique_subjects}")
        else:
            logger.error("没有找到学生分数数据!")
            return False
        
        # 3. 执行多科目统计计算
        logger.info(f"\n🚀 执行多科目统计计算...")
        progress_tracker = ProgressTracker("多科目计算")
        
        start_time = datetime.now()
        try:
            # 直接调用多科目整合方法测试
            result = await calculation_service._consolidate_multi_subject_results(
                batch_code, student_data
            )
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"\n✅ 多科目计算完成! 总耗时: {duration:.2f}s")
            
            # 4. 验证结果
            logger.info(f"\n🔍 验证多科目统计结果...")
            
            if 'academic_subjects' in result:
                subjects_calculated = len(result['academic_subjects'])
                logger.info(f"✅ 成功计算了 {subjects_calculated} 个科目的统计数据")
                
                # 显示每个科目的基本信息
                for subject_name, subject_data in result['academic_subjects'].items():
                    stats = subject_data.get('school_stats', {})
                    student_count = stats.get('student_count', 0)
                    avg_score = stats.get('avg_score', 0)
                    score_rate = stats.get('score_rate', 0)
                    
                    # 百分位数
                    percentiles = subject_data.get('percentiles', {})
                    p10 = percentiles.get('P10', 0)
                    p50 = percentiles.get('P50', 0)
                    p90 = percentiles.get('P90', 0)
                    
                    logger.info(f"  📖 {subject_name}:")
                    logger.info(f"     - 学生数: {student_count}")
                    logger.info(f"     - 平均分: {avg_score:.2f}")
                    logger.info(f"     - 得分率: {score_rate:.1%}")
                    logger.info(f"     - P10/P50/P90: {p10:.1f}/{p50:.1f}/{p90:.1f}")
                
                # 保存详细结果到文件
                result_file = f"multi_subject_result_{batch_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(result_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2, default=str)
                logger.info(f"📄 详细结果已保存到: {result_file}")
                
                return True
            else:
                logger.error("❌ 结果中没有包含学术科目数据")
                return False
                
        except Exception as e:
            logger.error(f"❌ 多科目计算失败: {str(e)}", exc_info=True)
            return False
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {str(e)}", exc_info=True)
        return False
    
    finally:
        db.close()


async def main():
    """主测试函数"""
    logger.info("🎯 开始多科目统计计算测试")
    
    success = await test_multi_subject_calculation()
    
    if success:
        logger.info("\n🎉 多科目统计计算测试成功!")
        logger.info("💡 G7-2025批次现在支持所有科目的完整统计分析")
    else:
        logger.error("\n💥 多科目统计计算测试失败")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())