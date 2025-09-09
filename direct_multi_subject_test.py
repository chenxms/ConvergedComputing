#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直接测试多科目计算功能
绕过API直接调用计算服务
"""
import asyncio
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from app.services.calculation_service import CalculationService
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel

async def direct_multi_subject_test():
    print("=== 直接测试多科目计算功能 ===")
    print()
    
    db = next(get_db())
    
    try:
        calculation_service = CalculationService(db)
        repo = StatisticalAggregationRepository(db)
        batch_code = "G7-2025"
        
        print(f"开始直接调用多科目计算: {batch_code}")
        
        # 先删除现有的统计数据以确保重新计算
        print("1. 清理现有统计数据...")
        try:
            # 删除现有的区域和学校统计数据
            existing_regional = repo.get_by_batch_code_and_level(batch_code, AggregationLevel.REGIONAL)
            if existing_regional:
                repo.delete(existing_regional)
                print("  - 已删除现有区域级数据")
            
            existing_schools = repo.get_all_school_statistics(batch_code)
            for school_data in existing_schools:
                repo.delete(school_data)
            print(f"  - 已删除 {len(existing_schools)} 个学校级数据")
            
            db.commit()
        except Exception as e:
            print(f"清理数据时出错: {e}")
            db.rollback()
        
        print()
        print("2. 执行多科目区域级计算...")
        
        # 创建简单的进度回调
        def progress_callback(progress, message):
            print(f"  进度: {progress:.1f}% - {message}")
        
        # 直接调用增强的多科目计算
        result = await calculation_service.calculate_batch_statistics(
            batch_code=batch_code,
            progress_callback=progress_callback
        )
        
        print()
        print("3. 验证多科目计算结果...")
        
        # 检查区域级数据
        regional_data = repo.get_by_batch_code_and_level(batch_code, AggregationLevel.REGIONAL)
        if regional_data and regional_data.statistics_data:
            stats_data = regional_data.statistics_data
            if 'academic_subjects' in stats_data:
                subjects = stats_data['academic_subjects']
                print(f"SUCCESS: 区域级数据包含 {len(subjects)} 个科目")
                
                for subject_name, subject_data in subjects.items():
                    if 'school_stats' in subject_data:
                        stats = subject_data['school_stats']
                        avg_score = stats.get('avg_score', 0)
                        student_count = stats.get('student_count', 0)
                        score_rate = stats.get('score_rate', 0)
                        
                        # 检查百分位数
                        percentiles = subject_data.get('percentiles', {})
                        p10 = percentiles.get('P10', 0)
                        p50 = percentiles.get('P50', 0) 
                        p90 = percentiles.get('P90', 0)
                        
                        print(f"  {subject_name}:")
                        print(f"    平均分: {avg_score:.2f}")
                        print(f"    学生数: {student_count}")
                        print(f"    得分率: {score_rate:.1%}")
                        print(f"    P10/P50/P90: {p10:.1f}/{p50:.1f}/{p90:.1f}")
            else:
                print("ERROR: 区域级数据中没有academic_subjects")
        else:
            print("ERROR: 没有找到区域级数据")
        
        # 检查学校级数据
        school_data_list = repo.get_all_school_statistics(batch_code)
        print(f"  学校级数据: {len(school_data_list)} 个学校")
        
        if len(school_data_list) > 0:
            first_school = school_data_list[0]
            if first_school.statistics_data and 'academic_subjects' in first_school.statistics_data:
                school_subjects = first_school.statistics_data['academic_subjects']
                print(f"  示例学校 {first_school.school_id}: {len(school_subjects)} 个科目")
        
        print()
        print("=== 多科目计算测试完成 ===")
        
        # 统计成功情况
        if regional_data and 'academic_subjects' in regional_data.statistics_data:
            subject_count = len(regional_data.statistics_data['academic_subjects'])
            school_count = len(school_data_list)
            
            if subject_count >= 10:  # 应该有10个科目
                print(f"SUCCESS: 多科目计算成功!")
                print(f"  区域级: {subject_count} 个科目 (包含P10/P50/P90)")
                print(f"  学校级: {school_count} 个学校")
                return True
            else:
                print(f"PARTIAL: 只计算了 {subject_count} 个科目，期望10个")
                return False
        else:
            print("FAILED: 多科目计算失败")
            return False
            
    except Exception as e:
        print(f"ERROR: 直接测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

async def main():
    success = await direct_multi_subject_test()
    if success:
        print("\n多科目统计计算系统已准备就绪!")
    else:
        print("\n多科目统计计算需要进一步调试")

if __name__ == "__main__":
    asyncio.run(main())