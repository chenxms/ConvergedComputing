#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""调试区域级增强统计数据结构"""

import json
import asyncio
from sqlalchemy import text
from app.database.connection import get_db
from app.services.calculation_service import CalculationService

async def debug_regional_stats():
    """调试区域级统计数据结构"""
    
    batch_code = "G4-2025"
    
    with next(get_db()) as db:
        calc_service = CalculationService(db)
        
        # 获取学生分数数据
        print("获取学生分数数据...")
        scores_df = await calc_service._fetch_student_scores(batch_code)
        print(f"数据量: {len(scores_df)}")
        
        # 计算区域级统计
        print("\n计算区域级增强统计...")
        regional_stats = await calc_service._consolidate_multi_subject_results(
            batch_code=batch_code,
            scores_df=scores_df,
            validation_result={'is_valid': True, 'warnings': []}
        )
        
        # 打印数据结构
        print("\n区域级增强统计数据结构:")
        print(json.dumps({
            "keys": list(regional_stats.keys()),
            "academic_subjects": list(regional_stats.get('academic_subjects', {}).keys()),
            "non_academic_subjects": list(regional_stats.get('non_academic_subjects', {}).keys())
        }, indent=2, ensure_ascii=False))
        
        # 打印一个科目的详细结构
        if 'academic_subjects' in regional_stats:
            for subject_name, subject_data in regional_stats['academic_subjects'].items():
                print(f"\n科目 '{subject_name}' 的数据结构:")
                
                # 打印顶层键
                print(f"  顶层键: {list(subject_data.keys())}")
                
                # 打印关键字段
                if 'percentiles' in subject_data:
                    print(f"  百分位数: {subject_data['percentiles']}")
                
                if 'discrimination' in subject_data:
                    print(f"  区分度: {subject_data['discrimination']}")
                
                if 'grade_distribution' in subject_data:
                    grade_dist = subject_data['grade_distribution']
                    if isinstance(grade_dist, dict):
                        print(f"  等级分布键: {list(grade_dist.keys())[:5]}...")  # 只显示前5个键
                
                break  # 只看一个科目

if __name__ == "__main__":
    asyncio.run(debug_regional_stats())