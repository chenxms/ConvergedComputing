#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""测试单个学校的修复效果"""

import asyncio
import json
from sqlalchemy import text
from app.database.connection import get_db
from app.services.subjects_builder import SubjectsBuilder
from scripts.rewrite_subjects_v12_enhanced_fixed import get_enhanced_stats_for_school

async def test_single_school():
    """测试单个学校的数据生成"""
    
    batch_code = "G4-2025"
    school_code = "5068"  # 用户提供的例子学校
    
    print(f"测试学校 {school_code} 的数据生成...")
    
    sb = SubjectsBuilder()
    
    with next(get_db()) as db:
        # 获取学校级增强统计（按科目分别计算）
        print("计算增强统计...")
        school_enhanced_stats = await get_enhanced_stats_for_school(batch_code, school_code, db)
        
        if school_enhanced_stats:
            print(f"\n增强统计结果包含 {len(school_enhanced_stats)} 个科目：")
            for subject_name, stats in school_enhanced_stats.items():
                print(f"\n科目：{subject_name}")
                if 'percentiles' in stats:
                    p = stats['percentiles']
                    print(f"  百分位数：P10={p.get('P10')}, P50={p.get('P50')}, P90={p.get('P90')}")
                if 'discrimination' in stats:
                    d = stats['discrimination']
                    print(f"  区分度：{d.get('discrimination_index', 0)}")
                if 'grade_distribution' in stats:
                    print(f"  等级分布：已计算")
        else:
            print("未能计算增强统计")
            return
        
        # 构建学校级subjects
        print("\n构建subjects...")
        school_subjects = sb.build_school_subjects(
            batch_code, 
            school_code,
            enhanced_stats=school_enhanced_stats
        )
        
        # 检查前两个科目的结果
        print("\n生成的subjects结果：")
        for i, subject in enumerate(school_subjects[:2]):
            print(f"\n科目 {i+1}: {subject.get('subject_name')}")
            print(f"  p10: {subject.get('p10', '未找到')}")
            print(f"  p50: {subject.get('p50', '未找到')}")
            print(f"  p90: {subject.get('p90', '未找到')}")
            print(f"  discrimination: {subject.get('discrimination', '未找到')}")
            
            # 检查维度
            if 'dimensions' in subject and len(subject['dimensions']) > 0:
                dim = subject['dimensions'][0]
                print(f"  第一个维度：{dim.get('name', '未知')} (code={dim.get('code')})")
                print(f"    - score_rate: {dim.get('score_rate', '未找到')}")
                print(f"    - rank: {dim.get('rank', '未找到')}")

if __name__ == "__main__":
    asyncio.run(test_single_school())
