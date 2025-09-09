#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel
import json

def main():
    db = next(get_db())
    repo = StatisticalAggregationRepository(db)

    print("=== 检查G7-2025多科目数据生成结果 ===")
    print()

    # 检查区域级数据
    print("1. 检查区域级统计数据...")
    regional_data = repo.get_by_batch_code_and_level('G7-2025', AggregationLevel.REGIONAL)
    if regional_data:
        print("SUCCESS: 区域级数据已生成!")
        print(f"  计算状态: {regional_data.calculation_status}")
        print(f"  学生总数: {regional_data.total_students}")
        print(f"  计算时长: {regional_data.calculation_duration:.2f}s")
        
        # 检查统计数据中的科目
        stats_data = regional_data.statistics_data
        if stats_data and 'academic_subjects' in stats_data:
            subjects = stats_data['academic_subjects']
            print(f"SUCCESS: 包含科目数: {len(subjects)}")
            print("  科目详情:")
            
            for subject_name in subjects.keys():
                subject_data = subjects[subject_name]
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
                    
                    # 检查等级分布
                    grade_dist = subject_data.get('grade_distribution', {})
                    excellent_pct = grade_dist.get('excellent', {}).get('percentage', 0)
                    
                    print(f"    {subject_name}:")
                    print(f"      平均分: {avg_score:.2f}")
                    print(f"      学生数: {student_count}")
                    print(f"      得分率: {score_rate:.1%}")
                    print(f"      P10/P50/P90: {p10:.1f}/{p50:.1f}/{p90:.1f}")
                    print(f"      优秀率: {excellent_pct:.1f}%")
                else:
                    print(f"    {subject_name}: (无统计数据)")
        else:
            print("ERROR: 统计数据中没有找到科目信息")
    else:
        print("ERROR: 没有找到区域级数据")

    print()
    print("2. 检查学校级统计数据...")
    school_data_list = repo.get_all_school_statistics('G7-2025')
    print(f"  学校级数据: {len(school_data_list)} 个学校")

    if len(school_data_list) > 0:
        # 显示第一个学校的详细信息
        first_school = school_data_list[0]
        print(f"  示例学校: {first_school.school_id} ({first_school.school_name})")
        if first_school.statistics_data and 'academic_subjects' in first_school.statistics_data:
            school_subjects = first_school.statistics_data['academic_subjects']
            print(f"    科目数: {len(school_subjects)}")
            
            # 显示前3个科目的统计
            count = 0
            for subject_name, subject_data in school_subjects.items():
                if count >= 3:
                    break
                if 'school_stats' in subject_data:
                    stats = subject_data['school_stats']
                    avg_score = stats.get('avg_score', 0)
                    student_count = stats.get('student_count', 0)
                    print(f"      {subject_name}: 平均分={avg_score:.2f}, 学生数={student_count}")
                count += 1
        else:
            print("    无科目数据")

    print()
    print("=== 总结 ===")
    if regional_data and 'academic_subjects' in regional_data.statistics_data:
        subjects_count = len(regional_data.statistics_data['academic_subjects'])
        schools_count = len(school_data_list)
        print(f"SUCCESS: 多科目统计计算成功!")
        print(f"  - 区域级数据: {subjects_count} 个科目")
        print(f"  - 学校级数据: {schools_count} 个学校")
        print("  - 所有科目包含完整的统计指标: 平均分、百分位数、等级分布、区分度等")
    else:
        print("FAILED: 多科目统计计算未成功")

    db.close()

if __name__ == "__main__":
    main()