#!/usr/bin/env python3
"""
快速测试汇聚引擎 - 仅处理单个学校
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from final_aggregation_engine import FinalAggregationEngine
import logging

def main():
    """快速测试 - 仅计算区域级和一个学校"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    engine = FinalAggregationEngine()
    batch_code = "G4-2025"
    
    print("=== 快速汇聚测试 ===")
    print(f"批次: {batch_code}")
    
    # 1. 测试区域级汇聚
    print("\n1. 区域级汇聚测试...")
    regional_result = engine.aggregate_regional_level(batch_code)
    
    if 'error' in regional_result:
        print(f"区域级汇聚失败: {regional_result['error']}")
        return
    
    print(f"[成功] 区域级汇聚成功")
    print(f"  - 总学生数: {regional_result.get('total_students', 0)}")
    print(f"  - 总学校数: {regional_result.get('total_schools', 0)}")
    print(f"  - 考试科目数: {len(regional_result.get('academic_subjects', {}))}")
    print(f"  - 问卷科目数: {len(regional_result.get('questionnaire_subjects', {}))}")
    
    # 显示考试科目统计
    for subject_name, subject_data in regional_result.get('academic_subjects', {}).items():
        stats = subject_data.get('subject_stats', {})
        school_rankings = subject_data.get('school_rankings', [])
        print(f"  - {subject_name}: 平均分={stats.get('avg', 0):.2f}, 难度系数={stats.get('difficulty', 0):.2f}, 区分度={stats.get('discrimination', 0):.2f}")
        ranking_text = ', '.join([f"{s['school_name']}({s['avg_score']:.1f})" for s in school_rankings[:3]])
        print(f"    学校排名前3: {ranking_text}")
    
    # 保存区域级结果
    if engine.save_aggregation_result(regional_result):
        print("[成功] 区域级结果保存成功")
    else:
        print("[失败] 区域级结果保存失败")
    
    # 2. 测试一个学校的汇聚
    print("\n2. 学校级汇聚测试...")
    df = engine.get_batch_data(batch_code)
    if not df.empty:
        # 选择第一个学校进行测试
        first_school_id = df['school_id'].iloc[0]
        first_school_name = df['school_name'].iloc[0]
        
        print(f"测试学校: {first_school_name} (ID: {first_school_id})")
        
        school_result = engine.aggregate_school_level(batch_code, first_school_id)
        
        if 'error' in school_result:
            print(f"学校级汇聚失败: {school_result['error']}")
        else:
            print(f"[成功] 学校级汇聚成功")
            print(f"  - 学校学生数: {school_result.get('total_students', 0)}")
            print(f"  - 考试科目数: {len(school_result.get('academic_subjects', {}))}")
            
            # 显示学校科目统计和排名
            for subject_name, subject_data in school_result.get('academic_subjects', {}).items():
                stats = subject_data.get('school_stats', {})
                regional_comp = subject_data.get('regional_comparison', {})
                print(f"  - {subject_name}: 平均分={stats.get('avg', 0):.2f}")
                if regional_comp:
                    print(f"    区域排名: {regional_comp.get('region_rank', 'N/A')}/{regional_comp.get('total_schools', 'N/A')}")
            
            # 保存学校级结果
            if engine.save_aggregation_result(school_result):
                print("[成功] 学校级结果保存成功")
            else:
                print("[失败] 学校级结果保存失败")
    
    print("\n=== 测试完成 ===")
    print("汇聚引擎功能验证完毕!")

if __name__ == "__main__":
    main()