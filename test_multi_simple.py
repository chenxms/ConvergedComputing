#!/usr/bin/env python3
"""
简化的多层级汇聚器测试
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from multi_layer_aggregator import MultiLayerAggregator

def simple_test():
    """简单测试"""
    print("=== 简化测试多层级汇聚器 ===\n")
    
    aggregator = MultiLayerAggregator()
    
    # 1. 测试批次概览 - 最简单的查询
    print("1. 批次概览:")
    try:
        overview = aggregator.get_batch_overview('G4-2025')
        
        if 'error' not in overview:
            overview_data = overview['overview']
            print(f"   [OK] 总学生数: {overview_data['total_students']}")
            print(f"   [OK] 总学校数: {overview_data['total_schools']}")  
            print(f"   [OK] 考试学科: {overview_data['exam_subjects']}个")
            print(f"   [OK] 问卷学科: {overview_data['questionnaire_subjects']}个")
        else:
            print(f"   [ERROR] 失败: {overview.get('error', '未知错误')}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    # 2. 测试单独的考试汇聚器
    print("\n2. 考试汇聚器单独测试:")
    try:
        exam_result = aggregator.exam_aggregator.aggregate_subject_level('G4-2025')
        
        if 'error' not in exam_result and 'subject_analysis' in exam_result:
            exam_count = len(exam_result['subject_analysis'])
            print(f"   [OK] 成功汇聚 {exam_count} 个考试学科")
        else:
            print(f"   [ERROR] 失败: {exam_result.get('error', '未知错误')}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    # 3. 测试单独的问卷汇聚器
    print("\n3. 问卷汇聚器单独测试:")
    try:
        questionnaire_result = aggregator.questionnaire_aggregator.aggregate_subject_level('G4-2025')
        
        if 'error' not in questionnaire_result and 'subject_analysis' in questionnaire_result:
            questionnaire_count = len(questionnaire_result['subject_analysis'])
            print(f"   [OK] 成功汇聚 {questionnaire_count} 个问卷学科")
        else:
            print(f"   [ERROR] 失败: {questionnaire_result.get('error', '未知错误')}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    print("\n=== 简化测试完成 ===")

if __name__ == "__main__":
    simple_test()