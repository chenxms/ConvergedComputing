#!/usr/bin/env python3
"""
API核心功能测试（不依赖Flask）
直接测试汇聚计算功能
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from multi_layer_aggregator import MultiLayerAggregator
import json

def test_api_core_functionality():
    """测试API核心汇聚功能"""
    print("=== 测试API核心汇聚功能 ===\n")
    
    aggregator = MultiLayerAggregator()
    batch_code = 'G4-2025'
    
    # 1. 批次概览功能
    print("1. 批次概览功能:")
    try:
        result = aggregator.get_batch_overview(batch_code)
        
        if 'error' not in result:
            overview = result['overview']
            print(f"   [SUCCESS] 总学生数: {overview['total_students']}")
            print(f"   [SUCCESS] 总学校数: {overview['total_schools']}")
            print(f"   [SUCCESS] 考试学科: {overview['exam_subjects']}个")
            print(f"   [SUCCESS] 问卷学科: {overview['questionnaire_subjects']}个")
        else:
            print(f"   [FAIL] {result['error']}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    # 2. 多学科汇聚功能
    print("\n2. 多学科汇聚功能:")
    try:
        result = aggregator.aggregate_all_subjects(batch_code)
        
        if 'error' not in result:
            analysis = result['analysis']
            exam_count = len(analysis['exam_subjects'])
            questionnaire_count = len(analysis['questionnaire_subjects'])
            summary = result.get('summary', {})
            
            print(f"   [SUCCESS] 考试学科: {exam_count}个")
            print(f"   [SUCCESS] 问卷学科: {questionnaire_count}个")
            print(f"   [SUCCESS] 考试平均得分率: {summary.get('avg_exam_score_rate', 0):.2%}")
            print(f"   [SUCCESS] 问卷平均得分率: {summary.get('avg_questionnaire_score_rate', 0):.2%}")
        else:
            print(f"   [FAIL] {result['error']}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    # 3. 维度汇聚功能
    print("\n3. 维度汇聚功能:")
    try:
        result = aggregator.aggregate_all_dimensions(batch_code)
        
        if 'error' not in result:
            dimension_analysis = result['dimension_analysis']
            exam_dims = dimension_analysis.get('exam_dimensions', {})
            questionnaire_dims = dimension_analysis.get('questionnaire_dimensions', {})
            options_dist = dimension_analysis.get('questionnaire_options', {})
            
            total_exam_dims = sum(len(dims) for dims in exam_dims.values())
            total_quest_dims = sum(len(dims) for dims in questionnaire_dims.values())
            
            print(f"   [SUCCESS] 考试维度总数: {total_exam_dims}")
            print(f"   [SUCCESS] 问卷维度总数: {total_quest_dims}")
            print(f"   [SUCCESS] 选项分布维度: {len(options_dist)}")
        else:
            print(f"   [FAIL] {result['error']}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    # 4. 学校排名功能
    print("\n4. 学校排名功能:")
    try:
        result = aggregator.get_school_ranking(batch_code)
        
        if 'error' not in result:
            rankings = result.get('rankings', [])
            total_schools = result.get('total_schools', 0)
            
            print(f"   [SUCCESS] 参与排名学校: {total_schools}所")
            
            if rankings:
                # 显示前3名
                print("   [INFO] 前3名学校:")
                for school in rankings[:3]:
                    performance = school['performance']
                    print(f"      第{school['rank']}名: {school['school_name']} "
                         f"(得分率{performance['overall_score_rate']:.2%})")
        else:
            print(f"   [FAIL] {result['error']}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    # 5. 问卷选项分布功能
    print("\n5. 问卷选项分布功能:")
    try:
        result = aggregator.questionnaire_aggregator.get_option_distribution(batch_code)
        
        if 'error' not in result:
            options = result.get('option_distribution', {})
            print(f"   [SUCCESS] 选项分布维度: {len(options)}个")
            
            if options:
                # 显示第一个维度的详情
                first_dim = list(options.keys())[0]
                dim_data = options[first_dim]
                questions = dim_data.get('questions', {})
                
                print(f"   [INFO] 示例维度 {first_dim}:")
                print(f"      维度名称: {dim_data.get('dimension_name', '未知')}")
                print(f"      题目数量: {len(questions)}")
                
                if questions:
                    first_q = list(questions.keys())[0]
                    q_options = questions[first_q]['options']
                    print(f"      示例题目 {first_q} 选项:")
                    for opt in q_options[:2]:  # 显示前2个选项
                        print(f"         {opt['label']}: {opt['count']}人 ({opt['percentage']}%)")
        else:
            print(f"   [FAIL] {result['error']}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    print("\n=== API核心功能测试完成 ===")

def generate_api_summary():
    """生成API功能总结"""
    print("\n=== API接口总结 ===\n")
    
    api_endpoints = {
        "系统管理": {
            "健康检查": "GET /api/health",
            "API文档": "GET /api/docs",
            "批次列表": "GET /api/batches"
        },
        "批次分析": {
            "批次概览": "GET /api/batch/{batch_code}/overview",
            "学校列表": "GET /api/batch/{batch_code}/schools",
            "完整报告": "GET /api/batch/{batch_code}/complete"
        },
        "学科分析": {
            "学科汇聚": "GET /api/batch/{batch_code}/subjects[?school_code=XXX]",
            "学科对比": "GET /api/batch/{batch_code}/subjects/comparison[?school_codes=A,B,C]"
        },
        "维度分析": {
            "维度汇聚": "GET /api/batch/{batch_code}/dimensions[?school_code=XXX&subject_name=XXX]"
        },
        "问卷分析": {
            "选项分布": "GET /api/batch/{batch_code}/questionnaire/options[?school_code=XXX&dimension_code=XXX]",
            "问卷汇总": "GET /api/batch/{batch_code}/questionnaire/summary[?school_code=XXX]"
        },
        "排名分析": {
            "学校排名": "GET /api/batch/{batch_code}/rankings[?subject_name=XXX]"
        }
    }
    
    total_endpoints = 0
    for category, endpoints in api_endpoints.items():
        print(f"{category}:")
        for name, endpoint in endpoints.items():
            print(f"   • {name}: {endpoint}")
            total_endpoints += 1
        print()
    
    print(f"API接口总数: {total_endpoints}个")
    
    print("\n功能特性:")
    print("   ✓ 支持学校层级和区域层级汇聚")
    print("   ✓ 支持三种学科类型：考试、人机交互、问卷")
    print("   ✓ 提供完整统计指标：平均分、得分率、标准差、区分度、难度、P10/P50/P90")
    print("   ✓ 问卷特有功能：选项分布分析")
    print("   ✓ 学校排名与对比分析")
    print("   ✓ RESTful API设计，支持JSON格式")
    print("   ✓ 统一错误处理和响应格式")
    print("   ✓ 内置缓存机制提升性能")
    
    print("\n=== API接口总结完成 ===")

if __name__ == "__main__":
    test_api_core_functionality()
    generate_api_summary()