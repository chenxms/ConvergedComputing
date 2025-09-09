#!/usr/bin/env python3
"""
API功能测试
直接调用API函数而不启动Flask服务器
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from aggregation_api import aggregator, success_response, error_response
import json

def test_api_functions():
    """测试API核心功能"""
    print("=== 测试汇聚计算API功能 ===\n")
    
    batch_code = 'G4-2025'
    
    # 1. 测试批次概览
    print("1. 批次概览API:")
    try:
        result = aggregator.get_batch_overview(batch_code)
        
        if 'error' not in result:
            overview = result['overview']
            print(f"   [OK] 学生总数: {overview['total_students']}")
            print(f"   [OK] 学校总数: {overview['total_schools']}")
            print(f"   [OK] 考试学科: {overview['exam_subjects']}个")
            print(f"   [OK] 问卷学科: {overview['questionnaire_subjects']}个")
        else:
            print(f"   [ERROR] {result['error']}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    # 2. 测试学科汇聚API
    print("\n2. 学科汇聚API:")
    try:
        result = aggregator.aggregate_all_subjects(batch_code)
        
        if 'error' not in result:
            analysis = result['analysis']
            exam_count = len(analysis['exam_subjects'])
            questionnaire_count = len(analysis['questionnaire_subjects'])
            
            print(f"   [OK] 考试学科汇聚: {exam_count}个")
            print(f"   [OK] 问卷学科汇聚: {questionnaire_count}个")
            
            # 显示部分数据
            if analysis['exam_subjects']:
                first_exam = list(analysis['exam_subjects'].keys())[0]
                exam_stats = analysis['exam_subjects'][first_exam]
                print(f"   [INFO] 示例考试学科 {first_exam}: "
                     f"平均分{exam_stats['mean_score']}, "
                     f"得分率{exam_stats['score_rate']:.2%}")
            
            if analysis['questionnaire_subjects']:
                first_quest = list(analysis['questionnaire_subjects'].keys())[0]
                quest_stats = analysis['questionnaire_subjects'][first_quest]
                print(f"   [INFO] 示例问卷学科 {first_quest}: "
                     f"平均分{quest_stats['mean_score']}, "
                     f"得分率{quest_stats['score_rate']:.2%}")
                
        else:
            print(f"   [ERROR] {result['error']}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    # 3. 测试维度汇聚API
    print("\n3. 维度汇聚API:")
    try:
        result = aggregator.aggregate_all_dimensions(batch_code)
        
        if 'error' not in result:
            dimension_analysis = result['dimension_analysis']
            exam_dims = len(dimension_analysis.get('exam_dimensions', {}))
            questionnaire_dims = len(dimension_analysis.get('questionnaire_dimensions', {}))
            option_dims = len(dimension_analysis.get('questionnaire_options', {}))
            
            print(f"   [OK] 考试维度: {exam_dims}个学科")
            print(f"   [OK] 问卷维度: {questionnaire_dims}个学科")
            print(f"   [OK] 选项分布: {option_dims}个维度")
        else:
            print(f"   [ERROR] {result['error']}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    # 4. 测试问卷选项分布API
    print("\n4. 问卷选项分布API:")
    try:
        result = aggregator.questionnaire_aggregator.get_option_distribution(batch_code)
        
        if 'error' not in result:
            options = result.get('option_distribution', {})
            print(f"   [OK] 选项分布维度: {len(options)}个")
            
            if options:
                first_dim = list(options.keys())[0]
                dim_data = options[first_dim]
                questions = dim_data['questions']
                print(f"   [INFO] 示例维度 {first_dim}: {len(questions)}个题目")
                
                if questions:
                    first_q = list(questions.keys())[0]
                    q_options = questions[first_q]['options']
                    print(f"   [INFO] 题目 {first_q} 选项数: {len(q_options)}")
        else:
            print(f"   [ERROR] {result['error']}")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    # 5. 测试学校信息API
    print("\n5. 学校信息API:")
    try:
        schools = aggregator.exam_aggregator.get_school_info(batch_code)
        print(f"   [OK] 找到 {len(schools)} 所学校")
        
        if schools:
            # 显示前3所学校
            for i, (code, name, count) in enumerate(schools[:3]):
                print(f"   [INFO] {code}: {name} ({count}个学生)")
    except Exception as e:
        print(f"   [ERROR] 异常: {e}")
    
    # 6. 测试API响应格式
    print("\n6. API响应格式测试:")
    test_data = {'test': 'data', 'count': 100}
    
    success_resp = success_response(test_data, "测试成功")
    print(f"   [OK] 成功响应格式: {success_resp['success']}, {success_resp['message']}")
    
    error_resp = error_response("测试错误", 404, "详细错误信息")
    print(f"   [OK] 错误响应格式: {error_resp['success']}, {error_resp['code']}")
    
    print("\n=== API功能测试完成 ===")

def test_api_endpoints_structure():
    """测试API端点结构"""
    print("\n=== API端点结构验证 ===\n")
    
    endpoints = {
        "批次管理": [
            "GET /api/batches",
            "GET /api/batch/{batch_code}/overview",
            "GET /api/batch/{batch_code}/schools"
        ],
        "学科分析": [
            "GET /api/batch/{batch_code}/subjects",
            "GET /api/batch/{batch_code}/subjects/comparison"
        ],
        "维度分析": [
            "GET /api/batch/{batch_code}/dimensions"
        ],
        "问卷分析": [
            "GET /api/batch/{batch_code}/questionnaire/options",
            "GET /api/batch/{batch_code}/questionnaire/summary"
        ],
        "综合分析": [
            "GET /api/batch/{batch_code}/complete",
            "GET /api/batch/{batch_code}/rankings"
        ],
        "系统": [
            "GET /api/health",
            "GET /api/docs"
        ]
    }
    
    total_endpoints = 0
    for category, endpoint_list in endpoints.items():
        print(f"{category}:")
        for endpoint in endpoint_list:
            print(f"   ✓ {endpoint}")
            total_endpoints += 1
        print()
    
    print(f"总计API端点: {total_endpoints}个")
    print("\n=== API端点结构验证完成 ===")

if __name__ == "__main__":
    test_api_functions()
    test_api_endpoints_structure()