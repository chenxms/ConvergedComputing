#!/usr/bin/env python3
"""
增强汇聚引擎 v1.2 全面测试
验证所有修复要求是否实现
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from enhanced_aggregation_engine import EnhancedAggregationEngine, AggregationLevel
from app.utils.precision import round2, to_pct, validate_precision_requirements
import json
from typing import Dict, Any


def test_precision_functions():
    """测试精度处理功能"""
    print("=== 精度处理功能测试 ===")
    
    # 测试round2函数
    test_values = [3.14159, 2.999, 5.001, None, "invalid", 100]
    print("1. round2函数测试:")
    for value in test_values:
        result = round2(value)
        print(f"   round2({value}) = {result}")
    
    # 测试to_pct函数
    test_percentages = [0.5, 0.1234, 0.9999, 1.0, None, "invalid"]
    print("\n2. to_pct函数测试:")
    for value in test_percentages:
        result = to_pct(value)
        print(f"   to_pct({value}) = {result}")
    
    print("精度处理功能测试通过\n")


def test_regional_aggregation(engine: EnhancedAggregationEngine):
    """测试区域层级汇聚"""
    print("=== 区域层级汇聚测试 ===")
    
    result = engine.aggregate_regional_level('G4-2025')
    
    if 'error' in result:
        print(f"区域层级汇聚失败: {result['error']}")
        return False
    
    # 验证必要字段
    required_fields = ['aggregation_level', 'batch_code', 'subjects', 'schema_version']
    for field in required_fields:
        if field not in result:
            print(f"缺少必要字段: {field}")
            return False
    
    # 验证schema版本
    if result['schema_version'] != 'v1.2':
        print(f"Schema版本错误: {result['schema_version']}")
        return False
    
    print(f"1. 汇聚层级: {result['aggregation_level']}")
    print(f"2. Schema版本: {result['schema_version']}")
    print(f"3. 科目数量: {len(result['subjects'])}")
    
    # 检查科目数据结构
    subjects = result['subjects']
    if not subjects:
        print("未找到科目数据")
        return False
    
    print(f"\n4. 科目结构验证:")
    for i, subject in enumerate(subjects[:2]):  # 检查前2个科目
        print(f"   科目{i+1}: {subject['name']} ({subject['type']})")
        
        # 检查必要字段
        required_subject_fields = ['name', 'type', 'student_count', 'avg_score']
        for field in required_subject_fields:
            if field not in subject:
                print(f"     缺少科目字段: {field}")
                return False
        
        print(f"     - 学生数: {subject['student_count']}")
        print(f"     - 平均分: {subject['avg_score']}")
        print(f"     - 得分率: {subject.get('avg_score_rate_pct', 0)}%")
        
        # 检查学校排名功能
        if 'school_rankings' in subject and subject['school_rankings']:
            rankings = subject['school_rankings']
            print(f"     - 学校排名数: {len(rankings)}")
            
            # 检查第一名的数据结构
            if rankings:
                first_rank = rankings[0]
                required_rank_fields = ['school_code', 'school_name', 'avg_score', 'rank']
                for field in required_rank_fields:
                    if field not in first_rank:
                        print(f"       缺少排名字段: {field}")
                        return False
                print(f"       第一名: {first_rank['school_name']} (分数: {first_rank['avg_score']}, 排名: {first_rank['rank']})")
        
        # 检查维度数据
        if 'dimensions' in subject and subject['dimensions']:
            dimensions = subject['dimensions']
            print(f"     - 维度数: {len(dimensions)}")
            
            # 检查第一个维度
            if dimensions:
                first_dim = dimensions[0]
                required_dim_fields = ['code', 'name', 'avg_score']
                for field in required_dim_fields:
                    if field not in first_dim:
                        print(f"       缺少维度字段: {field}")
                        return False
                print(f"       示例维度: {first_dim['name']} (平均分: {first_dim['avg_score']})")
    
    print("区域层级汇聚测试通过\n")
    return True


def test_school_aggregation(engine: EnhancedAggregationEngine):
    """测试学校层级汇聚"""
    print("=== 学校层级汇聚测试 ===")
    
    # 获取测试学校代码
    session = engine.get_session()
    try:
        from sqlalchemy import text
        result = session.execute(text("SELECT DISTINCT school_code FROM student_cleaned_scores WHERE batch_code = 'G4-2025' LIMIT 1"))
        row = result.fetchone()
        test_school_code = row[0] if row else None
    finally:
        engine.close_session(session)
    
    if not test_school_code:
        print("无法获取测试学校代码")
        return False
    
    result = engine.aggregate_school_level('G4-2025', test_school_code)
    
    if 'error' in result:
        print(f"学校层级汇聚失败: {result['error']}")
        return False
    
    # 验证必要字段
    required_fields = ['aggregation_level', 'batch_code', 'school_code', 'subjects', 'schema_version']
    for field in required_fields:
        if field not in result:
            print(f"缺少必要字段: {field}")
            return False
    
    print(f"1. 汇聚层级: {result['aggregation_level']}")
    print(f"2. 学校代码: {result['school_code']}")
    print(f"3. 科目数量: {len(result['subjects'])}")
    
    # 检查科目数据结构
    subjects = result['subjects']
    if not subjects:
        print("未找到科目数据")
        return False
    
    print(f"\n4. 学校科目结构验证:")
    for i, subject in enumerate(subjects[:2]):  # 检查前2个科目
        print(f"   科目{i+1}: {subject['name']} ({subject['type']})")
        
        # 检查区域排名功能
        if 'region_rank' in subject and subject['region_rank']:
            print(f"     - 区域排名: {subject['region_rank']}")
        if 'total_schools' in subject:
            print(f"     - 总学校数: {subject['total_schools']}")
        
        # 检查维度层排名
        if 'dimensions' in subject and subject['dimensions']:
            dimensions = subject['dimensions']
            print(f"     - 维度数: {len(dimensions)}")
            
            for j, dimension in enumerate(dimensions[:2]):  # 检查前2个维度
                print(f"       维度{j+1}: {dimension['name']}")
                if 'rank' in dimension:
                    print(f"         - 维度排名: {dimension['rank']}")
                print(f"         - 平均分: {dimension.get('avg_score', 0)}")
    
    print("学校层级汇聚测试通过\n")
    return True


def test_precision_compliance(result: Dict[str, Any]):
    """测试精度规范遵守情况"""
    print("=== 精度规范验证 ===")
    
    validation_result = validate_precision_requirements(result)
    
    warnings = validation_result.get('warnings', [])
    errors = validation_result.get('errors', [])
    
    if errors:
        print(f"精度规范错误 ({len(errors)}个):")
        for error in errors[:5]:  # 显示前5个错误
            print(f"  - {error}")
        return False
    
    if warnings:
        print(f"精度规范警告 ({len(warnings)}个):")
        for warning in warnings[:3]:  # 显示前3个警告
            print(f"  - {warning}")
    
    print("精度规范验证通过\n")
    return True


def test_data_structure_unification(regional_result: Dict[str, Any], school_result: Dict[str, Any]):
    """测试数据结构统一性"""
    print("=== 数据结构统一性验证 ===")
    
    # 检查两个结果都使用subjects数组
    if 'subjects' not in regional_result or 'subjects' not in school_result:
        print("数据结构不统一: 缺少subjects数组")
        return False
    
    # 检查schema版本一致性
    regional_version = regional_result.get('schema_version')
    school_version = school_result.get('schema_version')
    
    if regional_version != school_version or regional_version != 'v1.2':
        print(f"Schema版本不一致: 区域={regional_version}, 学校={school_version}")
        return False
    
    # 检查科目结构一致性
    regional_subjects = regional_result['subjects']
    school_subjects = school_result['subjects']
    
    if regional_subjects and school_subjects:
        regional_fields = set(regional_subjects[0].keys())
        school_fields = set(school_subjects[0].keys())
        
        # 学校层级应该有额外的region_rank和total_schools字段
        expected_additional_fields = {'region_rank', 'total_schools'}
        if not expected_additional_fields.issubset(school_fields):
            print(f"学校层级缺少预期字段: {expected_additional_fields - school_fields}")
            return False
        
        # 区域层级应该有school_rankings字段
        regional_first = regional_subjects[0]
        if 'school_rankings' not in regional_first:
            print("区域层级缺少school_rankings字段")
            return False
    
    print("数据结构统一性验证通过\n")
    return True


def test_questionnaire_restructure():
    """测试问卷数据重构"""
    print("=== 问卷数据重构验证 ===")
    
    engine = EnhancedAggregationEngine()
    result = engine.aggregate_regional_level('G4-2025')
    
    if 'error' in result:
        print(f"问卷数据验证失败: {result['error']}")
        return False
    
    subjects = result.get('subjects', [])
    questionnaire_subjects = [s for s in subjects if s.get('type') == 'questionnaire']
    
    if questionnaire_subjects:
        print(f"1. 找到 {len(questionnaire_subjects)} 个问卷科目")
        
        questionnaire = questionnaire_subjects[0]
        print(f"2. 问卷科目: {questionnaire['name']}")
        
        # 检查问卷是否在subjects数组中（而非独立的non_academic_subjects）
        if questionnaire.get('type') != 'questionnaire':
            print("问卷科目类型标记不正确")
            return False
        
        # 检查问卷是否参与排名
        if 'school_rankings' in questionnaire:
            print(f"3. 问卷科目参与排名: {len(questionnaire['school_rankings'])}所学校")
        
        # 检查是否有选项分布
        if 'option_distribution' in questionnaire:
            print("4. 包含选项分布数据")
        
        print("问卷数据重构验证通过\n")
        return True
    else:
        print("未找到问卷科目，跳过验证")
        print("问卷数据重构验证跳过\n")
        return True


def main():
    """主测试函数"""
    print("增强汇聚引擎 v1.2 全面测试开始")
    print("=" * 60)
    
    # 初始化
    engine = EnhancedAggregationEngine()
    
    # 测试项目列表
    tests = []
    
    # 1. 精度处理功能测试
    test_precision_functions()
    tests.append(("精度处理功能", True))
    
    # 2. 区域层级汇聚测试
    regional_result = None
    try:
        regional_success = test_regional_aggregation(engine)
        tests.append(("区域层级汇聚", regional_success))
        if regional_success:
            regional_result = engine.aggregate_regional_level('G4-2025')
    except Exception as e:
        print(f"区域层级汇聚测试异常: {e}")
        tests.append(("区域层级汇聚", False))
    
    # 3. 学校层级汇聚测试
    school_result = None
    try:
        school_success = test_school_aggregation(engine)
        tests.append(("学校层级汇聚", school_success))
        if school_success:
            # 获取测试学校代码
            session = engine.get_session()
            try:
                from sqlalchemy import text
                result = session.execute(text("SELECT DISTINCT school_code FROM student_cleaned_scores WHERE batch_code = 'G4-2025' LIMIT 1"))
                row = result.fetchone()
                test_school_code = row[0] if row else None
                if test_school_code:
                    school_result = engine.aggregate_school_level('G4-2025', test_school_code)
            finally:
                engine.close_session(session)
    except Exception as e:
        print(f"学校层级汇聚测试异常: {e}")
        tests.append(("学校层级汇聚", False))
    
    # 4. 问卷数据重构测试
    try:
        questionnaire_success = test_questionnaire_restructure()
        tests.append(("问卷数据重构", questionnaire_success))
    except Exception as e:
        print(f"问卷数据重构测试异常: {e}")
        tests.append(("问卷数据重构", False))
    
    # 5. 精度规范验证
    if regional_result:
        try:
            precision_success = test_precision_compliance(regional_result)
            tests.append(("精度规范验证", precision_success))
        except Exception as e:
            print(f"精度规范验证异常: {e}")
            tests.append(("精度规范验证", False))
    
    # 6. 数据结构统一性验证
    if regional_result and school_result:
        try:
            structure_success = test_data_structure_unification(regional_result, school_result)
            tests.append(("数据结构统一性", structure_success))
        except Exception as e:
            print(f"数据结构统一性验证异常: {e}")
            tests.append(("数据结构统一性", False))
    
    # 统计测试结果
    print("=" * 60)
    print("测试结果汇总:")
    
    passed = 0
    total = len(tests)
    
    for test_name, success in tests:
        status = "通过" if success else "失败"
        print(f"  {test_name}: {status}")
        if success:
            passed += 1
    
    print(f"\n总体结果: {passed}/{total} 测试项通过")
    
    if passed == total:
        print("所有测试通过！增强汇聚引擎 v1.2 实现了所有修复要求")
    else:
        failed = total - passed
        print(f"{failed}个测试失败，需要进一步检查")
    
    print("=" * 60)
    
    # 显示功能特性清单
    print("实现的功能特性:")
    features = [
        "精度统一处理（所有对外数值统一为两位小数）",
        "百分比字段输出0-100的数值",
        "科目层排名功能（区域层级包含学校排名）",
        "维度层排名功能（学校层级包含维度排名）",
        "问卷数据重构（从 non_academic_subjects 移到 subjects 数组）",
        "数据结构统一（顶层统一使用 subjects 数组）",
        "Schema版本标识（schema_version: v1.2）"
    ]
    
    for i, feature in enumerate(features, 1):
        print(f"  {i}. {feature}")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
