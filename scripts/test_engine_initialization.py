#!/usr/bin/env python3
"""
统计计算引擎初始化测试脚本
验证所有计算策略是否正确注册和初始化
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.calculation import initialize_calculation_system
from app.calculation.engine import CalculationEngine
import numpy as np


def test_engine_initialization():
    """测试计算引擎初始化"""
    print("[TEST] 开始测试计算引擎初始化...")
    
    try:
        # 初始化计算系统
        engine = initialize_calculation_system()
        print("[PASS] 计算引擎初始化成功")
        
        # 验证引擎类型
        if not isinstance(engine, CalculationEngine):
            print("[FAIL] 引擎类型错误")
            return False
        print("[PASS] 引擎类型验证通过")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] 计算引擎初始化失败: {str(e)}")
        return False


def test_strategy_registration():
    """测试策略注册"""
    print("\n[TEST] 开始测试策略注册...")
    
    try:
        engine = initialize_calculation_system()
        
        # 预期的策略列表
        expected_strategies = [
            'basic_statistics',
            'percentiles', 
            'educational_metrics',
            'discrimination',
            'grade_distribution',
            'dimension_statistics'
        ]
        
        # 获取已注册策略
        registered_strategies = engine.get_available_strategies()
        print(f"[PASS] 已注册策略数量: {len(registered_strategies)}")
        print(f"   策略列表: {registered_strategies}")
        
        # 验证每个预期策略是否注册
        missing_strategies = []
        for strategy in expected_strategies:
            if strategy in registered_strategies:
                print(f"   [PASS] {strategy}: 已注册")
            else:
                print(f"   [FAIL] {strategy}: 未找到")
                missing_strategies.append(strategy)
        
        if missing_strategies:
            print(f"[FAIL] 缺少策略: {missing_strategies}")
            return False
        
        print("[PASS] 所有预期策略都已正确注册")
        return True
        
    except Exception as e:
        print(f"[FAIL] 策略注册测试失败: {str(e)}")
        return False


def test_strategy_metadata():
    """测试策略元数据"""
    print("\n[TEST] 开始测试策略元数据...")
    
    try:
        engine = initialize_calculation_system()
        
        strategies_to_test = [
            'basic_statistics',
            'percentiles',
            'educational_metrics', 
            'discrimination',
            'grade_distribution'
        ]
        
        for strategy_name in strategies_to_test:
            try:
                metadata = engine.get_strategy_info(strategy_name)
                
                # 验证必需字段
                required_fields = ['name', 'description', 'version', 'features']
                missing_fields = [field for field in required_fields if field not in metadata]
                
                if missing_fields:
                    print(f"   [FAIL] {strategy_name}: 缺少字段 {missing_fields}")
                    return False
                    
                print(f"   [PASS] {strategy_name}: 元数据完整")
                print(f"      描述: {metadata.get('description', 'N/A')[:50]}...")
                print(f"      版本: {metadata.get('version', 'N/A')}")
                print(f"      特性数: {len(metadata.get('features', []))}")
                
            except Exception as e:
                print(f"   [FAIL] {strategy_name}: 元数据获取失败 - {str(e)}")
                return False
        
        print("[PASS] 策略元数据测试通过")
        return True
        
    except Exception as e:
        print(f"[FAIL] 策略元数据测试失败: {str(e)}")
        return False


def test_basic_calculation():
    """测试基本计算功能"""
    print("\n[TEST] 开始测试基本计算功能...")
    
    try:
        engine = initialize_calculation_system()
        
        # 准备测试数据
        test_data = [85, 92, 78, 88, 95, 82, 90, 87, 83, 89]
        
        # 测试基础统计
        config = {'data_type': 'scores'}
        result = engine.calculate('basic_statistics', test_data, config)
        
        if not result or 'mean' not in result:
            print("[FAIL] 基础统计计算失败")
            return False
        
        expected_mean = np.mean(test_data)
        calculated_mean = result['mean']
        
        if abs(calculated_mean - expected_mean) > 0.001:
            print(f"[FAIL] 平均值计算错误: 期望 {expected_mean}, 得到 {calculated_mean}")
            return False
        
        print(f"[PASS] 基础统计计算正确")
        print(f"   平均值: {calculated_mean:.2f}")
        print(f"   标准差: {result.get('std_dev', 'N/A'):.2f}")
        print(f"   中位数: {result.get('median', 'N/A'):.2f}")
        
        # 测试百分位数计算
        percentile_result = engine.calculate('percentiles', test_data, config)
        
        if not percentile_result or 'P50' not in percentile_result:
            print("[FAIL] 百分位数计算失败")
            return False
        
        print(f"[PASS] 百分位数计算正确")
        print(f"   P10: {percentile_result.get('P10', 'N/A'):.1f}")
        print(f"   P50: {percentile_result.get('P50', 'N/A'):.1f}")
        print(f"   P90: {percentile_result.get('P90', 'N/A'):.1f}")
        
        return True
        
    except Exception as e:
        print(f"[FAIL] 基本计算功能测试失败: {str(e)}")
        import traceback
        print("详细错误信息:")
        traceback.print_exc()
        return False


def test_educational_metrics():
    """测试教育统计指标计算"""
    print("\n[TEST] 开始测试教育统计指标...")
    
    try:
        engine = initialize_calculation_system()
        
        # 准备测试数据
        test_data = [85, 92, 78, 88, 95, 82, 90, 87, 83, 89]
        config = {
            'max_score': 100,
            'data_type': 'exam_scores'
        }
        
        # 测试教育指标计算
        result = engine.calculate('educational_metrics', test_data, config)
        
        if not result:
            print("[FAIL] 教育统计指标计算失败")
            return False
        
        print("[PASS] 教育统计指标计算成功")
        print(f"   得分率: {result.get('score_rate', 'N/A'):.3f}")
        print(f"   难度系数: {result.get('difficulty_coefficient', 'N/A'):.3f}")
        print(f"   优秀率: {result.get('excellence_rate', 'N/A'):.3f}")
        print(f"   及格率: {result.get('pass_rate', 'N/A'):.3f}")
        
        # 验证得分率计算正确性
        expected_score_rate = np.mean(test_data) / 100
        actual_score_rate = result.get('score_rate', 0)
        
        if abs(actual_score_rate - expected_score_rate) > 0.001:
            print(f"[FAIL] 得分率计算错误: 期望 {expected_score_rate:.3f}, 得到 {actual_score_rate:.3f}")
            return False
        
        print("[PASS] 得分率计算验证通过")
        return True
        
    except Exception as e:
        print(f"[FAIL] 教育统计指标测试失败: {str(e)}")
        return False


def test_grade_distribution():
    """测试等级分布计算"""
    print("\n[TEST] 开始测试等级分布计算...")
    
    try:
        engine = initialize_calculation_system()
        
        # 小学测试数据
        primary_data = [95, 88, 75, 92, 85, 78, 90, 82, 87, 93]
        primary_config = {
            'grade_level': '3rd_grade',
            'max_score': 100
        }
        
        result = engine.calculate('grade_distribution', primary_data, primary_config)
        
        if not result or 'grade_distribution' not in result:
            print("[FAIL] 等级分布计算失败")
            return False
        
        print("[PASS] 小学等级分布计算成功")
        distribution = result['grade_distribution']
        print(f"   优秀: {distribution.get('excellent', {}).get('percentage', 0):.1f}%")
        print(f"   良好: {distribution.get('good', {}).get('percentage', 0):.1f}%")
        print(f"   及格: {distribution.get('pass', {}).get('percentage', 0):.1f}%")
        print(f"   不及格: {distribution.get('fail', {}).get('percentage', 0):.1f}%")
        
        # 验证百分比总和为100%
        total_percentage = sum([
            distribution.get(grade, {}).get('percentage', 0) 
            for grade in ['excellent', 'good', 'pass', 'fail']
        ])
        
        if abs(total_percentage - 100.0) > 0.1:
            print(f"[FAIL] 等级分布百分比总和错误: {total_percentage:.1f}%")
            return False
        
        print("[PASS] 等级分布百分比验证通过")
        return True
        
    except Exception as e:
        print(f"[FAIL] 等级分布测试失败: {str(e)}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("[START] Data-Calculation 统计引擎初始化测试")
    print("=" * 60)
    
    # 运行所有测试
    tests = [
        ("引擎初始化", test_engine_initialization),
        ("策略注册", test_strategy_registration),
        ("策略元数据", test_strategy_metadata),
        ("基本计算功能", test_basic_calculation),
        ("教育统计指标", test_educational_metrics),
        ("等级分布计算", test_grade_distribution)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n[STEP] {test_name}")
        print("-" * 40)
        result = test_func()
        results.append((test_name, result))
    
    # 总结结果
    print("\n" + "=" * 60)
    print("[SUMMARY] 测试结果总结")
    print("=" * 60)
    
    all_passed = True
    for test_name, passed in results:
        status = "[PASS] PASS" if passed else "[FAIL] FAIL"
        print(f"   {test_name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n[SUCCESS] 所有测试通过！统计计算引擎运行正常。")
        sys.exit(0)
    else:
        print("\n[FAIL] 部分测试失败，请检查错误信息并修复后重试。")
        sys.exit(1)