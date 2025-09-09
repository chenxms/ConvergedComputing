#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统计计算引擎简化测试脚本（无Emoji版本）
针对Windows PowerShell GBK编码兼容
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.calculation import initialize_calculation_system
import numpy as np
import pandas as pd


def test_engine_initialization():
    """测试计算引擎初始化"""
    print("[TEST] 开始测试计算引擎初始化...")
    
    try:
        engine = initialize_calculation_system()
        print("[PASS] 计算引擎初始化成功")
        return True, engine
    except Exception as e:
        print(f"[FAIL] 计算引擎初始化失败: {str(e)}")
        return False, None


def test_basic_calculation(engine):
    """测试基本计算功能"""
    print("[TEST] 开始测试基本计算功能...")
    
    try:
        # 准备测试数据
        test_data = pd.DataFrame({'score': [85, 92, 78, 88, 95, 82, 90, 87, 83, 89]})
        config = {'data_type': 'scores'}
        
        # 测试基础统计
        result = engine.calculate('basic_statistics', test_data, config)
        
        if not result or 'mean' not in result:
            print("[FAIL] 基础统计计算返回结果为空")
            return False
        
        expected_mean = np.mean(test_data)
        calculated_mean = result['mean']
        
        if abs(calculated_mean - expected_mean) > 0.001:
            print(f"[FAIL] 平均值计算错误: 期望 {expected_mean}, 得到 {calculated_mean}")
            return False
        
        print(f"[PASS] 基础统计计算正确 - 平均值: {calculated_mean:.2f}")
        return True
        
    except Exception as e:
        print(f"[FAIL] 基本计算功能测试失败: {str(e)}")
        print(f"[DEBUG] 错误类型: {type(e)}")
        import traceback
        print("[DEBUG] 详细错误:")
        traceback.print_exc()
        return False


def test_percentiles_calculation(engine):
    """测试百分位数计算"""
    print("[TEST] 开始测试百分位数计算...")
    
    try:
        test_data = pd.DataFrame({'score': [85, 92, 78, 88, 95, 82, 90, 87, 83, 89]})
        config = {'data_type': 'scores'}
        
        result = engine.calculate('percentiles', test_data, config)
        
        if not result or 'P50' not in result:
            print("[FAIL] 百分位数计算失败")
            return False
        
        print(f"[PASS] 百分位数计算正确 - P50: {result.get('P50', 0):.1f}")
        return True
        
    except Exception as e:
        print(f"[FAIL] 百分位数测试失败: {str(e)}")
        return False


def test_grade_distribution(engine):
    """测试等级分布计算"""
    print("[TEST] 开始测试等级分布计算...")
    
    try:
        test_data = pd.DataFrame({'score': [95, 88, 75, 92, 85, 78, 90, 82, 87, 93]})
        config = {
            'grade_level': '3rd_grade',
            'max_score': 100
        }
        
        result = engine.calculate('grade_distribution', test_data, config)
        
        if not result or 'distribution' not in result:
            print("[FAIL] 等级分布计算失败")
            return False
        
        distribution = result['distribution']
        print("[PASS] 等级分布计算成功")
        print(f"  优秀: {distribution['percentages'].get('excellent', 0):.1f}%")
        print(f"  良好: {distribution['percentages'].get('good', 0):.1f}%")
        return True
        
    except Exception as e:
        print(f"[FAIL] 等级分布测试失败: {str(e)}")
        return False


def main():
    print("=" * 60)
    print("[START] Data-Calculation 统计引擎测试 (简化版)")
    print("=" * 60)
    
    # 测试步骤
    tests = [
        ("引擎初始化", lambda: test_engine_initialization()),
        ("基本计算功能", lambda: test_basic_calculation(engine) if engine else (False,)),
        ("百分位数计算", lambda: test_percentiles_calculation(engine) if engine else (False,)),
        ("等级分布计算", lambda: test_grade_distribution(engine) if engine else (False,))
    ]
    
    engine = None
    results = []
    
    for test_name, test_func in tests:
        print(f"\n[STEP] {test_name}")
        print("-" * 40)
        
        if test_name == "引擎初始化":
            success, engine = test_func()
        else:
            success = test_func()
        
        results.append((test_name, success))
        
        if not success and test_name == "引擎初始化":
            print("[ABORT] 引擎初始化失败，停止后续测试")
            break
    
    # 总结结果
    print("\n" + "=" * 60)
    print("[SUMMARY] 测试结果总结")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results:
        status = "[PASS]" if success else "[FAIL]"
        print(f"  {test_name}: {status}")
        if success:
            passed += 1
    
    success_rate = (passed / total * 100) if total > 0 else 0
    print(f"\n[RESULT] 通过率: {passed}/{total} ({success_rate:.1f}%)")
    
    if success_rate == 100:
        print("[SUCCESS] 所有测试通过！统计计算引擎运行正常。")
        sys.exit(0)
    else:
        print("[FAILURE] 部分测试失败，需要修复问题。")
        sys.exit(1)


if __name__ == "__main__":
    main()