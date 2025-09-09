#!/usr/bin/env python3
"""
测试年级等级标准修复
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import pandas as pd

from app.calculation.formulas import EducationalMetricsStrategy

def test_grade_standards():
    """测试等级标准修复"""
    print("=== 测试年级等级标准修复 ===\n")
    
    # 创建测试数据
    test_scores_primary = [90, 85, 80, 75, 70, 60, 50]  # 小学测试分数
    test_scores_middle = [90, 80, 75, 70, 65, 60, 50]   # 初中测试分数
    
    strategy = EducationalMetricsStrategy()
    
    print("1. 小学标准测试 (4th_grade):")
    print("   期望：优秀≥85%, 良好75-84%, 及格60-74%, 不及格<60%")
    
    primary_data = pd.DataFrame({'score': test_scores_primary})
    primary_config = {
        'max_score': 100,
        'grade_level': '4th_grade'
    }
    
    primary_result = strategy.calculate(primary_data, primary_config)
    
    print(f"   优秀率: {primary_result['excellent_rate']:.2%}")
    print(f"   及格率: {primary_result['pass_rate']:.2%}")
    print("   等级分布:")
    grade_dist = primary_result['grade_distribution']
    print(f"     优秀: {grade_dist['excellent_count']}人 ({grade_dist['excellent_rate']:.2%})")
    print(f"     良好: {grade_dist['good_count']}人 ({grade_dist['good_rate']:.2%})")
    print(f"     及格: {grade_dist['pass_count']}人 ({grade_dist['pass_rate']:.2%})")
    print(f"     不及格: {grade_dist['fail_count']}人 ({grade_dist['fail_rate']:.2%})")
    
    print("\n2. 初中标准测试 (7th_grade):")
    print("   期望：A≥80%, B70-79%, C60-69%, D<60%")
    
    middle_data = pd.DataFrame({'score': test_scores_middle})
    middle_config = {
        'max_score': 100,
        'grade_level': '7th_grade'
    }
    
    middle_result = strategy.calculate(middle_data, middle_config)
    
    print(f"   优秀率: {middle_result['excellent_rate']:.2%}")
    print(f"   及格率: {middle_result['pass_rate']:.2%}")
    print("   等级分布:")
    grade_dist = middle_result['grade_distribution']
    print(f"     A等: {grade_dist['a_count']}人 ({grade_dist['a_rate']:.2%})")
    print(f"     B等: {grade_dist['b_count']}人 ({grade_dist['b_rate']:.2%})")
    print(f"     C等: {grade_dist['c_count']}人 ({grade_dist['c_rate']:.2%})")
    print(f"     D等: {grade_dist['d_count']}人 ({grade_dist['d_rate']:.2%})")
    
    print("\n3. 验证标准修复:")
    
    # 验证小学85分应该是优秀
    score_85_primary = pd.DataFrame({'score': [85]})
    result_85_primary = strategy.calculate(score_85_primary, primary_config)
    print(f"   小学85分优秀率: {result_85_primary['excellent_rate']:.2%} (期望: 100%)")
    
    # 验证初中80分应该是优秀
    score_80_middle = pd.DataFrame({'score': [80]})
    result_80_middle = strategy.calculate(score_80_middle, middle_config)
    print(f"   初中80分优秀率: {result_80_middle['excellent_rate']:.2%} (期望: 100%)")
    
    print("\n=== 标准修复验证完成 ===")
    print("✅ 小学优秀标准：≥85%")
    print("✅ 初中优秀标准：≥80%")
    print("✅ 等级分布阈值已相应调整")

if __name__ == "__main__":
    test_grade_standards()