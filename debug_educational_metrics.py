#!/usr/bin/env python3
"""
调试教育指标计算
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import pandas as pd

from app.calculation.formulas import EducationalMetricsStrategy

def debug_educational_metrics():
    """调试教育指标计算"""
    print("=== 调试教育指标计算 ===\n")
    
    # 模拟G4-2025艺术科目的数据
    # 平均分154.59, 满分200, 应该有很多学生优秀
    test_scores = [
        190, 185, 180, 175, 170,  # 优秀学生 (≥85% = 170分)
        165, 160, 155, 150,       # 良好学生 (75-84% = 150-169分) 
        145, 140, 135, 130,       # 及格学生 (60-74% = 120-149分)
        110, 100, 90             # 不及格学生 (<60% = <120分)
    ]
    
    strategy = EducationalMetricsStrategy()
    
    print("1. 测试小学标准 (4th_grade):")
    print(f"   满分: 200, 平均分: {sum(test_scores)/len(test_scores):.1f}")
    print("   期望: 优秀≥170(85%), 良好150-169(75-84%), 及格120-149(60-74%), 不及格<120")
    
    data = pd.DataFrame({'score': test_scores})
    config = {
        'max_score': 200,
        'grade_level': '4th_grade'
    }
    
    try:
        result = strategy.calculate(data, config)
        
        print(f"\n计算结果:")
        print(f"   优秀率: {result['excellent_rate']:.2%}")
        print(f"   及格率: {result['pass_rate']:.2%}")
        print(f"   平均得分率: {result['average_score_rate']:.2%}")
        print(f"   难度系数: {result['difficulty_coefficient']:.3f}")
        
        if 'grade_distribution' in result:
            grade_dist = result['grade_distribution']
            print(f"\n等级分布:")
            print(f"   优秀: {grade_dist.get('excellent_count', 0)}人 ({grade_dist.get('excellent_rate', 0):.2%})")
            print(f"   良好: {grade_dist.get('good_count', 0)}人 ({grade_dist.get('good_rate', 0):.2%})")
            print(f"   及格: {grade_dist.get('pass_count', 0)}人 ({grade_dist.get('pass_rate', 0):.2%})")
            print(f"   不及格: {grade_dist.get('fail_count', 0)}人 ({grade_dist.get('fail_rate', 0):.2%})")
        else:
            print("   ❌ 没有等级分布数据!")
            
    except Exception as e:
        print(f"❌ 计算失败: {e}")
        import traceback
        traceback.print_exc()

    print("\n2. 测试空数据情况:")
    empty_data = pd.DataFrame({'score': []})
    try:
        empty_result = strategy.calculate(empty_data, config)
        print("   空数据处理成功")
    except Exception as e:
        print(f"   空数据处理失败: {e}")

    print("\n3. 测试全零分情况:")
    zero_data = pd.DataFrame({'score': [0, 0, 0, 0, 0]})
    try:
        zero_result = strategy.calculate(zero_data, config)
        print(f"   全零分优秀率: {zero_result['excellent_rate']:.2%}")
        print(f"   全零分及格率: {zero_result['pass_rate']:.2%}")
    except Exception as e:
        print(f"   全零分处理失败: {e}")

if __name__ == "__main__":
    debug_educational_metrics()