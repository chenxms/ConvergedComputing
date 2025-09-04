# 问卷数据处理简单演示
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np
import logging
from app.calculation.calculators.survey_calculator import SurveyCalculator
from app.calculation.calculators.strategy_registry import initialize_calculation_system

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_sample_data():
    """创建示例问卷数据"""
    np.random.seed(42)
    n_samples = 50
    
    # 创建问卷数据
    data = {
        'Q1': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.05, 0.15, 0.25, 0.35, 0.20]),  # 正向题目
        'Q2': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.20, 0.35, 0.25, 0.15, 0.05]),  # 反向题目
        'Q3': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.10, 0.15, 0.20, 0.30, 0.25]),  # 正向题目
        'Q4': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.25, 0.30, 0.20, 0.15, 0.10]),  # 反向题目
    }
    
    df = pd.DataFrame(data)
    
    # 添加一些缺失值
    df.loc[45:49, 'Q4'] = np.nan
    
    return df


def create_sample_config():
    """创建示例配置"""
    return {
        'survey_id': 'test_survey',
        'name': '测试问卷',
        'dimensions': {
            'dimension1': {
                'name': '维度1',
                'forward_questions': ['Q1', 'Q3'],
                'reverse_questions': ['Q2', 'Q4'],
                'weight': 1.0
            }
        },
        'scale_config': {
            'forward': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5},
            'reverse': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
        },
        'quality_rules': {
            'straight_line_max': 8,
            'completion_rate_min': 0.8,
            'variance_threshold': 0.1
        }
    }


def main():
    """主演示函数"""
    print("=" * 60)
    print("问卷数据处理系统演示")
    print("=" * 60)
    
    # 初始化系统
    print("1. 初始化计算系统...")
    initialize_calculation_system()
    calculator = SurveyCalculator()
    
    # 创建测试数据
    print("2. 创建测试数据...")
    survey_data = create_sample_data()
    survey_config = create_sample_config()
    
    print(f"   样本数量: {len(survey_data)}")
    print(f"   题目数量: {len([col for col in survey_data.columns if col.startswith('Q')])}")
    
    # 显示原始数据示例
    print("\n3. 原始数据示例:")
    print(survey_data.head().to_string(index=False))
    
    # 处理数据
    print("\n4. 执行数据处理...")
    results = calculator.process_survey_data(
        survey_data, 
        survey_config,
        include_quality_check=True,
        include_frequencies=True,
        include_dimensions=True
    )
    
    # 展示结果
    print("\n" + "=" * 60)
    print("处理结果")
    print("=" * 60)
    
    # 质量分析
    print("\n[数据质量分析]")
    quality = results['quality_analysis']['quality_summary']
    print(f"   总响应数: {quality['total_responses']}")
    print(f"   有效响应数: {quality['valid_responses']}")
    print(f"   有效性率: {quality['validity_rate']:.2%}")
    
    # 量表转换
    print("\n[量表转换结果]")
    transformation = results['scale_transformation']['transformation_summary']
    for question, info in transformation.items():
        print(f"   {question} ({info['type']}量表): 转换{info['valid_count']}个响应")
    
    # 维度分析
    print("\n[维度统计分析]")
    dimension_stats = results['dimension_analysis']['dimension_statistics']
    for dim_name, stats in dimension_stats.items():
        print(f"   {dim_name}:")
        print(f"     平均分: {stats['mean']:.2f}")
        print(f"     标准差: {stats['std']:.2f}")
        print(f"     有效样本: {stats['count']}")
    
    # 频率分析 - 显示Q1的结果
    print("\n[频率分析] (Q1题目)")
    q1_freq = results['frequency_analysis']['question_frequencies']['Q1']
    print("   选项分布:")
    for option in sorted(q1_freq['frequencies'].keys()):
        if pd.notna(option):
            count = q1_freq['frequencies'][option]
            percentage = q1_freq['percentages'][option]
            print(f"     选项{int(option)}: {count}人 ({percentage:.1%})")
    
    # 演示独立功能
    print("\n" + "=" * 60)
    print("独立功能演示")
    print("=" * 60)
    
    # 量表转换演示
    print("\n[量表转换演示]")
    question_configs = {'Q1': 'forward', 'Q2': 'reverse'}
    test_data = survey_data[['Q1', 'Q2']].head(3)
    
    print("原始数据:")
    print(test_data.to_string(index=False))
    
    transformed = calculator.transform_likert_scale(test_data, question_configs, '5point')
    print("转换后数据:")
    print(transformed.to_string(index=False))
    
    # 数据验证演示
    print("\n[数据验证演示]")
    validation = calculator.validate_survey_data(survey_data, survey_config)
    print(f"   整体有效性: {validation['overall_valid']}")
    print(f"   错误数量: {len(validation['all_errors'])}")
    print(f"   警告数量: {len(validation['all_warnings'])}")
    
    print("\n" + "=" * 60)
    print("演示完成!")
    print("问卷数据处理系统功能:")
    print("  [v] 5级李克特量表转换 (正向/反向)")
    print("  [v] 选项频率统计和分布分析")
    print("  [v] 多维度汇总计算")
    print("  [v] 数据质量检查")
    print("  [v] 综合报告生成")
    print("=" * 60)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"演示过程中发生错误: {e}")
        import traceback
        traceback.print_exc()