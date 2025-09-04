# 问卷数据处理演示
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import numpy as np
import json
import logging
from app.calculation.calculators.survey_calculator import SurveyCalculator
from app.calculation.calculators.strategy_registry import initialize_calculation_system

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_sample_survey_data():
    """创建示例问卷数据"""
    logger.info("创建示例问卷数据...")
    
    np.random.seed(42)
    n_samples = 200
    
    # 创建好奇心和观察能力问卷数据
    data = {
        # 好奇心维度题目
        'Q1': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.05, 0.15, 0.25, 0.35, 0.20]),  # 正向: 我对新事物很感兴趣
        'Q2': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.20, 0.35, 0.25, 0.15, 0.05]),  # 反向: 我不喜欢探索未知的事物
        'Q3': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.10, 0.15, 0.20, 0.30, 0.25]),  # 正向: 我经常提出问题
        'Q4': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.25, 0.30, 0.20, 0.15, 0.10]),  # 反向: 我满足于现状，不愿尝试新方法
        'Q5': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.08, 0.12, 0.25, 0.35, 0.20]),  # 正向: 我喜欢学习新知识
        
        # 观察能力维度题目
        'Q6': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.05, 0.10, 0.20, 0.40, 0.25]),  # 正向: 我能注意到细节
        'Q7': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.30, 0.25, 0.20, 0.15, 0.10]),  # 反向: 我经常忽略重要信息
        'Q8': np.random.choice([1, 2, 3, 4, 5], n_samples, p=[0.08, 0.15, 0.22, 0.30, 0.25]),  # 正向: 我善于发现模式和规律
    }
    
    # 创建DataFrame
    df = pd.DataFrame(data)
    
    # 添加一些缺失值来模拟真实数据
    for col in ['Q2', 'Q7']:
        mask = np.random.choice([True, False], n_samples, p=[0.95, 0.05])
        df[col] = df[col].where(mask, np.nan)
    
    # 添加响应时间数据
    df['response_time'] = np.random.normal(300, 100, n_samples)
    df['response_time'] = np.maximum(df['response_time'], 30)
    
    # 添加一些质量问题样本
    # 直线响应样本
    for i in range(5):
        df.loc[i, 'Q1'] = 3
        df.loc[i, 'Q3'] = 3
        df.loc[i, 'Q5'] = 3
        df.loc[i, 'Q6'] = 3
        df.loc[i, 'Q8'] = 3
    
    # 完成率低的样本
    for i in range(195, 200):
        for col in ['Q4', 'Q5', 'Q6', 'Q7']:
            df.loc[i, col] = np.nan
    
    return df


def create_survey_config():
    """创建问卷配置"""
    return {
        'survey_id': 'curiosity_observation_survey',
        'name': '学生好奇心与观察能力调查问卷',
        'dimensions': {
            'curiosity': {
                'name': '好奇心',
                'forward_questions': ['Q1', 'Q3', 'Q5'],
                'reverse_questions': ['Q2', 'Q4'],
                'weight': 1.0,
                'description': '衡量学生对新事物的兴趣和探索欲望'
            },
            'observation': {
                'name': '观察能力',
                'forward_questions': ['Q6', 'Q8'],
                'reverse_questions': ['Q7'],
                'weight': 1.2,
                'description': '衡量学生的观察细节和发现规律的能力'
            }
        },
        'scale_config': {
            'forward': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5},  # 正向量表
            'reverse': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}   # 反向量表
        },
        'quality_rules': {
            'response_time_min': 30,
            'response_time_max': 1800,
            'straight_line_max': 8,
            'completion_rate_min': 0.8,
            'variance_threshold': 0.1
        },
        'version': '1.0'
    }


def demonstrate_survey_processing():
    """演示问卷数据处理功能"""
    print("=" * 80)
    print("问卷数据处理系统演示")
    print("=" * 80)
    
    # 初始化计算系统
    logger.info("初始化计算系统...")
    initialize_calculation_system()
    
    # 创建计算器
    calculator = SurveyCalculator()
    
    # 创建示例数据
    survey_data = create_sample_survey_data()
    survey_config = create_survey_config()
    
    print(f"\n📊 数据概览:")
    print(f"   样本数量: {len(survey_data)}")
    print(f"   题目数量: {len([col for col in survey_data.columns if col.startswith('Q')])}")
    print(f"   维度数量: {len(survey_config['dimensions'])}")
    
    print(f"\n🔧 维度配置:")
    for dim_name, dim_config in survey_config['dimensions'].items():
        forward_q = dim_config['forward_questions']
        reverse_q = dim_config['reverse_questions']
        print(f"   {dim_config['name']}: 正向题目{forward_q}, 反向题目{reverse_q}, 权重{dim_config['weight']}")
    
    # 执行完整的数据处理管道
    print(f"\n🚀 开始处理问卷数据...")
    results = calculator.process_survey_data(
        survey_data, 
        survey_config,
        include_quality_check=True,
        include_frequencies=True,
        include_dimensions=True
    )
    
    # 展示处理结果
    print("\n" + "=" * 80)
    print("处理结果分析")
    print("=" * 80)
    
    # 1. 数据质量分析
    print(f"\n🔍 数据质量分析:")
    quality_analysis = results['quality_analysis']
    quality_summary = quality_analysis['quality_summary']
    
    print(f"   总响应数: {quality_summary['total_responses']}")
    print(f"   有效响应数: {quality_summary['valid_responses']}")
    print(f"   有效性率: {quality_summary['validity_rate']:.2%}")
    
    quality_flags = quality_analysis['quality_flags']
    print(f"   低完成率响应: {quality_flags['low_completion']['count']} ({quality_flags['low_completion']['percentage']:.1%})")
    print(f"   直线响应: {quality_flags['straight_line']['count']} ({quality_flags['straight_line']['percentage']:.1%})")
    print(f"   无变化响应: {quality_flags['no_variance']['count']} ({quality_flags['no_variance']['percentage']:.1%})")
    
    # 2. 量表转换结果
    print(f"\n🔄 量表转换结果:")
    scale_transformation = results['scale_transformation']
    transformation_summary = scale_transformation['transformation_summary']
    
    for question, info in transformation_summary.items():
        print(f"   {question} ({info['type']}量表): 有效转换{info['valid_count']}个响应")
    
    # 3. 维度统计分析
    print(f"\n📈 维度统计分析:")
    dimension_analysis = results['dimension_analysis']
    dimension_statistics = dimension_analysis['dimension_statistics']
    
    for dim_name, stats in dimension_statistics.items():
        dim_config = survey_config['dimensions'][dim_name]
        print(f"   {dim_config['name']}维度:")
        print(f"     平均分: {stats['mean']:.2f} (权重调整后: {stats['weighted_mean']:.2f})")
        print(f"     标准差: {stats['std']:.2f}")
        print(f"     有效样本: {stats['count']}")
        print(f"     分数范围: {stats['min']:.2f} - {stats['max']:.2f}")
    
    # 4. 维度间相关性
    if 'dimension_correlations' in dimension_analysis:
        correlations = dimension_analysis['dimension_correlations']
        print(f"\n🔗 维度间相关性:")
        for corr_name, corr_data in correlations.items():
            print(f"   {corr_name}: {corr_data['correlation']:.3f} ({corr_data['strength']})")
    
    # 5. 频率分布分析（展示部分题目）
    print(f"\n📊 选项频率分布 (示例题目Q1):")
    frequency_analysis = results['frequency_analysis']
    q1_freq = frequency_analysis['question_frequencies']['Q1']
    
    print(f"   选项分布:")
    for option, percentage in q1_freq['valid_percentages'].items():
        print(f"     选项{option}: {q1_freq['frequencies'][option]}人 ({percentage:.1%})")
    
    print(f"   响应率: {q1_freq['response_rate']:.1%}")
    print(f"   平均分: {q1_freq['statistics']['mean']:.2f}")
    
    # 6. 综合报告
    print(f"\n📋 综合分析报告:")
    summary_report = results['summary_report']
    
    print("   关键发现:")
    for finding in summary_report['key_findings']:
        print(f"     • {finding}")
    
    print("   建议:")
    for recommendation in summary_report['recommendations']:
        print(f"     • {recommendation}")
    
    # 7. 演示数据导出
    print(f"\n💾 数据导出:")
    exported_data = calculator.export_results_to_dict(results)
    
    print(f"   导出版本: {exported_data['survey_analysis_version']}")
    print(f"   处理时间: {exported_data['processing_timestamp']}")
    print(f"   数据大小: {len(json.dumps(exported_data, ensure_ascii=False, default=str))} 字符")
    
    return results


def demonstrate_individual_functions():
    """演示各个独立功能"""
    print("\n" + "=" * 80)
    print("独立功能演示")
    print("=" * 80)
    
    calculator = SurveyCalculator()
    survey_data = create_sample_survey_data()
    
    # 1. 演示量表转换
    print(f"\n🔄 量表转换演示:")
    question_configs = {
        'Q1': 'forward',  # 正向题目
        'Q2': 'reverse',  # 反向题目
        'Q3': 'forward'   # 正向题目
    }
    
    test_data = survey_data[['Q1', 'Q2', 'Q3']].head(5)
    print("原始数据:")
    print(test_data.to_string(index=False))
    
    transformed_data = calculator.transform_likert_scale(test_data, question_configs, '5point')
    print("\n转换后数据:")
    print(transformed_data.to_string(index=False))
    
    # 2. 演示质量检查
    print(f"\n🔍 质量检查演示:")
    quality_result = calculator.analyze_response_quality(survey_data)
    
    print(f"总响应数: {quality_result['quality_summary']['total_responses']}")
    print(f"有效性率: {quality_result['quality_summary']['validity_rate']:.2%}")
    print("质量建议:")
    for rec in quality_result['recommendations'][:2]:  # 显示前两个建议
        print(f"  • {rec}")
    
    # 3. 演示频率分析
    print(f"\n📊 频率分析演示 (Q1题目):")
    freq_result = calculator.get_frequency_distribution(survey_data, ['Q1'])
    q1_freq = freq_result['question_frequencies']['Q1']
    
    print("选项频率:")
    for option in sorted(q1_freq['frequencies'].keys()):
        if pd.notna(option):
            count = q1_freq['frequencies'][option]
            percentage = q1_freq['percentages'][option]
            print(f"  选项{int(option)}: {count}人 ({percentage:.1%})")


if __name__ == '__main__':
    try:
        # 主要演示
        results = demonstrate_survey_processing()
        
        # 独立功能演示
        demonstrate_individual_functions()
        
        print("\n" + "=" * 80)
        print("演示完成! 🎉")
        print("问卷数据处理系统已成功实现以下功能:")
        print("  ✅ 5级李克特量表转换 (正向/反向)")
        print("  ✅ 选项频率统计和分布分析")
        print("  ✅ 多维度汇总计算和相关性分析")
        print("  ✅ 数据质量检查和异常检测")
        print("  ✅ 综合报告生成和结果导出")
        print("=" * 80)
        
    except Exception as e:
        logger.error(f"演示过程中发生错误: {e}")
        import traceback
        traceback.print_exc()