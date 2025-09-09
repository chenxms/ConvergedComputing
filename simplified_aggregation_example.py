# 简化汇聚系统使用示例
"""
这个文件展示了如何使用新创建的简化汇聚系统核心模块：
1. 数据精度处理工具 (precision_handler)
2. 简化统计计算器 (simplified_calculator) 
3. 通用学校排名算法 (ranking_service)
4. 维度级统计计算

注意：这些示例需要数据库连接，在实际使用时需要替换数据库会话
"""

import pandas as pd
from sqlalchemy.orm import Session
from app.utils.precision_handler import (
    format_decimal, format_percentage, batch_format_dict,
    create_statistics_summary, validate_numeric_ranges, safe_divide
)
from app.calculation.simplified_calculator import (
    SimplifiedStatisticsCalculator, CalculatorFactory,
    calculate_subject_statistics, calculate_dimension_statistics
)
from app.services.ranking_service import (
    RankingService, get_school_rankings, get_school_rank_info
)


def demo_precision_handler():
    """演示精度处理工具的使用"""
    print("=== 数据精度处理工具演示 ===")
    
    # 1. 基本数值格式化
    print("\n1. 基本数值格式化：")
    print(f"format_decimal(3.14159): {format_decimal(3.14159)}")
    print(f"format_decimal(None): {format_decimal(None)}")
    print(f"format_percentage(0.8567): {format_percentage(0.8567)}")
    
    # 2. 批量字典格式化
    print("\n2. 批量字典格式化：")
    test_data = {
        'avg_score': 85.4567,
        'excellent_rate': 0.234567,
        'difficulty_coefficient': 0.78432,
        'student_count': 100,
        'nested_stats': {
            'pass_rate': 0.8934,
            'median_score': 75.12345,
            'std_deviation': 12.6789
        }
    }
    
    formatted_data = batch_format_dict(test_data)
    print("原始数据:", test_data)
    print("格式化后:", formatted_data)
    
    # 3. 统计摘要创建
    print("\n3. 统计摘要创建：")
    raw_stats = {
        'count': 150,
        'mean': 78.456789,
        'median': 80.123456,
        'std': 15.987654,
        'min': 45.0,
        'max': 98.5,
        'P10': 58.7634,
        'P50': 80.123456,
        'P90': 92.4567,
        'difficulty_coefficient': 0.78456789,
        'discrimination_index': 0.4578,
        'grade_distribution': {
            'excellent_rate': 0.2834,
            'good_rate': 0.3567,
            'pass_rate': 0.8934,
            'fail_rate': 0.1066
        }
    }
    
    summary = create_statistics_summary(raw_stats)
    print("统计摘要:", summary)
    
    # 4. 安全除法
    print("\n4. 安全除法示例：")
    print(f"safe_divide(100, 20): {safe_divide(100, 20)}")
    print(f"safe_divide(100, 0): {safe_divide(100, 0)}")
    print(f"safe_divide(None, 20): {safe_divide(None, 20)}")


def demo_simplified_calculator():
    """演示简化计算器的使用（需要数据库连接）"""
    print("\n=== 简化统计计算器演示 ===")
    
    # 这里只展示API接口，实际使用需要数据库会话
    print("\n1. 创建计算器实例：")
    print("""
    # 需要数据库会话
    # db_session = get_db_session()
    # calculator = CalculatorFactory.create_calculator(db_session, "simplified")
    """)
    
    print("\n2. 计算科目级统计指标：")
    print("""
    # 计算某批次某科目的统计指标
    subject_metrics = calculator.calculate_subject_metrics(
        batch_code="BATCH_2024_001",
        subject_name="数学",
        subject_type="考试类",
        aggregation_level="regional"  # 或 "school"
    )
    
    # 返回格式示例：
    {
        'subject_name': '数学',
        'subject_type': '考试类',
        'total_students': 1500,
        'avg_score': 78.45,
        'median_score': 80.12,
        'std_deviation': 15.98,
        'min_score': 45.0,
        'max_score': 98.5,
        'difficulty_coefficient': 0.78,
        'discrimination_index': 0.46,
        'P10': 58.76,
        'P50': 80.12,
        'P90': 92.45,
        'grade_distribution': {
            'excellent': {'count': 425, 'percentage': 0.28},
            'good': {'count': 535, 'percentage': 0.36},
            'pass': {'count': 380, 'percentage': 0.25},
            'fail': {'count': 160, 'percentage': 0.11}
        }
    }
    """)
    
    print("\n3. 计算维度级统计指标：")
    print("""
    # 计算维度统计
    dimension_metrics = calculator.calculate_dimension_metrics(
        batch_code="BATCH_2024_001",
        subject_name="数学",
        aggregation_level="regional"
    )
    
    # 返回格式示例：
    {
        '数与代数': {
            'dimension_name': '数与代数',
            'student_count': 1500,
            'avg_score': 15.67,
            'score_rate': 0.78,
            'std_deviation': 3.24,
            'min_score': 8.0,
            'max_score': 20.0
        },
        '图形与几何': {
            'dimension_name': '图形与几何',
            'student_count': 1500,
            'avg_score': 18.23,
            'score_rate': 0.81,
            'std_deviation': 2.89,
            'min_score': 12.0,
            'max_score': 22.5
        }
    }
    """)
    
    print("\n4. 便捷函数使用：")
    print("""
    # 使用便捷函数
    subject_stats = calculate_subject_statistics(
        db_session, "BATCH_2024_001", "数学", "考试类", "school", "SCHOOL_001"
    )
    
    dimension_stats = calculate_dimension_statistics(
        db_session, "BATCH_2024_001", "数学", "school", "SCHOOL_001"
    )
    """)


def demo_ranking_service():
    """演示排名服务的使用（需要数据库连接）"""
    print("\n=== 通用学校排名服务演示 ===")
    
    print("\n1. 创建排名服务实例：")
    print("""
    # 需要数据库会话
    # ranking_service = RankingService(db_session)
    """)
    
    print("\n2. 计算学校排名：")
    print("""
    # 按平均分排名
    rankings = ranking_service.calculate_school_rankings(
        batch_code="BATCH_2024_001",
        ranking_field="avg_score",
        ranking_order="desc",
        subject_name="数学"  # 可选，不指定则为综合排名
    )
    
    # 返回格式示例：
    {
        'batch_code': 'BATCH_2024_001',
        'ranking_field': 'avg_score',
        'ranking_order': 'desc',
        'subject_name': '数学',
        'total_schools': 50,
        'rankings': [
            {
                'rank': 1,
                'school_id': 'SCHOOL_001',
                'school_name': '实验小学',
                'ranking_value': 92.5,
                'data': {...}
            },
            {
                'rank': 2,
                'school_id': 'SCHOOL_002', 
                'school_name': '中心小学',
                'ranking_value': 91.8,
                'data': {...}
            }
            # ...
        ],
        'ranking_stats': {
            'mean': 78.5,
            'median': 79.2,
            'std': 8.9,
            'min': 58.3,
            'max': 92.5
        }
    }
    """)
    
    print("\n3. 获取特定学校排名：")
    print("""
    # 获取特定学校的排名信息
    school_rank = ranking_service.get_school_rank(
        batch_code="BATCH_2024_001",
        school_id="SCHOOL_001",
        ranking_field="avg_score",
        subject_name="数学"
    )
    
    # 返回格式示例：
    {
        'batch_code': 'BATCH_2024_001',
        'school_id': 'SCHOOL_001',
        'school_name': '实验小学',
        'rank': 1,
        'ranking_value': 92.5,
        'total_schools': 50,
        'percentile': 98.0,  # 百分位数
        'rank_category': 'top_10_percent',
        'nearby_schools': [
            {'rank': 2, 'school_name': '中心小学', 'ranking_value': 91.8},
            {'rank': 3, 'school_name': '第二小学', 'ranking_value': 90.5}
        ]
    }
    """)
    
    print("\n4. 多字段综合排名：")
    print("""
    # 多字段加权排名
    ranking_fields = [
        {'field': 'avg_score', 'weight': 0.4, 'order': 'desc'},
        {'field': 'excellent_rate', 'weight': 0.3, 'order': 'desc'},
        {'field': 'pass_rate', 'weight': 0.3, 'order': 'desc'}
    ]
    
    multi_rankings = ranking_service.calculate_multi_field_rankings(
        batch_code="BATCH_2024_001",
        ranking_fields=ranking_fields,
        subject_name="数学"
    )
    """)
    
    print("\n5. 便捷函数使用：")
    print("""
    # 使用便捷函数
    school_rankings = get_school_rankings(
        db_session, "BATCH_2024_001", "avg_score", "数学"
    )
    
    school_rank_info = get_school_rank_info(
        db_session, "BATCH_2024_001", "SCHOOL_001", "avg_score", "数学"
    )
    """)


def demo_integration_example():
    """演示模块集成使用"""
    print("\n=== 模块集成使用示例 ===")
    
    print("""
    # 完整的数据处理和分析流程示例

    def process_batch_statistics(db_session, batch_code):
        # 1. 创建计算器
        calculator = CalculatorFactory.create_calculator(db_session)
        
        # 2. 计算所有科目的统计指标
        subjects = ["数学", "语文", "英语"]
        batch_results = {}
        
        for subject_name in subjects:
            # 区域级统计
            regional_stats = calculator.calculate_subject_metrics(
                batch_code, subject_name, "考试类", "regional"
            )
            
            # 维度统计
            dimension_stats = calculator.calculate_dimension_metrics(
                batch_code, subject_name, "regional"
            )
            
            batch_results[subject_name] = {
                'subject_stats': regional_stats,
                'dimension_stats': dimension_stats
            }
        
        # 3. 计算学校排名
        ranking_service = RankingService(db_session)
        
        # 各科目排名
        subject_rankings = {}
        for subject_name in subjects:
            rankings = ranking_service.calculate_school_rankings(
                batch_code, "avg_score", "desc", subject_name
            )
            subject_rankings[subject_name] = rankings
        
        # 综合排名
        multi_field_config = [
            {'field': 'avg_score', 'weight': 0.4, 'order': 'desc'},
            {'field': 'excellent_rate', 'weight': 0.3, 'order': 'desc'},
            {'field': 'pass_rate', 'weight': 0.3, 'order': 'desc'}
        ]
        
        comprehensive_ranking = ranking_service.calculate_multi_field_rankings(
            batch_code, multi_field_config
        )
        
        # 4. 格式化所有结果
        final_results = batch_format_dict({
            'batch_code': batch_code,
            'subject_statistics': batch_results,
            'subject_rankings': subject_rankings,
            'comprehensive_ranking': comprehensive_ranking
        })
        
        # 5. 验证数据合理性
        validation_results = {}
        for subject_name, subject_data in batch_results.items():
            validation = validate_numeric_ranges(subject_data['subject_stats'])
            if validation['warnings'] or validation['errors']:
                validation_results[subject_name] = validation
        
        return {
            'statistics': final_results,
            'validation': validation_results
        }

    # 使用示例
    # result = process_batch_statistics(db_session, "BATCH_2024_001")
    """)


def main():
    """运行所有演示"""
    print("简化汇聚系统核心模块使用演示")
    print("=" * 50)
    
    # 运行各个演示
    demo_precision_handler()
    demo_simplified_calculator() 
    demo_ranking_service()
    demo_integration_example()
    
    print("\n" + "=" * 50)
    print("演示完成！")
    print("\n核心特性总结：")
    print("1. 数据精度处理工具 - 统一处理小数位数，支持2位小数格式化")
    print("2. 简化统计计算器 - 基于cleaned_scores表，计算核心指标")
    print("3. 通用学校排名算法 - 支持单字段和多字段排名，处理并列情况")
    print("4. 维度级统计计算 - 基于dimension_scores字段，简化输出")
    print("5. 性能优化 - 考虑大数据集处理，避免OOM")
    print("6. 异常处理 - 完整的错误处理和日志记录")
    print("7. 类型注解 - 完整的类型标注，便于维护")


if __name__ == "__main__":
    main()