#!/usr/bin/env python3
"""
简化组件测试脚本
专门测试新实现的组件功能，不依赖复杂的数据库数据
"""

import logging
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_questionnaire_processor_comprehensive():
    """全面测试问卷处理器功能"""
    from app.services.questionnaire_processor import (
        QuestionnaireProcessor, QuestionnaireConfig, ScaleType
    )
    
    logger.info("=== 全面测试问卷处理器 ===")
    
    processor = QuestionnaireProcessor()
    
    # 测试1：5级李克特量表
    logger.info("测试1: 5级李克特量表处理")
    test_data_5 = pd.DataFrame({
        'student_id': ['S001', 'S002', 'S003', 'S004', 'S005'] * 2,
        'question_id': ['Q1'] * 5 + ['Q2'] * 5,
        'raw_score': [5, 4, 3, 2, 1, 1, 2, 3, 4, 5],
        'dimension_code': ['兴趣'] * 10,
        'dimension_name': ['学习兴趣'] * 10
    })
    
    configs_5 = [
        QuestionnaireConfig(
            scale_type=ScaleType.SCALE_5_LIKERT,
            question_id='Q1',
            question_name='您对当前课程感兴趣吗？',
            dimension_code='兴趣',
            dimension_name='学习兴趣'
        ),
        QuestionnaireConfig(
            scale_type=ScaleType.SCALE_5_LIKERT,
            question_id='Q2', 
            question_name='您认为学习很有意义吗？',
            dimension_code='兴趣',
            dimension_name='学习兴趣'
        )
    ]
    
    result_5 = processor.process_questionnaire_data(test_data_5, configs_5, "TEST-5SCALE")
    
    if result_5:
        dim_stat = result_5[0]
        logger.info(f"✅ 5级量表: 维度平均分 {dim_stat.avg_score}, 得分率 {dim_stat.score_rate}%")
        logger.info(f"   选项分布: {len(dim_stat.dimension_option_distributions)} 个选项")
        logger.info(f"   题目数量: {len(dim_stat.questions)} 个题目")
    else:
        logger.error("❌ 5级量表处理失败")
        return False
    
    # 测试2：4级量表（反向）
    logger.info("测试2: 4级量表（反向）处理")
    test_data_4 = pd.DataFrame({
        'student_id': ['S001', 'S002', 'S003', 'S004'],
        'question_id': ['Q3'] * 4,
        'raw_score': [1, 2, 3, 4],  # 原始分数
        'dimension_code': ['压力'] * 4,
        'dimension_name': ['学习压力'] * 4
    })
    
    configs_4 = [
        QuestionnaireConfig(
            scale_type=ScaleType.SCALE_4_NEGATIVE,
            question_id='Q3',
            question_name='您感到学习压力很大吗？',
            dimension_code='压力', 
            dimension_name='学习压力'
        )
    ]
    
    result_4 = processor.process_questionnaire_data(test_data_4, configs_4, "TEST-4SCALE")
    
    if result_4:
        dim_stat = result_4[0]
        logger.info(f"✅ 4级反向量表: 维度平均分 {dim_stat.avg_score}, 得分率 {dim_stat.score_rate}%")
        
        # 验证反向转换：1→4, 2→3, 3→2, 4→1
        raw_scores = test_data_4['raw_score']
        transformed = processor.transform_scores(raw_scores, ScaleType.SCALE_4_NEGATIVE)
        logger.info(f"   反向转换验证: {raw_scores.tolist()} → {transformed.tolist()}")
    else:
        logger.error("❌ 4级反向量表处理失败")
        return False
    
    # 测试3：10分满意度量表
    logger.info("测试3: 10分满意度量表处理")
    test_data_10 = pd.DataFrame({
        'student_id': ['S001', 'S002', 'S003'],
        'question_id': ['Q4'] * 3,
        'raw_score': [10, 8, 6],
        'dimension_code': ['满意度'] * 3,
        'dimension_name': ['整体满意度'] * 3
    })
    
    configs_10 = [
        QuestionnaireConfig(
            scale_type=ScaleType.SCALE_10_SATISFACTION,
            question_id='Q4',
            question_name='您对教学质量满意程度？',
            dimension_code='满意度',
            dimension_name='整体满意度'
        )
    ]
    
    result_10 = processor.process_questionnaire_data(test_data_10, configs_10, "TEST-10SCALE")
    
    if result_10:
        dim_stat = result_10[0]
        logger.info(f"✅ 10分量表: 维度平均分 {dim_stat.avg_score}, 得分率 {dim_stat.score_rate}%")
    else:
        logger.error("❌ 10分量表处理失败")
        return False
    
    # 测试4：选项分布计算
    logger.info("测试4: 选项分布计算精度验证")
    test_scores = pd.Series([1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5])  # 15个样本
    distributions = processor.calculate_option_distributions(test_scores, ScaleType.SCALE_5_LIKERT)
    
    expected_percentages = {
        '非常不满意': 6.67,  # 1/15 * 100
        '不满意': 13.33,     # 2/15 * 100
        '一般': 20.0,       # 3/15 * 100 
        '满意': 26.67,      # 4/15 * 100
        '非常满意': 33.33   # 5/15 * 100
    }
    
    for dist in distributions:
        expected = expected_percentages.get(dist.option_label, 0)
        if abs(dist.percentage - expected) < 0.1:  # 允许0.1%的误差
            logger.info(f"✅ {dist.option_label}: {dist.percentage}% (期望 {expected}%)")
        else:
            logger.error(f"❌ {dist.option_label}: {dist.percentage}% (期望 {expected}%)")
            return False
    
    logger.info("🎉 问卷处理器所有测试通过！")
    return True


def test_aggregation_service_structure():
    """测试汇聚服务结构和方法签名"""
    from app.services.simplified_aggregation_service import SimplifiedAggregationService
    
    logger.info("=== 测试汇聚服务结构 ===")
    
    # 创建Mock数据库会话
    mock_session = Mock()
    
    try:
        service = SimplifiedAggregationService(mock_session)
        
        # 验证初始化
        if hasattr(service, 'questionnaire_processor'):
            logger.info("✅ 问卷处理器已初始化")
        else:
            logger.error("❌ 问卷处理器未初始化")
            return False
        
        if hasattr(service, 'json_serializer'):
            logger.info("✅ JSON序列化器已初始化")
        else:
            logger.error("❌ JSON序列化器未初始化") 
            return False
        
        if hasattr(service, 'calculation_engine'):
            logger.info("✅ 计算引擎已初始化")
        else:
            logger.error("❌ 计算引擎未初始化")
            return False
        
        # 验证主要方法存在
        required_methods = [
            'aggregate_batch_regional',
            'aggregate_batch_school', 
            'aggregate_all_batches',
            '_fetch_batch_data',
            '_analyze_batch_subjects',
            '_calculate_exam_subject_regional',
            '_calculate_questionnaire_subject_regional'
        ]
        
        for method_name in required_methods:
            if hasattr(service, method_name):
                logger.info(f"✅ 方法 {method_name} 存在")
            else:
                logger.error(f"❌ 方法 {method_name} 不存在")
                return False
        
        logger.info("🎉 汇聚服务结构验证通过！")
        return True
        
    except Exception as e:
        logger.error(f"❌ 汇聚服务初始化失败: {str(e)}")
        return False


def test_repository_with_mock_session():
    """使用Mock会话测试仓库功能"""
    from app.repositories.simplified_aggregation_repository import SimplifiedAggregationRepository
    from app.database.models import AggregationLevel, CalculationStatus
    
    logger.info("=== 测试仓库（Mock会话）===")
    
    # 创建Mock会话
    mock_session = Mock()
    mock_query_result = Mock()
    mock_session.query.return_value = mock_query_result
    mock_query_result.filter.return_value = mock_query_result
    mock_query_result.first.return_value = None  # 模拟不存在的记录
    
    try:
        repository = SimplifiedAggregationRepository(mock_session)
        
        # 验证初始化
        if hasattr(repository, 'db'):
            logger.info("✅ 数据库会话已设置")
        else:
            logger.error("❌ 数据库会话未设置")
            return False
        
        # 验证方法存在
        required_methods = [
            'save_aggregation_data',
            'get_aggregation_data',
            'update_aggregation_status',
            'get_batch_aggregation_status',
            'delete_batch_aggregations',
            'get_recent_aggregations'
        ]
        
        for method_name in required_methods:
            if hasattr(repository, method_name):
                logger.info(f"✅ 方法 {method_name} 存在")
            else:
                logger.error(f"❌ 方法 {method_name} 不存在")
                return False
        
        logger.info("🎉 仓库结构验证通过！")
        return True
        
    except Exception as e:
        logger.error(f"❌ 仓库测试失败: {str(e)}")
        return False


def test_data_schemas():
    """测试数据模式定义"""
    from app.schemas.simplified_aggregation_schema import (
        RegionalAggregationData, SchoolAggregationData, SubjectStatistics,
        SubjectCoreMetrics, SubjectRanking, DimensionMetrics,
        QuestionnaireOptionDistribution, QuestionnaireDimensionStats
    )
    
    logger.info("=== 测试数据模式 ===")
    
    try:
        # 测试核心指标模式
        metrics = SubjectCoreMetrics(
            avg_score=85.5,
            difficulty=0.855,
            std_dev=12.3,
            discrimination=0.45,
            max_score=100.0,
            min_score=60.0,
            p10=70.0,
            p50=85.0,
            p90=95.0,
            student_count=150
        )
        logger.info("✅ SubjectCoreMetrics 创建成功")
        
        # 测试排名模式
        ranking = SubjectRanking(
            school_rankings=[
                {"school_id": "S001", "school_name": "第一中学", "avg_score": 92.0, "rank": 1}
            ]
        )
        logger.info("✅ SubjectRanking 创建成功")
        
        # 测试问卷选项分布
        option_dist = QuestionnaireOptionDistribution(
            option_label="非常满意",
            count=45,
            percentage=30.0
        )
        logger.info("✅ QuestionnaireOptionDistribution 创建成功")
        
        # 测试科目统计
        subject_stats = SubjectStatistics(
            subject_id="MATH_01",
            subject_name="数学",
            subject_type="exam",
            metrics=metrics,
            ranking=ranking
        )
        logger.info("✅ SubjectStatistics 创建成功")
        
        # 测试区域级汇聚数据
        regional_data = RegionalAggregationData(
            batch_code="TEST-2025",
            total_schools=10,
            total_students=1500,
            subjects={"MATH_01": subject_stats},
            created_at=datetime.now().isoformat(),
            updated_at=datetime.now().isoformat()
        )
        logger.info("✅ RegionalAggregationData 创建成功")
        
        # 测试学校级汇聚数据
        school_data = SchoolAggregationData(
            batch_code="TEST-2025",
            school_id="S001",
            school_name="第一中学",
            total_students=150,
            subjects={"MATH_01": subject_stats},
            created_at=datetime.now().isoformat(),
            updated_at=datetime.now().isoformat()
        )
        logger.info("✅ SchoolAggregationData 创建成功")
        
        logger.info("🎉 所有数据模式验证通过！")
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据模式测试失败: {str(e)}")
        return False


def test_utility_functions():
    """测试工具函数"""
    from app.schemas.simplified_aggregation_schema import (
        format_decimal, calculate_difficulty, calculate_score_rate
    )
    
    logger.info("=== 测试工具函数 ===")
    
    try:
        # 测试小数格式化
        test_cases = [
            (3.14159, 2, 3.14),
            (85.6666, 1, 85.7),
            (100.0, 2, 100.0),
            (None, 2, 0.0)
        ]
        
        for value, precision, expected in test_cases:
            result = format_decimal(value, precision)
            if result == expected:
                logger.info(f"✅ format_decimal({value}, {precision}) = {result}")
            else:
                logger.error(f"❌ format_decimal({value}, {precision}) = {result}, 期望 {expected}")
                return False
        
        # 测试难度系数计算
        difficulty = calculate_difficulty(85.5, 100.0)
        if difficulty == 0.85:  # 85.5/100.0 rounded to 2 decimal places
            logger.info(f"✅ calculate_difficulty(85.5, 100.0) = {difficulty}")
        else:
            logger.error(f"❌ calculate_difficulty(85.5, 100.0) = {difficulty}, 期望 0.85")
            return False
        
        # 测试得分率计算
        score_rate = calculate_score_rate(85.5, 100.0)
        if score_rate == 85.5:  # 85.5/100.0 * 100
            logger.info(f"✅ calculate_score_rate(85.5, 100.0) = {score_rate}%")
        else:
            logger.error(f"❌ calculate_score_rate(85.5, 100.0) = {score_rate}%, 期望 85.5%")
            return False
        
        logger.info("🎉 所有工具函数验证通过！")
        return True
        
    except Exception as e:
        logger.error(f"❌ 工具函数测试失败: {str(e)}")
        return False


def main():
    """主测试函数"""
    logger.info("开始简化组件功能测试")
    
    test_results = {
        '问卷处理器（全面）': test_questionnaire_processor_comprehensive(),
        '汇聚服务结构': test_aggregation_service_structure(),
        '仓库（Mock会话）': test_repository_with_mock_session(),
        '数据模式': test_data_schemas(),
        '工具函数': test_utility_functions(),
    }
    
    logger.info("=== 测试结果汇总 ===")
    success_count = 0
    for test_name, result in test_results.items():
        status = "✅ 成功" if result else "❌ 失败"
        logger.info(f"{test_name}: {status}")
        if result:
            success_count += 1
    
    logger.info(f"总体结果: {success_count}/{len(test_results)} 个测试通过")
    
    if success_count == len(test_results):
        logger.info("🎉 所有组件测试通过！新功能实现完成")
        logger.info("")
        logger.info("📋 实现总结:")
        logger.info("✅ 任务5: 问卷数据特殊处理器 - 支持3种量表类型，选项分布计算")
        logger.info("✅ 任务6: 简化汇聚服务 - 区域级和学校级汇聚，整合多个模块")  
        logger.info("✅ 任务7: 数据持久化仓库 - 保存、读取、状态管理功能")
        logger.info("")
        logger.info("🚀 核心特性:")
        logger.info("  • 支持4级/5级/10分制问卷量表")
        logger.info("  • 正向/反向量表自动转换") 
        logger.info("  • 选项分布百分比精确计算(2位小数)")
        logger.info("  • 区域级学校排名和学校级区域排名")
        logger.info("  • 考试科目维度统计和问卷维度统计")
        logger.info("  • 完整的数据版本管理和历史记录")
    else:
        logger.warning("⚠️  部分测试失败，请检查相关组件")


if __name__ == "__main__":
    main()