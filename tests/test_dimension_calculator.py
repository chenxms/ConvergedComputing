# 维度计算器测试
import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.orm import Session

from app.calculation.calculators.dimension_calculator import (
    DimensionCalculator,
    DimensionDataProvider,
    DimensionStatisticsStrategy,
    DimensionMapping,
    DimensionStats,
    DimensionType,
    create_dimension_calculator,
    create_dimension_statistics_strategy
)


class TestDimensionMapping:
    """测试维度映射数据结构"""
    
    def test_dimension_mapping_creation(self):
        """测试维度映射创建"""
        mapping = DimensionMapping(
            question_id="Q001",
            dimension_type="knowledge_point",
            dimension_value="数学运算",
            hierarchy_level=1,
            parent_dimension=None,
            weight=1.0
        )
        
        assert mapping.question_id == "Q001"
        assert mapping.dimension_type == "knowledge_point"
        assert mapping.dimension_value == "数学运算"
        assert mapping.hierarchy_level == 1
        assert mapping.weight == 1.0


class TestDimensionDataProvider:
    """测试维度数据提供者"""
    
    @pytest.fixture
    def mock_db_session(self):
        """模拟数据库会话"""
        mock_session = Mock(spec=Session)
        mock_result = Mock()
        mock_result.fetchall.return_value = [
            Mock(
                question_id="Q001",
                dimension_type="knowledge_point",
                dimension_value="数学运算",
                hierarchy_level=1,
                parent_dimension=None,
                weight=1.0,
                metadata=None
            ),
            Mock(
                question_id="Q002",
                dimension_type="knowledge_point", 
                dimension_value="逻辑推理",
                hierarchy_level=1,
                parent_dimension=None,
                weight=1.0,
                metadata=None
            )
        ]
        mock_session.execute.return_value = mock_result
        return mock_session
    
    def test_get_dimension_mappings(self, mock_db_session):
        """测试获取维度映射"""
        provider = DimensionDataProvider(mock_db_session)
        
        mappings = provider.get_dimension_mappings("BATCH_001")
        
        assert len(mappings) == 2
        assert mappings[0].question_id == "Q001"
        assert mappings[0].dimension_value == "数学运算"
        assert mappings[1].question_id == "Q002"
        assert mappings[1].dimension_value == "逻辑推理"
    
    def test_get_dimension_mappings_with_filter(self, mock_db_session):
        """测试带过滤条件获取维度映射"""
        provider = DimensionDataProvider(mock_db_session)
        
        mappings = provider.get_dimension_mappings(
            "BATCH_001", 
            dimension_types=["knowledge_point"]
        )
        
        # 验证SQL查询包含过滤条件
        call_args = mock_db_session.execute.call_args
        assert ":type_0" in str(call_args)
    
    @patch('pandas.read_sql')
    def test_get_student_score_data(self, mock_read_sql, mock_db_session):
        """测试获取学生答题数据"""
        # 准备测试数据
        test_data = pd.DataFrame({
            'student_id': ['S001', 'S002', 'S001', 'S002'],
            'question_id': ['Q001', 'Q001', 'Q002', 'Q002'], 
            'score': [85, 75, 90, 80],
            'max_score': [100, 100, 100, 100],
            'grade_level': ['7th_grade', '7th_grade', '7th_grade', '7th_grade'],
            'school_id': ['SCH001', 'SCH001', 'SCH001', 'SCH001'],
            'school_name': ['第一中学', '第一中学', '第一中学', '第一中学']
        })
        mock_read_sql.return_value = test_data
        
        provider = DimensionDataProvider(mock_db_session)
        result = provider.get_student_score_data("BATCH_001")
        
        assert len(result) == 4
        assert 'student_id' in result.columns
        assert 'score' in result.columns
        assert result['score'].dtype in [np.float64, float]


class TestDimensionCalculator:
    """测试维度计算器"""
    
    @pytest.fixture
    def sample_mappings(self):
        """样本维度映射数据"""
        return [
            DimensionMapping(
                question_id="Q001",
                dimension_type="knowledge_point",
                dimension_value="数学运算",
                hierarchy_level=1,
                weight=1.0
            ),
            DimensionMapping(
                question_id="Q002", 
                dimension_type="knowledge_point",
                dimension_value="数学运算",
                hierarchy_level=1,
                weight=1.0
            ),
            DimensionMapping(
                question_id="Q003",
                dimension_type="knowledge_point", 
                dimension_value="逻辑推理",
                hierarchy_level=1,
                weight=1.0
            ),
            DimensionMapping(
                question_id="Q004",
                dimension_type="ability",
                dimension_value="分析能力",
                hierarchy_level=1,
                weight=1.0
            )
        ]
    
    @pytest.fixture
    def sample_score_data(self):
        """样本学生答题数据"""
        return pd.DataFrame({
            'student_id': ['S001'] * 4 + ['S002'] * 4 + ['S003'] * 4,
            'question_id': ['Q001', 'Q002', 'Q003', 'Q004'] * 3,
            'score': [85, 90, 75, 80, 70, 85, 80, 75, 95, 88, 92, 90],
            'max_score': [100, 100, 100, 100] * 3,
            'grade_level': ['7th_grade'] * 12,
            'school_id': ['SCH001'] * 12,
            'school_name': ['第一中学'] * 12
        })
    
    @pytest.fixture
    def mock_data_provider(self, sample_mappings, sample_score_data):
        """模拟数据提供者"""
        provider = Mock()
        provider.get_dimension_mappings.return_value = sample_mappings
        provider.get_student_score_data.return_value = sample_score_data
        return provider
    
    def test_group_mappings_by_type(self, mock_data_provider):
        """测试按维度类型分组映射"""
        calculator = DimensionCalculator(mock_data_provider)
        mappings = mock_data_provider.get_dimension_mappings("BATCH_001")
        
        grouped = calculator._group_mappings_by_type(mappings)
        
        assert "knowledge_point" in grouped
        assert "ability" in grouped
        assert len(grouped["knowledge_point"]) == 3
        assert len(grouped["ability"]) == 1
    
    def test_group_mappings_by_value_and_level(self, mock_data_provider):
        """测试按维度值和层级分组映射"""
        calculator = DimensionCalculator(mock_data_provider)
        mappings = mock_data_provider.get_dimension_mappings("BATCH_001")
        
        grouped = calculator._group_mappings_by_value_and_level(mappings)
        
        assert ("数学运算", 1) in grouped
        assert ("逻辑推理", 1) in grouped
        assert ("分析能力", 1) in grouped
        assert len(grouped[("数学运算", 1)]) == 2
    
    def test_aggregate_student_scores_with_weights(self, mock_data_provider, sample_mappings, sample_score_data):
        """测试按权重聚合学生分数"""
        calculator = DimensionCalculator(mock_data_provider)
        
        # 过滤数学运算相关数据
        math_mappings = [m for m in sample_mappings if m.dimension_value == "数学运算"]
        math_questions = [m.question_id for m in math_mappings]
        math_data = sample_score_data[sample_score_data['question_id'].isin(math_questions)]
        
        result = calculator._aggregate_student_scores_with_weights(math_data, math_mappings)
        
        assert len(result) == 3  # 3个学生
        assert 'total_score' in result.columns
        assert 'student_id' in result.columns
        
        # 验证第一个学生的总分计算 (85 + 90 = 175)
        student_1_score = result[result['student_id'] == 'S001']['total_score'].iloc[0]
        assert student_1_score == 175
    
    def test_calculate_basic_stats(self, mock_data_provider):
        """测试基础统计计算"""
        calculator = DimensionCalculator(mock_data_provider)
        scores = pd.Series([85, 90, 75, 80, 70])
        
        stats = calculator._calculate_basic_stats(scores)
        
        assert stats['count'] == 5
        assert stats['mean'] == 80.0
        assert stats['min'] == 70.0
        assert stats['max'] == 90.0
        assert stats['std'] > 0
    
    def test_calculate_basic_stats_empty(self, mock_data_provider):
        """测试空数据的基础统计计算"""
        calculator = DimensionCalculator(mock_data_provider)
        scores = pd.Series([], dtype=float)
        
        stats = calculator._calculate_basic_stats(scores)
        
        assert stats['count'] == 0
        assert stats['mean'] == 0
        assert stats['std'] == 0
    
    def test_calculate_educational_metrics(self, mock_data_provider):
        """测试教育指标计算"""
        calculator = DimensionCalculator(mock_data_provider)
        scores = pd.Series([95, 85, 75, 65, 55])  # 分别对应优秀、良好、及格、及格、不及格
        max_score = 100.0
        grade_level = "7th_grade"
        
        metrics = calculator._calculate_educational_metrics(scores, max_score, grade_level)
        
        assert 'difficulty_coefficient' in metrics
        assert 'grade_distribution' in metrics
        assert 'pass_rate' in metrics
        assert 'excellent_rate' in metrics
        
        # 验证难度系数
        expected_difficulty = scores.mean() / max_score
        assert abs(metrics['difficulty_coefficient'] - expected_difficulty) < 0.001
        
        # 验证等级分布（初中标准）
        grade_dist = metrics['grade_distribution']
        assert 'A' in grade_dist  # 初中使用A,B,C,D等级
        assert grade_dist['A']['count'] == 1  # 95分 >= 85%
    
    def test_calculate_percentiles(self, mock_data_provider):
        """测试百分位数计算"""
        calculator = DimensionCalculator(mock_data_provider)
        scores = pd.Series(range(1, 101))  # 1-100的连续分数
        
        percentiles = calculator._calculate_percentiles(scores)
        
        assert 'P10' in percentiles
        assert 'P25' in percentiles
        assert 'P50' in percentiles
        assert 'P75' in percentiles
        assert 'P90' in percentiles
        
        # 验证中位数
        assert percentiles['P50'] == 50.0
    
    def test_calculate_discrimination(self, mock_data_provider):
        """测试区分度计算"""
        calculator = DimensionCalculator(mock_data_provider)
        # 创建有明显区分度的分数分布
        scores = pd.Series([95, 90, 85, 80, 75, 70, 65, 60, 55, 50] * 3)  # 30个分数
        max_score = 100.0
        
        discrimination = calculator._calculate_discrimination(scores, max_score)
        
        assert 'discrimination_index' in discrimination
        assert 'interpretation' in discrimination
        assert 'high_group_mean' in discrimination
        assert 'low_group_mean' in discrimination
        
        # 区分度应该大于0（高分组平均分 > 低分组平均分）
        assert discrimination['discrimination_index'] > 0
        assert discrimination['high_group_mean'] > discrimination['low_group_mean']
    
    def test_calculate_discrimination_insufficient_data(self, mock_data_provider):
        """测试数据不足时的区分度计算"""
        calculator = DimensionCalculator(mock_data_provider)
        scores = pd.Series([80, 75, 70])  # 只有3个分数
        max_score = 100.0
        
        discrimination = calculator._calculate_discrimination(scores, max_score)
        
        assert discrimination['discrimination_index'] == 0
        assert discrimination['interpretation'] == 'insufficient_data'
    
    def test_is_primary_grade(self, mock_data_provider):
        """测试年级判断"""
        calculator = DimensionCalculator(mock_data_provider)
        
        assert calculator._is_primary_grade("1st_grade") == True
        assert calculator._is_primary_grade("6th_grade") == True
        assert calculator._is_primary_grade("7th_grade") == False
        assert calculator._is_primary_grade("9th_grade") == False
    
    @patch.object(DimensionDataProvider, 'get_dimension_mappings')
    @patch.object(DimensionDataProvider, 'get_student_score_data')
    def test_calculate_dimension_statistics_integration(self, mock_score_data, mock_mappings, 
                                                       sample_mappings, sample_score_data):
        """测试维度统计计算的集成流程"""
        # 设置mock返回值
        mock_mappings.return_value = sample_mappings
        mock_score_data.return_value = sample_score_data
        
        calculator = DimensionCalculator()
        result = calculator.calculate_dimension_statistics("BATCH_001")
        
        assert 'batch_code' in result
        assert 'dimension_statistics' in result
        assert 'metadata' in result
        
        # 验证维度类型统计
        dim_stats = result['dimension_statistics']
        assert 'knowledge_point' in dim_stats
        assert 'ability' in dim_stats
        
        # 验证元数据
        metadata = result['metadata']
        assert metadata['total_questions'] == 4
        assert metadata['total_students'] == 3


class TestDimensionStatisticsStrategy:
    """测试维度统计策略"""
    
    @patch.object(DimensionCalculator, 'calculate_dimension_statistics')
    def test_calculate(self, mock_calculate):
        """测试策略计算方法"""
        mock_calculate.return_value = {
            'batch_code': 'BATCH_001',
            'dimension_statistics': {}
        }
        
        strategy = DimensionStatisticsStrategy()
        config = {
            'batch_code': 'BATCH_001',
            'dimension_types': ['knowledge_point'],
            'aggregation_level': 'regional'
        }
        
        result = strategy.calculate(pd.DataFrame(), config)
        
        assert result['batch_code'] == 'BATCH_001'
        mock_calculate.assert_called_once_with('BATCH_001', ['knowledge_point'], 'regional')
    
    def test_validate_input_valid(self):
        """测试有效输入验证"""
        strategy = DimensionStatisticsStrategy()
        config = {
            'batch_code': 'BATCH_001',
            'dimension_types': ['knowledge_point']
        }
        
        result = strategy.validate_input(pd.DataFrame(), config)
        
        assert result['is_valid'] == True
        assert len(result['errors']) == 0
    
    def test_validate_input_missing_batch_code(self):
        """测试缺少batch_code的输入验证"""
        strategy = DimensionStatisticsStrategy()
        config = {}
        
        result = strategy.validate_input(pd.DataFrame(), config)
        
        assert result['is_valid'] == False
        assert "缺少必需配置: batch_code" in result['errors']
    
    def test_validate_input_invalid_dimension_types(self):
        """测试无效dimension_types的输入验证"""
        strategy = DimensionStatisticsStrategy()
        config = {
            'batch_code': 'BATCH_001',
            'dimension_types': 'not_a_list'  # 应该是列表
        }
        
        result = strategy.validate_input(pd.DataFrame(), config)
        
        assert "dimension_types应为列表格式" in result['warnings']
    
    def test_get_algorithm_info(self):
        """测试算法信息获取"""
        strategy = DimensionStatisticsStrategy()
        
        info = strategy.get_algorithm_info()
        
        assert info['name'] == 'DimensionStatistics'
        assert info['version'] == '1.0'
        assert 'features' in info
        assert 'database_tables' in info
        assert 'question_dimension_mapping' in info['database_tables']


class TestFactoryFunctions:
    """测试工厂函数"""
    
    def test_create_dimension_calculator(self):
        """测试创建维度计算器"""
        calculator = create_dimension_calculator()
        
        assert isinstance(calculator, DimensionCalculator)
        assert hasattr(calculator, 'data_provider')
    
    def test_create_dimension_statistics_strategy(self):
        """测试创建维度统计策略"""
        strategy = create_dimension_statistics_strategy()
        
        assert isinstance(strategy, DimensionStatisticsStrategy)
        assert hasattr(strategy, 'calculator')


# 性能测试
class TestDimensionCalculatorPerformance:
    """维度计算器性能测试"""
    
    @pytest.mark.skip(reason="性能测试，仅在需要时运行")
    def test_large_dataset_performance(self):
        """测试大数据集性能"""
        import time
        
        # 创建大量测试数据
        n_students = 10000
        n_questions = 100
        
        mappings = [
            DimensionMapping(
                question_id=f"Q{i:03d}",
                dimension_type="knowledge_point",
                dimension_value=f"维度{i % 10}",
                hierarchy_level=1,
                weight=1.0
            ) for i in range(n_questions)
        ]
        
        score_data = pd.DataFrame({
            'student_id': [f"S{i:05d}" for i in range(n_students) for _ in range(n_questions)],
            'question_id': [f"Q{j:03d}" for _ in range(n_students) for j in range(n_questions)],
            'score': np.random.randint(60, 100, n_students * n_questions),
            'max_score': [100] * (n_students * n_questions),
            'grade_level': ['7th_grade'] * (n_students * n_questions),
            'school_id': ['SCH001'] * (n_students * n_questions),
            'school_name': ['测试学校'] * (n_students * n_questions)
        })
        
        mock_provider = Mock()
        mock_provider.get_dimension_mappings.return_value = mappings
        mock_provider.get_student_score_data.return_value = score_data
        
        calculator = DimensionCalculator(mock_provider)
        
        start_time = time.time()
        result = calculator.calculate_dimension_statistics("BATCH_001")
        execution_time = time.time() - start_time
        
        print(f"处理 {n_students} 学生 {n_questions} 题目耗时: {execution_time:.2f} 秒")
        
        # 性能要求：10万学生数据应在30分钟内完成
        assert execution_time < 1800  # 30分钟 = 1800秒（为测试调整为更短时间）
        assert result['metadata']['total_students'] == n_students
        assert result['metadata']['total_questions'] == n_questions


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])