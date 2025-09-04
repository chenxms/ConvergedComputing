# 难度系数计算器测试
import pytest
import pandas as pd
import numpy as np
from app.calculation.calculators.difficulty_calculator import (
    DifficultyCalculator,
    calculate_difficulty_coefficient,
    classify_difficulty_level,
    batch_calculate_difficulty
)


class TestDifficultyCalculator:
    """难度系数计算器测试"""
    
    def setup_method(self):
        """测试初始化"""
        self.calculator = DifficultyCalculator()
    
    def test_basic_difficulty_calculation(self):
        """测试基本难度系数计算"""
        # 创建测试数据：平均分70，满分100，难度系数应为0.7（中等）
        scores = [80, 75, 70, 65, 60]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        assert 'difficulty_coefficient' in result
        assert result['difficulty_coefficient'] == 0.7
        assert result['average_score'] == 70.0
        assert result['max_score'] == 100
        assert result['difficulty_level'] == 'medium'
        assert result['sample_size'] == 5
    
    def test_easy_difficulty_level(self):
        """测试简单题目（难度系数>0.7）"""
        # 平均分85，难度系数0.85，应分类为easy
        scores = [90, 85, 80, 85, 85]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        assert result['difficulty_coefficient'] == 0.85
        assert result['difficulty_level'] == 'easy'
        assert 'easy' in result['interpretation']['en'].lower()
    
    def test_hard_difficulty_level(self):
        """测试困难题目（难度系数<0.3）"""
        # 平均分20，难度系数0.2，应分类为hard
        scores = [25, 20, 15, 20, 20]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        assert result['difficulty_coefficient'] == 0.2
        assert result['difficulty_level'] == 'hard'
        assert 'hard' in result['interpretation']['en'].lower()
    
    def test_medium_difficulty_level(self):
        """测试中等题目（难度系数0.3-0.7）"""
        # 平均分50，难度系数0.5，应分类为medium
        scores = [60, 50, 40, 50, 50]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        assert result['difficulty_coefficient'] == 0.5
        assert result['difficulty_level'] == 'medium'
    
    def test_different_max_scores(self):
        """测试不同满分情况"""
        # 测试满分150的情况
        scores = [120, 135, 105, 90, 75]  # 平均分105
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 150}
        
        result = self.calculator.calculate(data, config)
        
        assert result['difficulty_coefficient'] == 0.7
        assert result['max_score'] == 150
        assert result['average_score'] == 105.0
        assert result['difficulty_level'] == 'medium'
    
    def test_question_stats_calculation(self):
        """测试题目统计信息计算"""
        scores = [100, 80, 60, 40, 20, 0]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        stats = result['question_stats']
        
        assert stats['min_score'] == 0.0
        assert stats['max_score_achieved'] == 100.0
        assert stats['median_score'] == 50.0
        assert stats['perfect_score_rate'] == 1/6  # 1个满分
        assert stats['zero_score_rate'] == 1/6     # 1个0分
        assert 'score_distribution' in stats
    
    def test_score_distribution(self):
        """测试分数分布计算"""
        # 创建覆盖各分数段的数据
        scores = [95, 85, 75, 65, 55, 45, 35, 25, 15, 5]  # 每个分数段1个
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        distribution = result['question_stats']['score_distribution']
        
        # 每个分数段应该有0.1的比例
        assert len(distribution) == 5
        for rate in distribution.values():
            assert 0 <= rate <= 1
    
    def test_batch_calculation(self):
        """测试批量计算功能"""
        # 创建多题目数据
        data = pd.DataFrame({
            'score': [90, 85, 80, 70, 65, 60, 40, 35, 30],
            'question_id': ['Q1', 'Q1', 'Q1', 'Q2', 'Q2', 'Q2', 'Q3', 'Q3', 'Q3']
        })
        config = {'max_score': 100}
        
        result = self.calculator.calculate_batch_difficulty(data, config)
        
        # 应该有3个题目的结果
        assert 'Q1' in result
        assert 'Q2' in result  
        assert 'Q3' in result
        assert '_summary' in result
        
        # Q1: 平均分85，easy
        assert result['Q1']['difficulty_coefficient'] == 0.85
        assert result['Q1']['difficulty_level'] == 'easy'
        
        # Q2: 平均分65，medium
        assert result['Q2']['difficulty_coefficient'] == 0.65
        assert result['Q2']['difficulty_level'] == 'medium'
        
        # Q3: 平均分35，hard
        assert result['Q3']['difficulty_coefficient'] == 0.35
        assert result['Q3']['difficulty_level'] == 'medium'
    
    def test_batch_summary_statistics(self):
        """测试批量计算汇总统计"""
        data = pd.DataFrame({
            'score': [80, 60, 20] * 3,  # 每题1个样本：easy, medium, hard
            'question_id': ['Q1', 'Q2', 'Q3'] * 3
        })
        config = {'max_score': 100}
        
        result = self.calculator.calculate_batch_difficulty(data, config)
        summary = result['_summary']
        
        assert 'difficulty_statistics' in summary
        assert 'difficulty_distribution' in summary
        assert 'quality_assessment' in summary
        
        # 检查难度分布
        distribution = summary['difficulty_distribution']
        assert distribution['total_questions'] == 3
        assert distribution['easy_count'] + distribution['medium_count'] + distribution['hard_count'] == 3
    
    def test_quality_assessment(self):
        """测试试卷质量评估"""
        # 创建理想分布：40%易题，40%中等题，20%难题
        data = pd.DataFrame({
            'score': ([85] * 4 + [50] * 4 + [25] * 2) * 5,  # 重复数据增加样本量
            'question_id': (['E1', 'E2', 'E3', 'E4', 'M1', 'M2', 'M3', 'M4', 'H1', 'H2'] * 5)
        })
        config = {'max_score': 100}
        
        result = self.calculator.calculate_batch_difficulty(data, config)
        assessment = result['_summary']['quality_assessment']
        
        assert 'quality_level' in assessment
        assert 'suggestion' in assessment
        # 这个分布应该被评为good或excellent
        assert assessment['quality_level'] in ['good', 'excellent']
    
    def test_empty_data_validation(self):
        """测试空数据验证"""
        data = pd.DataFrame()
        config = {'max_score': 100}
        
        validation = self.calculator.validate_input(data, config)
        assert not validation['is_valid']
        assert '数据集为空' in validation['errors'][0]
    
    def test_missing_score_column_validation(self):
        """测试缺少分数列验证"""
        data = pd.DataFrame({'student_id': [1, 2, 3]})
        config = {'max_score': 100}
        
        validation = self.calculator.validate_input(data, config)
        assert not validation['is_valid']
        assert 'score' in validation['errors'][0]
    
    def test_invalid_max_score_validation(self):
        """测试无效满分验证"""
        data = pd.DataFrame({'score': [80, 70, 60]})
        config = {'max_score': -10}
        
        validation = self.calculator.validate_input(data, config)
        assert not validation['is_valid']
        assert '满分配置无效' in validation['errors'][0]
    
    def test_small_sample_warning(self):
        """测试小样本量警告"""
        data = pd.DataFrame({'score': [80, 70]})  # 只有2个样本
        config = {'max_score': 100}
        
        validation = self.calculator.validate_input(data, config)
        assert validation['is_valid']
        assert len(validation['warnings']) > 0
        assert '样本数量过少' in validation['warnings'][0]
    
    def test_out_of_range_scores_warning(self):
        """测试超出范围分数警告"""
        data = pd.DataFrame({'score': [120, 80, -10, 60]})  # 包含超出范围的分数
        config = {'max_score': 100}
        
        validation = self.calculator.validate_input(data, config)
        assert validation['is_valid']
        assert any('超出范围' in warning for warning in validation['warnings'])
    
    def test_null_scores_handling(self):
        """测试空值处理"""
        data = pd.DataFrame({'score': [80, None, 60, np.nan, 70]})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 应该忽略空值，只计算有效分数
        assert result['sample_size'] == 3
        assert result['average_score'] == 70.0
        assert result['difficulty_coefficient'] == 0.7
    
    def test_calculate_with_question_id(self):
        """测试包含题目ID的计算"""
        data = pd.DataFrame({'score': [80, 70, 60, 50]})
        config = {'max_score': 100, 'question_id': 'MATH001'}
        
        result = self.calculator.calculate(data, config)
        
        assert result['question_id'] == 'MATH001'
        assert result['difficulty_coefficient'] == 0.65
    
    def test_algorithm_info(self):
        """测试算法信息"""
        info = self.calculator.get_algorithm_info()
        
        assert info['name'] == 'DifficultyCoefficient'
        assert 'formula' in info
        assert '平均分 / 满分' in info['formula']
        assert 'classification' in info
        
    def test_perfect_scores_only(self):
        """测试全满分情况"""
        data = pd.DataFrame({'score': [100, 100, 100, 100]})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        assert result['difficulty_coefficient'] == 1.0
        assert result['difficulty_level'] == 'easy'
        assert result['question_stats']['perfect_score_rate'] == 1.0
    
    def test_zero_scores_only(self):
        """测试全零分情况"""
        data = pd.DataFrame({'score': [0, 0, 0, 0]})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        assert result['difficulty_coefficient'] == 0.0
        assert result['difficulty_level'] == 'hard'
        assert result['question_stats']['zero_score_rate'] == 1.0


class TestDifficultyCalculatorUtilityFunctions:
    """测试难度计算器辅助函数"""
    
    def test_calculate_difficulty_coefficient_function(self):
        """测试难度系数计算函数"""
        scores = [80, 70, 60, 50]
        coefficient = calculate_difficulty_coefficient(scores, 100)
        assert coefficient == 0.65
        
        # 测试列表输入
        scores_list = [90, 80, 70]
        coefficient = calculate_difficulty_coefficient(scores_list, 100)
        assert coefficient == 0.8
    
    def test_classify_difficulty_level_function(self):
        """测试难度等级分类函数"""
        assert classify_difficulty_level(0.8) == 'easy'
        assert classify_difficulty_level(0.5) == 'medium'
        assert classify_difficulty_level(0.2) == 'hard'
        assert classify_difficulty_level(0.7) == 'medium'  # 边界值
        assert classify_difficulty_level(0.3) == 'medium'  # 边界值
    
    def test_batch_calculate_difficulty_function(self):
        """测试批量难度计算函数"""
        data = pd.DataFrame({
            'score': [80, 70, 60, 90, 85, 80],
            'question_id': ['Q1', 'Q1', 'Q1', 'Q2', 'Q2', 'Q2']
        })
        
        result_df = batch_calculate_difficulty(data)
        
        assert len(result_df) == 2
        assert 'difficulty_coefficient' in result_df.columns
        assert 'difficulty_level' in result_df.columns
        assert 'average_score' in result_df.columns
        assert 'sample_size' in result_df.columns
        
        # Q1的难度系数应该是70/100=0.7
        q1_result = result_df[result_df['question_id'] == 'Q1'].iloc[0]
        assert q1_result['difficulty_coefficient'] == 0.7
        assert q1_result['difficulty_level'] == 'medium'
    
    def test_empty_scores_edge_case(self):
        """测试空分数边界情况"""
        empty_scores = pd.Series([], dtype=float)
        coefficient = calculate_difficulty_coefficient(empty_scores, 100)
        assert coefficient == 0.0


class TestDifficultyCalculatorEdgeCases:
    """测试难度计算器边界情况"""
    
    def setup_method(self):
        self.calculator = DifficultyCalculator()
    
    def test_single_score(self):
        """测试单个分数情况"""
        data = pd.DataFrame({'score': [75]})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        assert result['difficulty_coefficient'] == 0.75
        assert result['sample_size'] == 1
        assert result['question_stats']['std_dev'] == 0.0  # 单个值标准差为0
    
    def test_identical_scores(self):
        """测试相同分数情况"""
        data = pd.DataFrame({'score': [80, 80, 80, 80]})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        assert result['difficulty_coefficient'] == 0.8
        assert result['question_stats']['std_dev'] == 0.0
        assert result['question_stats']['score_variance'] == 0.0
    
    def test_extreme_max_score(self):
        """测试极端满分值"""
        data = pd.DataFrame({'score': [5, 4, 3, 2, 1]})
        config = {'max_score': 10}  # 小满分
        
        result = self.calculator.calculate(data, config)
        
        assert result['difficulty_coefficient'] == 0.3
        assert result['difficulty_level'] == 'medium'
        
        # 测试大满分
        data = pd.DataFrame({'score': [750, 700, 650]})
        config = {'max_score': 1000}
        
        result = self.calculator.calculate(data, config)
        
        assert result['difficulty_coefficient'] == 0.7
        assert result['difficulty_level'] == 'medium'
    
    def test_floating_point_precision(self):
        """测试浮点数精度"""
        # 创建可能导致精度问题的数据
        data = pd.DataFrame({'score': [66.66666, 33.33333, 100.00000]})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 结果应该是合理的浮点数
        assert isinstance(result['difficulty_coefficient'], float)
        assert 0 <= result['difficulty_coefficient'] <= 1
        assert abs(result['difficulty_coefficient'] - 0.6666666333333334) < 1e-10
    
    def test_batch_with_error_questions(self):
        """测试批量计算中包含错误题目"""
        # 创建包含无效数据的批量数据
        data = pd.DataFrame({
            'score': [80, 70, None, None, 90, 85],  # Q2全为空值
            'question_id': ['Q1', 'Q1', 'Q2', 'Q2', 'Q3', 'Q3']
        })
        config = {'max_score': 100}
        
        result = self.calculator.calculate_batch_difficulty(data, config)
        
        # Q1和Q3应该成功计算
        assert 'Q1' in result
        assert 'Q3' in result
        assert result['Q1']['difficulty_coefficient'] == 0.75
        assert result['Q3']['difficulty_coefficient'] == 0.875
        
        # Q2应该出错
        assert 'Q2' in result
        assert 'error' in result['Q2']
    
    @pytest.mark.parametrize("scores,max_score,expected_level", [
        ([100, 90, 80], 100, 'easy'),      # 0.9 > 0.7
        ([70, 60, 50], 100, 'medium'),     # 0.6 in [0.3, 0.7]
        ([30, 20, 10], 100, 'hard'),       # 0.2 < 0.3
        ([70, 70, 70], 100, 'medium'),     # 0.7 boundary
        ([30, 30, 30], 100, 'medium'),     # 0.3 boundary
        ([80, 70], 100, 'easy'),           # 0.75 > 0.7
    ])
    def test_difficulty_level_classification_parametrized(self, scores, max_score, expected_level):
        """参数化测试难度等级分类"""
        data = pd.DataFrame({'score': scores})
        config = {'max_score': max_score}
        
        result = self.calculator.calculate(data, config)
        
        assert result['difficulty_level'] == expected_level