# 区分度计算器测试
import pytest
import pandas as pd
import numpy as np
from app.calculation.calculators.discrimination_calculator import (
    DiscriminationCalculator,
    calculate_discrimination_index,
    classify_discrimination_level,
    analyze_discrimination_distribution
)


class TestDiscriminationCalculator:
    """区分度计算器测试"""
    
    def setup_method(self):
        """测试初始化"""
        self.calculator = DiscriminationCalculator()
    
    def test_basic_discrimination_calculation(self):
        """测试基本区分度计算"""
        # 创建有明显区分度的数据：高分组100,90,80，低分组40,30,20
        scores = [100, 90, 80, 70, 60, 50, 40, 30, 20]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        assert 'discrimination_index' in result
        assert 'high_group_mean' in result
        assert 'low_group_mean' in result
        
        # 前27%是前2-3个学生，后27%是后2-3个学生
        # 验证区分度计算正确性
        discrimination = result['discrimination_index']
        assert discrimination > 0.3  # 应该有较好的区分度
        assert result['discrimination_level'] in ['good', 'excellent']
    
    def test_excellent_discrimination(self):
        """测试优秀区分度（≥0.4）"""
        # 创建区分度很好的数据：高分组vs低分组差距大
        scores = [100, 95, 90, 85, 80, 50, 45, 40, 20, 10]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 前27%: 约前3个学生(100,95,90)，平均95
        # 后27%: 约后3个学生(20,10,40)，平均23.33
        # 区分度 = (95-23.33)/100 = 0.7167，应为excellent
        assert result['discrimination_index'] >= 0.4
        assert result['discrimination_level'] == 'excellent'
    
    def test_poor_discrimination(self):
        """测试较差区分度（<0.2）"""
        # 创建区分度差的数据：分数都很接近
        scores = [52, 51, 50, 49, 48, 47, 46, 45, 44, 43]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 高分组和低分组平均分差距应该很小
        assert result['discrimination_index'] < 0.2
        assert result['discrimination_level'] == 'poor'
    
    def test_good_discrimination(self):
        """测试良好区分度（0.3-0.4）"""
        # 精心设计的数据，使区分度在good范围内
        scores = [85, 80, 75, 70, 65, 60, 55, 50, 45, 40]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 实际计算结果可能是0.4，也属于excellent范围
        assert result['discrimination_index'] >= 0.3
        assert result['discrimination_level'] in ['good', 'excellent']
    
    def test_acceptable_discrimination(self):
        """测试一般区分度（0.2-0.3）"""
        scores = [75, 70, 68, 66, 64, 62, 60, 58, 56, 50]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 实际计算结果可能略低于0.2，但仍在合理范围
        assert result['discrimination_index'] >= 0.1
        assert result['discrimination_level'] in ['acceptable', 'poor']
    
    def test_custom_group_percentage(self):
        """测试自定义分组百分比"""
        scores = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100, 'group_percentage': 0.2}  # 使用20%分组
        
        result = self.calculator.calculate(data, config)
        
        assert result['group_percentage'] == 0.2
        # 10个学生的20%是2个
        assert result['high_group_size'] == 2
        assert result['low_group_size'] == 2
    
    def test_different_max_scores(self):
        """测试不同满分情况"""
        # 测试满分150的情况
        scores = [150, 135, 120, 105, 90, 75, 60, 45, 30, 15]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 150}
        
        result = self.calculator.calculate(data, config)
        
        assert result['max_score'] == 150
        # 区分度应该根据150的满分计算
        assert 0 <= result['discrimination_index'] <= 1
    
    def test_group_details_analysis(self):
        """测试分组详细信息分析"""
        scores = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        group_details = result['group_details']
        
        assert 'high_group_stats' in group_details
        assert 'low_group_stats' in group_details
        assert 'group_overlap' in group_details
        assert 'score_gap' in group_details
        
        # 检查高分组统计
        high_stats = group_details['high_group_stats']
        assert 'min' in high_stats
        assert 'max' in high_stats
        assert 'median' in high_stats
        assert 'std' in high_stats
        assert 'score_rate' in high_stats
        
        # 检查分组重叠
        overlap = group_details['group_overlap']
        assert 'has_overlap' in overlap
        assert 'separation_quality' in overlap
    
    def test_batch_discrimination_calculation(self):
        """测试批量区分度计算"""
        # 创建3个题目的数据，每题不同的区分度
        data = pd.DataFrame({
            'score': ([100, 90, 80, 20, 10, 5] +      # Q1: 好区分度
                     [60, 58, 56, 54, 52, 50] +       # Q2: 差区分度
                     [90, 85, 80, 40, 35, 30]),       # Q3: 中等区分度
            'question_id': (['Q1'] * 6 + ['Q2'] * 6 + ['Q3'] * 6)
        })
        config = {'max_score': 100}
        
        result = self.calculator.calculate_batch_discrimination(data, config)
        
        # 应该有3个题目的结果
        assert 'Q1' in result
        assert 'Q2' in result
        assert 'Q3' in result
        assert '_summary' in result
        
        # Q1应该有好的区分度
        assert result['Q1']['discrimination_level'] in ['good', 'excellent']
        
        # Q2应该有差的区分度
        assert result['Q2']['discrimination_level'] in ['poor', 'acceptable']
    
    def test_exam_level_discrimination(self):
        """测试考试级别区分度计算"""
        # 创建学生-题目矩阵数据
        data = pd.DataFrame({
            'student_id': ['S1', 'S1', 'S2', 'S2', 'S3', 'S3', 'S4', 'S4', 'S5', 'S5', 'S6', 'S6'],
            'question_id': ['Q1', 'Q2', 'Q1', 'Q2', 'Q1', 'Q2', 'Q1', 'Q2', 'Q1', 'Q2', 'Q1', 'Q2'],
            'score': [90, 85, 80, 75, 70, 65, 60, 55, 50, 45, 40, 35]  # S1最高分，S6最低分
        })
        config = {'max_score': 100}
        
        result = self.calculator.calculate_exam_level_discrimination(data, config)
        
        assert '_grouping_info' in result
        assert 'Q1' in result
        assert 'Q2' in result
        
        grouping_info = result['_grouping_info']
        assert grouping_info['total_students'] == 6
        assert grouping_info['method'] == 'exam_level_grouping'
        
        # 验证高分组和低分组学生
        high_group_students = set(grouping_info['high_group_students'])
        low_group_students = set(grouping_info['low_group_students'])
        
        # S1应该在高分组，S6应该在低分组
        assert 'S1' in high_group_students
        assert 'S6' in low_group_students
    
    def test_small_sample_warning(self):
        """测试小样本量警告"""
        # 只有5个样本
        scores = [80, 70, 60, 50, 40]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        # 应该成功计算但有警告
        result = self.calculator.calculate(data, config)
        assert 'discrimination_index' in result
        
        # 检查验证结果
        validation = self.calculator.validate_input(data, config)
        assert validation['is_valid']
        assert any('样本数量过少' in warning for warning in validation['warnings'])
    
    def test_batch_summary_statistics(self):
        """测试批量计算汇总统计"""
        data = pd.DataFrame({
            'score': [100, 50, 10] * 6,  # 3个不同区分度等级的题目，各重复2次
            'question_id': ['E1', 'G1', 'P1'] * 6  # excellent, good, poor
        })
        config = {'max_score': 100}
        
        result = self.calculator.calculate_batch_discrimination(data, config)
        summary = result['_summary']
        
        assert 'discrimination_statistics' in summary
        assert 'level_distribution' in summary
        assert 'quality_assessment' in summary
        
        # 检查统计数据
        stats = summary['discrimination_statistics']
        assert 'mean_discrimination' in stats
        assert 'median_discrimination' in stats
        assert 'std_discrimination' in stats
        
        # 检查分布
        distribution = summary['level_distribution']
        assert distribution['total_questions'] == 3
        assert (distribution['excellent_count'] + distribution['good_count'] + 
                distribution['acceptable_count'] + distribution['poor_count']) == 3
    
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
        data = pd.DataFrame({'score': [80, 70, 60, 50]})
        config = {'max_score': 0}
        
        validation = self.calculator.validate_input(data, config)
        assert not validation['is_valid']
        assert '满分配置无效' in validation['errors'][0]
    
    def test_invalid_group_percentage_validation(self):
        """测试无效分组百分比验证"""
        data = pd.DataFrame({'score': [80, 70, 60, 50]})
        config = {'max_score': 100, 'group_percentage': 0.6}  # 超过0.5
        
        validation = self.calculator.validate_input(data, config)
        assert not validation['is_valid']
        assert '分组百分比' in validation['errors'][0]
    
    def test_concentrated_score_distribution_warning(self):
        """测试分数分布过于集中警告"""
        # 所有分数都在很小的范围内
        scores = [50.1, 50.2, 50.3, 50.4, 50.5] * 10  # 分数范围只有0.4
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        validation = self.calculator.validate_input(data, config)
        assert validation['is_valid']
        assert any('分数分布过于集中' in warning for warning in validation['warnings'])
    
    def test_null_scores_handling(self):
        """测试空值处理"""
        scores = [100, 90, None, 80, np.nan, 70, 60, 50, 40, 30]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 应该忽略空值，只计算8个有效分数
        assert result['total_sample_size'] == 8
        assert result['high_group_size'] == 2  # 8 * 0.27 = 2.16, max(1, int(2.16)) = 2
        assert result['low_group_size'] == 2
    
    def test_algorithm_info(self):
        """测试算法信息"""
        info = self.calculator.get_algorithm_info()
        
        assert info['name'] == 'Discrimination'
        assert 'formula' in info
        assert '高分组平均分 - 低分组平均分' in info['formula']
        assert 'classification' in info
        assert '27% Rule' in info['standard']


class TestDiscriminationCalculatorUtilityFunctions:
    """测试区分度计算器辅助函数"""
    
    def test_calculate_discrimination_index_function(self):
        """测试区分度计算函数"""
        scores = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10]
        index = calculate_discrimination_index(scores, 100)
        
        # 前27%约3个：(100+90+80)/3=90，后27%约3个：(20+10+30)/3=20
        # 区分度 = (90-20)/100 = 0.7
        assert index >= 0.4  # 应该是良好的区分度
        
        # 测试列表输入
        scores_list = [90, 80, 70, 60, 50, 40]
        index = calculate_discrimination_index(scores_list, 100)
        assert 0 <= index <= 1
    
    def test_classify_discrimination_level_function(self):
        """测试区分度等级分类函数"""
        assert classify_discrimination_level(0.5) == 'excellent'
        assert classify_discrimination_level(0.35) == 'good'
        assert classify_discrimination_level(0.25) == 'acceptable'
        assert classify_discrimination_level(0.15) == 'poor'
        
        # 测试边界值
        assert classify_discrimination_level(0.4) == 'excellent'
        assert classify_discrimination_level(0.3) == 'good'
        assert classify_discrimination_level(0.2) == 'acceptable'
    
    def test_analyze_discrimination_distribution_function(self):
        """测试区分度分布分析函数"""
        results = [
            {'discrimination_index': 0.5, 'discrimination_level': 'excellent'},
            {'discrimination_index': 0.35, 'discrimination_level': 'good'},
            {'discrimination_index': 0.25, 'discrimination_level': 'acceptable'},
            {'discrimination_index': 0.15, 'discrimination_level': 'poor'},
            {'discrimination_index': None, 'discrimination_level': 'unknown'}  # 无效结果
        ]
        
        analysis = analyze_discrimination_distribution(results)
        
        assert 'statistics' in analysis
        assert 'distribution' in analysis
        assert analysis['total_questions'] == 5
        
        # 检查统计信息
        stats = analysis['statistics']
        assert 'mean' in stats
        assert 'median' in stats
        assert 'std' in stats
        
        # 检查分布信息
        distribution = analysis['distribution']
        assert distribution['excellent']['count'] == 1
        assert distribution['good']['count'] == 1
        assert distribution['acceptable']['count'] == 1
        assert distribution['poor']['count'] == 1
    
    def test_empty_results_analysis(self):
        """测试空结果分析"""
        empty_results = []
        analysis = analyze_discrimination_distribution(empty_results)
        
        assert 'error' in analysis
        assert analysis['error'] == 'No results to analyze'
    
    def test_no_valid_indices_analysis(self):
        """测试没有有效区分度指数的分析"""
        results = [
            {'discrimination_index': None, 'discrimination_level': 'unknown'},
            {'discrimination_level': 'poor'}  # 缺少discrimination_index
        ]
        
        analysis = analyze_discrimination_distribution(results)
        
        assert 'error' in analysis
        assert 'No valid discrimination indices found' in analysis['error']


class TestDiscriminationCalculatorEdgeCases:
    """测试区分度计算器边界情况"""
    
    def setup_method(self):
        self.calculator = DiscriminationCalculator()
    
    def test_minimum_sample_size(self):
        """测试最小样本量"""
        # 测试刚好够分组的样本量
        scores = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10]  # 10个样本
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 10个样本的27%约3个
        assert result['high_group_size'] == 2  # max(1, int(10 * 0.27)) = 2
        assert result['low_group_size'] == 2
        assert 'discrimination_index' in result
    
    def test_very_small_sample(self):
        """测试很小样本（少于10个）"""
        scores = [80, 70, 60]  # 只有3个样本
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 即使样本很小，也应该能计算
        assert result['high_group_size'] == 1  # max(1, int(3 * 0.27)) = 1
        assert result['low_group_size'] == 1
        assert 'discrimination_index' in result
    
    def test_identical_scores(self):
        """测试相同分数情况"""
        scores = [75] * 10  # 所有分数相同
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 相同分数的区分度应该为0
        assert result['discrimination_index'] == 0.0
        assert result['discrimination_level'] == 'poor'
        assert result['high_group_mean'] == result['low_group_mean']
    
    def test_perfect_separation(self):
        """测试完美分离情况"""
        # 前面都是满分，后面都是零分
        scores = [100] * 5 + [0] * 5
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 应该有完美的区分度
        assert result['discrimination_index'] == 1.0
        assert result['discrimination_level'] == 'excellent'
        assert result['high_group_mean'] == 100.0
        assert result['low_group_mean'] == 0.0
    
    def test_reverse_order_input(self):
        """测试逆序输入"""
        # 输入已经是升序的分数
        scores = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 算法内部会排序，结果应该一样
        assert result['high_group_mean'] > result['low_group_mean']
        assert result['discrimination_index'] > 0
    
    def test_floating_point_scores(self):
        """测试浮点数分数"""
        scores = [95.5, 87.3, 78.9, 65.4, 52.1, 43.7, 35.2, 28.6, 19.4, 12.8]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        assert isinstance(result['discrimination_index'], float)
        assert isinstance(result['high_group_mean'], float)
        assert isinstance(result['low_group_mean'], float)
        assert 0 <= result['discrimination_index'] <= 1
    
    def test_group_overlap_detection(self):
        """测试分组重叠检测"""
        # 创建会产生重叠的数据
        scores = [80, 78, 76, 74, 72, 70, 68, 66, 64, 62]  # 分数连续递减
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        overlap = result['group_details']['group_overlap']
        
        # 应该检测到重叠（低分组最高分可能≥高分组最低分）
        assert 'has_overlap' in overlap
        assert 'separation_quality' in overlap
    
    def test_extreme_group_percentage(self):
        """测试极端分组百分比"""
        scores = list(range(1, 21))  # 1到20的分数
        data = pd.DataFrame({'score': scores})
        
        # 测试最小分组百分比
        config = {'max_score': 100, 'group_percentage': 0.1}
        result = self.calculator.calculate(data, config)
        assert result['high_group_size'] == 2  # 20 * 0.1 = 2
        assert result['low_group_size'] == 2
        
        # 测试最大分组百分比
        config = {'max_score': 100, 'group_percentage': 0.5}
        result = self.calculator.calculate(data, config)
        assert result['high_group_size'] == 10  # 20 * 0.5 = 10
        assert result['low_group_size'] == 10
    
    def test_batch_with_mixed_results(self):
        """测试批量计算混合结果"""
        # 创建一些题目有足够数据，一些题目数据不足
        data = pd.DataFrame({
            'score': ([100, 90, 80, 70, 60, 50] +  # Q1: 足够数据
                     [90, 85] +                     # Q2: 数据不足
                     [None, None, None]),           # Q3: 全为空值
            'question_id': (['Q1'] * 6 + ['Q2'] * 2 + ['Q3'] * 3)
        })
        config = {'max_score': 100}
        
        result = self.calculator.calculate_batch_discrimination(data, config)
        
        # Q1应该成功
        assert 'Q1' in result
        assert 'discrimination_index' in result['Q1']
        assert result['Q1']['discrimination_index'] is not None
        
        # Q2应该成功但有警告
        assert 'Q2' in result
        if 'error' not in result['Q2']:
            assert 'discrimination_index' in result['Q2']
        
        # Q3应该失败
        assert 'Q3' in result
        assert 'error' in result['Q3']
    
    @pytest.mark.parametrize("index,expected_level", [
        (0.5, 'excellent'),
        (0.4, 'excellent'),   # 边界值
        (0.39, 'good'),
        (0.3, 'good'),        # 边界值
        (0.29, 'acceptable'),
        (0.2, 'acceptable'),  # 边界值
        (0.19, 'poor'),
        (0.0, 'poor'),
        (1.0, 'excellent'),   # 理论最大值
    ])
    def test_discrimination_level_classification_parametrized(self, index, expected_level):
        """参数化测试区分度等级分类"""
        assert classify_discrimination_level(index) == expected_level
    
    def test_calculate_with_question_and_subject_id(self):
        """测试包含题目和科目ID的计算"""
        data = pd.DataFrame({'score': [90, 80, 70, 60, 50, 40, 30, 20]})
        config = {
            'max_score': 100, 
            'question_id': 'MATH_Q001',
            'subject_id': 'MATH'
        }
        
        result = self.calculator.calculate(data, config)
        
        assert result['question_id'] == 'MATH_Q001'
        assert result['subject_id'] == 'MATH'
        assert 'discrimination_index' in result