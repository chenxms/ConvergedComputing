# 统计计算引擎测试
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch

from app.calculation.engine import (
    CalculationEngine, DataValidator, MemoryManager, ChunkProcessor,
    ParallelCalculationEngine, PerformanceMonitor
)
from app.calculation.formulas import (
    BasicStatisticsStrategy, EducationalPercentileStrategy, 
    EducationalMetricsStrategy, DiscriminationStrategy, AnomalyDetector,
    calculate_average, calculate_standard_deviation, calculate_pass_rate,
    calculate_excellent_rate, calculate_percentile, calculate_difficulty_coefficient,
    calculate_discrimination_index
)
from app.calculation.calculators import initialize_calculation_system


class TestBasicStatisticsStrategy:
    """基础统计策略测试"""
    
    def setup_method(self):
        self.strategy = BasicStatisticsStrategy()
        
    def test_basic_statistics_calculation(self):
        """测试基础统计计算"""
        # 构造测试数据
        scores = [85, 90, 78, 92, 88, 76, 95, 82, 89, 91]
        data = pd.DataFrame({'score': scores})
        config = {}
        
        result = self.strategy.calculate(data, config)
        
        # 验证基础指标
        assert result['count'] == 10
        assert abs(result['mean'] - 86.6) < 0.1
        assert abs(result['std'] - 5.99) < 0.1  # 样本标准差
        assert result['min'] == 76
        assert result['max'] == 95
        assert result['range'] == 19
        
    def test_single_value_statistics(self):
        """测试单个数值的统计计算"""
        data = pd.DataFrame({'score': [85]})
        config = {}
        
        result = self.strategy.calculate(data, config)
        
        assert result['count'] == 1
        assert result['mean'] == 85
        assert result['min'] == 85
        assert result['max'] == 85
        assert result['range'] == 0
        assert result['std'] == 0.0  # 单个值的标准差为0
        
    def test_identical_values_statistics(self):
        """测试相同分数的统计计算"""
        data = pd.DataFrame({'score': [80] * 100})
        config = {}
        
        result = self.strategy.calculate(data, config)
        
        assert result['count'] == 100
        assert result['mean'] == 80
        assert result['std'] == 0.0
        assert result['variance'] == 0.0
        assert result['mode'] == 80
        
    def test_input_validation_empty_data(self):
        """测试空数据验证"""
        data = pd.DataFrame()
        config = {}
        
        validation = self.strategy.validate_input(data, config)
        assert not validation['is_valid']
        assert "数据集为空" in validation['errors']
        
    def test_input_validation_missing_score_column(self):
        """测试缺失score列验证"""
        data = pd.DataFrame({'name': ['A', 'B', 'C']})
        config = {}
        
        validation = self.strategy.validate_input(data, config)
        assert not validation['is_valid']
        assert "缺少必需字段: score" in validation['errors']
        
    def test_input_validation_invalid_scores(self):
        """测试无效分数验证"""
        data = pd.DataFrame({'score': ['abc', None, '85', 'xyz', 90]})
        config = {}
        
        validation = self.strategy.validate_input(data, config)
        assert validation['is_valid']  # 部分数据有效
        assert len(validation['warnings']) > 0
        assert "无效分数值" in validation['warnings'][0]
        
    def test_algorithm_info(self):
        """测试算法信息"""
        info = self.strategy.get_algorithm_info()
        assert info['name'] == 'BasicStatistics'
        assert info['version'] == '1.0'
        assert 'std_formula' in info


class TestEducationalPercentileStrategy:
    """教育百分位数策略测试"""
    
    def setup_method(self):
        self.strategy = EducationalPercentileStrategy()
        
    def test_percentile_calculation_accuracy(self):
        """测试百分位数计算精度"""
        # 构造已知结果的测试数据
        scores = list(range(1, 101))  # 1到100的分数
        data = pd.DataFrame({'score': scores})
        config = {'percentiles': [25, 50, 75]}
        
        result = self.strategy.calculate(data, config)
        
        # 使用floor算法验证：floor(n * p / 100)
        # n=100, P25: floor(100 * 25 / 100) = floor(25) = 25, scores[24] = 25
        # n=100, P50: floor(100 * 50 / 100) = floor(50) = 50, scores[49] = 50  
        # n=100, P75: floor(100 * 75 / 100) = floor(75) = 75, scores[74] = 75
        assert result['P25'] == 25
        assert result['P50'] == 50
        assert result['P75'] == 75
        assert result['IQR'] == 50  # P75 - P25
        
    def test_percentile_edge_cases(self):
        """测试百分位数边界情况"""
        # 单个数据点
        data = pd.DataFrame({'score': [85]})
        config = {'percentiles': [25, 50, 75]}
        
        result = self.strategy.calculate(data, config)
        assert result['P25'] == result['P50'] == result['P75'] == 85
        
        # 两个数据点
        data = pd.DataFrame({'score': [80, 90]})
        result = self.strategy.calculate(data, config)
        # n=2, P25: floor(2 * 25 / 100) = floor(0.5) = 0, scores[0] = 80
        # n=2, P50: floor(2 * 50 / 100) = floor(1) = 1, scores[1] = 90
        assert result['P25'] == 80
        assert result['P50'] == 90
        assert result['P75'] == 90
        
    def test_percentile_with_duplicates(self):
        """测试重复分数的百分位数计算"""
        scores = [70, 70, 70, 80, 80, 90, 90, 90, 90]
        data = pd.DataFrame({'score': scores})
        config = {'percentiles': [50]}
        
        result = self.strategy.calculate(data, config)
        # n=9, P50: floor(9 * 50 / 100) = floor(4.5) = 4, scores[4] = 80
        assert result['P50'] == 80
        
    def test_algorithm_info(self):
        """测试算法信息"""
        info = self.strategy.get_algorithm_info()
        assert info['name'] == 'EducationalPercentile'
        assert info['algorithm'] == 'floor(n * p / 100)'
        assert info['standard'] == 'Chinese Educational Statistics'


class TestEducationalMetricsStrategy:
    """教育指标策略测试"""
    
    def setup_method(self):
        self.strategy = EducationalMetricsStrategy()
        
    def test_primary_grade_distribution(self):
        """测试小学年级等级分布"""
        # 构造测试数据：100分满分，小学标准
        scores = [95, 92, 88, 85, 75, 70, 65, 50, 45, 30]  # 10个学生
        data = pd.DataFrame({'score': scores})
        config = {
            'max_score': 100,
            'grade_level': '3rd_grade'  # 小学三年级
        }
        
        result = self.strategy.calculate(data, config)
        
        # 验证得分率
        expected_score_rate = sum(scores) / len(scores) / 100  # 69.5/100 = 0.695
        assert abs(result['average_score_rate'] - expected_score_rate) < 0.001
        
        # 验证小学等级分布
        # 优秀≥90: 95,92 = 2人
        # 良好80-89: 88,85 = 2人  
        # 及格60-79: 75,70,65 = 3人
        # 不及格<60: 50,45,30 = 3人
        grade_dist = result['grade_distribution']
        assert grade_dist['excellent_count'] == 2
        assert grade_dist['good_count'] == 2
        assert grade_dist['pass_count'] == 3
        assert grade_dist['fail_count'] == 3
        
        # 验证比例
        assert abs(grade_dist['excellent_rate'] - 0.2) < 0.001
        assert abs(grade_dist['good_rate'] - 0.2) < 0.001
        assert abs(grade_dist['pass_rate'] - 0.3) < 0.001
        assert abs(grade_dist['fail_rate'] - 0.3) < 0.001
        
        # 验证通用指标
        assert abs(result['pass_rate'] - 0.7) < 0.001  # 及格率：7/10
        assert abs(result['excellent_rate'] - 0.4) < 0.001  # 优秀率(≥85)：4/10
        
    def test_middle_grade_distribution(self):
        """测试初中年级等级分布"""
        scores = [90, 87, 82, 75, 68, 55, 45, 35, 25, 15]  # 10个学生
        data = pd.DataFrame({'score': scores})
        config = {
            'max_score': 100,
            'grade_level': '8th_grade'  # 初中八年级
        }
        
        result = self.strategy.calculate(data, config)
        
        # 验证初中等级分布
        # A≥85: 90,87 = 2人
        # B70-84: 82,75 = 2人
        # C60-69: 68 = 1人
        # D<60: 55,45,35,25,15 = 5人
        grade_dist = result['grade_distribution']
        assert grade_dist['a_count'] == 2
        assert grade_dist['b_count'] == 2  
        assert grade_dist['c_count'] == 1
        assert grade_dist['d_count'] == 5
        
    def test_difficulty_coefficient(self):
        """测试难度系数计算"""
        scores = [80, 80, 80, 80, 80]  # 平均分80
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.strategy.calculate(data, config)
        
        # 难度系数 = 平均分/满分 = 80/100 = 0.8
        assert abs(result['difficulty_coefficient'] - 0.8) < 0.001
        assert abs(result['average_score_rate'] - 0.8) < 0.001
        
    def test_perfect_scores(self):
        """测试满分情况"""
        scores = [100] * 10
        data = pd.DataFrame({'score': scores})
        config = {
            'max_score': 100,
            'grade_level': '5th_grade'
        }
        
        result = self.strategy.calculate(data, config)
        
        assert result['average_score_rate'] == 1.0
        assert result['difficulty_coefficient'] == 1.0
        assert result['excellent_rate'] == 1.0
        assert result['pass_rate'] == 1.0
        
        grade_dist = result['grade_distribution']
        assert grade_dist['excellent_count'] == 10
        assert grade_dist['excellent_rate'] == 1.0
        

class TestDiscriminationStrategy:
    """区分度策略测试"""
    
    def setup_method(self):
        self.strategy = DiscriminationStrategy()
        
    def test_discrimination_calculation(self):
        """测试区分度计算"""
        # 构造有区分度的数据
        scores = list(range(60, 100, 1))  # 60-99分，40个学生
        np.random.shuffle(scores)  # 打乱顺序
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        result = self.strategy.calculate(data, config)
        
        # 验证分组大小
        n = len(scores)
        expected_group_size = int(n * 0.27)  # 40 * 0.27 = 10.8 -> 10
        assert result['high_group_size'] == expected_group_size
        assert result['low_group_size'] == expected_group_size
        
        # 高分组平均分应该大于低分组
        assert result['high_group_mean'] > result['low_group_mean']
        
        # 区分度应该大于0
        assert result['discrimination_index'] > 0
        
        # 验证解释
        assert result['interpretation'] in ['poor', 'acceptable', 'good', 'excellent']
        
    def test_discrimination_interpretation(self):
        """测试区分度解释"""
        # 优秀区分度 (≥0.4)
        scores_high = [95] * 5 + [60] * 5  # 高分组95，低分组60
        data_high = pd.DataFrame({'score': scores_high})
        config = {'max_score': 100}
        
        result_high = self.strategy.calculate(data_high, config)
        # 区分度 = (95-60)/100 = 0.35，应该是good
        assert result_high['interpretation'] == 'good'
        
        # 低区分度
        scores_low = [80] * 20  # 所有分数相同
        data_low = pd.DataFrame({'score': scores_low})
        
        result_low = self.strategy.calculate(data_low, config)
        # 区分度 = 0，应该是poor
        assert result_low['discrimination_index'] == 0.0
        assert result_low['interpretation'] == 'poor'
        
    def test_small_dataset_warning(self):
        """测试小数据集警告"""
        scores = [80, 85, 90]  # 只有3个数据
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100}
        
        validation = self.strategy.validate_input(data, config)
        assert validation['is_valid']
        assert any('数据量过少' in warning for warning in validation['warnings'])
        
    def test_algorithm_info(self):
        """测试算法信息"""
        info = self.strategy.get_algorithm_info()
        assert info['name'] == 'Discrimination'
        assert '27%' in info['standard']
        assert '高分组平均分 - 低分组平均分' in info['formula']


class TestAnomalyDetector:
    """异常检测器测试"""
    
    def setup_method(self):
        self.detector = AnomalyDetector()
        
    def test_iqr_outlier_detection(self):
        """测试IQR异常检测"""
        # 构造带异常值的数据
        normal_scores = [75, 80, 78, 82, 85, 77, 83, 79, 81, 76]
        outliers = [30, 99]  # 明显的异常值
        all_scores = normal_scores + outliers
        
        data = pd.Series(all_scores)
        result = self.detector.detect_outliers(data, method='iqr')
        
        assert result['method'] == 'IQR'
        assert result['outlier_count'] == 2
        assert result['outlier_percentage'] == 2/12
        
        # 验证异常值被正确识别
        assert 30 in [all_scores[i] for i in result['outlier_indices']]
        assert 99 in [all_scores[i] for i in result['outlier_indices']]
        
    def test_zscore_outlier_detection(self):
        """测试Z-score异常检测"""
        # 构造正态分布数据 + 异常值
        np.random.seed(42)
        normal_data = np.random.normal(80, 10, 100)  # 均值80，标准差10
        outlier_data = [150, 10]  # 明显异常值
        all_data = np.concatenate([normal_data, outlier_data])
        
        data = pd.Series(all_data)
        result = self.detector.detect_outliers(data, method='zscore', threshold=3.0)
        
        assert result['method'] == 'Z-Score'
        assert result['threshold'] == 3.0
        assert result['outlier_count'] >= 2  # 至少包含我们添加的2个异常值
        
    def test_no_outliers(self):
        """测试无异常值情况"""
        # 构造正常数据
        normal_scores = [75, 80, 78, 82, 85, 77, 83, 79, 81, 76]
        data = pd.Series(normal_scores)
        
        result = self.detector.detect_outliers(data, method='iqr')
        assert result['outlier_count'] == 0
        assert result['outlier_percentage'] == 0.0


class TestCalculationEngine:
    """计算引擎测试"""
    
    def setup_method(self):
        self.engine = initialize_calculation_system()
        
    def test_engine_initialization(self):
        """测试引擎初始化"""
        strategies = self.engine.get_registered_strategies()
        expected_strategies = ['basic_statistics', 'percentiles', 'educational_metrics', 'discrimination']
        
        for strategy in expected_strategies:
            assert strategy in strategies
            
    def test_basic_calculation_workflow(self):
        """测试基础计算流程"""
        # 准备测试数据
        scores = [85, 90, 78, 92, 88, 76, 95, 82, 89, 91]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100, 'grade_level': '5th_grade'}
        
        # 执行基础统计计算
        result = self.engine.calculate('basic_statistics', data, config)
        
        assert 'count' in result
        assert 'mean' in result
        assert 'std' in result
        assert '_meta' in result
        assert result['_meta']['data_size'] == 10
        
    def test_advanced_calculation_workflow(self):
        """测试高级计算流程"""
        scores = [85, 90, 78, 92, 88, 76, 95, 82, 89, 91]
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100, 'grade_level': '5th_grade'}
        
        # 执行高级统计计算
        result = self.engine.calculate_advanced_statistics(data, config)
        
        # 验证包含所有策略的结果
        assert 'count' in result  # 基础统计
        assert 'P50' in result    # 百分位数
        assert 'pass_rate' in result  # 教育指标
        assert 'discrimination_index' in result  # 区分度
        
    def test_large_dataset_processing(self):
        """测试大数据集处理"""
        # 生成大数据集
        np.random.seed(42)
        large_scores = np.random.normal(75, 15, 15000)  # 15000个学生
        large_scores = np.clip(large_scores, 0, 100)
        
        data = pd.DataFrame({'score': large_scores})
        config = {'max_score': 100}
        
        # 测试分块处理
        result = self.engine.calculate('basic_statistics', data, config)
        
        assert result['count'] == 15000
        assert 70 < result['mean'] < 80  # 应该接近75
        assert result['_meta']['data_size'] == 15000
        
    def test_performance_monitoring(self):
        """测试性能监控"""
        scores = [85, 90, 78, 92, 88]
        data = pd.DataFrame({'score': scores})
        config = {}
        
        # 执行计算
        self.engine.calculate('basic_statistics', data, config)
        
        # 获取性能统计
        stats = self.engine.get_performance_stats()
        
        assert stats['total_operations'] >= 1
        assert stats['successful_operations'] >= 1
        assert stats['success_rate'] > 0
        assert stats['avg_execution_time'] >= 0
        
    def test_error_handling(self):
        """测试错误处理"""
        # 测试无效策略
        data = pd.DataFrame({'score': [80, 85, 90]})
        
        with pytest.raises(ValueError, match="未知的计算策略"):
            self.engine.calculate('invalid_strategy', data, {})
            
        # 测试无效数据
        invalid_data = pd.DataFrame({'invalid_column': [1, 2, 3]})
        
        with pytest.raises(ValueError, match="数据验证失败"):
            self.engine.calculate('basic_statistics', invalid_data, {})


class TestTraditionalFunctions:
    """传统函数式接口测试"""
    
    def test_calculate_average(self):
        """测试平均分计算"""
        scores = pd.Series([80, 85, 90, 75, 95])
        avg = calculate_average(scores)
        assert abs(avg - 85.0) < 0.001
        
        # 测试空数据
        empty_scores = pd.Series([])
        assert calculate_average(empty_scores) == 0.0
        
    def test_calculate_standard_deviation(self):
        """测试标准差计算"""
        scores = pd.Series([80, 85, 90, 75, 95])
        std = calculate_standard_deviation(scores)
        expected_std = scores.std(ddof=1)
        assert abs(std - expected_std) < 0.001
        
        # 测试单个值
        single_score = pd.Series([80])
        assert calculate_standard_deviation(single_score) == 0.0
        
    def test_calculate_pass_rate(self):
        """测试及格率计算"""
        scores = pd.Series([95, 85, 75, 65, 55, 45])  # 4个及格，2个不及格
        pass_rate = calculate_pass_rate(scores, pass_score=60, max_score=100)
        assert abs(pass_rate - 4/6) < 0.001
        
    def test_calculate_excellent_rate(self):
        """测试优秀率计算"""
        scores = pd.Series([95, 85, 75, 65, 55, 45])  # 2个优秀
        excellent_rate = calculate_excellent_rate(scores, excellent_score=85, max_score=100)
        assert abs(excellent_rate - 2/6) < 0.001
        
    def test_calculate_percentile(self):
        """测试百分位数计算"""
        scores = pd.Series(list(range(1, 101)))  # 1-100
        p50 = calculate_percentile(scores, 50)
        assert p50 == 50
        
        p25 = calculate_percentile(scores, 25)
        assert p25 == 25
        
    def test_calculate_difficulty_coefficient(self):
        """测试难度系数计算"""
        scores = pd.Series([80, 80, 80, 80, 80])  # 平均分80
        difficulty = calculate_difficulty_coefficient(scores, max_score=100)
        assert abs(difficulty - 0.8) < 0.001
        
    def test_calculate_discrimination_index(self):
        """测试区分度计算"""
        # 构造有区分度的数据
        high_scores = [90] * 10
        low_scores = [70] * 10  
        all_scores = high_scores + low_scores
        scores = pd.Series(all_scores)
        
        discrimination = calculate_discrimination_index(scores, max_score=100)
        # 高分组均值90，低分组均值70，区分度=(90-70)/100=0.2
        assert abs(discrimination - 0.2) < 0.001
        
        # 测试数据量不足
        small_scores = pd.Series([80, 85, 90])
        discrimination_small = calculate_discrimination_index(small_scores)
        assert discrimination_small == 0.0


class TestPerformanceRequirements:
    """性能要求测试"""
    
    def test_large_dataset_performance(self):
        """测试大数据集性能要求"""
        engine = initialize_calculation_system()
        
        # 生成10万条测试数据
        np.random.seed(42)
        scores = np.random.normal(75, 15, 100000)
        scores = np.clip(scores, 0, 100)
        
        data = pd.DataFrame({'score': scores})
        config = {'max_score': 100, 'grade_level': '5th_grade'}
        
        import time
        start_time = time.time()
        result = engine.calculate('basic_statistics', data, config)
        calculation_time = time.time() - start_time
        
        # 性能要求：10万数据基础统计应该在合理时间内完成
        assert calculation_time < 30.0  # 30秒内完成
        
        # 验证结果合理性
        assert 70 < result['mean'] < 80
        assert 10 < result['std'] < 20
        assert result['count'] == 100000
        
    def test_memory_usage_optimization(self):
        """测试内存使用优化"""
        engine = initialize_calculation_system()
        
        # 生成大数据集
        np.random.seed(42)
        scores = np.random.normal(80, 10, 50000)
        data = pd.DataFrame({
            'score': scores,
            'student_id': range(50000),
            'class_name': ['Class_' + str(i % 100) for i in range(50000)]
        })
        
        # 监控内存使用
        memory_manager = engine.memory_manager
        initial_memory = memory_manager.get_memory_usage()
        
        # 执行计算
        result = engine.calculate('basic_statistics', data, {})
        
        # 验证内存没有显著增长
        final_memory = memory_manager.get_memory_usage()
        memory_increase = final_memory - initial_memory
        
        # 内存增长应该控制在合理范围内
        assert memory_increase < 0.2  # 不超过20%
        assert result['count'] == 50000


if __name__ == '__main__':
    pytest.main([__file__, '-v'])