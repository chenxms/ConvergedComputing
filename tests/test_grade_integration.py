# 年级等级分布计算集成测试
import pytest
import pandas as pd
import numpy as np
from app.calculation import get_calculation_engine, initialize_calculation_system


class TestGradeDistributionIntegration:
    """测试年级等级分布计算与计算引擎的集成"""
    
    def setup_method(self):
        """测试前准备"""
        self.engine = initialize_calculation_system()
    
    def test_grade_distribution_strategy_registration(self):
        """测试等级分布策略注册"""
        strategies = self.engine.get_registered_strategies()
        assert 'grade_distribution' in strategies
    
    def test_elementary_grade_calculation_via_engine(self):
        """测试通过计算引擎进行小学等级计算"""
        # 创建测试数据
        data = pd.DataFrame({
            'score': [95, 85, 75, 55, 92, 88, 78, 68, 58, 45],
            'grade_level': ['3rd_grade'] * 10
        })
        
        config = {
            'grade_level': '3rd_grade',
            'max_score': 100
        }
        
        # 通过计算引擎执行计算
        result = self.engine.calculate('grade_distribution', data, config)
        
        # 验证结果结构
        assert result['grade_level'] == '3rd_grade'
        assert result['grade_type'] == 'elementary'
        assert result['total_count'] == 10
        
        # 验证分布计算
        distribution = result['distribution']
        assert 'excellent' in distribution['counts']
        assert 'good' in distribution['counts']
        assert 'pass' in distribution['counts']
        assert 'fail' in distribution['counts']
        
        # 验证元信息
        assert '_meta' in result
        assert 'algorithm_info' in result['_meta']
        assert result['_meta']['algorithm_info']['name'] == 'GradeLevelDistribution'
    
    def test_middle_school_grade_calculation_via_engine(self):
        """测试通过计算引擎进行初中等级计算"""
        data = pd.DataFrame({
            'score': [90, 80, 70, 55, 88, 75, 65, 50],
            'grade_level': ['8th_grade'] * 8
        })
        
        config = {
            'grade_level': '8th_grade',
            'max_score': 100
        }
        
        result = self.engine.calculate('grade_distribution', data, config)
        
        assert result['grade_type'] == 'middle_school'
        distribution = result['distribution']
        assert 'A' in distribution['counts']
        assert 'B' in distribution['counts']
        assert 'C' in distribution['counts']
        assert 'D' in distribution['counts']
    
    def test_mixed_grades_calculation_via_engine(self):
        """测试通过计算引擎进行混合年级计算"""
        data = pd.DataFrame({
            'student_id': range(30),
            'score': np.random.normal(75, 12, 30),
            'grade_level': ['2nd_grade'] * 10 + ['5th_grade'] * 10 + ['8th_grade'] * 10
        })
        
        config = {'max_score': 100}
        
        result = self.engine.calculate('grade_distribution', data, config)
        
        assert result['type'] == 'mixed_grades'
        assert len(result['grade_results']) == 3
        assert '2nd_grade' in result['grade_results']
        assert '5th_grade' in result['grade_results']
        assert '8th_grade' in result['grade_results']
    
    def test_performance_monitoring(self):
        """测试性能监控"""
        # 清空性能统计
        self.engine.reset_performance_stats()
        
        # 执行多次计算
        for i in range(3):
            data = pd.DataFrame({
                'score': np.random.normal(80, 10, 100),
                'grade_level': ['6th_grade'] * 100
            })
            config = {'grade_level': '6th_grade', 'max_score': 100}
            self.engine.calculate('grade_distribution', data, config)
        
        # 检查性能统计
        stats = self.engine.get_performance_stats()
        assert stats['total_operations'] == 3
        assert stats['successful_operations'] == 3
        assert stats['success_rate'] == 1.0
        assert stats['total_data_processed'] == 300
    
    def test_error_handling_via_engine(self):
        """测试通过计算引擎的错误处理"""
        # 测试无效数据
        invalid_data = pd.DataFrame({'invalid_column': [1, 2, 3]})
        config = {'grade_level': '4th_grade', 'max_score': 100}
        
        with pytest.raises(ValueError, match="数据验证失败"):
            self.engine.calculate('grade_distribution', invalid_data, config)
        
        # 检查错误被记录到性能监控
        stats = self.engine.get_performance_stats()
        assert stats['failed_operations'] > 0
    
    def test_calculation_engine_integration_with_other_strategies(self):
        """测试等级分布计算与其他策略的集成"""
        data = pd.DataFrame({
            'score': [95, 85, 75, 65, 90, 80, 70, 60],
            'grade_level': ['7th_grade'] * 8
        })
        
        config = {'grade_level': '7th_grade', 'max_score': 100}
        
        # 执行多个策略计算
        basic_stats = self.engine.calculate('basic_statistics', data, config)
        grade_dist = self.engine.calculate('grade_distribution', data, config)
        percentiles = self.engine.calculate('percentiles', data, config)
        
        # 验证结果一致性
        assert basic_stats['count'] == grade_dist['total_count']
        assert abs(basic_stats['mean'] - grade_dist['statistics']['mean']) < 0.01
        
        # 验证等级分布与统计指标的逻辑一致性
        total_students = grade_dist['total_count']
        distribution_sum = sum(grade_dist['distribution']['counts'].values())
        assert total_students == distribution_sum


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])