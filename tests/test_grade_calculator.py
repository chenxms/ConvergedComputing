# 年级差异化等级分布计算器测试
import pytest
import pandas as pd
import numpy as np
from typing import Dict, Any

from app.calculation.calculators.grade_calculator import (
    GradeLevelDistributionCalculator,
    GradeLevelConfig,
    calculate_individual_grade,
    batch_calculate_grades,
    create_grade_summary_report
)


class TestGradeLevelConfig:
    """测试年级配置类"""
    
    def test_elementary_grade_identification(self):
        """测试小学年级识别"""
        elementary_grades = ['1st_grade', '2nd_grade', '3rd_grade', 
                           '4th_grade', '5th_grade', '6th_grade']
        
        for grade in elementary_grades:
            assert GradeLevelConfig.is_elementary_grade(grade)
            assert not GradeLevelConfig.is_middle_school_grade(grade)
            assert GradeLevelConfig.get_grade_type(grade) == 'elementary'
    
    def test_middle_school_grade_identification(self):
        """测试初中年级识别"""
        middle_school_grades = ['7th_grade', '8th_grade', '9th_grade']
        
        for grade in middle_school_grades:
            assert GradeLevelConfig.is_middle_school_grade(grade)
            assert not GradeLevelConfig.is_elementary_grade(grade)
            assert GradeLevelConfig.get_grade_type(grade) == 'middle_school'
    
    def test_unknown_grade_handling(self):
        """测试未知年级处理"""
        unknown_grades = ['10th_grade', 'kindergarten', 'invalid_grade']
        
        for grade in unknown_grades:
            assert not GradeLevelConfig.is_elementary_grade(grade)
            assert not GradeLevelConfig.is_middle_school_grade(grade)
            assert GradeLevelConfig.get_grade_type(grade) == 'unknown'
    
    def test_elementary_thresholds(self):
        """测试小学阈值配置"""
        for grade in GradeLevelConfig.ELEMENTARY_GRADES:
            thresholds = GradeLevelConfig.get_thresholds(grade)
            
            assert thresholds['excellent'] == 0.90
            assert thresholds['good'] == 0.80
            assert thresholds['pass'] == 0.60
            assert thresholds['fail'] == 0.00
    
    def test_middle_school_thresholds(self):
        """测试初中阈值配置"""
        for grade in GradeLevelConfig.MIDDLE_SCHOOL_GRADES:
            thresholds = GradeLevelConfig.get_thresholds(grade)
            
            assert thresholds['A'] == 0.85
            assert thresholds['B'] == 0.70
            assert thresholds['C'] == 0.60
            assert thresholds['D'] == 0.00
    
    def test_grade_name_mapping(self):
        """测试等级名称映射"""
        # 小学等级名称
        elementary_names = GradeLevelConfig.get_grade_names('1st_grade')
        assert elementary_names['excellent'] == '优秀'
        assert elementary_names['good'] == '良好'
        assert elementary_names['pass'] == '及格'
        assert elementary_names['fail'] == '不及格'
        
        # 初中等级名称
        middle_school_names = GradeLevelConfig.get_grade_names('7th_grade')
        assert middle_school_names['A'] == 'A等'
        assert middle_school_names['B'] == 'B等'
        assert middle_school_names['C'] == 'C等'
        assert middle_school_names['D'] == 'D等'


class TestIndividualGradeCalculation:
    """测试单个学生等级计算"""
    
    def test_elementary_grade_calculation(self):
        """测试小学等级计算"""
        test_cases = [
            (95, '5th_grade', 100, 'excellent', '优秀'),
            (85, '3rd_grade', 100, 'good', '良好'),
            (70, '1st_grade', 100, 'pass', '及格'),
            (50, '2nd_grade', 100, 'fail', '不及格'),
            (90, '6th_grade', 100, 'excellent', '优秀'),  # 边界值测试
            (80, '4th_grade', 100, 'good', '良好'),      # 边界值测试
            (60, '5th_grade', 100, 'pass', '及格')       # 边界值测试
        ]
        
        for score, grade_level, max_score, expected_grade, expected_name in test_cases:
            result = calculate_individual_grade(score, grade_level, max_score)
            assert result['grade'] == expected_grade, f"分数{score}年级{grade_level}应为{expected_grade}"
            assert result['grade_name'] == expected_name
            assert result['score_rate'] == score / max_score
            assert result['threshold_met'] is True
    
    def test_middle_school_grade_calculation(self):
        """测试初中等级计算"""
        test_cases = [
            (90, '7th_grade', 100, 'A', 'A等'),
            (80, '8th_grade', 100, 'B', 'B等'),
            (65, '9th_grade', 100, 'C', 'C等'),
            (55, '7th_grade', 100, 'D', 'D等'),
            (85, '8th_grade', 100, 'A', 'A等'),  # 边界值测试
            (70, '9th_grade', 100, 'B', 'B等'),  # 边界值测试
            (60, '7th_grade', 100, 'C', 'C等')   # 边界值测试
        ]
        
        for score, grade_level, max_score, expected_grade, expected_name in test_cases:
            result = calculate_individual_grade(score, grade_level, max_score)
            assert result['grade'] == expected_grade, f"分数{score}年级{grade_level}应为{expected_grade}"
            assert result['grade_name'] == expected_name
            assert result['score_rate'] == score / max_score
            assert result['threshold_met'] is True
    
    def test_different_max_scores(self):
        """测试不同满分情况"""
        # 测试满分为150的情况
        result = calculate_individual_grade(135, '7th_grade', 150)
        assert result['grade'] == 'A'
        assert result['score_rate'] == 0.9
        
        # 测试满分为50的情况 - 42.5/50 = 0.85，小学标准下为good
        result = calculate_individual_grade(42.5, '3rd_grade', 50)
        assert result['grade'] == 'good'  # 修正：0.85在小学标准下是good，不是excellent
        assert result['score_rate'] == 0.85
    
    def test_invalid_score_handling(self):
        """测试无效分数处理"""
        # 测试负分
        result = calculate_individual_grade(-10, '5th_grade', 100)
        assert result['grade'] is None
        assert result['grade_name'] == '无效分数'
        
        # 测试NaN
        result = calculate_individual_grade(np.nan, '7th_grade', 100)
        assert result['grade'] is None
        assert result['grade_name'] == '无效分数'


class TestBatchGradeCalculation:
    """测试批量等级计算"""
    
    def test_elementary_batch_calculation(self):
        """测试小学批量计算"""
        data = pd.DataFrame({
            'student_id': ['s001', 's002', 's003', 's004'],
            'grade_level': ['3rd_grade', '3rd_grade', '3rd_grade', '3rd_grade'],
            'score': [95, 85, 70, 50]
        })
        
        result = batch_calculate_grades(data)
        
        assert list(result['calculated_grade']) == ['excellent', 'good', 'pass', 'fail']
        assert list(result['grade_name']) == ['优秀', '良好', '及格', '不及格']
        assert all(result['threshold_met'])  # 所有分数都在有效范围内
    
    def test_middle_school_batch_calculation(self):
        """测试初中批量计算"""
        data = pd.DataFrame({
            'student_id': ['s001', 's002', 's003', 's004'],
            'grade_level': ['8th_grade', '8th_grade', '8th_grade', '8th_grade'],
            'score': [90, 80, 65, 55]
        })
        
        result = batch_calculate_grades(data)
        
        assert list(result['calculated_grade']) == ['A', 'B', 'C', 'D']
        assert list(result['grade_name']) == ['A等', 'B等', 'C等', 'D等']
    
    def test_mixed_grade_batch_calculation(self):
        """测试混合年级批量计算"""
        data = pd.DataFrame({
            'student_id': ['s001', 's002', 's003', 's004'],
            'grade_level': ['5th_grade', '8th_grade', '2nd_grade', '9th_grade'],
            'score': [92, 88, 78, 62]
        })
        
        result = batch_calculate_grades(data)
        
        # 验证不同年级使用不同标准
        assert result.iloc[0]['calculated_grade'] == 'excellent'  # 小学92分为优秀
        assert result.iloc[1]['calculated_grade'] == 'A'          # 初中88分为A等
        assert result.iloc[2]['calculated_grade'] == 'pass'       # 小学78分为及格
        assert result.iloc[3]['calculated_grade'] == 'C'          # 初中62分为C等


class TestGradeLevelDistributionCalculator:
    """测试等级分布计算器"""
    
    def setup_method(self):
        """测试前准备"""
        self.calculator = GradeLevelDistributionCalculator()
    
    def test_elementary_distribution_calculation(self):
        """测试小学等级分布计算"""
        # 创建测试数据：100个学生，分数分布在各个等级
        np.random.seed(42)  # 确保结果可重现
        data = pd.DataFrame({
            'score': [95, 92, 88, 85, 82, 78, 75, 72, 68, 65,  # 10个优秀+良好
                     88, 85, 82, 79, 76, 73, 70, 67, 64, 61,  # 10个良好+及格
                     75, 72, 69, 66, 63, 78, 75, 72, 69, 66,  # 10个及格
                     58, 55, 52, 49, 46, 43, 40, 37, 34, 31,  # 10个不及格
                     90, 87, 84, 81, 78, 75, 72, 69, 66, 63] + # 剩余50个混合分数
                     list(np.random.normal(75, 15, 40))  # 40个正态分布分数
        })
        
        config = {
            'grade_level': '4th_grade',
            'max_score': 100
        }
        
        result = self.calculator.calculate(data, config)
        
        # 验证基本结构
        assert result['grade_level'] == '4th_grade'
        assert result['grade_type'] == 'elementary'
        assert result['max_score'] == 100
        assert 'distribution' in result
        assert 'statistics' in result
        
        # 验证分布数据
        distribution = result['distribution']
        assert 'counts' in distribution
        assert 'rates' in distribution
        assert 'percentages' in distribution
        assert 'labels' in distribution
        
        # 验证所有学生都被分类
        total_classified = sum(distribution['counts'].values())
        assert total_classified == result['total_count']
        
        # 验证比例计算正确
        for grade_key in distribution['counts'].keys():
            expected_rate = distribution['counts'][grade_key] / result['total_count']
            assert abs(distribution['rates'][grade_key] - expected_rate) < 0.001
    
    def test_middle_school_distribution_calculation(self):
        """测试初中等级分布计算"""
        data = pd.DataFrame({
            'score': [90, 88, 85, 83, 80, 78, 75, 73, 70, 68,  # A和B等
                     80, 78, 75, 73, 70, 68, 65, 63, 60, 58,   # B和C等
                     65, 63, 60, 58, 55, 53, 50, 48, 45, 42]   # C和D等
        })
        
        config = {
            'grade_level': '8th_grade',
            'max_score': 100
        }
        
        result = self.calculator.calculate(data, config)
        
        assert result['grade_type'] == 'middle_school'
        distribution = result['distribution']
        
        # 验证初中等级标识
        assert 'A' in distribution['counts']
        assert 'B' in distribution['counts']
        assert 'C' in distribution['counts']
        assert 'D' in distribution['counts']
        
        # 验证等级名称
        assert distribution['labels']['A'] == 'A等'
        assert distribution['labels']['B'] == 'B等'
        assert distribution['labels']['C'] == 'C等'
        assert distribution['labels']['D'] == 'D等'
    
    def test_mixed_grades_calculation(self):
        """测试混合年级计算"""
        data = pd.DataFrame({
            'student_id': range(60),
            'grade_level': ['3rd_grade'] * 20 + ['7th_grade'] * 20 + ['5th_grade'] * 20,
            'score': list(np.random.normal(80, 10, 60))
        })
        
        config = {'max_score': 100}
        
        result = self.calculator.calculate(data, config)
        
        # 验证混合年级结果结构
        assert result['type'] == 'mixed_grades'
        assert 'grade_results' in result
        assert 'overall_statistics' in result
        assert len(result['grade_results']) == 3  # 3个不同年级
        
        # 验证每个年级都有单独的计算结果
        for grade_level, grade_result in result['grade_results'].items():
            assert grade_result['grade_level'] == grade_level
            assert 'distribution' in grade_result
            assert 'statistics' in grade_result
    
    def test_custom_thresholds(self):
        """测试自定义阈值配置"""
        custom_thresholds = {
            'elementary': {
                'excellent': 0.95,  # 提高优秀标准
                'good': 0.85,
                'pass': 0.70,
                'fail': 0.00
            }
        }
        
        calculator = GradeLevelDistributionCalculator(custom_thresholds)
        
        data = pd.DataFrame({'score': [96, 90, 80, 65]})
        config = {'grade_level': '2nd_grade', 'max_score': 100}
        
        result = calculator.calculate(data, config)
        distribution = result['distribution']
        
        # 验证自定义阈值生效
        assert distribution['counts']['excellent'] == 1  # 只有96分达到95%标准
        assert distribution['counts']['good'] == 1       # 90分为良好(85-95%)
        assert distribution['counts']['pass'] == 1       # 80分为及格(70-85%) 
        assert distribution['counts']['fail'] == 1       # 65分不及格(70%以下)
    
    def test_validation_errors(self):
        """测试输入验证错误"""
        # 空数据集 - validate_input返回验证结果，不抛异常
        empty_data = pd.DataFrame()
        config = {'grade_level': '5th_grade', 'max_score': 100}
        validation = self.calculator.validate_input(empty_data, config)
        assert not validation['is_valid']
        assert "数据集为空" in validation['errors'][0]
        
        # 缺少score列
        with pytest.raises(ValueError, match="数据中缺少'score'列"):
            data = pd.DataFrame({'student_id': [1, 2, 3]})
            config = {'grade_level': '5th_grade', 'max_score': 100}
            self.calculator.calculate(data, config)
        
        # 没有有效分数
        with pytest.raises(ValueError, match="没有有效的分数数据"):
            data = pd.DataFrame({'score': [np.nan, None, 'invalid']})
            config = {'grade_level': '5th_grade', 'max_score': 100}
            self.calculator.calculate(data, config)
        
        # 缺少年级信息
        with pytest.raises(ValueError, match="必须指定grade_level参数"):
            data = pd.DataFrame({'score': [80, 90, 70]})
            config = {'max_score': 100}
            self.calculator.calculate(data, config)
    
    def test_validation_warnings(self):
        """测试输入验证警告"""
        data = pd.DataFrame({
            'score': [95, 85, np.nan, 120, -10, 75, 'invalid', 65],  # 包含各种问题数据
            'grade_level': ['5th_grade'] * 8
        })
        config = {'max_score': 100}
        
        validation = self.calculator.validate_input(data, config)
        
        assert validation['is_valid']  # 仍然有效，但有警告
        assert len(validation['warnings']) > 0
        
        # 检查具体警告
        warning_messages = ' '.join(validation['warnings'])
        assert '无效分数值' in warning_messages or '超出范围' in warning_messages
    
    def test_algorithm_info(self):
        """测试算法信息"""
        info = self.calculator.get_algorithm_info()
        
        assert info['name'] == 'GradeLevelDistribution'
        assert info['version'] == '1.0'
        assert 'elementary_standard' in info
        assert 'middle_school_standard' in info
        assert info['supports_custom_thresholds'] is True
    
    def test_performance_with_large_dataset(self):
        """测试大数据集性能"""
        # 创建10000个学生的数据
        large_data = pd.DataFrame({
            'score': np.random.normal(75, 15, 10000),
            'grade_level': ['6th_grade'] * 10000
        })
        
        config = {'max_score': 100}
        
        import time
        start_time = time.time()
        result = self.calculator.calculate(large_data, config)
        execution_time = time.time() - start_time
        
        # 验证性能要求（应该在合理时间内完成）
        assert execution_time < 5.0  # 5秒内完成
        assert result['total_count'] == 10000
        assert 'distribution' in result


class TestGradeSummaryReport:
    """测试等级分布汇总报告"""
    
    def test_single_grade_report(self):
        """测试单年级汇总报告"""
        distribution_result = {
            'grade_level': '4th_grade',
            'grade_type': 'elementary',
            'total_count': 100,
            'distribution': {
                'percentages': {
                    'excellent': 15.0,
                    'good': 25.0,
                    'pass': 45.0,
                    'fail': 15.0
                }
            },
            'statistics': {
                'mean': 75.5,
                'score_rate': 0.755,
                'pass_rate': 0.85,
                'excellent_rate': 0.15
            },
            'trends': {
                'recommendations': ['建议加强基础知识教学']
            }
        }
        
        report = create_grade_summary_report(distribution_result)
        
        assert report['grade_level'] == '4th_grade'
        assert report['summary']['total_students'] == 100
        assert report['summary']['average_score'] == 75.5
        assert report['summary']['pass_rate'] == 85.0
        assert 'recommendations' in report
        assert 'performance_level' in report
        assert 'report_generated_at' in report
    
    def test_mixed_grade_report(self):
        """测试混合年级汇总报告"""
        mixed_result = {
            'type': 'mixed_grades',
            'total_students': 200,
            'grade_count': 2,
            'overall_statistics': {
                'mean': 78.0,
                'pass_rate': 0.82
            },
            'grade_results': {
                '5th_grade': {
                    'grade_level': '5th_grade',
                    'grade_type': 'elementary',
                    'total_count': 100,
                    'distribution': {'percentages': {'excellent': 20.0}},
                    'statistics': {'mean': 80.0, 'score_rate': 0.80},
                    'trends': {'recommendations': []}
                },
                '8th_grade': {
                    'grade_level': '8th_grade',
                    'grade_type': 'middle_school', 
                    'total_count': 100,
                    'distribution': {'percentages': {'A': 18.0}},
                    'statistics': {'mean': 76.0, 'score_rate': 0.76},
                    'trends': {'recommendations': []}
                }
            }
        }
        
        report = create_grade_summary_report(mixed_result)
        
        assert report['type'] == 'mixed_grades_report'
        assert report['overall_summary']['total_students'] == 200
        assert report['overall_summary']['grade_count'] == 2
        assert len(report['grade_summaries']) == 2
        assert '5th_grade' in report['grade_summaries']
        assert '8th_grade' in report['grade_summaries']
        assert 'cross_grade_analysis' in report


class TestEdgeCases:
    """测试边界情况"""
    
    def test_single_student(self):
        """测试单个学生数据"""
        calculator = GradeLevelDistributionCalculator()
        
        data = pd.DataFrame({'score': [85]})
        config = {'grade_level': '3rd_grade', 'max_score': 100}
        
        result = calculator.calculate(data, config)
        
        assert result['total_count'] == 1
        assert result['distribution']['counts']['good'] == 1
        assert result['distribution']['rates']['good'] == 1.0
    
    def test_perfect_scores(self):
        """测试满分情况"""
        calculator = GradeLevelDistributionCalculator()
        
        data = pd.DataFrame({'score': [100, 100, 100]})
        config = {'grade_level': '7th_grade', 'max_score': 100}
        
        result = calculator.calculate(data, config)
        
        assert result['distribution']['counts']['A'] == 3
        assert result['statistics']['mean'] == 100.0
        assert result['statistics']['score_rate'] == 1.0
    
    def test_zero_scores(self):
        """测试零分情况"""
        calculator = GradeLevelDistributionCalculator()
        
        data = pd.DataFrame({'score': [0, 0, 0]})
        config = {'grade_level': '5th_grade', 'max_score': 100}
        
        result = calculator.calculate(data, config)
        
        assert result['distribution']['counts']['fail'] == 3
        assert result['statistics']['mean'] == 0.0
        assert result['statistics']['score_rate'] == 0.0
    
    def test_boundary_scores(self):
        """测试边界分数"""
        calculator = GradeLevelDistributionCalculator()
        
        # 测试所有边界值
        data = pd.DataFrame({'score': [90, 80, 60]})  # 小学边界值
        config = {'grade_level': '6th_grade', 'max_score': 100}
        
        result = calculator.calculate(data, config)
        distribution = result['distribution']
        
        assert distribution['counts']['excellent'] == 1  # 90分
        assert distribution['counts']['good'] == 1       # 80分
        assert distribution['counts']['pass'] == 1       # 60分
        assert distribution['counts']['fail'] == 0
        
        # 测试初中边界值
        data = pd.DataFrame({'score': [85, 70, 60]})
        config = {'grade_level': '9th_grade', 'max_score': 100}
        
        result = calculator.calculate(data, config)
        distribution = result['distribution']
        
        assert distribution['counts']['A'] == 1  # 85分
        assert distribution['counts']['B'] == 1  # 70分
        assert distribution['counts']['C'] == 1  # 60分
        assert distribution['counts']['D'] == 0


if __name__ == '__main__':
    # 运行测试
    pytest.main([__file__, '-v', '--tb=short'])