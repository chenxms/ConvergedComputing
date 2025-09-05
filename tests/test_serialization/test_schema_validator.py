"""
JSON Schema验证器测试

测试JSON数据格式验证功能，确保符合json-data-specification.md规范。
"""

import pytest
from datetime import datetime

from app.services.serialization.schema_validator import SchemaValidator, ValidationResult


class TestSchemaValidator:
    """Schema验证器测试类"""
    
    @pytest.fixture
    def validator(self):
        """创建验证器实例"""
        return SchemaValidator()
    
    @pytest.fixture
    def valid_regional_data(self):
        """有效的区域级数据"""
        return {
            'data_version': '1.0',
            'schema_version': '2025-09-04',
            'batch_info': {
                'batch_code': 'BATCH_2025_001',
                'grade_level': '初中',
                'total_schools': 25,
                'total_students': 8500,
                'calculation_time': '2025-09-04T18:30:00Z'
            },
            'academic_subjects': {
                '数学': {
                    'subject_id': 'MATH_001',
                    'subject_type': '考试类',
                    'total_score': 100,
                    'regional_stats': {
                        'avg_score': 78.5,
                        'score_rate': 0.785,
                        'difficulty': 0.785,
                        'discrimination': 0.65,
                        'std_dev': 12.3,
                        'max_score': 98,
                        'min_score': 32
                    },
                    'grade_distribution': {
                        'excellent': {'count': 2550, 'percentage': 0.30},
                        'good': {'count': 3400, 'percentage': 0.40},
                        'pass': {'count': 1700, 'percentage': 0.20},
                        'fail': {'count': 850, 'percentage': 0.10}
                    },
                    'school_rankings': [],
                    'dimensions': {
                        '数学运算': {
                            'dimension_id': 'MATH_CALC',
                            'dimension_name': '数学运算',
                            'total_score': 40,
                            'avg_score': 32.5,
                            'score_rate': 0.8125,
                            'regional_ranking_avg': 0.8125
                        }
                    }
                }
            },
            'non_academic_subjects': {
                '创新思维': {
                    'subject_id': 'INNOVATION_001',
                    'subject_type': '问卷类',
                    'total_schools_participated': 23,
                    'total_students_participated': 7890,
                    'dimensions': {
                        '好奇心': {
                            'dimension_id': 'CURIOSITY',
                            'dimension_name': '好奇心',
                            'total_score': 25,
                            'avg_score': 20.5,
                            'score_rate': 0.82,
                            'question_analysis': []
                        }
                    }
                }
            },
            'radar_chart_data': {
                'academic_dimensions': [
                    {
                        'dimension_name': '数学运算',
                        'score_rate': 0.8125,
                        'max_rate': 1.0
                    }
                ],
                'non_academic_dimensions': [
                    {
                        'dimension_name': '好奇心',
                        'score_rate': 0.82,
                        'max_rate': 1.0
                    }
                ]
            }
        }
    
    @pytest.fixture
    def valid_school_data(self):
        """有效的学校级数据"""
        return {
            'data_version': '1.0',
            'schema_version': '2025-09-04',
            'school_info': {
                'school_id': 'SCH_001',
                'school_name': '第一中学',
                'batch_code': 'BATCH_2025_001',
                'total_students': 340,
                'calculation_time': '2025-09-04T18:35:00Z'
            },
            'academic_subjects': {
                '数学': {
                    'subject_id': 'MATH_001',
                    'subject_type': '考试类',
                    'total_score': 100,
                    'school_stats': {
                        'avg_score': 85.2,
                        'score_rate': 0.852,
                        'std_dev': 10.5,
                        'max_score': 98,
                        'min_score': 58,
                        'regional_ranking': 1
                    },
                    'percentiles': {
                        'P10': 95,
                        'P50': 86,
                        'P90': 68
                    },
                    'grade_distribution': {
                        'excellent': {'count': 136, 'percentage': 0.40},
                        'good': {'count': 136, 'percentage': 0.40},
                        'pass': {'count': 51, 'percentage': 0.15},
                        'fail': {'count': 17, 'percentage': 0.05}
                    },
                    'regional_comparison': {
                        'regional_avg_score': 78.5,
                        'regional_score_rate': 0.785,
                        'difference': 6.7,
                        'rate_difference': 0.067,
                        'performance_level': '优秀'
                    },
                    'dimensions': {
                        '数学运算': {
                            'dimension_id': 'MATH_CALC',
                            'dimension_name': '数学运算',
                            'total_score': 40,
                            'school_avg_score': 34.8,
                            'school_score_rate': 0.87,
                            'regional_avg_score': 32.5,
                            'regional_score_rate': 0.8125,
                            'difference': 2.3,
                            'rate_difference': 0.0575,
                            'regional_ranking': 2
                        }
                    }
                }
            },
            'non_academic_subjects': {},
            'radar_chart_data': {
                'academic_dimensions': [
                    {
                        'dimension_name': '数学运算',
                        'school_score_rate': 0.87,
                        'regional_score_rate': 0.8125,
                        'max_rate': 1.0
                    }
                ],
                'non_academic_dimensions': []
            }
        }
    
    def test_validate_regional_data_success(self, validator, valid_regional_data):
        """测试有效区域数据验证成功"""
        result = validator.validate_regional_data(valid_regional_data)
        
        assert result.is_valid == True
        assert len(result.errors) == 0
        # 可能会有一些警告，但不应该有错误
    
    def test_validate_regional_data_missing_required_fields(self, validator):
        """测试区域数据缺少必填字段"""
        incomplete_data = {
            'data_version': '1.0',
            # 缺少其他必填字段
        }
        
        result = validator.validate_regional_data(incomplete_data)
        
        assert result.is_valid == False
        assert len(result.errors) > 0
        
        # 检查错误消息
        error_messages = '\n'.join(result.errors)
        assert 'batch_info' in error_messages or '缺少必填字段' in error_messages
    
    def test_validate_school_data_success(self, validator, valid_school_data):
        """测试有效学校数据验证成功"""
        result = validator.validate_school_data(valid_school_data)
        
        assert result.is_valid == True
        assert len(result.errors) == 0
    
    def test_validate_school_data_missing_required_fields(self, validator):
        """测试学校数据缺少必填字段"""
        incomplete_data = {
            'data_version': '1.0',
            'schema_version': '2025-09-04',
            # 缺少学校信息等必填字段
        }
        
        result = validator.validate_school_data(incomplete_data)
        
        assert result.is_valid == False
        assert len(result.errors) > 0
        
        error_messages = '\n'.join(result.errors)
        assert 'school_info' in error_messages or '缺少必填字段' in error_messages
    
    def test_validate_batch_code_format(self, validator, valid_regional_data):
        """测试批次代码格式验证"""
        # 测试无效的批次代码格式
        invalid_codes = [
            'INVALID_CODE',
            'BATCH_2025',
            'BATCH_25_001',
            'batch_2025_001',
            ''
        ]
        
        for invalid_code in invalid_codes:
            test_data = valid_regional_data.copy()
            test_data['batch_info']['batch_code'] = invalid_code
            
            result = validator.validate_regional_data(test_data)
            
            # 应该有批次代码格式错误
            error_messages = '\n'.join(result.errors)
            assert '批次代码格式' in error_messages or result.is_valid == False
    
    def test_validate_score_rates_range(self, validator, valid_regional_data):
        """测试得分率范围验证"""
        # 测试超出范围的得分率
        test_data = valid_regional_data.copy()
        test_data['academic_subjects']['数学']['regional_stats']['score_rate'] = 1.5  # 超出范围
        
        result = validator.validate_regional_data(test_data)
        
        assert result.is_valid == False
        error_messages = '\n'.join(result.errors)
        assert '得分率超出范围' in error_messages
    
    def test_validate_radar_chart_data(self, validator):
        """测试雷达图数据验证"""
        # 测试缺少雷达图数据
        incomplete_data = {
            'data_version': '1.0',
            'schema_version': '2025-09-04',
            'batch_info': {
                'batch_code': 'BATCH_2025_001',
                'grade_level': '初中',
                'total_schools': 25,
                'total_students': 8500,
                'calculation_time': '2025-09-04T18:30:00Z'
            },
            'academic_subjects': {},
            'non_academic_subjects': {},
            'radar_chart_data': {
                # 缺少 academic_dimensions
                'non_academic_dimensions': []
            }
        }
        
        result = validator.validate_regional_data(incomplete_data)
        
        assert result.is_valid == False
        error_messages = '\n'.join(result.errors)
        assert 'academic_dimensions' in error_messages or '缺少必需字段' in error_messages
    
    def test_validate_radar_chart_dimensions_format(self, validator):
        """测试雷达图维度格式验证"""
        test_data = {
            'data_version': '1.0',
            'schema_version': '2025-09-04',
            'batch_info': {
                'batch_code': 'BATCH_2025_001',
                'grade_level': '初中',
                'total_schools': 25,
                'total_students': 8500,
                'calculation_time': '2025-09-04T18:30:00Z'
            },
            'academic_subjects': {},
            'non_academic_subjects': {},
            'radar_chart_data': {
                'academic_dimensions': [
                    {
                        'dimension_name': '数学运算',
                        'score_rate': 1.5,  # 超出范围
                        'max_rate': 1.0
                    }
                ],
                'non_academic_dimensions': []
            }
        }
        
        result = validator.validate_regional_data(test_data)
        
        assert result.is_valid == False
        error_messages = '\n'.join(result.errors)
        assert '得分率超出范围' in error_messages
    
    def test_validate_school_radar_chart_data(self, validator):
        """测试学校级雷达图数据验证"""
        test_data = {
            'data_version': '1.0',
            'schema_version': '2025-09-04',
            'school_info': {
                'school_id': 'SCH_001',
                'school_name': '第一中学',
                'batch_code': 'BATCH_2025_001',
                'total_students': 340,
                'calculation_time': '2025-09-04T18:35:00Z'
            },
            'academic_subjects': {},
            'non_academic_subjects': {},
            'radar_chart_data': {
                'academic_dimensions': [
                    {
                        'dimension_name': '数学运算',
                        'school_score_rate': 0.87,
                        'regional_score_rate': 1.2,  # 超出范围
                        'max_rate': 1.0
                    }
                ],
                'non_academic_dimensions': []
            }
        }
        
        result = validator.validate_school_data(test_data)
        
        assert result.is_valid == False
        error_messages = '\n'.join(result.errors)
        assert 'regional_score_rate超出范围' in error_messages
    
    def test_validate_data_consistency(self, validator):
        """测试数据一致性验证"""
        regional_data = {
            'batch_info': {
                'batch_code': 'BATCH_2025_001',
                'total_schools': 2
            },
            'radar_chart_data': {
                'academic_dimensions': [
                    {'dimension_name': '数学运算', 'score_rate': 0.8, 'max_rate': 1.0}
                ],
                'non_academic_dimensions': []
            }
        }
        
        schools_data = [
            {
                'school_info': {
                    'school_id': 'SCH_001',
                    'batch_code': 'BATCH_2025_001'  # 一致的批次代码
                },
                'radar_chart_data': {
                    'academic_dimensions': [
                        {'dimension_name': '数学运算', 'school_score_rate': 0.85, 'regional_score_rate': 0.8, 'max_rate': 1.0}
                    ],
                    'non_academic_dimensions': []
                }
            },
            {
                'school_info': {
                    'school_id': 'SCH_002',
                    'batch_code': 'BATCH_2025_002'  # 不一致的批次代码
                },
                'radar_chart_data': {
                    'academic_dimensions': [],
                    'non_academic_dimensions': []
                }
            }
        ]
        
        result = validator.validate_data_consistency(regional_data, schools_data)
        
        assert result.is_valid == False
        error_messages = '\n'.join(result.errors)
        assert '批次代码' in error_messages and '不一致' in error_messages
    
    def test_validation_result_methods(self):
        """测试ValidationResult类的方法"""
        result = ValidationResult()
        
        # 测试添加错误
        result.add_error('测试错误', 'test_field')
        assert result.is_valid == False
        assert len(result.errors) == 1
        assert 'test_field: 测试错误' in result.errors
        
        # 测试添加警告
        result.add_warning('测试警告', 'warn_field')
        assert len(result.warnings) == 1
        assert 'warn_field: 测试警告' in result.warnings
        
        # 测试转换为字典
        result_dict = result.to_dict()
        assert 'is_valid' in result_dict
        assert 'errors' in result_dict
        assert 'warnings' in result_dict
        assert 'details' in result_dict
    
    def test_validation_edge_cases(self, validator):
        """测试验证的边界情况"""
        # 测试空数据
        result = validator.validate_regional_data({})
        assert result.is_valid == False
        
        # 测试None值
        result = validator.validate_regional_data(None)
        assert result.is_valid == False
        
        # 测试异常处理
        malformed_data = {'invalid': 'structure'}
        result = validator.validate_regional_data(malformed_data)
        assert result.is_valid == False
    
    def test_version_info_validation(self, validator, valid_regional_data):
        """测试版本信息验证"""
        # 测试缺少版本信息
        test_data = valid_regional_data.copy()
        del test_data['data_version']
        
        result = validator.validate_regional_data(test_data)
        
        assert result.is_valid == False
        error_messages = '\n'.join(result.errors)
        assert 'data_version' in error_messages or '缺少数据版本' in error_messages