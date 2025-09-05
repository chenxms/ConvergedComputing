"""
统计JSON序列化器测试

测试JSON数据序列化的各个功能模块，确保输出格式符合规范。
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime

from app.services.serialization.statistics_json_serializer import StatisticsJsonSerializer, SerializationException
from app.services.serialization.schema_validator import ValidationResult


class TestStatisticsJsonSerializer:
    """统计JSON序列化器测试类"""
    
    @pytest.fixture
    def mock_db_session(self):
        """模拟数据库会话"""
        return Mock()
    
    @pytest.fixture
    def serializer(self, mock_db_session):
        """创建序列化器实例"""
        return StatisticsJsonSerializer(mock_db_session)
    
    @pytest.fixture
    def sample_integrated_data(self):
        """示例集成数据"""
        return {
            'batch_code': 'BATCH_2025_001',
            'collection_time': datetime.utcnow().isoformat(),
            'batch_info': {
                'batch_code': 'BATCH_2025_001',
                'grade_level': '初中',
                'total_schools': 25,
                'total_students': 8500,
                'calculation_time': datetime.utcnow().isoformat()
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
                    'school_rankings': [
                        {
                            'school_id': 'SCH_001',
                            'school_name': '第一中学',
                            'avg_score': 85.2,
                            'score_rate': 0.852,
                            'ranking': 1
                        }
                    ],
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
            'dimensions': {
                '数学运算': {
                    'dimension_id': 'MATH_CALC',
                    'dimension_name': '数学运算',
                    'total_score': 40,
                    'avg_score': 32.5,
                    'score_rate': 0.8125,
                    'regional_ranking_avg': 0.8125
                }
            },
            'task_metadata': {
                'task_id': 'task_123',
                'batch_code': 'BATCH_2025_001',
                'calculation_time': datetime.utcnow().isoformat(),
                'status': 'completed'
            }
        }
    
    @pytest.mark.asyncio
    async def test_serialize_regional_data_success(self, serializer, sample_integrated_data):
        """测试区域数据序列化成功"""
        # Mock 数据集成器
        with patch.object(serializer.data_integrator, 'collect_all_statistics', new_callable=AsyncMock) as mock_collect:
            mock_collect.return_value = sample_integrated_data
            
            # Mock 缓存检查
            with patch.object(serializer, '_get_cached_regional_data', new_callable=AsyncMock) as mock_cache:
                mock_cache.return_value = None
                
                # Mock 数据保存
                with patch.object(serializer, '_save_regional_data', new_callable=AsyncMock) as mock_save:
                    mock_save.return_value = None
                    
                    result = await serializer.serialize_regional_data('BATCH_2025_001')
                    
                    # 验证结果结构
                    assert 'batch_info' in result
                    assert 'academic_subjects' in result
                    assert 'non_academic_subjects' in result
                    assert 'radar_chart_data' in result
                    assert 'data_version' in result
                    assert 'schema_version' in result
                    
                    # 验证批次信息
                    batch_info = result['batch_info']
                    assert batch_info['batch_code'] == 'BATCH_2025_001'
                    assert batch_info['grade_level'] == '初中'
                    assert batch_info['total_schools'] == 25
                    assert batch_info['total_students'] == 8500
                    
                    # 验证学业科目
                    assert '数学' in result['academic_subjects']
                    math_subject = result['academic_subjects']['数学']
                    assert math_subject['subject_id'] == 'MATH_001'
                    assert math_subject['total_score'] == 100
                    assert 'regional_stats' in math_subject
                    assert 'dimensions' in math_subject
                    
                    # 验证数据格式精度
                    regional_stats = math_subject['regional_stats']
                    assert regional_stats['avg_score'] == 78.5  # 1位小数
                    assert regional_stats['score_rate'] == 0.785  # 3位小数
    
    @pytest.mark.asyncio
    async def test_serialize_regional_data_with_cache(self, serializer):
        """测试使用缓存的区域数据序列化"""
        cached_data = {
            'batch_info': {'batch_code': 'BATCH_2025_001'},
            'academic_subjects': {},
            'non_academic_subjects': {},
            'radar_chart_data': {'academic_dimensions': [], 'non_academic_dimensions': []}
        }
        
        with patch.object(serializer, '_get_cached_regional_data', new_callable=AsyncMock) as mock_cache:
            mock_cache.return_value = cached_data
            
            result = await serializer.serialize_regional_data('BATCH_2025_001')
            
            assert result == cached_data
            mock_cache.assert_called_once_with('BATCH_2025_001')
    
    @pytest.mark.asyncio
    async def test_serialize_regional_data_validation_failure(self, serializer, sample_integrated_data):
        """测试区域数据验证失败"""
        # Mock数据集成器
        with patch.object(serializer.data_integrator, 'collect_all_statistics', new_callable=AsyncMock) as mock_collect:
            mock_collect.return_value = sample_integrated_data
            
            # Mock缓存检查
            with patch.object(serializer, '_get_cached_regional_data', new_callable=AsyncMock) as mock_cache:
                mock_cache.return_value = None
                
                # Mock验证失败
                validation_result = ValidationResult(is_valid=False)
                validation_result.errors = ['测试验证错误']
                
                with patch.object(serializer.schema_validator, 'validate_regional_data') as mock_validate:
                    mock_validate.return_value = validation_result
                    
                    with pytest.raises(SerializationException) as exc_info:
                        await serializer.serialize_regional_data('BATCH_2025_001')
                    
                    assert "数据验证失败" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_serialize_school_data_success(self, serializer):
        """测试学校数据序列化成功"""
        sample_school_data = {
            'batch_code': 'BATCH_2025_001',
            'school_id': 'SCH_001',
            'school_info': {
                'school_id': 'SCH_001',
                'school_name': '第一中学',
                'batch_code': 'BATCH_2025_001',
                'total_students': 340,
                'calculation_time': datetime.utcnow().isoformat()
            },
            'academic_subjects': {
                '数学': {
                    'subject_id': 'MATH_001',
                    'school_stats': {
                        'avg_score': 85.2,
                        'score_rate': 0.852,
                        'regional_ranking': 1
                    },
                    'dimensions': {}
                }
            },
            'non_academic_subjects': {},
            'dimensions': {}
        }
        
        sample_regional_data = {
            'academic_subjects': {
                '数学': {
                    'regional_stats': {
                        'avg_score': 78.5,
                        'score_rate': 0.785
                    },
                    'dimensions': {}
                }
            },
            'non_academic_subjects': {}
        }
        
        # Mock各种依赖
        with patch.object(serializer, '_get_cached_school_data', new_callable=AsyncMock) as mock_cache:
            mock_cache.return_value = None
            
            with patch.object(serializer, '_collect_school_statistics', new_callable=AsyncMock) as mock_collect:
                mock_collect.return_value = sample_school_data
                
                with patch.object(serializer, '_get_regional_data_for_comparison', new_callable=AsyncMock) as mock_regional:
                    mock_regional.return_value = sample_regional_data
                    
                    with patch.object(serializer, '_save_school_data', new_callable=AsyncMock) as mock_save:
                        mock_save.return_value = None
                        
                        result = await serializer.serialize_school_data('BATCH_2025_001', 'SCH_001')
                        
                        # 验证结果结构
                        assert 'school_info' in result
                        assert 'academic_subjects' in result
                        assert 'non_academic_subjects' in result
                        assert 'radar_chart_data' in result
                        assert 'data_version' in result
                        assert 'schema_version' in result
                        
                        # 验证学校信息
                        school_info = result['school_info']
                        assert school_info['school_id'] == 'SCH_001'
                        assert school_info['school_name'] == '第一中学'
                        assert school_info['batch_code'] == 'BATCH_2025_001'
    
    @pytest.mark.asyncio
    async def test_get_radar_chart_data_regional(self, serializer):
        """测试获取区域级雷达图数据"""
        regional_data = {
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
        
        with patch.object(serializer, 'serialize_regional_data', new_callable=AsyncMock) as mock_serialize:
            mock_serialize.return_value = regional_data
            
            result = await serializer.get_radar_chart_data('BATCH_2025_001')
            
            assert result == regional_data['radar_chart_data']
            mock_serialize.assert_called_once_with('BATCH_2025_001')
    
    @pytest.mark.asyncio
    async def test_get_radar_chart_data_school(self, serializer):
        """测试获取学校级雷达图数据"""
        school_data = {
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
        
        with patch.object(serializer, 'serialize_school_data', new_callable=AsyncMock) as mock_serialize:
            mock_serialize.return_value = school_data
            
            result = await serializer.get_radar_chart_data('BATCH_2025_001', 'SCH_001')
            
            assert result == school_data['radar_chart_data']
            mock_serialize.assert_called_once_with('BATCH_2025_001', 'SCH_001')
    
    @pytest.mark.asyncio 
    async def test_validate_json_data(self, serializer):
        """测试JSON数据验证"""
        test_data = {
            'batch_info': {'batch_code': 'BATCH_2025_001'},
            'academic_subjects': {},
            'non_academic_subjects': {},
            'radar_chart_data': {'academic_dimensions': [], 'non_academic_dimensions': []}
        }
        
        validation_result = ValidationResult(is_valid=True)
        
        with patch.object(serializer.schema_validator, 'validate_regional_data') as mock_validate:
            mock_validate.return_value = validation_result
            
            result = await serializer.validate_json_data(test_data, 'regional')
            
            assert result.is_valid == True
            mock_validate.assert_called_once_with(test_data)
    
    def test_validate_json_data_invalid_type(self, serializer):
        """测试无效的数据类型"""
        with pytest.raises(ValueError) as exc_info:
            asyncio.run(serializer.validate_json_data({}, 'invalid_type'))
        
        assert "不支持的数据类型" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_serialization_exception_handling(self, serializer):
        """测试序列化异常处理"""
        with patch.object(serializer.data_integrator, 'collect_all_statistics', new_callable=AsyncMock) as mock_collect:
            mock_collect.side_effect = Exception("数据库连接错误")
            
            with patch.object(serializer, '_get_cached_regional_data', new_callable=AsyncMock) as mock_cache:
                mock_cache.return_value = None
                
                with pytest.raises(SerializationException) as exc_info:
                    await serializer.serialize_regional_data('BATCH_2025_001')
                
                assert "序列化区域级数据失败" in str(exc_info.value)
                assert "数据库连接错误" in str(exc_info.value)
    
    def test_serializer_initialization(self, mock_db_session):
        """测试序列化器初始化"""
        serializer = StatisticsJsonSerializer(mock_db_session)
        
        assert serializer.db_session == mock_db_session
        assert serializer.data_integrator is not None
        assert serializer.regional_serializer is not None
        assert serializer.school_serializer is not None
        assert serializer.radar_formatter is not None
        assert serializer.schema_validator is not None
        assert serializer.version_manager is not None
        assert serializer.aggregation_repo is not None