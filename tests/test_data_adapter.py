import pytest
import json
from datetime import datetime
from typing import Dict, Any, List
from unittest.mock import MagicMock, patch, Mock

from app.database.repositories import DataAdapterRepository, DimensionJSONParser, RepositoryError
from app.database.models import StatisticalAggregation, AggregationLevel, CalculationStatus


class TestDataAdapterRepository:
    """测试数据适配器Repository"""
    
    def setup_method(self):
        """设置测试"""
        self.mock_db = MagicMock()
        self.repo = DataAdapterRepository(self.mock_db)
    
    def test_check_data_readiness_with_cleaned_data(self):
        """测试检查数据准备状态 - 有清洗数据"""
        batch_code = "G7-2025"
        
        # Mock清洗数据查询结果
        mock_cleaned_result = Mock()
        mock_cleaned_result.count = 1000
        mock_cleaned_result.students = 500
        
        # Mock原始数据查询结果
        mock_original_result = Mock()
        mock_original_result.students = 500
        
        # Mock问卷数据查询结果
        mock_questionnaire_result = Mock()
        mock_questionnaire_result.count = 15000
        mock_questionnaire_result.students = 500
        
        # 设置execute方法的返回值序列
        self.mock_db.execute.side_effect = [
            Mock(fetchone=Mock(return_value=mock_cleaned_result)),
            Mock(fetchone=Mock(return_value=mock_original_result)),
            Mock(fetchone=Mock(return_value=mock_questionnaire_result))
        ]
        
        result = self.repo.check_data_readiness(batch_code)
        
        # 验证结果
        assert result['batch_code'] == batch_code
        assert result['is_ready'] is True
        assert result['cleaned_records'] == 1000
        assert result['cleaned_students'] == 500
        assert result['original_students'] == 500
        assert result['questionnaire_records'] == 15000
        assert result['completeness_ratio'] == 1.0
        assert result['data_sources']['has_cleaned_data'] is True
        assert result['data_sources']['has_questionnaire_data'] is True
        assert result['data_sources']['has_original_data'] is True
    
    def test_check_data_readiness_incomplete_cleaned_data(self):
        """测试检查数据准备状态 - 清洗数据不完整"""
        batch_code = "G7-2025"
        
        # Mock清洗数据不完整
        mock_cleaned_result = Mock()
        mock_cleaned_result.count = 900
        mock_cleaned_result.students = 450  # 90%完成度
        
        mock_original_result = Mock()
        mock_original_result.students = 500
        
        mock_questionnaire_result = Mock()
        mock_questionnaire_result.count = 0
        mock_questionnaire_result.students = 0
        
        self.mock_db.execute.side_effect = [
            Mock(fetchone=Mock(return_value=mock_cleaned_result)),
            Mock(fetchone=Mock(return_value=mock_original_result)),
            Mock(fetchone=Mock(return_value=mock_questionnaire_result))
        ]
        
        result = self.repo.check_data_readiness(batch_code)
        
        assert result['is_ready'] is False  # 90% < 95%阈值
        assert result['completeness_ratio'] == 0.9
        assert result['data_sources']['has_cleaned_data'] is True
        assert result['data_sources']['has_questionnaire_data'] is False
    
    def test_check_data_readiness_no_data(self):
        """测试检查数据准备状态 - 无数据"""
        batch_code = "G7-2025"
        
        # Mock无数据
        mock_empty_result = Mock()
        mock_empty_result.count = 0
        mock_empty_result.students = 0
        
        self.mock_db.execute.side_effect = [
            Mock(fetchone=Mock(return_value=mock_empty_result)),
            Mock(fetchone=Mock(return_value=mock_empty_result)),
            Mock(fetchone=Mock(return_value=mock_empty_result))
        ]
        
        result = self.repo.check_data_readiness(batch_code)
        
        assert result['is_ready'] is False
        assert result['completeness_ratio'] == 0.0
        assert result['data_sources']['has_cleaned_data'] is False
        assert result['data_sources']['has_questionnaire_data'] is False
        assert result['data_sources']['has_original_data'] is False
    
    def test_get_student_scores_uses_cleaned_data(self):
        """测试获取学生分数 - 使用清洗数据源"""
        batch_code = "G7-2025"
        
        # Mock readiness检查返回清洗数据可用
        mock_readiness = {
            'data_sources': {
                'has_cleaned_data': True,
                'has_original_data': True
            }
        }
        self.repo.check_data_readiness = Mock(return_value=mock_readiness)
        self.repo._get_cleaned_student_scores = Mock(return_value=[
            {
                'student_id': '12345',
                'subject_name': '数学',
                'subject_type': 'exam',
                'total_score': 85.0,
                'max_score': 100.0,
                'data_source': 'cleaned'
            }
        ])
        
        result = self.repo.get_student_scores(batch_code)
        
        # 验证使用了清洗数据源
        self.repo._get_cleaned_student_scores.assert_called_once_with(batch_code, None, None)
        assert len(result) == 1
        assert result[0]['data_source'] == 'cleaned'
    
    def test_get_student_scores_fallback_to_legacy(self):
        """测试获取学生分数 - 回退到原始数据源"""
        batch_code = "G7-2025"
        
        # Mock readiness检查返回无清洗数据，有原始数据
        mock_readiness = {
            'data_sources': {
                'has_cleaned_data': False,
                'has_original_data': True
            }
        }
        self.repo.check_data_readiness = Mock(return_value=mock_readiness)
        self.repo._get_legacy_student_scores = Mock(return_value=[
            {
                'student_id': '12345',
                'subject_name': '数学',
                'subject_type': 'exam',
                'total_score': 85.0,
                'max_score': 100.0,
                'data_source': 'legacy'
            }
        ])
        
        result = self.repo.get_student_scores(batch_code)
        
        # 验证使用了原始数据源
        self.repo._get_legacy_student_scores.assert_called_once_with(batch_code, None, None)
        assert len(result) == 1
        assert result[0]['data_source'] == 'legacy'
    
    def test_get_student_scores_no_data_raises_error(self):
        """测试获取学生分数 - 无数据时抛出异常"""
        batch_code = "G7-2025"
        
        mock_readiness = {
            'data_sources': {
                'has_cleaned_data': False,
                'has_original_data': False
            }
        }
        self.repo.check_data_readiness = Mock(return_value=mock_readiness)
        
        with pytest.raises(RepositoryError, match="No data available for batch"):
            self.repo.get_student_scores(batch_code)
    
    def test_get_cleaned_student_scores(self):
        """测试从清洗数据表获取学生分数"""
        batch_code = "G7-2025"
        
        # Mock数据库查询结果
        mock_row = Mock()
        mock_row.student_id = '12345'
        mock_row.subject_name = '数学'
        mock_row.subject_type = 'exam'
        mock_row.total_score = 85.5
        mock_row.max_score = 100.0
        mock_row.dimension_scores = '{"运算能力": 30, "推理能力": 25}'
        mock_row.dimension_max_scores = '{"运算能力": 40, "推理能力": 35}'
        mock_row.school_id = 'SCHOOL001'
        mock_row.school_name = '实验中学'
        mock_row.grade = '7th_grade'
        mock_row.student_count = 1
        
        self.mock_db.execute.return_value.fetchall.return_value = [mock_row]
        
        # Mock JSON解析器
        mock_dimensions = {
            '运算能力': {'score': 30.0, 'max_score': 40.0, 'score_rate': 0.75},
            '推理能力': {'score': 25.0, 'max_score': 35.0, 'score_rate': 0.714}
        }
        self.repo.json_parser.parse_dimension_scores = Mock(return_value=mock_dimensions)
        
        result = self.repo._get_cleaned_student_scores(batch_code)
        
        # 验证结果
        assert len(result) == 1
        score_data = result[0]
        assert score_data['student_id'] == '12345'
        assert score_data['subject_name'] == '数学'
        assert score_data['subject_type'] == 'exam'
        assert score_data['total_score'] == 85.5
        assert score_data['max_score'] == 100.0
        assert score_data['school_id'] == 'SCHOOL001'
        assert score_data['school_name'] == '实验中学'
        assert score_data['grade'] == '7th_grade'
        assert score_data['data_source'] == 'cleaned'
        assert 'dimensions' in score_data
        assert score_data['dimensions'] == mock_dimensions
    
    def test_get_cleaned_student_scores_with_filters(self):
        """测试带过滤条件的清洗数据查询"""
        batch_code = "G7-2025"
        subject_type = "exam"
        school_id = "SCHOOL001"
        
        self.mock_db.execute.return_value.fetchall.return_value = []
        
        self.repo._get_cleaned_student_scores(batch_code, subject_type, school_id)
        
        # 验证SQL查询包含过滤条件
        call_args = self.mock_db.execute.call_args
        sql_query = call_args[0][0]
        params = call_args[0][1]
        
        assert "AND subject_type = %s" in sql_query
        assert "AND school_id = %s" in sql_query
        assert params == [batch_code, subject_type, school_id]
    
    def test_get_legacy_student_scores(self):
        """测试从原始数据表获取学生分数"""
        batch_code = "G7-2025"
        
        # Mock原始数据查询结果
        mock_row = Mock()
        mock_row.student_id = '12345'
        mock_row.subject_name = '数学'
        mock_row.total_score = 85.5
        mock_row.max_score = 100.0
        mock_row.school_id = 'SCHOOL001'
        mock_row.grade = '7th_grade'
        mock_row.student_count = 1
        
        self.mock_db.execute.return_value.fetchall.return_value = [mock_row]
        
        result = self.repo._get_legacy_student_scores(batch_code)
        
        assert len(result) == 1
        score_data = result[0]
        assert score_data['student_id'] == '12345'
        assert score_data['subject_name'] == '数学'
        assert score_data['subject_type'] == 'exam'  # 默认类型
        assert score_data['total_score'] == 85.5
        assert score_data['max_score'] == 100.0
        assert score_data['school_id'] == 'SCHOOL001'
        assert score_data['school_name'] is None  # 原始数据不包含
        assert score_data['data_source'] == 'legacy'
        assert score_data['dimensions'] == {}  # 原始数据维度为空
    
    def test_get_questionnaire_details(self):
        """测试获取问卷明细数据"""
        batch_code = "G7-2025"
        subject_name = "问卷"
        
        # Mock问卷明细查询结果
        mock_row = Mock()
        mock_row.student_id = '12345'
        mock_row.subject_name = '问卷'
        mock_row.question_id = 'Q001'
        mock_row.original_score = 3.0
        mock_row.max_score = 5.0
        mock_row.scale_level = 5
        mock_row.instrument_type = 'likert_5'
        mock_row.school_id = 'SCHOOL001'
        mock_row.school_name = '实验中学'
        mock_row.grade = '7th_grade'
        
        self.mock_db.execute.return_value.fetchall.return_value = [mock_row]
        
        result = self.repo.get_questionnaire_details(batch_code, subject_name)
        
        assert len(result) == 1
        detail = result[0]
        assert detail['student_id'] == '12345'
        assert detail['subject_name'] == '问卷'
        assert detail['question_id'] == 'Q001'
        assert detail['original_score'] == 3.0
        assert detail['max_score'] == 5.0
        assert detail['scale_level'] == 5
        assert detail['instrument_type'] == 'likert_5'
        assert detail['school_id'] == 'SCHOOL001'
    
    def test_get_questionnaire_distribution(self):
        """测试获取问卷选项分布统计"""
        batch_code = "G7-2025"
        
        # Mock分布查询结果
        mock_row = Mock()
        mock_row.subject_name = '问卷'
        mock_row.question_id = 'Q001'
        mock_row.option_level = 3
        mock_row.student_count = 150
        mock_row.percentage = 30.5
        mock_row.scale_level = 5
        
        self.mock_db.execute.return_value.fetchall.return_value = [mock_row]
        
        result = self.repo.get_questionnaire_distribution(batch_code)
        
        assert len(result) == 1
        dist = result[0]
        assert dist['subject_name'] == '问卷'
        assert dist['question_id'] == 'Q001'
        assert dist['option_level'] == 3
        assert dist['student_count'] == 150
        assert dist['percentage'] == 30.5
        assert dist['scale_level'] == 5
    
    def test_get_subject_configurations(self):
        """测试获取科目配置信息"""
        batch_code = "G7-2025"
        
        # Mock科目配置查询结果
        mock_row1 = Mock()
        mock_row1.subject_name = '数学'
        mock_row1.subject_type = 'exam'
        mock_row1.max_score = 100.0
        mock_row1.question_count = 20
        mock_row1.question_type_enum = None
        
        mock_row2 = Mock()
        mock_row2.subject_name = '问卷'
        mock_row2.subject_type = 'survey'
        mock_row2.max_score = 100.0
        mock_row2.question_count = 25
        mock_row2.question_type_enum = 'questionnaire'
        
        self.mock_db.execute.return_value.fetchall.return_value = [mock_row1, mock_row2]
        
        result = self.repo.get_subject_configurations(batch_code)
        
        assert len(result) == 2
        
        # 验证考试类科目
        exam_subject = result[0]
        assert exam_subject['subject_name'] == '数学'
        assert exam_subject['subject_type'] == 'exam'
        assert exam_subject['max_score'] == 100.0
        
        # 验证问卷类科目
        questionnaire_subject = result[1]
        assert questionnaire_subject['subject_name'] == '问卷'
        assert questionnaire_subject['subject_type'] == 'questionnaire'  # 通过normalize转换
        assert questionnaire_subject['question_type_enum'] == 'questionnaire'
    
    def test_normalize_subject_type(self):
        """测试科目类型标准化"""
        # 测试问卷类型
        result1 = self.repo._normalize_subject_type(None, 'questionnaire')
        assert result1 == 'questionnaire'
        
        result2 = self.repo._normalize_subject_type(None, 'QUESTIONNAIRE')
        assert result2 == 'questionnaire'
        
        # 测试考试类型
        result3 = self.repo._normalize_subject_type('exam', None)
        assert result3 == 'exam'
        
        result4 = self.repo._normalize_subject_type('EXAM', None)
        assert result4 == 'exam'
        
        # 测试默认类型
        result5 = self.repo._normalize_subject_type(None, None)
        assert result5 == 'exam'
    
    def test_get_batch_summary(self):
        """测试获取批次数据摘要"""
        batch_code = "G7-2025"
        
        # Mock readiness检查
        mock_readiness = {
            'batch_code': batch_code,
            'is_ready': True,
            'cleaned_students': 500,
            'completeness_ratio': 1.0
        }
        self.repo.check_data_readiness = Mock(return_value=mock_readiness)
        
        # Mock科目配置
        mock_subject_configs = [
            {'subject_name': '数学', 'subject_type': 'exam'},
            {'subject_name': '语文', 'subject_type': 'exam'},
            {'subject_name': '问卷', 'subject_type': 'questionnaire'}
        ]
        self.repo.get_subject_configurations = Mock(return_value=mock_subject_configs)
        
        result = self.repo.get_batch_summary(batch_code)
        
        assert result['batch_code'] == batch_code
        assert result['readiness'] == mock_readiness
        assert result['subjects']['total'] == 3
        assert result['subjects']['exam'] == 2
        assert result['subjects']['questionnaire'] == 1
        assert result['subjects']['exam_subjects'] == ['数学', '语文']
        assert result['subjects']['questionnaire_subjects'] == ['问卷']
        assert result['data_source'] == 'cleaned'
    
    def test_database_error_handling(self):
        """测试数据库错误处理"""
        batch_code = "G7-2025"
        self.mock_db.execute.side_effect = Exception("Database connection failed")
        
        with pytest.raises(RepositoryError):
            self.repo.check_data_readiness(batch_code)


class TestDimensionJSONParser:
    """测试JSON维度数据解析器"""
    
    def setup_method(self):
        """设置测试"""
        self.parser = DimensionJSONParser()
    
    def test_parse_dimension_scores_valid_json(self):
        """测试解析有效的JSON维度数据"""
        scores_json = '{"运算能力": 30, "推理能力": 25, "应用能力": 20}'
        max_scores_json = '{"运算能力": 40, "推理能力": 35, "应用能力": 25}'
        
        result = self.parser.parse_dimension_scores(scores_json, max_scores_json)
        
        assert len(result) == 3
        assert result['运算能力']['score'] == 30.0
        assert result['运算能力']['max_score'] == 40.0
        assert result['运算能力']['score_rate'] == 0.75
        assert result['推理能力']['score'] == 25.0
        assert result['推理能力']['max_score'] == 35.0
        assert abs(result['推理能力']['score_rate'] - 0.714) < 0.001
        assert result['应用能力']['score'] == 20.0
        assert result['应用能力']['max_score'] == 25.0
        assert result['应用能力']['score_rate'] == 0.8
    
    def test_parse_dimension_scores_dict_input(self):
        """测试解析字典格式的维度数据"""
        scores_dict = {"运算能力": 30, "推理能力": 25}
        max_scores_dict = {"运算能力": 40, "推理能力": 35}
        
        result = self.parser.parse_dimension_scores(scores_dict, max_scores_dict)
        
        assert len(result) == 2
        assert result['运算能力']['score'] == 30.0
        assert result['运算能力']['max_score'] == 40.0
        assert result['推理能力']['score'] == 25.0
        assert result['推理能力']['max_score'] == 35.0
    
    def test_parse_dimension_scores_missing_max_score(self):
        """测试解析缺失最大分数的情况"""
        scores_json = '{"运算能力": 30, "推理能力": 25, "应用能力": 20}'
        max_scores_json = '{"运算能力": 40, "推理能力": 35}'  # 缺少应用能力
        
        result = self.parser.parse_dimension_scores(scores_json, max_scores_json)
        
        assert len(result) == 3
        assert result['应用能力']['score'] == 20.0
        assert result['应用能力']['max_score'] == 0.0  # 默认为0
        assert result['应用能力']['score_rate'] == 0.0  # 0除以0为0
    
    def test_parse_dimension_scores_zero_max_score(self):
        """测试解析最大分数为0的情况"""
        scores_json = '{"运算能力": 30}'
        max_scores_json = '{"运算能力": 0}'
        
        result = self.parser.parse_dimension_scores(scores_json, max_scores_json)
        
        assert result['运算能力']['score'] == 30.0
        assert result['运算能力']['max_score'] == 0.0
        assert result['运算能力']['score_rate'] == 0.0
    
    def test_parse_dimension_scores_null_values(self):
        """测试解析空值的情况"""
        scores_json = '{"运算能力": null, "推理能力": 25}'
        max_scores_json = '{"运算能力": 40, "推理能力": null}'
        
        result = self.parser.parse_dimension_scores(scores_json, max_scores_json)
        
        assert result['运算能力']['score'] == 0.0
        assert result['运算能力']['max_score'] == 40.0
        assert result['运算能力']['score_rate'] == 0.0
        assert result['推理能力']['score'] == 25.0
        assert result['推理能力']['max_score'] == 0.0
        assert result['推理能力']['score_rate'] == 0.0
    
    def test_parse_dimension_scores_invalid_json(self):
        """测试解析无效JSON的情况"""
        invalid_json = '{"运算能力": 30, "推理能力":'  # 无效JSON
        max_scores_json = '{"运算能力": 40, "推理能力": 35}'
        
        result = self.parser.parse_dimension_scores(invalid_json, max_scores_json)
        
        assert result == {}  # 返回空字典
    
    def test_parse_dimension_scores_non_dict_input(self):
        """测试解析非字典类型输入的情况"""
        scores_list = '[30, 25, 20]'  # 列表而非字典
        max_scores_json = '{"运算能力": 40, "推理能力": 35}'
        
        result = self.parser.parse_dimension_scores(scores_list, max_scores_json)
        
        assert result == {}  # 返回空字典
    
    def test_format_dimensions_for_calculation_dict_format(self):
        """测试格式化字典格式的维度数据用于计算"""
        dimensions = {
            '运算能力': {'score': 30.0, 'max_score': 40.0, 'score_rate': 0.75},
            '推理能力': {'score': 25.0, 'max_score': 35.0, 'score_rate': 0.714}
        }
        
        result = self.parser.format_dimensions_for_calculation(dimensions)
        
        assert len(result) == 2
        assert result['运算能力'] == 30.0
        assert result['推理能力'] == 25.0
    
    def test_format_dimensions_for_calculation_numeric_format(self):
        """测试格式化数值格式的维度数据用于计算"""
        dimensions = {
            '运算能力': 30.0,
            '推理能力': 25,
            '应用能力': 20.5
        }
        
        result = self.parser.format_dimensions_for_calculation(dimensions)
        
        assert len(result) == 3
        assert result['运算能力'] == 30.0
        assert result['推理能力'] == 25.0
        assert result['应用能力'] == 20.5
    
    def test_format_dimensions_for_calculation_mixed_format(self):
        """测试格式化混合格式的维度数据用于计算"""
        dimensions = {
            '运算能力': {'score': 30.0, 'max_score': 40.0},
            '推理能力': 25.0,  # 直接数值
            '应用能力': {'invalid': 'data'}  # 无score字段
        }
        
        result = self.parser.format_dimensions_for_calculation(dimensions)
        
        assert len(result) == 2  # 只有有效的两个维度
        assert result['运算能力'] == 30.0
        assert result['推理能力'] == 25.0
        assert '应用能力' not in result
    
    def test_format_dimensions_for_calculation_error_handling(self):
        """测试格式化维度数据时的错误处理"""
        invalid_dimensions = "invalid_input"  # 非字典类型
        
        result = self.parser.format_dimensions_for_calculation(invalid_dimensions)
        
        assert result == {}  # 返回空字典


# 集成测试示例
class TestDataAdapterIntegration:
    """数据适配器集成测试"""
    
    @pytest.mark.integration
    def test_end_to_end_data_adapter_workflow(self):
        """测试端到端数据适配器工作流"""
        # 这个测试需要真实的数据库连接和清洗数据
        # 在实际环境中运行
        pass
    
    @pytest.mark.integration
    def test_cleaned_data_legacy_data_consistency(self):
        """测试清洗数据与原始数据的一致性"""
        # 测试同一批次的清洗数据和原始数据计算结果是否一致
        pass
    
    @pytest.mark.integration
    def test_dimension_json_parsing_performance(self):
        """测试JSON维度解析性能"""
        # 测试大量维度数据的解析性能
        pass


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])