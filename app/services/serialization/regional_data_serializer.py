"""
区域级数据序列化器

负责将区域统计数据序列化为JSON格式，严格遵循
json-data-specification.md中的区域级数据格式规范。
"""

import logging
from typing import Dict, Any, List
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

from .radar_chart_formatter import RadarChartFormatter
from .version_manager import VersionManager

logger = logging.getLogger(__name__)


class RegionalDataSerializer:
    """区域级数据序列化器"""
    
    def __init__(self):
        self.radar_formatter = RadarChartFormatter()
        self.version_manager = VersionManager()
    
    def serialize(self, integrated_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        序列化区域级统计数据
        
        Args:
            integrated_data: 集成的统计数据
            
        Returns:
            符合规范的区域级JSON数据
        """
        logger.info(f"开始序列化区域级数据: {integrated_data.get('batch_code')}")
        
        try:
            batch_code = integrated_data['batch_code']
            
            # 构建区域级JSON数据
            regional_json = {
                # 版本信息
                'data_version': self.version_manager.get_current_data_version(),
                'schema_version': self.version_manager.get_current_schema_version(),
                
                # 批次基础信息
                'batch_info': self._build_batch_info(integrated_data),
                
                # 学业科目统计
                'academic_subjects': self._build_academic_subjects(integrated_data),
                
                # 非学业科目统计
                'non_academic_subjects': self._build_non_academic_subjects(integrated_data),
                
                # 雷达图数据
                'radar_chart_data': self._build_radar_chart_data(integrated_data)
            }
            
            logger.info(f"区域级数据序列化完成: {batch_code}")
            return regional_json
            
        except Exception as e:
            logger.error(f"区域级数据序列化失败: {str(e)}")
            raise
    
    def _build_batch_info(self, integrated_data: Dict[str, Any]) -> Dict[str, Any]:
        """构建批次信息"""
        batch_info = integrated_data.get('batch_info', {})
        
        return {
            'batch_code': batch_info.get('batch_code', ''),
            'grade_level': batch_info.get('grade_level', '初中'),
            'total_schools': batch_info.get('total_schools', 0),
            'total_students': batch_info.get('total_students', 0),
            'calculation_time': batch_info.get('calculation_time', datetime.utcnow().isoformat())
        }
    
    def _build_academic_subjects(self, integrated_data: Dict[str, Any]) -> Dict[str, Any]:
        """构建学业科目数据"""
        academic_subjects = integrated_data.get('academic_subjects', {})
        result = {}
        
        for subject_name, subject_data in academic_subjects.items():
            result[subject_name] = self._serialize_academic_subject(subject_data)
        
        return result
    
    def _serialize_academic_subject(self, subject_data: Dict[str, Any]) -> Dict[str, Any]:
        """序列化单个学业科目"""
        regional_stats = subject_data.get('regional_stats', {})
        
        return {
            'subject_id': subject_data.get('subject_id', ''),
            'subject_type': subject_data.get('subject_type', '考试类'),
            'total_score': subject_data.get('total_score', 100),
            'regional_stats': {
                'avg_score': self._round_score(regional_stats.get('avg_score', 0)),
                'score_rate': self._round_rate(regional_stats.get('score_rate', 0)),
                'difficulty': self._round_rate(regional_stats.get('difficulty', 0)),
                'discrimination': self._round_rate(regional_stats.get('discrimination', 0)),
                'std_dev': self._round_score(regional_stats.get('std_dev', 0)),
                'max_score': regional_stats.get('max_score', 0),
                'min_score': regional_stats.get('min_score', 0)
            },
            'grade_distribution': self._serialize_grade_distribution(
                subject_data.get('grade_distribution', {})
            ),
            'school_rankings': self._serialize_school_rankings(
                subject_data.get('school_rankings', [])
            ),
            'dimensions': self._serialize_dimensions(
                subject_data.get('dimensions', {})
            )
        }
    
    def _build_non_academic_subjects(self, integrated_data: Dict[str, Any]) -> Dict[str, Any]:
        """构建非学业科目数据"""
        non_academic_subjects = integrated_data.get('non_academic_subjects', {})
        result = {}
        
        for subject_name, subject_data in non_academic_subjects.items():
            if subject_data.get('subject_type') == '问卷类':
                result[subject_name] = self._serialize_survey_subject(subject_data)
            elif subject_data.get('subject_type') == '人机交互类':
                result[subject_name] = self._serialize_interactive_subject(subject_data)
        
        return result
    
    def _serialize_survey_subject(self, subject_data: Dict[str, Any]) -> Dict[str, Any]:
        """序列化问卷类科目"""
        return {
            'subject_id': subject_data.get('subject_id', ''),
            'subject_type': '问卷类',
            'total_schools_participated': subject_data.get('total_schools_participated', 0),
            'total_students_participated': subject_data.get('total_students_participated', 0),
            'dimensions': self._serialize_survey_dimensions(
                subject_data.get('dimensions', {})
            )
        }
    
    def _serialize_interactive_subject(self, subject_data: Dict[str, Any]) -> Dict[str, Any]:
        """序列化人机交互类科目"""
        regional_stats = subject_data.get('regional_stats', {})
        
        return {
            'subject_id': subject_data.get('subject_id', ''),
            'subject_type': '人机交互类',
            'total_schools_participated': subject_data.get('total_schools_participated', 0),
            'total_students_participated': subject_data.get('total_students_participated', 0),
            'regional_stats': {
                'avg_score': self._round_score(regional_stats.get('avg_score', 0)),
                'score_rate': self._round_rate(regional_stats.get('score_rate', 0)),
                'total_score': regional_stats.get('total_score', 60),
                'std_dev': self._round_score(regional_stats.get('std_dev', 0))
            },
            'dimensions': self._serialize_dimensions(
                subject_data.get('dimensions', {})
            )
        }
    
    def _serialize_grade_distribution(self, grade_data: Dict[str, Any]) -> Dict[str, Any]:
        """序列化等级分布数据"""
        return {
            'excellent': {
                'count': grade_data.get('excellent', {}).get('count', 0),
                'percentage': self._round_percentage(
                    grade_data.get('excellent', {}).get('percentage', 0)
                )
            },
            'good': {
                'count': grade_data.get('good', {}).get('count', 0),
                'percentage': self._round_percentage(
                    grade_data.get('good', {}).get('percentage', 0)
                )
            },
            'pass': {
                'count': grade_data.get('pass', {}).get('count', 0),
                'percentage': self._round_percentage(
                    grade_data.get('pass', {}).get('percentage', 0)
                )
            },
            'fail': {
                'count': grade_data.get('fail', {}).get('count', 0),
                'percentage': self._round_percentage(
                    grade_data.get('fail', {}).get('percentage', 0)
                )
            }
        }
    
    def _serialize_school_rankings(self, rankings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """序列化学校排名数据"""
        result = []
        
        for ranking in rankings:
            result.append({
                'school_id': ranking.get('school_id', ''),
                'school_name': ranking.get('school_name', ''),
                'avg_score': self._round_score(ranking.get('avg_score', 0)),
                'score_rate': self._round_rate(ranking.get('score_rate', 0)),
                'ranking': ranking.get('ranking', 0)
            })
        
        return result
    
    def _serialize_dimensions(self, dimensions: Dict[str, Any]) -> Dict[str, Any]:
        """序列化维度数据"""
        result = {}
        
        for dim_name, dim_data in dimensions.items():
            result[dim_name] = {
                'dimension_id': dim_data.get('dimension_id', ''),
                'dimension_name': dim_name,
                'total_score': dim_data.get('total_score', 0),
                'avg_score': self._round_score(dim_data.get('avg_score', 0)),
                'score_rate': self._round_rate(dim_data.get('score_rate', 0)),
                'regional_ranking_avg': self._round_rate(dim_data.get('regional_ranking_avg', 0))
            }
        
        return result
    
    def _serialize_survey_dimensions(self, dimensions: Dict[str, Any]) -> Dict[str, Any]:
        """序列化问卷维度数据"""
        result = {}
        
        for dim_name, dim_data in dimensions.items():
            result[dim_name] = {
                'dimension_id': dim_data.get('dimension_id', ''),
                'dimension_name': dim_name,
                'total_score': dim_data.get('total_score', 0),
                'avg_score': self._round_score(dim_data.get('avg_score', 0)),
                'score_rate': self._round_rate(dim_data.get('score_rate', 0)),
                'question_analysis': self._serialize_question_analysis(
                    dim_data.get('question_analysis', [])
                )
            }
        
        return result
    
    def _serialize_question_analysis(self, questions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """序列化题目分析数据"""
        result = []
        
        for question in questions:
            option_dist = question.get('option_distribution', {})
            serialized_options = {}
            
            for option, data in option_dist.items():
                serialized_options[option] = {
                    'count': data.get('count', 0),
                    'percentage': self._round_percentage(data.get('percentage', 0))
                }
            
            result.append({
                'question_id': question.get('question_id', ''),
                'question_text': question.get('question_text', ''),
                'scale_type': question.get('scale_type', '正向'),
                'option_distribution': serialized_options
            })
        
        return result
    
    def _build_radar_chart_data(self, integrated_data: Dict[str, Any]) -> Dict[str, Any]:
        """构建雷达图数据"""
        academic_subjects = integrated_data.get('academic_subjects', {})
        non_academic_subjects = integrated_data.get('non_academic_subjects', {})
        
        # 提取所有维度数据
        dimensions = self.radar_formatter.extract_dimensions_from_subjects(
            academic_subjects, non_academic_subjects
        )
        
        # 格式化为雷达图数据
        return self.radar_formatter.format_regional_radar_data(dimensions)
    
    def _round_score(self, score: float) -> float:
        """分数保留1位小数"""
        if not isinstance(score, (int, float)):
            return 0.0
        
        decimal_score = Decimal(str(score))
        rounded_score = decimal_score.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP)
        return float(rounded_score)
    
    def _round_rate(self, rate: float) -> float:
        """得分率保留3位小数"""
        if not isinstance(rate, (int, float)):
            return 0.0
        
        decimal_rate = Decimal(str(rate))
        rounded_rate = decimal_rate.quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)
        return float(rounded_rate)
    
    def _round_percentage(self, percentage: float) -> float:
        """百分比保留2位小数"""
        if not isinstance(percentage, (int, float)):
            return 0.0
        
        decimal_percentage = Decimal(str(percentage))
        rounded_percentage = decimal_percentage.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        return float(rounded_percentage)