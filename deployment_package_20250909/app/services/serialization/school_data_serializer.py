"""
学校级数据序列化器

负责将学校统计数据序列化为JSON格式，严格遵循
json-data-specification.md中的学校级数据格式规范。
"""

import logging
from typing import Dict, Any, List
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

from .radar_chart_formatter import RadarChartFormatter
from .version_manager import VersionManager

logger = logging.getLogger(__name__)


class SchoolDataSerializer:
    """学校级数据序列化器"""
    
    def __init__(self):
        self.radar_formatter = RadarChartFormatter()
        self.version_manager = VersionManager()
    
    def serialize(
        self, 
        school_data: Dict[str, Any], 
        regional_data: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        序列化学校级统计数据
        
        Args:
            school_data: 学校统计数据
            regional_data: 区域统计数据（用于对比）
            
        Returns:
            符合规范的学校级JSON数据
        """
        logger.info(f"开始序列化学校级数据: {school_data.get('school_id')}")
        
        try:
            # 构建学校级JSON数据
            school_json = {
                # 版本信息
                'data_version': self.version_manager.get_current_data_version(),
                'schema_version': self.version_manager.get_current_schema_version(),
                
                # 学校基础信息
                'school_info': self._build_school_info(school_data),
                
                # 学业科目统计
                'academic_subjects': self._build_academic_subjects(school_data, regional_data),
                
                # 非学业科目统计
                'non_academic_subjects': self._build_non_academic_subjects(school_data, regional_data),
                
                # 雷达图数据
                'radar_chart_data': self._build_radar_chart_data(school_data, regional_data)
            }
            
            logger.info(f"学校级数据序列化完成: {school_data.get('school_id')}")
            return school_json
            
        except Exception as e:
            logger.error(f"学校级数据序列化失败: {str(e)}")
            raise
    
    def _build_school_info(self, school_data: Dict[str, Any]) -> Dict[str, Any]:
        """构建学校信息"""
        school_info = school_data.get('school_info', {})
        
        return {
            'school_id': school_info.get('school_id', ''),
            'school_name': school_info.get('school_name', ''),
            'batch_code': school_info.get('batch_code', ''),
            'total_students': school_info.get('total_students', 0),
            'calculation_time': school_info.get('calculation_time', datetime.utcnow().isoformat())
        }
    
    def _build_academic_subjects(
        self, 
        school_data: Dict[str, Any], 
        regional_data: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """构建学业科目数据"""
        school_academic = school_data.get('academic_subjects', {})
        regional_academic = regional_data.get('academic_subjects', {}) if regional_data else {}
        result = {}
        
        for subject_name, subject_data in school_academic.items():
            regional_subject = regional_academic.get(subject_name, {})
            result[subject_name] = self._serialize_school_academic_subject(
                subject_data, regional_subject
            )
        
        return result
    
    def _serialize_school_academic_subject(
        self, 
        school_subject: Dict[str, Any], 
        regional_subject: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """序列化学校学业科目"""
        school_stats = school_subject.get('school_stats', {})
        regional_stats = regional_subject.get('regional_stats', {}) if regional_subject else {}
        
        return {
            'subject_id': school_subject.get('subject_id', ''),
            'subject_type': school_subject.get('subject_type', '考试类'),
            'total_score': school_subject.get('total_score', 100),
            'school_stats': {
                'avg_score': self._round_score(school_stats.get('avg_score', 0)),
                'score_rate': self._round_rate(school_stats.get('score_rate', 0)),
                'std_dev': self._round_score(school_stats.get('std_dev', 0)),
                'max_score': school_stats.get('max_score', 0),
                'min_score': school_stats.get('min_score', 0),
                'regional_ranking': school_stats.get('regional_ranking', 0)
            },
            'percentiles': self._serialize_percentiles(
                school_subject.get('percentiles', {})
            ),
            'grade_distribution': self._serialize_grade_distribution(
                school_subject.get('grade_distribution', {})
            ),
            'regional_comparison': self._build_regional_comparison(
                school_stats, regional_stats
            ),
            'dimensions': self._serialize_school_dimensions(
                school_subject.get('dimensions', {}),
                regional_subject.get('dimensions', {}) if regional_subject else {}
            )
        }
    
    def _build_non_academic_subjects(
        self, 
        school_data: Dict[str, Any], 
        regional_data: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """构建非学业科目数据"""
        school_non_academic = school_data.get('non_academic_subjects', {})
        regional_non_academic = regional_data.get('non_academic_subjects', {}) if regional_data else {}
        result = {}
        
        for subject_name, subject_data in school_non_academic.items():
            regional_subject = regional_non_academic.get(subject_name, {})
            
            if subject_data.get('subject_type') == '问卷类':
                result[subject_name] = self._serialize_school_survey_subject(
                    subject_data, regional_subject
                )
            elif subject_data.get('subject_type') == '人机交互类':
                result[subject_name] = self._serialize_school_interactive_subject(
                    subject_data, regional_subject
                )
        
        return result
    
    def _serialize_school_survey_subject(
        self, 
        school_subject: Dict[str, Any], 
        regional_subject: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """序列化学校问卷类科目"""
        return {
            'subject_id': school_subject.get('subject_id', ''),
            'subject_type': '问卷类',
            'participated_students': school_subject.get('participated_students', 0),
            'dimensions': self._serialize_school_survey_dimensions(
                school_subject.get('dimensions', {}),
                regional_subject.get('dimensions', {}) if regional_subject else {}
            )
        }
    
    def _serialize_school_interactive_subject(
        self, 
        school_subject: Dict[str, Any], 
        regional_subject: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """序列化学校人机交互类科目"""
        school_stats = school_subject.get('school_stats', {})
        regional_stats = regional_subject.get('regional_stats', {}) if regional_subject else {}
        
        return {
            'subject_id': school_subject.get('subject_id', ''),
            'subject_type': '人机交互类',
            'participated_students': school_subject.get('participated_students', 0),
            'school_stats': {
                'avg_score': self._round_score(school_stats.get('avg_score', 0)),
                'score_rate': self._round_rate(school_stats.get('score_rate', 0)),
                'std_dev': self._round_score(school_stats.get('std_dev', 0)),
                'max_score': school_stats.get('max_score', 0),
                'min_score': school_stats.get('min_score', 0),
                'regional_ranking': school_stats.get('regional_ranking', 0)
            },
            'percentiles': self._serialize_percentiles(
                school_subject.get('percentiles', {})
            ),
            'regional_comparison': self._build_regional_comparison(
                school_stats, regional_stats
            ),
            'dimensions': self._serialize_school_dimensions(
                school_subject.get('dimensions', {}),
                regional_subject.get('dimensions', {}) if regional_subject else {}
            )
        }
    
    def _serialize_percentiles(self, percentiles: Dict[str, Any]) -> Dict[str, Any]:
        """序列化百分位数数据"""
        return {
            'P10': percentiles.get('P10', 0),
            'P50': percentiles.get('P50', 0),
            'P90': percentiles.get('P90', 0)
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
    
    def _build_regional_comparison(
        self, 
        school_stats: Dict[str, Any], 
        regional_stats: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """构建区域对比数据"""
        if not regional_stats:
            return {
                'regional_avg_score': 0,
                'regional_score_rate': 0,
                'difference': 0,
                'rate_difference': 0,
                'performance_level': '未知'
            }
        
        school_avg = school_stats.get('avg_score', 0)
        school_rate = school_stats.get('score_rate', 0)
        regional_avg = regional_stats.get('avg_score', 0)
        regional_rate = regional_stats.get('score_rate', 0)
        
        difference = school_avg - regional_avg
        rate_difference = school_rate - regional_rate
        
        # 判断表现水平
        if rate_difference >= 0.05:  # 得分率差异 >= 5%
            performance_level = '优秀'
        elif rate_difference >= 0.02:  # 得分率差异 >= 2%
            performance_level = '良好'
        elif rate_difference >= -0.02:  # 得分率差异 >= -2%
            performance_level = '一般'
        else:
            performance_level = '待提升'
        
        return {
            'regional_avg_score': self._round_score(regional_avg),
            'regional_score_rate': self._round_rate(regional_rate),
            'difference': self._round_score(difference),
            'rate_difference': self._round_rate(rate_difference),
            'performance_level': performance_level
        }
    
    def _serialize_school_dimensions(
        self, 
        school_dimensions: Dict[str, Any], 
        regional_dimensions: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """序列化学校维度数据"""
        result = {}
        
        for dim_name, school_dim in school_dimensions.items():
            regional_dim = regional_dimensions.get(dim_name, {}) if regional_dimensions else {}
            
            school_avg = school_dim.get('school_avg_score', 0)
            school_rate = school_dim.get('school_score_rate', 0)
            regional_avg = regional_dim.get('avg_score', 0)
            regional_rate = regional_dim.get('score_rate', 0)
            
            result[dim_name] = {
                'dimension_id': school_dim.get('dimension_id', ''),
                'dimension_name': dim_name,
                'total_score': school_dim.get('total_score', 0),
                'school_avg_score': self._round_score(school_avg),
                'school_score_rate': self._round_rate(school_rate),
                'regional_avg_score': self._round_score(regional_avg),
                'regional_score_rate': self._round_rate(regional_rate),
                'difference': self._round_score(school_avg - regional_avg),
                'rate_difference': self._round_rate(school_rate - regional_rate),
                'regional_ranking': school_dim.get('regional_ranking', 0)
            }
        
        return result
    
    def _serialize_school_survey_dimensions(
        self, 
        school_dimensions: Dict[str, Any], 
        regional_dimensions: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """序列化学校问卷维度数据"""
        result = {}
        
        for dim_name, school_dim in school_dimensions.items():
            regional_dim = regional_dimensions.get(dim_name, {}) if regional_dimensions else {}
            
            school_avg = school_dim.get('school_avg_score', 0)
            school_rate = school_dim.get('school_score_rate', 0)
            regional_avg = regional_dim.get('avg_score', 0)
            regional_rate = regional_dim.get('score_rate', 0)
            
            result[dim_name] = {
                'dimension_id': school_dim.get('dimension_id', ''),
                'dimension_name': dim_name,
                'total_score': school_dim.get('total_score', 0),
                'school_avg_score': self._round_score(school_avg),
                'school_score_rate': self._round_rate(school_rate),
                'regional_avg_score': self._round_score(regional_avg),
                'regional_score_rate': self._round_rate(regional_rate),
                'difference': self._round_score(school_avg - regional_avg),
                'rate_difference': self._round_rate(school_rate - regional_rate),
                'regional_ranking': school_dim.get('regional_ranking', 0)
            }
        
        return result
    
    def _build_radar_chart_data(
        self, 
        school_data: Dict[str, Any], 
        regional_data: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """构建学校雷达图数据（包含区域对比）"""
        if not regional_data:
            # 如果没有区域数据，只生成学校数据
            school_academic = school_data.get('academic_subjects', {})
            school_non_academic = school_data.get('non_academic_subjects', {})
            school_dimensions = self.radar_formatter.extract_dimensions_from_subjects(
                school_academic, school_non_academic
            )
            return self.radar_formatter.format_regional_radar_data(school_dimensions)
        
        # 有区域数据时，生成对比数据
        return self.radar_formatter.build_comparative_radar_data(school_data, regional_data)
    
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