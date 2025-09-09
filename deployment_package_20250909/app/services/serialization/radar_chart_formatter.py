"""
雷达图数据格式化器

负责生成前端雷达图专用的数据格式，支持：
- 区域级雷达图数据
- 学校级雷达图数据（包含区域对比）
- 学业维度和非学业维度分类
"""

import logging
from typing import Dict, Any, List
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger(__name__)


class RadarChartFormatter:
    """雷达图数据格式化器"""
    
    def __init__(self):
        self.max_rate = 1.0  # 雷达图最大刻度固定为1.0
    
    def format_regional_radar_data(self, dimensions_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化区域级雷达图数据
        
        Args:
            dimensions_data: 维度统计数据字典
            
        Returns:
            格式化后的雷达图数据
        """
        logger.debug("格式化区域级雷达图数据")
        
        try:
            academic_dimensions = []
            non_academic_dimensions = []
            
            for dimension_name, dimension_data in dimensions_data.items():
                if not isinstance(dimension_data, dict):
                    continue
                
                score_rate = dimension_data.get('score_rate', 0)
                dimension_type = dimension_data.get('dimension_type', 'academic')
                
                chart_item = {
                    'dimension_name': dimension_name,
                    'score_rate': self._round_score_rate(score_rate),
                    'max_rate': self.max_rate
                }
                
                # 根据维度类型分类
                if self._is_academic_dimension(dimension_name, dimension_type):
                    academic_dimensions.append(chart_item)
                else:
                    non_academic_dimensions.append(chart_item)
            
            return {
                'academic_dimensions': academic_dimensions,
                'non_academic_dimensions': non_academic_dimensions
            }
            
        except Exception as e:
            logger.error(f"格式化区域级雷达图数据失败: {str(e)}")
            return self._get_empty_radar_data()
    
    def format_school_radar_data(
        self, 
        school_dimensions: Dict[str, Any], 
        regional_dimensions: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        格式化学校级雷达图数据（包含区域对比）
        
        Args:
            school_dimensions: 学校维度统计数据
            regional_dimensions: 区域维度统计数据
            
        Returns:
            包含学校vs区域对比的雷达图数据
        """
        logger.debug("格式化学校级雷达图数据（包含区域对比）")
        
        try:
            academic_dimensions = []
            non_academic_dimensions = []
            
            # 遍历学校维度数据
            for dimension_name, school_data in school_dimensions.items():
                if not isinstance(school_data, dict):
                    continue
                
                school_score_rate = school_data.get('school_score_rate', 0)
                dimension_type = school_data.get('dimension_type', 'academic')
                
                # 获取对应的区域数据
                regional_data = regional_dimensions.get(dimension_name, {})
                regional_score_rate = regional_data.get('score_rate', 0)
                
                chart_item = {
                    'dimension_name': dimension_name,
                    'school_score_rate': self._round_score_rate(school_score_rate),
                    'regional_score_rate': self._round_score_rate(regional_score_rate),
                    'max_rate': self.max_rate
                }
                
                # 根据维度类型分类
                if self._is_academic_dimension(dimension_name, dimension_type):
                    academic_dimensions.append(chart_item)
                else:
                    non_academic_dimensions.append(chart_item)
            
            return {
                'academic_dimensions': academic_dimensions,
                'non_academic_dimensions': non_academic_dimensions
            }
            
        except Exception as e:
            logger.error(f"格式化学校级雷达图数据失败: {str(e)}")
            return self._get_empty_school_radar_data()
    
    def extract_dimensions_from_subjects(
        self, 
        academic_subjects: Dict[str, Any], 
        non_academic_subjects: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        从科目数据中提取维度信息
        
        Args:
            academic_subjects: 学业科目数据
            non_academic_subjects: 非学业科目数据
            
        Returns:
            提取的维度数据字典
        """
        logger.debug("从科目数据中提取维度信息")
        
        dimensions = {}
        
        # 从学业科目中提取维度
        for subject_name, subject_data in academic_subjects.items():
            subject_dimensions = subject_data.get('dimensions', {})
            for dim_name, dim_data in subject_dimensions.items():
                dimensions[dim_name] = {
                    **dim_data,
                    'dimension_type': 'academic',
                    'source_subject': subject_name
                }
        
        # 从非学业科目中提取维度
        for subject_name, subject_data in non_academic_subjects.items():
            subject_dimensions = subject_data.get('dimensions', {})
            for dim_name, dim_data in subject_dimensions.items():
                dimensions[dim_name] = {
                    **dim_data,
                    'dimension_type': 'non_academic',
                    'source_subject': subject_name
                }
        
        return dimensions
    
    def build_comparative_radar_data(
        self,
        school_data: Dict[str, Any],
        regional_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        构建学校与区域对比的雷达图数据
        
        Args:
            school_data: 学校统计数据
            regional_data: 区域统计数据
            
        Returns:
            对比雷达图数据
        """
        logger.debug("构建学校与区域对比的雷达图数据")
        
        try:
            # 提取学校维度数据
            school_academic = school_data.get('academic_subjects', {})
            school_non_academic = school_data.get('non_academic_subjects', {})
            school_dimensions = self.extract_dimensions_from_subjects(
                school_academic, school_non_academic
            )
            
            # 提取区域维度数据  
            regional_academic = regional_data.get('academic_subjects', {})
            regional_non_academic = regional_data.get('non_academic_subjects', {})
            regional_dimensions = self.extract_dimensions_from_subjects(
                regional_academic, regional_non_academic
            )
            
            # 格式化对比数据
            return self.format_school_radar_data(school_dimensions, regional_dimensions)
            
        except Exception as e:
            logger.error(f"构建对比雷达图数据失败: {str(e)}")
            return self._get_empty_school_radar_data()
    
    def validate_radar_data_completeness(self, radar_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        验证雷达图数据完整性
        
        Args:
            radar_data: 雷达图数据
            
        Returns:
            验证结果
        """
        logger.debug("验证雷达图数据完整性")
        
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        # 检查必需字段
        required_keys = ['academic_dimensions', 'non_academic_dimensions']
        for key in required_keys:
            if key not in radar_data:
                validation_result['is_valid'] = False
                validation_result['errors'].append(f"缺少必需字段: {key}")
        
        # 检查学业维度数据
        academic_dims = radar_data.get('academic_dimensions', [])
        if not academic_dims:
            validation_result['warnings'].append("学业维度数据为空")
        else:
            self._validate_dimensions_list(academic_dims, 'academic', validation_result)
        
        # 检查非学业维度数据
        non_academic_dims = radar_data.get('non_academic_dimensions', [])
        if not non_academic_dims:
            validation_result['warnings'].append("非学业维度数据为空")
        else:
            self._validate_dimensions_list(non_academic_dims, 'non_academic', validation_result)
        
        return validation_result
    
    def _validate_dimensions_list(
        self, 
        dimensions: List[Dict[str, Any]], 
        dimension_type: str,
        validation_result: Dict[str, Any]
    ):
        """验证维度列表数据"""
        for i, dim in enumerate(dimensions):
            # 检查必需字段
            required_fields = ['dimension_name', 'max_rate']
            
            # 根据类型检查得分率字段
            if 'school_score_rate' in dim and 'regional_score_rate' in dim:
                # 学校级数据
                score_fields = ['school_score_rate', 'regional_score_rate']
            else:
                # 区域级数据
                score_fields = ['score_rate']
            
            all_required = required_fields + score_fields
            
            for field in all_required:
                if field not in dim:
                    validation_result['is_valid'] = False
                    validation_result['errors'].append(
                        f"{dimension_type}维度第{i+1}项缺少字段: {field}"
                    )
            
            # 检查得分率范围
            for score_field in score_fields:
                if score_field in dim:
                    score_value = dim[score_field]
                    if not (0 <= score_value <= 1):
                        validation_result['is_valid'] = False
                        validation_result['errors'].append(
                            f"{dimension_type}维度'{dim.get('dimension_name', 'unknown')}'的{score_field}超出范围: {score_value}"
                        )
            
            # 检查max_rate
            if 'max_rate' in dim and dim['max_rate'] != 1.0:
                validation_result['warnings'].append(
                    f"{dimension_type}维度'{dim.get('dimension_name', 'unknown')}'的max_rate不是1.0: {dim['max_rate']}"
                )
    
    def _is_academic_dimension(self, dimension_name: str, dimension_type: str = None) -> bool:
        """
        判断是否为学业维度
        
        Args:
            dimension_name: 维度名称
            dimension_type: 维度类型（如果有的话）
            
        Returns:
            是否为学业维度
        """
        if dimension_type:
            return dimension_type == 'academic'
        
        # 根据维度名称判断
        academic_keywords = [
            '数学', '运算', '逻辑', '推理', '阅读', '理解', '语言', '文字',
            '计算', '分析', '解题', '思维', '记忆', '应用'
        ]
        
        for keyword in academic_keywords:
            if keyword in dimension_name:
                return True
        
        return False
    
    def _round_score_rate(self, score_rate: float) -> float:
        """
        按照规范要求将得分率保留3位小数
        
        Args:
            score_rate: 原始得分率
            
        Returns:
            保留3位小数的得分率
        """
        if not isinstance(score_rate, (int, float)):
            return 0.0
        
        # 使用Decimal进行精确的四舍五入
        decimal_rate = Decimal(str(score_rate))
        rounded_rate = decimal_rate.quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)
        
        return float(rounded_rate)
    
    def _get_empty_radar_data(self) -> Dict[str, Any]:
        """获取空的雷达图数据结构"""
        return {
            'academic_dimensions': [],
            'non_academic_dimensions': []
        }
    
    def _get_empty_school_radar_data(self) -> Dict[str, Any]:
        """获取空的学校雷达图数据结构"""
        return {
            'academic_dimensions': [],
            'non_academic_dimensions': []
        }