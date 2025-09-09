"""
JSON Schema验证器

负责验证JSON数据格式是否符合json-data-specification.md规范，
确保数据完整性和格式正确性。
"""

import logging
from typing import Dict, Any, List, Optional, Union
import re
from datetime import datetime

logger = logging.getLogger(__name__)


class ValidationResult:
    """验证结果类"""
    
    def __init__(self, is_valid: bool = True):
        self.is_valid = is_valid
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.details: Dict[str, Any] = {}
    
    def add_error(self, message: str, field: str = None):
        """添加错误"""
        self.is_valid = False
        if field:
            self.errors.append(f"{field}: {message}")
        else:
            self.errors.append(message)
    
    def add_warning(self, message: str, field: str = None):
        """添加警告"""
        if field:
            self.warnings.append(f"{field}: {message}")
        else:
            self.warnings.append(message)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'is_valid': self.is_valid,
            'errors': self.errors,
            'warnings': self.warnings,
            'details': self.details
        }


class SchemaValidator:
    """JSON Schema验证器"""
    
    def __init__(self):
        self.batch_code_pattern = re.compile(r'^BATCH_\d{4}_\d{3}$')
        
        # 数据格式要求
        self.validation_rules = {
            'score_rate_range': (0.0, 1.0),
            'percentage_range': (0.0, 1.0),
            'max_rate_value': 1.0,
            'required_academic_dimensions': ['数学运算', '逻辑推理', '阅读理解'],
            'required_non_academic_dimensions': ['好奇心', '观察能力']
        }
    
    def validate_regional_data(self, data: Dict[str, Any]) -> ValidationResult:
        """
        验证区域级数据格式
        
        Args:
            data: 区域级JSON数据
            
        Returns:
            验证结果
        """
        logger.debug("验证区域级数据格式")
        
        result = ValidationResult()
        
        try:
            # 检查必填字段
            self._check_regional_required_fields(data, result)
            
            if result.is_valid:
                # 检查批次信息
                self._validate_batch_info(data.get('batch_info', {}), result)
                
                # 检查学业科目
                self._validate_academic_subjects(data.get('academic_subjects', {}), result, 'regional')
                
                # 检查非学业科目
                self._validate_non_academic_subjects(data.get('non_academic_subjects', {}), result, 'regional')
                
                # 检查雷达图数据
                self._validate_radar_chart_data(data.get('radar_chart_data', {}), result, 'regional')
                
                # 检查版本信息
                self._validate_version_info(data, result)
            
        except Exception as e:
            result.add_error(f"验证过程出现异常: {str(e)}")
        
        logger.debug(f"区域级数据验证完成，有效性: {result.is_valid}")
        return result
    
    def validate_school_data(self, data: Dict[str, Any]) -> ValidationResult:
        """
        验证学校级数据格式
        
        Args:
            data: 学校级JSON数据
            
        Returns:
            验证结果
        """
        logger.debug("验证学校级数据格式")
        
        result = ValidationResult()
        
        try:
            # 检查必填字段
            self._check_school_required_fields(data, result)
            
            if result.is_valid:
                # 检查学校信息
                self._validate_school_info(data.get('school_info', {}), result)
                
                # 检查学业科目
                self._validate_academic_subjects(data.get('academic_subjects', {}), result, 'school')
                
                # 检查非学业科目
                self._validate_non_academic_subjects(data.get('non_academic_subjects', {}), result, 'school')
                
                # 检查雷达图数据
                self._validate_radar_chart_data(data.get('radar_chart_data', {}), result, 'school')
                
                # 检查版本信息
                self._validate_version_info(data, result)
            
        except Exception as e:
            result.add_error(f"验证过程出现异常: {str(e)}")
        
        logger.debug(f"学校级数据验证完成，有效性: {result.is_valid}")
        return result
    
    def validate_data_consistency(
        self, 
        regional_data: Dict[str, Any], 
        schools_data: List[Dict[str, Any]]
    ) -> ValidationResult:
        """
        验证区域数据与学校数据的一致性
        
        Args:
            regional_data: 区域数据
            schools_data: 学校数据列表
            
        Returns:
            一致性验证结果
        """
        logger.debug("验证区域与学校数据一致性")
        
        result = ValidationResult()
        
        try:
            # 检查批次代码一致性
            regional_batch = regional_data.get('batch_info', {}).get('batch_code', '')
            for i, school_data in enumerate(schools_data):
                school_batch = school_data.get('school_info', {}).get('batch_code', '')
                if school_batch != regional_batch:
                    result.add_error(
                        f"学校{i+1}的批次代码({school_batch})与区域数据不一致({regional_batch})"
                    )
            
            # 检查学校总数一致性
            regional_school_count = regional_data.get('batch_info', {}).get('total_schools', 0)
            actual_school_count = len(schools_data)
            
            if regional_school_count != actual_school_count:
                result.add_warning(
                    f"区域数据中的学校总数({regional_school_count})与实际学校数量({actual_school_count})不一致"
                )
            
            # 检查维度一致性
            regional_dims = self._extract_dimension_names(regional_data)
            for i, school_data in enumerate(schools_data):
                school_dims = self._extract_dimension_names(school_data)
                if regional_dims != school_dims:
                    result.add_warning(
                        f"学校{i+1}的维度列表与区域数据不一致"
                    )
            
        except Exception as e:
            result.add_error(f"一致性验证过程出现异常: {str(e)}")
        
        logger.debug(f"一致性验证完成，有效性: {result.is_valid}")
        return result
    
    def _check_regional_required_fields(self, data: Dict[str, Any], result: ValidationResult):
        """检查区域级必填字段"""
        required_fields = [
            'batch_info', 'academic_subjects', 'non_academic_subjects', 
            'radar_chart_data', 'data_version', 'schema_version'
        ]
        
        for field in required_fields:
            if field not in data:
                result.add_error(f"缺少必填字段", field)
    
    def _check_school_required_fields(self, data: Dict[str, Any], result: ValidationResult):
        """检查学校级必填字段"""
        required_fields = [
            'school_info', 'academic_subjects', 'non_academic_subjects',
            'radar_chart_data', 'data_version', 'schema_version'
        ]
        
        for field in required_fields:
            if field not in data:
                result.add_error(f"缺少必填字段", field)
    
    def _validate_batch_info(self, batch_info: Dict[str, Any], result: ValidationResult):
        """验证批次信息"""
        # 检查批次代码格式
        batch_code = batch_info.get('batch_code', '')
        if not self.batch_code_pattern.match(batch_code):
            result.add_error(f"批次代码格式不正确: {batch_code}", 'batch_info.batch_code')
        
        # 检查年级水平
        grade_level = batch_info.get('grade_level', '')
        if grade_level not in ['小学', '初中']:
            result.add_warning(f"年级水平值可能不正确: {grade_level}", 'batch_info.grade_level')
        
        # 检查学校和学生数量
        total_schools = batch_info.get('total_schools', 0)
        total_students = batch_info.get('total_students', 0)
        
        if not isinstance(total_schools, int) or total_schools < 0:
            result.add_error(f"学校总数必须为非负整数: {total_schools}", 'batch_info.total_schools')
        
        if not isinstance(total_students, int) or total_students < 0:
            result.add_error(f"学生总数必须为非负整数: {total_students}", 'batch_info.total_students')
    
    def _validate_school_info(self, school_info: Dict[str, Any], result: ValidationResult):
        """验证学校信息"""
        # 检查必填字段
        required_fields = ['school_id', 'school_name', 'batch_code']
        for field in required_fields:
            if not school_info.get(field):
                result.add_error(f"学校信息缺少必填字段", f'school_info.{field}')
        
        # 检查批次代码格式
        batch_code = school_info.get('batch_code', '')
        if batch_code and not self.batch_code_pattern.match(batch_code):
            result.add_error(f"批次代码格式不正确: {batch_code}", 'school_info.batch_code')
    
    def _validate_academic_subjects(
        self, 
        subjects: Dict[str, Any], 
        result: ValidationResult, 
        data_type: str
    ):
        """验证学业科目数据"""
        if not subjects:
            result.add_warning("学业科目数据为空", 'academic_subjects')
            return
        
        for subject_name, subject_data in subjects.items():
            self._validate_subject_data(subject_data, result, f'academic_subjects.{subject_name}', data_type)
    
    def _validate_non_academic_subjects(
        self, 
        subjects: Dict[str, Any], 
        result: ValidationResult, 
        data_type: str
    ):
        """验证非学业科目数据"""
        if not subjects:
            result.add_warning("非学业科目数据为空", 'non_academic_subjects')
            return
        
        for subject_name, subject_data in subjects.items():
            subject_type = subject_data.get('subject_type', '')
            if subject_type not in ['问卷类', '人机交互类']:
                result.add_warning(f"未知的非学业科目类型: {subject_type}", f'non_academic_subjects.{subject_name}')
    
    def _validate_subject_data(
        self, 
        subject_data: Dict[str, Any], 
        result: ValidationResult, 
        field_prefix: str,
        data_type: str
    ):
        """验证科目数据"""
        # 检查科目基础信息
        if 'subject_id' not in subject_data:
            result.add_error("缺少科目ID", f'{field_prefix}.subject_id')
        
        if 'subject_type' not in subject_data:
            result.add_error("缺少科目类型", f'{field_prefix}.subject_type')
        
        # 验证统计数据
        if data_type == 'regional':
            stats_key = 'regional_stats'
        else:
            stats_key = 'school_stats'
        
        if stats_key in subject_data:
            self._validate_stats_data(
                subject_data[stats_key], 
                result, 
                f'{field_prefix}.{stats_key}'
            )
        
        # 验证维度数据
        if 'dimensions' in subject_data:
            self._validate_dimensions_data(
                subject_data['dimensions'], 
                result, 
                f'{field_prefix}.dimensions'
            )
    
    def _validate_stats_data(
        self, 
        stats_data: Dict[str, Any], 
        result: ValidationResult, 
        field_prefix: str
    ):
        """验证统计数据"""
        # 检查得分率范围
        score_rate = stats_data.get('score_rate')
        if score_rate is not None:
            if not (0 <= score_rate <= 1):
                result.add_error(f"得分率超出范围[0,1]: {score_rate}", f'{field_prefix}.score_rate')
        
        # 检查平均分与最高分、最低分的逻辑关系
        avg_score = stats_data.get('avg_score')
        max_score = stats_data.get('max_score')
        min_score = stats_data.get('min_score')
        
        if all(x is not None for x in [avg_score, max_score, min_score]):
            if not (min_score <= avg_score <= max_score):
                result.add_warning(
                    f"分数逻辑不一致: min={min_score}, avg={avg_score}, max={max_score}",
                    field_prefix
                )
    
    def _validate_dimensions_data(
        self, 
        dimensions: Dict[str, Any], 
        result: ValidationResult, 
        field_prefix: str
    ):
        """验证维度数据"""
        for dim_name, dim_data in dimensions.items():
            # 检查得分率
            score_rate = dim_data.get('score_rate') or dim_data.get('school_score_rate')
            if score_rate is not None and not (0 <= score_rate <= 1):
                result.add_error(
                    f"维度得分率超出范围[0,1]: {score_rate}",
                    f'{field_prefix}.{dim_name}.score_rate'
                )
            
            # 检查总分
            total_score = dim_data.get('total_score')
            if total_score is not None and total_score <= 0:
                result.add_error(
                    f"维度总分必须大于0: {total_score}",
                    f'{field_prefix}.{dim_name}.total_score'
                )
    
    def _validate_radar_chart_data(
        self, 
        radar_data: Dict[str, Any], 
        result: ValidationResult, 
        data_type: str
    ):
        """验证雷达图数据"""
        # 检查必需字段
        required_keys = ['academic_dimensions', 'non_academic_dimensions']
        for key in required_keys:
            if key not in radar_data:
                result.add_error(f"雷达图数据缺少必需字段", f'radar_chart_data.{key}')
                return
        
        # 验证学业维度
        self._validate_radar_dimensions(
            radar_data.get('academic_dimensions', []), 
            result, 
            'radar_chart_data.academic_dimensions',
            data_type
        )
        
        # 验证非学业维度
        self._validate_radar_dimensions(
            radar_data.get('non_academic_dimensions', []), 
            result, 
            'radar_chart_data.non_academic_dimensions',
            data_type
        )
    
    def _validate_radar_dimensions(
        self, 
        dimensions: List[Dict[str, Any]], 
        result: ValidationResult, 
        field_prefix: str,
        data_type: str
    ):
        """验证雷达图维度数据"""
        if not dimensions:
            result.add_warning("雷达图维度数据为空", field_prefix)
            return
        
        for i, dim in enumerate(dimensions):
            dim_prefix = f'{field_prefix}[{i}]'
            
            # 检查必需字段
            if 'dimension_name' not in dim:
                result.add_error("缺少维度名称", f'{dim_prefix}.dimension_name')
            
            if 'max_rate' not in dim:
                result.add_error("缺少最大值", f'{dim_prefix}.max_rate')
            elif dim['max_rate'] != 1.0:
                result.add_warning(
                    f"雷达图最大值应为1.0: {dim['max_rate']}", 
                    f'{dim_prefix}.max_rate'
                )
            
            # 根据数据类型检查得分率字段
            if data_type == 'regional':
                if 'score_rate' not in dim:
                    result.add_error("缺少得分率", f'{dim_prefix}.score_rate')
                else:
                    score_rate = dim['score_rate']
                    if not (0 <= score_rate <= 1):
                        result.add_error(
                            f"得分率超出范围[0,1]: {score_rate}",
                            f'{dim_prefix}.score_rate'
                        )
            else:  # school
                required_rates = ['school_score_rate', 'regional_score_rate']
                for rate_field in required_rates:
                    if rate_field not in dim:
                        result.add_error(f"缺少{rate_field}", f'{dim_prefix}.{rate_field}')
                    else:
                        rate_value = dim[rate_field]
                        if not (0 <= rate_value <= 1):
                            result.add_error(
                                f"{rate_field}超出范围[0,1]: {rate_value}",
                                f'{dim_prefix}.{rate_field}'
                            )
    
    def _validate_version_info(self, data: Dict[str, Any], result: ValidationResult):
        """验证版本信息"""
        # 检查数据版本
        data_version = data.get('data_version')
        if not data_version:
            result.add_error("缺少数据版本信息", 'data_version')
        
        # 检查Schema版本
        schema_version = data.get('schema_version')
        if not schema_version:
            result.add_error("缺少Schema版本信息", 'schema_version')
    
    def _extract_dimension_names(self, data: Dict[str, Any]) -> set:
        """提取数据中的维度名称"""
        dimensions = set()
        
        # 从雷达图数据中提取
        radar_data = data.get('radar_chart_data', {})
        for dim_type in ['academic_dimensions', 'non_academic_dimensions']:
            dims_list = radar_data.get(dim_type, [])
            for dim in dims_list:
                if 'dimension_name' in dim:
                    dimensions.add(dim['dimension_name'])
        
        return dimensions