# 量表配置管理
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# 量表类型配置
SCALE_TYPES = {
    'forward': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5},    # 正向量表
    'reverse': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}     # 反向量表
}

# 李克特量表标签
LIKERT_LABELS = {
    1: '完全不同意',
    2: '不同意', 
    3: '中性',
    4: '同意',
    5: '完全同意'
}

# 数据质量检查规则
QUALITY_RULES = {
    'response_time_min': 30,         # 最少作答时间(秒)
    'response_time_max': 1800,       # 最长作答时间(秒)
    'straight_line_max': 10,         # 连续相同选项最大数量
    'completion_rate_min': 0.8,      # 最低完成率
    'variance_threshold': 0.1        # 方差阈值(检测无变化响应)
}

# 示例问卷维度配置
SAMPLE_SURVEY_DIMENSIONS = {
    'curiosity': {
        'name': '好奇心',
        'forward_questions': ['Q1', 'Q3', 'Q5'],
        'reverse_questions': ['Q2', 'Q4'],
        'weight': 1.0
    },
    'observation': {
        'name': '观察能力', 
        'forward_questions': ['Q6', 'Q8'],
        'reverse_questions': ['Q7'],
        'weight': 1.2
    }
}


@dataclass
class DimensionConfig:
    """维度配置数据类"""
    name: str
    forward_questions: List[str]
    reverse_questions: List[str]
    weight: float = 1.0
    description: str = ""


@dataclass
class SurveyConfig:
    """问卷配置数据类"""
    survey_id: str
    name: str
    dimensions: Dict[str, DimensionConfig]
    scale_config: Dict[str, Dict[int, int]]
    quality_rules: Dict[str, Any]
    version: str = "1.0"
    description: str = ""


class ScaleConfigManager:
    """量表配置管理器"""
    
    def __init__(self):
        self.scale_configs: Dict[str, Dict[int, int]] = SCALE_TYPES.copy()
        self.dimension_configs: Dict[str, DimensionConfig] = {}
        self.survey_configs: Dict[str, SurveyConfig] = {}
        self.quality_rules = QUALITY_RULES.copy()
    
    def add_scale_type(self, name: str, mapping: Dict[int, int]):
        """添加新的量表类型"""
        if not isinstance(mapping, dict):
            raise ValueError("量表映射必须是字典格式")
        
        # 验证映射的完整性
        for original, transformed in mapping.items():
            if not isinstance(original, int) or not isinstance(transformed, int):
                raise ValueError("量表映射的键值必须为整数")
        
        self.scale_configs[name] = mapping
        logger.info(f"已添加量表类型: {name}")
    
    def get_scale_config(self, scale_type: str) -> Dict[int, int]:
        """获取量表配置"""
        if scale_type not in self.scale_configs:
            raise ValueError(f"未知的量表类型: {scale_type}")
        return self.scale_configs[scale_type]
    
    def add_dimension_config(self, dimension_id: str, config: DimensionConfig):
        """添加维度配置"""
        self.dimension_configs[dimension_id] = config
        logger.info(f"已添加维度配置: {dimension_id}")
    
    def get_dimension_config(self, dimension_id: str) -> DimensionConfig:
        """获取维度配置"""
        if dimension_id not in self.dimension_configs:
            raise ValueError(f"未知的维度: {dimension_id}")
        return self.dimension_configs[dimension_id]
    
    def create_survey_config(self, survey_id: str, name: str, 
                           dimensions: Dict[str, Dict[str, Any]]) -> SurveyConfig:
        """创建问卷配置"""
        dimension_configs = {}
        
        for dim_id, dim_data in dimensions.items():
            dimension_config = DimensionConfig(
                name=dim_data.get('name', dim_id),
                forward_questions=dim_data.get('forward_questions', []),
                reverse_questions=dim_data.get('reverse_questions', []),
                weight=dim_data.get('weight', 1.0),
                description=dim_data.get('description', '')
            )
            dimension_configs[dim_id] = dimension_config
        
        survey_config = SurveyConfig(
            survey_id=survey_id,
            name=name,
            dimensions=dimension_configs,
            scale_config=self.scale_configs,
            quality_rules=self.quality_rules
        )
        
        self.survey_configs[survey_id] = survey_config
        logger.info(f"已创建问卷配置: {survey_id}")
        
        return survey_config
    
    def get_survey_config(self, survey_id: str) -> SurveyConfig:
        """获取问卷配置"""
        if survey_id not in self.survey_configs:
            raise ValueError(f"未知的问卷: {survey_id}")
        return self.survey_configs[survey_id]
    
    def validate_scale_mapping(self, mapping: Dict[int, int], 
                             scale_range: tuple = (1, 5)) -> bool:
        """验证量表映射的合理性"""
        min_val, max_val = scale_range
        
        # 检查原始值范围
        original_values = set(mapping.keys())
        expected_original = set(range(min_val, max_val + 1))
        if original_values != expected_original:
            logger.warning(f"量表原始值不完整: 期望{expected_original}, 实际{original_values}")
            return False
        
        # 检查转换值范围
        transformed_values = set(mapping.values())
        expected_transformed = set(range(min_val, max_val + 1))
        if transformed_values != expected_transformed:
            logger.warning(f"量表转换值不完整: 期望{expected_transformed}, 实际{transformed_values}")
            return False
        
        return True
    
    def get_question_scale_type(self, question_id: str, survey_config: SurveyConfig) -> str:
        """根据问卷配置判断题目的量表类型"""
        for dimension_config in survey_config.dimensions.values():
            if question_id in dimension_config.forward_questions:
                return 'forward'
            elif question_id in dimension_config.reverse_questions:
                return 'reverse'
        
        # 默认为正向量表
        logger.warning(f"题目 {question_id} 未找到在维度配置中，使用默认正向量表")
        return 'forward'
    
    def update_quality_rules(self, new_rules: Dict[str, Any]):
        """更新质量检查规则"""
        self.quality_rules.update(new_rules)
        logger.info(f"已更新质量检查规则: {list(new_rules.keys())}")
    
    def export_config(self, survey_id: str) -> Dict[str, Any]:
        """导出问卷配置为JSON格式"""
        survey_config = self.get_survey_config(survey_id)
        
        dimensions_dict = {}
        for dim_id, dim_config in survey_config.dimensions.items():
            dimensions_dict[dim_id] = {
                'name': dim_config.name,
                'forward_questions': dim_config.forward_questions,
                'reverse_questions': dim_config.reverse_questions,
                'weight': dim_config.weight,
                'description': dim_config.description
            }
        
        return {
            'survey_id': survey_config.survey_id,
            'name': survey_config.name,
            'dimensions': dimensions_dict,
            'scale_config': survey_config.scale_config,
            'quality_rules': survey_config.quality_rules,
            'version': survey_config.version,
            'description': survey_config.description
        }
    
    def import_config(self, config_data: Dict[str, Any]) -> SurveyConfig:
        """从JSON格式导入问卷配置"""
        survey_id = config_data['survey_id']
        name = config_data['name']
        dimensions = config_data['dimensions']
        
        # 更新量表配置（如果提供）
        if 'scale_config' in config_data:
            for scale_type, mapping in config_data['scale_config'].items():
                self.scale_configs[scale_type] = mapping
        
        # 更新质量规则（如果提供）
        if 'quality_rules' in config_data:
            self.quality_rules.update(config_data['quality_rules'])
        
        # 创建问卷配置
        survey_config = self.create_survey_config(survey_id, name, dimensions)
        survey_config.version = config_data.get('version', '1.0')
        survey_config.description = config_data.get('description', '')
        
        logger.info(f"已导入问卷配置: {survey_id}")
        return survey_config
    
    def list_available_configs(self) -> Dict[str, Any]:
        """列出所有可用的配置"""
        return {
            'scale_types': list(self.scale_configs.keys()),
            'dimensions': list(self.dimension_configs.keys()),
            'surveys': list(self.survey_configs.keys()),
            'quality_rules': list(self.quality_rules.keys())
        }