"""
版本管理器

负责JSON数据的版本控制和向后兼容性管理，
支持数据格式迁移和Schema版本验证。
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class SchemaVersion(str, Enum):
    """Schema版本枚举"""
    V1_0 = "2025-09-04"
    # 未来版本可以在这里添加
    # V1_1 = "2025-10-01"


class DataVersion(str, Enum):
    """数据版本枚举"""
    V1_0 = "1.0"
    # V1_1 = "1.1"


class VersionManager:
    """版本管理器"""
    
    def __init__(self):
        self.current_schema_version = SchemaVersion.V1_0
        self.current_data_version = DataVersion.V1_0
        
        # 版本兼容性映射
        self.compatibility_matrix = {
            SchemaVersion.V1_0: {
                "supported_data_versions": [DataVersion.V1_0],
                "breaking_changes": [],
                "deprecated_fields": [],
                "description": "初始版本，支持区域级和学校级统计数据序列化"
            }
            # 未来版本的兼容性信息
        }
    
    def get_current_schema_version(self) -> str:
        """获取当前Schema版本"""
        return self.current_schema_version.value
    
    def get_current_data_version(self) -> str:
        """获取当前数据版本"""
        return self.current_data_version.value
    
    def validate_version_compatibility(
        self, 
        schema_version: str, 
        data_version: str
    ) -> Dict[str, Any]:
        """
        验证版本兼容性
        
        Args:
            schema_version: 请求的Schema版本
            data_version: 请求的数据版本
            
        Returns:
            验证结果
        """
        logger.debug(f"验证版本兼容性: schema={schema_version}, data={data_version}")
        
        result = {
            'is_compatible': False,
            'warnings': [],
            'errors': [],
            'migration_required': False
        }
        
        # 检查Schema版本
        try:
            schema_enum = SchemaVersion(schema_version)
        except ValueError:
            result['errors'].append(f"不支持的Schema版本: {schema_version}")
            return result
        
        # 检查数据版本
        try:
            data_enum = DataVersion(data_version)
        except ValueError:
            result['errors'].append(f"不支持的数据版本: {data_version}")
            return result
        
        # 获取兼容性信息
        compatibility_info = self.compatibility_matrix.get(schema_enum, {})
        supported_data_versions = compatibility_info.get('supported_data_versions', [])
        
        if data_enum not in supported_data_versions:
            result['errors'].append(
                f"数据版本 {data_version} 不兼容Schema版本 {schema_version}"
            )
            result['migration_required'] = True
        else:
            result['is_compatible'] = True
        
        # 检查废弃字段警告
        deprecated_fields = compatibility_info.get('deprecated_fields', [])
        if deprecated_fields:
            result['warnings'].append(f"以下字段已废弃: {', '.join(deprecated_fields)}")
        
        # 检查重大变更
        breaking_changes = compatibility_info.get('breaking_changes', [])
        if breaking_changes:
            result['warnings'].append(f"包含重大变更: {', '.join(breaking_changes)}")
        
        return result
    
    def add_version_info(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        为JSON数据添加版本信息
        
        Args:
            json_data: 原始JSON数据
            
        Returns:
            添加了版本信息的JSON数据
        """
        logger.debug("为JSON数据添加版本信息")
        
        # 添加版本标识
        json_data['data_version'] = self.get_current_data_version()
        json_data['schema_version'] = self.get_current_schema_version()
        json_data['generated_at'] = datetime.utcnow().isoformat()
        
        return json_data
    
    def migrate_data_format(
        self, 
        data: Dict[str, Any], 
        from_version: str, 
        to_version: str
    ) -> Dict[str, Any]:
        """
        数据格式迁移
        
        Args:
            data: 原始数据
            from_version: 源版本
            to_version: 目标版本
            
        Returns:
            迁移后的数据
        """
        logger.info(f"数据格式迁移: {from_version} -> {to_version}")
        
        # 暂时只支持同版本
        if from_version == to_version:
            return data
        
        # 未来可以在这里实现具体的迁移逻辑
        raise NotImplementedError(f"暂不支持从版本 {from_version} 迁移到 {to_version}")
    
    def ensure_backward_compatibility(
        self, 
        json_data: Dict[str, Any], 
        target_version: str = None
    ) -> Dict[str, Any]:
        """
        确保向后兼容性
        
        Args:
            json_data: JSON数据
            target_version: 目标版本（如果不指定则使用当前版本）
            
        Returns:
            兼容性处理后的数据
        """
        target_version = target_version or self.get_current_schema_version()
        
        logger.debug(f"确保向后兼容性，目标版本: {target_version}")
        
        # 添加版本标识
        json_data = self.add_version_info(json_data)
        
        # 处理废弃字段
        json_data = self._handle_deprecated_fields(json_data, target_version)
        
        # 添加默认字段
        json_data = self._add_default_fields(json_data, target_version)
        
        return json_data
    
    def get_version_info(self, schema_version: str = None) -> Dict[str, Any]:
        """
        获取版本信息
        
        Args:
            schema_version: Schema版本（不指定则返回当前版本信息）
            
        Returns:
            版本信息
        """
        version = schema_version or self.get_current_schema_version()
        
        try:
            schema_enum = SchemaVersion(version)
            compatibility_info = self.compatibility_matrix.get(schema_enum, {})
            
            return {
                'schema_version': version,
                'data_version': self.get_current_data_version(),
                'description': compatibility_info.get('description', ''),
                'supported_data_versions': [v.value for v in compatibility_info.get('supported_data_versions', [])],
                'breaking_changes': compatibility_info.get('breaking_changes', []),
                'deprecated_fields': compatibility_info.get('deprecated_fields', []),
                'is_current': version == self.get_current_schema_version()
            }
        except ValueError:
            return {
                'error': f'未知的Schema版本: {version}',
                'supported_versions': [v.value for v in SchemaVersion]
            }
    
    def _handle_deprecated_fields(self, data: Dict[str, Any], target_version: str) -> Dict[str, Any]:
        """处理废弃字段"""
        try:
            schema_enum = SchemaVersion(target_version)
            compatibility_info = self.compatibility_matrix.get(schema_enum, {})
            deprecated_fields = compatibility_info.get('deprecated_fields', [])
            
            if deprecated_fields:
                logger.warning(f"数据包含废弃字段: {deprecated_fields}")
                # 这里可以实现具体的废弃字段处理逻辑
                # 比如删除废弃字段或添加替代字段
            
            return data
        except ValueError:
            logger.warning(f"无法处理未知版本的废弃字段: {target_version}")
            return data
    
    def _add_default_fields(self, data: Dict[str, Any], target_version: str) -> Dict[str, Any]:
        """添加默认字段"""
        # 根据版本添加必需的默认字段
        if target_version == SchemaVersion.V1_0.value:
            # 确保区域级数据包含必需字段
            if 'batch_info' in data and 'radar_chart_data' not in data:
                data['radar_chart_data'] = {
                    'academic_dimensions': [],
                    'non_academic_dimensions': []
                }
        
        return data
    
    def get_supported_versions(self) -> Dict[str, Any]:
        """获取所有支持的版本列表"""
        return {
            'schema_versions': [v.value for v in SchemaVersion],
            'data_versions': [v.value for v in DataVersion],
            'current_schema': self.get_current_schema_version(),
            'current_data': self.get_current_data_version(),
            'compatibility_matrix': {
                k.value: {
                    **v,
                    'supported_data_versions': [dv.value for dv in v.get('supported_data_versions', [])]
                } for k, v in self.compatibility_matrix.items()
            }
        }