"""
JSON数据序列化模块

本模块负责将统计计算结果转换为标准化的JSON格式，
严格遵循 json-data-specification.md 规范。

主要功能：
- 区域级统计数据序列化
- 学校级统计数据序列化
- 雷达图数据格式化
- JSON Schema版本控制
"""

from .statistics_json_serializer import StatisticsJsonSerializer
from .regional_data_serializer import RegionalDataSerializer
from .school_data_serializer import SchoolDataSerializer
from .radar_chart_formatter import RadarChartFormatter
from .version_manager import VersionManager
from .schema_validator import SchemaValidator
from .data_integrator import StatisticsDataIntegrator

__all__ = [
    "StatisticsJsonSerializer",
    "RegionalDataSerializer",
    "SchoolDataSerializer", 
    "RadarChartFormatter",
    "VersionManager",
    "SchemaValidator",
    "StatisticsDataIntegrator"
]