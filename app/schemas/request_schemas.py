from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum

from ..database.models import AggregationLevel, MetadataType, CalculationStatus, ChangeType, SubjectType

class BatchCreateRequest(BaseModel):
    """创建批次请求模型"""
    name: str = Field(..., description="批次名称", min_length=1, max_length=255)
    description: Optional[str] = Field(None, description="批次描述", max_length=1000)
    region_ids: List[int] = Field(..., description="区域ID列表", min_items=1)
    subject_ids: List[int] = Field(..., description="科目ID列表", min_items=1)
    
    class Config:
        schema_extra = {
            "example": {
                "name": "2024年第一学期期末统计",
                "description": "针对全市各区域的期末成绩统计分析",
                "region_ids": [1, 2, 3],
                "subject_ids": [1, 2, 3]
            }
        }

class BatchDeleteRequest(BaseModel):
    """删除批次请求模型"""
    force: bool = Field(False, description="是否强制删除（删除所有相关数据）")
    
class SubjectCreateRequest(BaseModel):
    """创建科目请求模型"""
    name: str = Field(..., description="科目名称", min_length=1, max_length=100)
    code: Optional[str] = Field(None, description="科目代码", max_length=50)
    
class RegionCreateRequest(BaseModel):
    """创建区域请求模型"""
    name: str = Field(..., description="区域名称", min_length=1, max_length=100)
    parent_id: Optional[int] = Field(None, description="父区域ID")


# 统计相关请求模型
class StatisticalAggregationCreateRequest(BaseModel):
    """创建统计汇聚数据请求模型"""
    batch_code: str = Field(..., description="批次代码", min_length=1, max_length=50)
    aggregation_level: AggregationLevel = Field(..., description="汇聚级别")
    school_id: Optional[str] = Field(None, description="学校ID(学校级时必填)", max_length=50)
    school_name: Optional[str] = Field(None, description="学校名称", max_length=100)
    statistics_data: Dict[str, Any] = Field(..., description="统计数据JSON")
    data_version: str = Field("1.0", description="数据版本号", max_length=10)
    total_students: int = Field(0, description="参与学生总数", ge=0)
    total_schools: int = Field(0, description="参与学校总数(区域级)", ge=0)
    change_reason: Optional[str] = Field(None, description="变更原因")
    triggered_by: Optional[str] = Field("system", description="触发者")
    
    @validator('school_id')
    def validate_school_id(cls, v, values):
        """验证学校级数据必须提供school_id"""
        if values.get('aggregation_level') == AggregationLevel.SCHOOL and not v:
            raise ValueError('学校级统计数据必须提供school_id')
        return v
    
    @validator('statistics_data')
    def validate_statistics_data(cls, v):
        """验证统计数据JSON格式"""
        required_keys = ['batch_info', 'academic_subjects']
        for key in required_keys:
            if key not in v:
                raise ValueError(f'统计数据必须包含{key}字段')
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "batch_code": "BATCH_2025_001",
                "aggregation_level": "regional",
                "statistics_data": {
                    "batch_info": {
                        "batch_code": "BATCH_2025_001",
                        "total_students": 15000,
                        "total_schools": 50
                    },
                    "academic_subjects": [
                        {
                            "subject_id": 1,
                            "subject_name": "语文",
                            "statistics": {
                                "average_score": 85.5,
                                "difficulty_coefficient": 0.71,
                                "discrimination_coefficient": 0.45
                            }
                        }
                    ]
                },
                "total_students": 15000,
                "total_schools": 50
            }
        }


class StatisticalAggregationUpdateRequest(BaseModel):
    """更新统计汇聚数据请求模型"""
    statistics_data: Optional[Dict[str, Any]] = Field(None, description="统计数据JSON")
    calculation_status: Optional[CalculationStatus] = Field(None, description="计算状态")
    calculation_duration: Optional[float] = Field(None, description="计算耗时(秒)", ge=0)
    total_students: Optional[int] = Field(None, description="参与学生总数", ge=0)
    total_schools: Optional[int] = Field(None, description="参与学校总数", ge=0)
    change_reason: Optional[str] = Field(None, description="变更原因")
    triggered_by: Optional[str] = Field("system", description="触发者")


class StatisticalMetadataCreateRequest(BaseModel):
    """创建统计元数据请求模型"""
    metadata_type: MetadataType = Field(..., description="元数据类型")
    metadata_key: str = Field(..., description="元数据键", min_length=1, max_length=100)
    metadata_value: Dict[str, Any] = Field(..., description="元数据内容")
    grade_level: Optional[str] = Field(None, description="适用年级", max_length=20)
    subject_type: Optional[SubjectType] = Field(None, description="适用科目类型")
    version: str = Field("1.0", description="版本号", max_length=10)
    description: Optional[str] = Field(None, description="配置描述")
    created_by: Optional[str] = Field("system", description="创建者", max_length=50)
    
    class Config:
        schema_extra = {
            "example": {
                "metadata_type": "grade_config",
                "metadata_key": "grade_thresholds_primary",
                "metadata_value": {
                    "excellent": 0.85,
                    "good": 0.70,
                    "pass": 0.60,
                    "description": "小学阶段等级分布阈值"
                },
                "grade_level": "primary",
                "description": "小学阶段成绩等级划分标准"
            }
        }


class StatisticalMetadataUpdateRequest(BaseModel):
    """更新统计元数据请求模型"""
    metadata_value: Optional[Dict[str, Any]] = Field(None, description="元数据内容")
    grade_level: Optional[str] = Field(None, description="适用年级", max_length=20)
    subject_type: Optional[SubjectType] = Field(None, description="适用科目类型")
    is_active: Optional[bool] = Field(None, description="是否激活")
    description: Optional[str] = Field(None, description="配置描述")
    created_by: Optional[str] = Field(None, description="更新者", max_length=50)


class BatchStatisticsQueryRequest(BaseModel):
    """批次统计数据查询请求模型"""
    batch_code: str = Field(..., description="批次代码", min_length=1, max_length=50)
    aggregation_level: Optional[AggregationLevel] = Field(None, description="汇聚级别")
    school_ids: Optional[List[str]] = Field(None, description="学校ID列表")
    calculation_status: Optional[CalculationStatus] = Field(None, description="计算状态")
    include_history: bool = Field(False, description="是否包含历史记录")


class HistoryQueryRequest(BaseModel):
    """历史记录查询请求模型"""
    aggregation_id: Optional[int] = Field(None, description="统计汇聚ID")
    batch_code: Optional[str] = Field(None, description="批次代码")
    change_type: Optional[ChangeType] = Field(None, description="变更类型")
    start_date: Optional[datetime] = Field(None, description="开始日期")
    end_date: Optional[datetime] = Field(None, description="结束日期")
    limit: int = Field(50, description="返回记录数限制", ge=1, le=1000)


class MetadataQueryRequest(BaseModel):
    """元数据查询请求模型"""
    metadata_type: Optional[MetadataType] = Field(None, description="元数据类型")
    metadata_key: Optional[str] = Field(None, description="元数据键")
    grade_level: Optional[str] = Field(None, description="适用年级")
    subject_type: Optional[SubjectType] = Field(None, description="适用科目类型")
    is_active: Optional[bool] = Field(True, description="是否只查询激活的配置")
    version: str = Field("1.0", description="版本号")