from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

from ..database.enums import AggregationLevel, MetadataType, CalculationStatus, ChangeType, SubjectType

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class BatchResponse(BaseModel):
    """批次响应模型"""
    id: int
    name: str
    description: Optional[str]
    created_at: datetime
    status: str
    
    class Config:
        from_attributes = True

class TaskResponse(BaseModel):
    """任务响应模型"""
    id: int
    batch_id: int
    status: TaskStatus
    progress: float = Field(..., ge=0.0, le=100.0, description="进度百分比")
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]
    
    class Config:
        from_attributes = True

class StatisticsData(BaseModel):
    """统计数据模型"""
    total_students: int = Field(..., description="学生总数")
    average_score: float = Field(..., description="平均分")
    max_score: float = Field(..., description="最高分")
    min_score: float = Field(..., description="最低分")
    standard_deviation: float = Field(..., description="标准差")
    pass_rate: float = Field(..., description="及格率")
    excellent_rate: float = Field(..., description="优秀率")

class SubjectStatistics(BaseModel):
    """科目统计模型"""
    subject_id: int
    subject_name: str
    statistics: StatisticsData

class RegionReportResponse(BaseModel):
    """区域报告响应模型"""
    region_id: int
    region_name: str
    batch_id: int
    batch_name: str
    generated_at: datetime
    subjects: List[SubjectStatistics]
    
    class Config:
        from_attributes = True

class SchoolReportResponse(BaseModel):
    """学校报告响应模型"""
    school_id: int
    school_name: str
    region_id: int
    region_name: str
    batch_id: int
    batch_name: str
    generated_at: datetime
    subjects: List[SubjectStatistics]
    
    class Config:
        from_attributes = True

class BatchListItem(BaseModel):
    """批次列表项模型"""
    id: int
    name: str
    created_at: datetime
    status: str
    subject_count: int
    region_count: int


# 新增的统计相关响应模型
class StatisticalAggregationResponse(BaseModel):
    """统计汇聚数据响应模型"""
    id: int
    batch_code: str
    aggregation_level: AggregationLevel
    school_id: Optional[str]
    school_name: Optional[str]
    statistics_data: Dict[str, Any]
    data_version: str
    calculation_status: CalculationStatus
    total_students: int
    total_schools: int
    calculation_duration: Optional[float]
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
        schema_extra = {
            "example": {
                "id": 1,
                "batch_code": "BATCH_2025_001",
                "aggregation_level": "regional",
                "school_id": None,
                "school_name": None,
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
                                "discrimination_coefficient": 0.45,
                                "percentiles": {
                                    "P10": 68.0,
                                    "P50": 85.0,
                                    "P90": 98.0
                                }
                            }
                        }
                    ]
                },
                "data_version": "1.0",
                "calculation_status": "completed",
                "total_students": 15000,
                "total_schools": 50,
                "calculation_duration": 125.5,
                "created_at": "2025-09-04T10:00:00Z",
                "updated_at": "2025-09-04T10:05:00Z"
            }
        }


class StatisticalMetadataResponse(BaseModel):
    """统计元数据响应模型"""
    id: int
    metadata_type: MetadataType
    metadata_key: str
    metadata_value: Dict[str, Any]
    grade_level: Optional[str]
    subject_type: Optional[SubjectType]
    is_active: bool
    version: str
    description: Optional[str]
    created_by: str
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True
        schema_extra = {
            "example": {
                "id": 1,
                "metadata_type": "grade_config",
                "metadata_key": "grade_thresholds_primary",
                "metadata_value": {
                    "excellent": 0.85,
                    "good": 0.70,
                    "pass": 0.60,
                    "description": "小学阶段等级分布阈值"
                },
                "grade_level": "primary",
                "subject_type": None,
                "is_active": True,
                "version": "1.0",
                "description": "小学阶段成绩等级划分标准",
                "created_by": "system",
                "created_at": "2025-09-04T10:00:00Z",
                "updated_at": "2025-09-04T10:00:00Z"
            }
        }


class StatisticalHistoryResponse(BaseModel):
    """统计历史记录响应模型"""
    id: int
    aggregation_id: int
    change_type: ChangeType
    previous_data: Optional[Dict[str, Any]]
    current_data: Optional[Dict[str, Any]]
    change_summary: Optional[Dict[str, Any]]
    change_reason: Optional[str]
    triggered_by: str
    batch_code: str
    created_at: datetime
    
    class Config:
        from_attributes = True
        schema_extra = {
            "example": {
                "id": 1,
                "aggregation_id": 1,
                "change_type": "updated",
                "previous_data": {
                    "calculation_status": "processing",
                    "total_students": 14800
                },
                "current_data": {
                    "calculation_status": "completed",
                    "total_students": 15000
                },
                "change_summary": {
                    "updated_fields": ["calculation_status", "total_students"],
                    "update_time": "2025-09-04T10:05:00Z"
                },
                "change_reason": "Final calculation completed",
                "triggered_by": "calculation_engine",
                "batch_code": "BATCH_2025_001",
                "created_at": "2025-09-04T10:05:00Z"
            }
        }


class BatchStatisticsSummaryResponse(BaseModel):
    """批次统计数据摘要响应模型"""
    batch_code: str
    has_regional_data: bool
    regional_status: Optional[str]
    total_schools: int
    total_students: int
    avg_calculation_duration: float
    
    class Config:
        schema_extra = {
            "example": {
                "batch_code": "BATCH_2025_001",
                "has_regional_data": True,
                "regional_status": "completed",
                "total_schools": 50,
                "total_students": 15000,
                "avg_calculation_duration": 125.5
            }
        }


class SchoolStatisticsListResponse(BaseModel):
    """学校统计数据列表响应模型"""
    batch_code: str
    total_count: int
    schools: List[StatisticalAggregationResponse]
    
    class Config:
        schema_extra = {
            "example": {
                "batch_code": "BATCH_2025_001",
                "total_count": 50,
                "schools": []
            }
        }


class StatisticsWithHistoryResponse(BaseModel):
    """统计数据及历史记录响应模型"""
    aggregation: StatisticalAggregationResponse
    history: List[StatisticalHistoryResponse]
    total_changes: int
    
    class Config:
        schema_extra = {
            "example": {
                "aggregation": {},
                "history": [],
                "total_changes": 5
            }
        }


class MetadataListResponse(BaseModel):
    """元数据列表响应模型"""
    metadata_type: Optional[MetadataType]
    total_count: int
    metadata_items: List[StatisticalMetadataResponse]
    
    class Config:
        schema_extra = {
            "example": {
                "metadata_type": "grade_config",
                "total_count": 3,
                "metadata_items": []
            }
        }


class HistoryListResponse(BaseModel):
    """历史记录列表响应模型"""
    batch_code: Optional[str]
    aggregation_id: Optional[int]
    total_count: int
    history_records: List[StatisticalHistoryResponse]
    
    class Config:
        schema_extra = {
            "example": {
                "batch_code": "BATCH_2025_001",
                "aggregation_id": None,
                "total_count": 25,
                "history_records": []
            }
        }


# 通用响应模型
class OperationResultResponse(BaseModel):
    """操作结果响应模型"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "操作成功完成",
                "data": {
                    "affected_rows": 1,
                    "operation_time": "2025-09-04T10:05:00Z"
                }
            }
        }


class ErrorResponse(BaseModel):
    """错误响应模型"""
    error: str
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.now)
    
    class Config:
        schema_extra = {
            "example": {
                "error": "VALIDATION_ERROR",
                "message": "请求参数验证失败",
                "details": {
                    "field": "school_id",
                    "reason": "学校级统计数据必须提供school_id"
                },
                "timestamp": "2025-09-04T10:05:00Z"
            }
        }


class PaginatedResponse(BaseModel):
    """分页响应基础模型"""
    total: int = Field(..., description="总记录数")
    page: int = Field(..., description="当前页码")
    page_size: int = Field(..., description="每页大小")
    total_pages: int = Field(..., description="总页数")
    
    class Config:
        schema_extra = {
            "example": {
                "total": 150,
                "page": 1,
                "page_size": 20,
                "total_pages": 8
            }
        }


class PaginatedStatisticsResponse(PaginatedResponse):
    """分页统计数据响应模型"""
    items: List[StatisticalAggregationResponse]


class PaginatedMetadataResponse(PaginatedResponse):
    """分页元数据响应模型"""
    items: List[StatisticalMetadataResponse]


class PaginatedHistoryResponse(PaginatedResponse):
    """分页历史记录响应模型"""
    items: List[StatisticalHistoryResponse]