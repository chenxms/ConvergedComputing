from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

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