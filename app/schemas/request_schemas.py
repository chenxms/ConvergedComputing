from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

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