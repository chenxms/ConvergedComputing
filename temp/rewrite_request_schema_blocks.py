# -*- coding: utf-8 -*-
from pathlib import Path
from textwrap import dedent

path = Path('app/schemas/request_schemas.py')
text = path.read_text(encoding='utf-8')
start = text.index('class StatisticalAggregationCreateRequest(BaseModel):')
end = text.index('\n\n\nclass StatisticalAggregationUpdateRequest', start)
new_block = dedent('''
class StatisticalAggregationCreateRequest(BaseModel):
    """创建统计汇聚数据请求模型"""
    batch_code: str = Field(..., description="批次代码", min_length=1, max_length=255)
    aggregation_level: AggregationLevel = Field(..., description="汇聚级别")
    school_id: Optional[str] = Field(None, description="学校ID(学校级时必填)", max_length=64)
    school_name: Optional[str] = Field(None, description="学校名称", max_length=255)
    statistics_data: Dict[str, Any] = Field(..., description="统计数据JSON")
    data_version: str = Field("1.0", description="数据版本号", max_length=255)
    total_students: int = Field(0, description="参与学生总数", ge=0)
    total_schools: int = Field(0, description="参与学校总数(区域级)", ge=0)
    change_reason: Optional[str] = Field(None, description="变更原因")
    triggered_by: Optional[str] = Field("system", description="触发者")

    @model_validator(mode='after')
    def validate_school_id_required(self):
        """校验学校级数据需提供 school_id"""
        if self.aggregation_level == AggregationLevel.SCHOOL and (not self.school_id or self.school_id.strip() == ""):
            raise ValueError('学校级汇聚数据必须提供school_id')
        return self

    @validator('statistics_data')
    def validate_statistics_data(cls, v):
        """校验统计数据JSON格式"""
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
''')
text = text[:start] + new_block + text[end:]
start = text.index('class BatchStatisticsQueryRequest(BaseModel):')
end = text.index('\n\n\nclass HistoryQueryRequest', start)
new_block = dedent('''
class BatchStatisticsQueryRequest(BaseModel):
    """批次统计数据查询请求模型"""
    batch_code: str = Field(..., description="批次代码", min_length=1, max_length=255)
    aggregation_level: Optional[AggregationLevel] = Field(None, description="汇聚级别")
    school_ids: Optional[List[str]] = Field(None, description="学校ID列表")
    calculation_status: Optional[CalculationStatus] = Field(None, description="计算状态")
    include_history: bool = Field(False, description="是否包含历史记录")
''')
text = text[:start] + new_block + text[end:]
path.write_text(text, encoding='utf-8')
