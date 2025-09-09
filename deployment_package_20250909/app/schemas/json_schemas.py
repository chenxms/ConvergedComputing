"""
JSON数据结构的Pydantic模型定义

严格遵循json-data-specification.md规范，
为前端提供类型安全的数据结构验证。
"""

from pydantic import BaseModel, Field, validator
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from decimal import Decimal
from enum import Enum


class GradeLevel(str, Enum):
    """年级水平枚举"""
    PRIMARY = "小学"
    MIDDLE = "初中"


class SubjectType(str, Enum):
    """科目类型枚举"""
    EXAM = "考试类"
    QUESTIONNAIRE = "问卷类"
    INTERACTIVE = "人机交互类"


class PerformanceLevel(str, Enum):
    """表现水平枚举"""
    EXCELLENT = "优秀"
    GOOD = "良好"
    AVERAGE = "一般"
    NEEDS_IMPROVEMENT = "待提升"
    UNKNOWN = "未知"


class ScaleType(str, Enum):
    """量表类型枚举"""
    POSITIVE = "正向"
    NEGATIVE = "反向"


# 基础数据模型
class BatchInfo(BaseModel):
    """批次信息模型"""
    batch_code: str = Field(..., pattern=r'^BATCH_\d{4}_\d{3}$', description="批次代码")
    grade_level: GradeLevel = Field(..., description="年级水平")
    total_schools: int = Field(..., ge=0, description="参与学校总数")
    total_students: int = Field(..., ge=0, description="参与学生总数")
    calculation_time: str = Field(..., description="计算完成时间(ISO格式)")
    
    @validator('calculation_time')
    def validate_datetime(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('时间格式必须为ISO格式')


class SchoolInfo(BaseModel):
    """学校信息模型"""
    school_id: str = Field(..., description="学校唯一标识")
    school_name: str = Field(..., description="学校名称")
    batch_code: str = Field(..., pattern=r'^BATCH_\d{4}_\d{3}$', description="批次代码")
    total_students: int = Field(..., ge=0, description="参与学生总数")
    calculation_time: str = Field(..., description="计算完成时间")
    
    @validator('calculation_time')
    def validate_datetime(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('时间格式必须为ISO格式')


class GradeCount(BaseModel):
    """等级人数统计模型"""
    count: int = Field(..., ge=0, description="人数")
    percentage: float = Field(..., ge=0.0, le=1.0, description="占比")


class GradeDistribution(BaseModel):
    """等级分布模型"""
    excellent: GradeCount = Field(..., description="优秀等级")
    good: GradeCount = Field(..., description="良好等级")
    pass_: GradeCount = Field(..., alias="pass", description="及格等级")
    fail: GradeCount = Field(..., description="不及格等级")
    
    @validator('excellent', 'good', 'pass_', 'fail')
    def validate_percentages(cls, v, values):
        # 验证百分比总和为1.0（允许小的浮点数误差）
        return v
    
    class Config:
        allow_population_by_field_name = True


class RegionalStats(BaseModel):
    """区域统计数据模型"""
    avg_score: float = Field(..., description="平均分(保留1位小数)")
    score_rate: float = Field(..., ge=0.0, le=1.0, description="得分率(保留3位小数)")
    difficulty: float = Field(..., ge=0.0, le=1.0, description="难度系数")
    discrimination: float = Field(..., ge=0.0, le=1.0, description="区分度")
    std_dev: float = Field(..., ge=0.0, description="标准差")
    max_score: Union[int, float] = Field(..., description="最高分")
    min_score: Union[int, float] = Field(..., description="最低分")
    
    @validator('avg_score')
    def round_avg_score(cls, v):
        return round(float(v), 1)
    
    @validator('score_rate', 'difficulty', 'discrimination')
    def round_rates(cls, v):
        return round(float(v), 3)
    
    @validator('std_dev')
    def round_std_dev(cls, v):
        return round(float(v), 1)


class SchoolStats(BaseModel):
    """学校统计数据模型"""
    avg_score: float = Field(..., description="平均分")
    score_rate: float = Field(..., ge=0.0, le=1.0, description="得分率")
    std_dev: float = Field(..., ge=0.0, description="标准差")
    max_score: Union[int, float] = Field(..., description="最高分")
    min_score: Union[int, float] = Field(..., description="最低分")
    regional_ranking: int = Field(..., ge=1, description="区域排名")
    
    @validator('avg_score', 'std_dev')
    def round_scores(cls, v):
        return round(float(v), 1)
    
    @validator('score_rate')
    def round_rate(cls, v):
        return round(float(v), 3)


class InteractiveRegionalStats(BaseModel):
    """人机交互类区域统计模型"""
    avg_score: float = Field(..., description="平均分")
    score_rate: float = Field(..., ge=0.0, le=1.0, description="得分率")
    total_score: int = Field(..., gt=0, description="满分")
    std_dev: float = Field(..., ge=0.0, description="标准差")
    
    @validator('avg_score', 'std_dev')
    def round_scores(cls, v):
        return round(float(v), 1)
    
    @validator('score_rate')
    def round_rate(cls, v):
        return round(float(v), 3)


class Percentiles(BaseModel):
    """百分位数模型"""
    P10: Union[int, float] = Field(..., description="P10百分位")
    P50: Union[int, float] = Field(..., description="P50百分位(中位数)")
    P90: Union[int, float] = Field(..., description="P90百分位")


class SchoolRanking(BaseModel):
    """学校排名模型"""
    school_id: str = Field(..., description="学校ID")
    school_name: str = Field(..., description="学校名称")
    avg_score: float = Field(..., description="平均分")
    score_rate: float = Field(..., ge=0.0, le=1.0, description="得分率")
    ranking: int = Field(..., ge=1, description="排名")
    
    @validator('avg_score')
    def round_score(cls, v):
        return round(float(v), 1)
    
    @validator('score_rate')
    def round_rate(cls, v):
        return round(float(v), 3)


class RegionalComparison(BaseModel):
    """区域对比模型"""
    regional_avg_score: float = Field(..., description="区域平均分")
    regional_score_rate: float = Field(..., ge=0.0, le=1.0, description="区域得分率")
    difference: float = Field(..., description="分数差异")
    rate_difference: float = Field(..., description="得分率差异")
    performance_level: PerformanceLevel = Field(..., description="表现水平")
    
    @validator('regional_avg_score', 'difference')
    def round_scores(cls, v):
        return round(float(v), 1)
    
    @validator('regional_score_rate', 'rate_difference')
    def round_rates(cls, v):
        return round(float(v), 3)


class DimensionStats(BaseModel):
    """维度统计数据模型"""
    dimension_id: str = Field(..., description="维度ID")
    dimension_name: str = Field(..., description="维度名称")
    total_score: int = Field(..., gt=0, description="维度满分")
    avg_score: float = Field(..., description="平均分")
    score_rate: float = Field(..., ge=0.0, le=1.0, description="得分率")
    regional_ranking_avg: float = Field(..., ge=0.0, le=1.0, description="区域平均得分率")
    
    @validator('avg_score')
    def round_score(cls, v):
        return round(float(v), 1)
    
    @validator('score_rate', 'regional_ranking_avg')
    def round_rates(cls, v):
        return round(float(v), 3)


class SchoolDimensionStats(BaseModel):
    """学校维度统计数据模型"""
    dimension_id: str = Field(..., description="维度ID")
    dimension_name: str = Field(..., description="维度名称")
    total_score: int = Field(..., gt=0, description="维度满分")
    school_avg_score: float = Field(..., description="学校平均分")
    school_score_rate: float = Field(..., ge=0.0, le=1.0, description="学校得分率")
    regional_avg_score: float = Field(..., description="区域平均分")
    regional_score_rate: float = Field(..., ge=0.0, le=1.0, description="区域得分率")
    difference: float = Field(..., description="分数差异")
    rate_difference: float = Field(..., description="得分率差异")
    regional_ranking: int = Field(..., ge=1, description="区域排名")
    
    @validator('school_avg_score', 'regional_avg_score', 'difference')
    def round_scores(cls, v):
        return round(float(v), 1)
    
    @validator('school_score_rate', 'regional_score_rate', 'rate_difference')
    def round_rates(cls, v):
        return round(float(v), 3)


class OptionCount(BaseModel):
    """选项统计模型"""
    count: int = Field(..., ge=0, description="选择人数")
    percentage: float = Field(..., ge=0.0, le=1.0, description="选择比例")
    
    @validator('percentage')
    def round_percentage(cls, v):
        return round(float(v), 2)


class QuestionAnalysis(BaseModel):
    """题目分析模型"""
    question_id: str = Field(..., description="题目ID")
    question_text: str = Field(..., description="题目内容")
    scale_type: ScaleType = Field(..., description="量表类型")
    option_distribution: Dict[str, OptionCount] = Field(..., description="选项分布")


class SurveyDimensionStats(BaseModel):
    """问卷维度统计模型"""
    dimension_id: str = Field(..., description="维度ID")
    dimension_name: str = Field(..., description="维度名称")
    total_score: int = Field(..., gt=0, description="维度满分")
    avg_score: float = Field(..., description="平均分")
    score_rate: float = Field(..., ge=0.0, le=1.0, description="得分率")
    question_analysis: List[QuestionAnalysis] = Field(..., description="题目分析")
    
    @validator('avg_score')
    def round_score(cls, v):
        return round(float(v), 1)
    
    @validator('score_rate')
    def round_rate(cls, v):
        return round(float(v), 3)


class SchoolSurveyDimensionStats(BaseModel):
    """学校问卷维度统计模型"""
    dimension_id: str = Field(..., description="维度ID")
    dimension_name: str = Field(..., description="维度名称")
    total_score: int = Field(..., gt=0, description="维度满分")
    school_avg_score: float = Field(..., description="学校平均分")
    school_score_rate: float = Field(..., ge=0.0, le=1.0, description="学校得分率")
    regional_avg_score: float = Field(..., description="区域平均分")
    regional_score_rate: float = Field(..., ge=0.0, le=1.0, description="区域得分率")
    difference: float = Field(..., description="分数差异")
    rate_difference: float = Field(..., description="得分率差异")
    regional_ranking: int = Field(..., ge=1, description="区域排名")
    
    @validator('school_avg_score', 'regional_avg_score', 'difference')
    def round_scores(cls, v):
        return round(float(v), 1)
    
    @validator('school_score_rate', 'regional_score_rate', 'rate_difference')
    def round_rates(cls, v):
        return round(float(v), 3)


# 雷达图数据模型
class RegionalRadarDimension(BaseModel):
    """区域级雷达图维度模型"""
    dimension_name: str = Field(..., description="维度名称")
    score_rate: float = Field(..., ge=0.0, le=1.0, description="得分率")
    max_rate: float = Field(1.0, description="最大刻度值")
    
    @validator('score_rate')
    def round_rate(cls, v):
        return round(float(v), 3)


class SchoolRadarDimension(BaseModel):
    """学校级雷达图维度模型"""
    dimension_name: str = Field(..., description="维度名称")
    school_score_rate: float = Field(..., ge=0.0, le=1.0, description="学校得分率")
    regional_score_rate: float = Field(..., ge=0.0, le=1.0, description="区域得分率")
    max_rate: float = Field(1.0, description="最大刻度值")
    
    @validator('school_score_rate', 'regional_score_rate')
    def round_rates(cls, v):
        return round(float(v), 3)


class RegionalRadarChartData(BaseModel):
    """区域级雷达图数据模型"""
    academic_dimensions: List[RegionalRadarDimension] = Field(..., description="学业维度")
    non_academic_dimensions: List[RegionalRadarDimension] = Field(..., description="非学业维度")


class SchoolRadarChartData(BaseModel):
    """学校级雷达图数据模型"""
    academic_dimensions: List[SchoolRadarDimension] = Field(..., description="学业维度")
    non_academic_dimensions: List[SchoolRadarDimension] = Field(..., description="非学业维度")


# 科目数据模型
class AcademicSubject(BaseModel):
    """学业科目模型"""
    subject_id: str = Field(..., description="科目ID")
    subject_type: SubjectType = Field(SubjectType.EXAM, description="科目类型")
    total_score: int = Field(..., gt=0, description="科目满分")
    regional_stats: RegionalStats = Field(..., description="区域统计")
    grade_distribution: GradeDistribution = Field(..., description="等级分布")
    school_rankings: List[SchoolRanking] = Field(..., description="学校排名")
    dimensions: Dict[str, DimensionStats] = Field(..., description="维度统计")


class SchoolAcademicSubject(BaseModel):
    """学校学业科目模型"""
    subject_id: str = Field(..., description="科目ID")
    subject_type: SubjectType = Field(SubjectType.EXAM, description="科目类型")
    total_score: int = Field(..., gt=0, description="科目满分")
    school_stats: SchoolStats = Field(..., description="学校统计")
    percentiles: Percentiles = Field(..., description="百分位数")
    grade_distribution: GradeDistribution = Field(..., description="等级分布")
    regional_comparison: RegionalComparison = Field(..., description="区域对比")
    dimensions: Dict[str, SchoolDimensionStats] = Field(..., description="维度统计")


class SurveySubject(BaseModel):
    """问卷科目模型"""
    subject_id: str = Field(..., description="科目ID")
    subject_type: SubjectType = Field(SubjectType.QUESTIONNAIRE, description="科目类型")
    total_schools_participated: int = Field(..., ge=0, description="参与学校数")
    total_students_participated: int = Field(..., ge=0, description="参与学生数")
    dimensions: Dict[str, SurveyDimensionStats] = Field(..., description="维度统计")


class SchoolSurveySubject(BaseModel):
    """学校问卷科目模型"""
    subject_id: str = Field(..., description="科目ID")
    subject_type: SubjectType = Field(SubjectType.QUESTIONNAIRE, description="科目类型")
    participated_students: int = Field(..., ge=0, description="参与学生数")
    dimensions: Dict[str, SchoolSurveyDimensionStats] = Field(..., description="维度统计")


class InteractiveSubject(BaseModel):
    """人机交互科目模型"""
    subject_id: str = Field(..., description="科目ID")
    subject_type: SubjectType = Field(SubjectType.INTERACTIVE, description="科目类型")
    total_schools_participated: int = Field(..., ge=0, description="参与学校数")
    total_students_participated: int = Field(..., ge=0, description="参与学生数")
    regional_stats: InteractiveRegionalStats = Field(..., description="区域统计")
    dimensions: Dict[str, DimensionStats] = Field(..., description="维度统计")


class SchoolInteractiveSubject(BaseModel):
    """学校人机交互科目模型"""
    subject_id: str = Field(..., description="科目ID")
    subject_type: SubjectType = Field(SubjectType.INTERACTIVE, description="科目类型")
    participated_students: int = Field(..., ge=0, description="参与学生数")
    school_stats: SchoolStats = Field(..., description="学校统计")
    percentiles: Percentiles = Field(..., description="百分位数")
    regional_comparison: RegionalComparison = Field(..., description="区域对比")
    dimensions: Dict[str, SchoolDimensionStats] = Field(..., description="维度统计")


# 主要JSON数据结构模型
class RegionalStatisticsData(BaseModel):
    """区域级统计数据模型"""
    data_version: str = Field("1.0", description="数据版本")
    schema_version: str = Field("2025-09-04", description="Schema版本")
    batch_info: BatchInfo = Field(..., description="批次信息")
    academic_subjects: Dict[str, AcademicSubject] = Field(..., description="学业科目")
    non_academic_subjects: Dict[str, Union[SurveySubject, InteractiveSubject]] = Field(..., description="非学业科目")
    radar_chart_data: RegionalRadarChartData = Field(..., description="雷达图数据")
    
    class Config:
        schema_extra = {
            "example": {
                "data_version": "1.0",
                "schema_version": "2025-09-04",
                "batch_info": {
                    "batch_code": "BATCH_2025_001",
                    "grade_level": "初中",
                    "total_schools": 25,
                    "total_students": 8500,
                    "calculation_time": "2025-09-04T18:30:00Z"
                },
                "academic_subjects": {},
                "non_academic_subjects": {},
                "radar_chart_data": {
                    "academic_dimensions": [],
                    "non_academic_dimensions": []
                }
            }
        }


class SchoolStatisticsData(BaseModel):
    """学校级统计数据模型"""
    data_version: str = Field("1.0", description="数据版本")
    schema_version: str = Field("2025-09-04", description="Schema版本")
    school_info: SchoolInfo = Field(..., description="学校信息")
    academic_subjects: Dict[str, SchoolAcademicSubject] = Field(..., description="学业科目")
    non_academic_subjects: Dict[str, Union[SchoolSurveySubject, SchoolInteractiveSubject]] = Field(..., description="非学业科目")
    radar_chart_data: SchoolRadarChartData = Field(..., description="雷达图数据")
    
    class Config:
        schema_extra = {
            "example": {
                "data_version": "1.0",
                "schema_version": "2025-09-04",
                "school_info": {
                    "school_id": "SCH_001",
                    "school_name": "第一中学",
                    "batch_code": "BATCH_2025_001",
                    "total_students": 340,
                    "calculation_time": "2025-09-04T18:35:00Z"
                },
                "academic_subjects": {},
                "non_academic_subjects": {},
                "radar_chart_data": {
                    "academic_dimensions": [],
                    "non_academic_dimensions": []
                }
            }
        }


# API响应模型
class RegionalReportResponse(BaseModel):
    """区域报告API响应模型"""
    code: int = Field(200, description="响应码")
    message: str = Field("success", description="响应消息")
    data: Dict[str, Any] = Field(..., description="响应数据")
    timestamp: str = Field(..., description="响应时间")
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('时间格式必须为ISO格式')
    
    class Config:
        schema_extra = {
            "example": {
                "code": 200,
                "message": "success",
                "data": {
                    "batch_code": "BATCH_2025_001",
                    "statistics": {}
                },
                "timestamp": "2025-09-04T18:30:00Z"
            }
        }


class SchoolReportResponse(BaseModel):
    """学校报告API响应模型"""
    code: int = Field(200, description="响应码")
    message: str = Field("success", description="响应消息")
    data: Dict[str, Any] = Field(..., description="响应数据")
    timestamp: str = Field(..., description="响应时间")
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
            return v
        except ValueError:
            raise ValueError('时间格式必须为ISO格式')
    
    class Config:
        schema_extra = {
            "example": {
                "code": 200,
                "message": "success",
                "data": {
                    "batch_code": "BATCH_2025_001",
                    "school_id": "SCH_001",
                    "statistics": {}
                },
                "timestamp": "2025-09-04T18:30:00Z"
            }
        }