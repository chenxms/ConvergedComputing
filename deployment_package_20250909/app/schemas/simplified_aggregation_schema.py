"""
简化的汇聚数据结构定义
支持所有批次的通用处理
"""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

# ============= 基础模型 =============

class SubjectCoreMetrics(BaseModel):
    """科目核心指标（保留2位小数）"""
    avg_score: float = Field(..., description="平均分")
    difficulty: float = Field(..., description="难度系数(平均分/满分)")
    std_dev: float = Field(..., description="标准差")
    discrimination: float = Field(..., description="区分度")
    max_score: float = Field(..., description="最高分")
    min_score: float = Field(..., description="最低分")
    p10: float = Field(..., description="10%分位数")
    p50: float = Field(..., description="50%分位数(中位数)")
    p90: float = Field(..., description="90%分位数")
    student_count: int = Field(..., description="学生人数")
    
class SubjectRanking(BaseModel):
    """科目排名信息"""
    # 区域级显示学校排名列表
    school_rankings: Optional[List[Dict[str, Any]]] = Field(None, description="学校排名列表")
    # 学校级显示在区域的排名
    regional_rank: Optional[int] = Field(None, description="在区域内的排名")
    total_schools: Optional[int] = Field(None, description="总学校数")

class DimensionMetrics(BaseModel):
    """维度指标（简化版）"""
    avg_score: float = Field(..., description="平均分")
    score_rate: float = Field(..., description="得分率")
    rank: Optional[int] = Field(None, description="维度排名")
    student_count: int = Field(..., description="学生人数")

class QuestionnaireOptionDistribution(BaseModel):
    """问卷选项分布"""
    option_label: str = Field(..., description="选项标签")
    count: int = Field(..., description="选择人数")
    percentage: float = Field(..., description="百分比")

class QuestionnaireQuestionStats(BaseModel):
    """问卷题目统计"""
    question_id: str = Field(..., description="题目ID")
    question_name: str = Field(..., description="题目名称")
    option_distributions: List[QuestionnaireOptionDistribution] = Field(..., description="选项分布")

class QuestionnaireDimensionStats(BaseModel):
    """问卷维度统计"""
    dimension_code: str = Field(..., description="维度代码")
    dimension_name: str = Field(..., description="维度名称")
    avg_score: float = Field(..., description="平均分")
    score_rate: float = Field(..., description="得分率")
    rank: Optional[int] = Field(None, description="维度排名")
    # 维度级选项占比
    dimension_option_distributions: List[QuestionnaireOptionDistribution] = Field(..., description="维度选项分布")
    # 题目级选项占比
    questions: List[QuestionnaireQuestionStats] = Field(..., description="题目统计")
    student_count: int = Field(..., description="学生人数")

# ============= 科目模型 =============

class SubjectStatistics(BaseModel):
    """科目统计数据"""
    subject_id: str = Field(..., description="科目ID")
    subject_name: str = Field(..., description="科目名称")
    subject_type: str = Field(..., description="科目类型：exam/questionnaire")
    
    # 核心指标
    metrics: SubjectCoreMetrics = Field(..., description="核心指标")
    
    # 排名信息
    ranking: SubjectRanking = Field(..., description="排名信息")
    
    # 维度统计（考试科目的知识点维度）
    dimensions: Optional[Dict[str, DimensionMetrics]] = Field(None, description="维度统计")
    
    # 问卷特殊数据（仅问卷类科目）
    questionnaire_dimensions: Optional[List[QuestionnaireDimensionStats]] = Field(None, description="问卷维度统计")

# ============= 汇聚级别模型 =============

class RegionalAggregationData(BaseModel):
    """区域级汇聚数据"""
    batch_code: str = Field(..., description="批次代码")
    aggregation_level: str = Field(default="REGIONAL", description="汇聚级别")
    
    # 基础信息
    total_schools: int = Field(..., description="学校总数")
    total_students: int = Field(..., description="学生总数")
    
    # 科目统计
    subjects: Dict[str, SubjectStatistics] = Field(..., description="科目统计数据")
    
    # 元数据
    created_at: str = Field(..., description="创建时间")
    updated_at: str = Field(..., description="更新时间")
    data_version: str = Field(default="2.0", description="数据版本")

class SchoolAggregationData(BaseModel):
    """学校级汇聚数据"""
    batch_code: str = Field(..., description="批次代码")
    aggregation_level: str = Field(default="SCHOOL", description="汇聚级别")
    school_id: str = Field(..., description="学校ID")
    school_name: str = Field(..., description="学校名称")
    
    # 基础信息
    total_students: int = Field(..., description="学生总数")
    
    # 科目统计
    subjects: Dict[str, SubjectStatistics] = Field(..., description="科目统计数据")
    
    # 元数据
    created_at: str = Field(..., description="创建时间")
    updated_at: str = Field(..., description="更新时间")
    data_version: str = Field(default="2.0", description="数据版本")

# ============= 工具函数 =============

def format_decimal(value: float, precision: int = 2) -> float:
    """格式化小数位数"""
    if value is None:
        return 0.0
    return round(float(value), precision)

def calculate_difficulty(avg_score: float, max_score: float) -> float:
    """计算难度系数"""
    if max_score == 0:
        return 0.0
    return format_decimal(avg_score / max_score, 2)

def calculate_score_rate(score: float, max_score: float) -> float:
    """计算得分率"""
    if max_score == 0:
        return 0.0
    return format_decimal(score / max_score * 100)

# ============= 示例数据结构 =============

"""
区域级JSON结构示例：
{
    "batch_code": "G7-2025",
    "aggregation_level": "REGIONAL",
    "total_schools": 120,
    "total_students": 15200,
    "subjects": {
        "SUBJ_001": {
            "subject_id": "SUBJ_001",
            "subject_name": "语文",
            "subject_type": "exam",
            "metrics": {
                "avg_score": 85.62,
                "difficulty": 0.86,
                "std_dev": 12.35,
                "discrimination": 0.42,
                "max_score": 100.00,
                "min_score": 45.00,
                "p10": 70.00,
                "p50": 86.00,
                "p90": 95.00,
                "student_count": 15200
            },
            "ranking": {
                "school_rankings": [
                    {"school_id": "5001", "school_name": "一中", "avg_score": 92.30, "rank": 1},
                    {"school_id": "5002", "school_name": "二中", "avg_score": 91.50, "rank": 2}
                ]
            },
            "dimensions": {
                "YW-jc": {
                    "avg_score": 28.50,
                    "score_rate": 85.50,
                    "rank": null,
                    "student_count": 15200
                }
            }
        },
        "SUBJ_WJ": {
            "subject_id": "SUBJ_WJ",
            "subject_name": "问卷",
            "subject_type": "questionnaire",
            "metrics": {
                "avg_score": 3.85,
                "difficulty": 0.77,
                "std_dev": 0.95,
                "discrimination": 0.38,
                "max_score": 5.00,
                "min_score": 1.00,
                "p10": 2.80,
                "p50": 3.90,
                "p90": 4.80,
                "student_count": 1074
            },
            "questionnaire_dimensions": [
                {
                    "dimension_code": "CZL-hqx",
                    "dimension_name": "好奇心",
                    "avg_score": 3.75,
                    "score_rate": 75.00,
                    "rank": 1,
                    "dimension_option_distributions": [
                        {"option_label": "非常满意", "count": 320, "percentage": 29.80},
                        {"option_label": "满意", "count": 450, "percentage": 41.90}
                    ],
                    "questions": [
                        {
                            "question_id": "Q001",
                            "question_name": "您对课堂互动满意吗",
                            "option_distributions": [
                                {"option_label": "非常满意", "count": 120, "percentage": 11.17},
                                {"option_label": "满意", "count": 350, "percentage": 32.59}
                            ]
                        }
                    ],
                    "student_count": 1074
                }
            ]
        }
    },
    "created_at": "2025-01-07T10:30:00",
    "updated_at": "2025-01-07T10:30:00",
    "data_version": "2.0"
}

学校级JSON结构示例：
{
    "batch_code": "G7-2025",
    "aggregation_level": "SCHOOL",
    "school_id": "5001",
    "school_name": "第一中学",
    "total_students": 320,
    "subjects": {
        "SUBJ_001": {
            "subject_id": "SUBJ_001",
            "subject_name": "语文",
            "subject_type": "exam",
            "metrics": {
                "avg_score": 92.30,
                "difficulty": 0.92,
                "std_dev": 8.65,
                "discrimination": 0.45,
                "max_score": 100.00,
                "min_score": 65.00,
                "p10": 82.00,
                "p50": 93.00,
                "p90": 98.00,
                "student_count": 320
            },
            "ranking": {
                "regional_rank": 1,
                "total_schools": 120
            },
            "dimensions": {
                "YW-jc": {
                    "avg_score": 31.20,
                    "score_rate": 93.60,
                    "rank": 1,
                    "student_count": 320
                }
            }
        }
    },
    "created_at": "2025-01-07T10:30:00",
    "updated_at": "2025-01-07T10:30:00",
    "data_version": "2.0"
}
"""