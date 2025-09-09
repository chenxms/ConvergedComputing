#!/usr/bin/env python3
"""
简化版汇聚数据模型
所有数值最多保留2位小数
"""
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP


def round_to_2(value: float) -> float:
    """保留2位小数"""
    if value is None:
        return None
    return float(Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


@dataclass
class SubjectDimensionStats:
    """科目维度统计"""
    dimension_name: str
    average_score: float  # 平均分
    score_rate: float     # 得分率
    
    def to_dict(self) -> Dict:
        return {
            "dimension_name": self.dimension_name,
            "average_score": round_to_2(self.average_score),
            "score_rate": round_to_2(self.score_rate)
        }


@dataclass
class SubjectStats:
    """科目统计信息"""
    subject_name: str
    subject_code: str
    
    # 基础统计
    average_score: float      # 平均分
    max_score: float          # 最高分
    min_score: float          # 最低分
    std_deviation: float      # 标准差
    
    # 教育指标
    difficulty: float         # 难度（平均分/满分）
    discrimination: float     # 区分度
    
    # 百分位数
    p10: float               # P10
    p50: float               # P50
    p90: float               # P90
    
    # 维度统计
    dimensions: List[SubjectDimensionStats] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            "subject_name": self.subject_name,
            "subject_code": self.subject_code,
            "average_score": round_to_2(self.average_score),
            "max_score": round_to_2(self.max_score),
            "min_score": round_to_2(self.min_score),
            "std_deviation": round_to_2(self.std_deviation),
            "difficulty": round_to_2(self.difficulty),
            "discrimination": round_to_2(self.discrimination),
            "p10": round_to_2(self.p10),
            "p50": round_to_2(self.p50),
            "p90": round_to_2(self.p90),
            "dimensions": [d.to_dict() for d in self.dimensions]
        }


@dataclass
class SchoolRanking:
    """学校排名信息"""
    school_code: str
    school_name: str
    average_score: float
    rank: int
    
    def to_dict(self) -> Dict:
        return {
            "school_code": self.school_code,
            "school_name": self.school_name,
            "average_score": round_to_2(self.average_score),
            "rank": self.rank
        }


@dataclass
class QuestionnaireOption:
    """问卷选项统计"""
    option_value: int      # 选项值（1-5）
    option_label: str      # 选项标签（如：非常满意）
    percentage: float      # 占比
    
    def to_dict(self) -> Dict:
        return {
            "option_value": self.option_value,
            "option_label": self.option_label,
            "percentage": round_to_2(self.percentage)
        }


@dataclass 
class QuestionnaireQuestionStats:
    """问卷题目统计"""
    question_code: str
    question_text: str
    option_distribution: List[QuestionnaireOption]
    
    def to_dict(self) -> Dict:
        return {
            "question_code": self.question_code,
            "question_text": self.question_text,
            "option_distribution": [o.to_dict() for o in self.option_distribution]
        }


@dataclass
class QuestionnaireDimensionStats:
    """问卷维度统计"""
    dimension_name: str
    score_rate: float     # 得分率
    option_distribution: List[QuestionnaireOption]  # 维度选项分布
    questions: List[QuestionnaireQuestionStats]     # 维度下的题目统计
    
    def to_dict(self) -> Dict:
        return {
            "dimension_name": self.dimension_name,
            "score_rate": round_to_2(self.score_rate),
            "option_distribution": [o.to_dict() for o in self.option_distribution],
            "questions": [q.to_dict() for q in self.questions]
        }


@dataclass
class QuestionnaireStats:
    """问卷统计信息（作为特殊科目）"""
    questionnaire_name: str
    questionnaire_code: str
    dimensions: List[QuestionnaireDimensionStats]
    
    def to_dict(self) -> Dict:
        return {
            "questionnaire_name": self.questionnaire_name,
            "questionnaire_code": self.questionnaire_code,
            "dimensions": [d.to_dict() for d in self.dimensions]
        }


@dataclass
class RegionAggregationData:
    """区域级汇聚数据"""
    batch_code: str
    grade_code: str
    region_code: str
    region_name: str
    
    # 考试科目统计
    subjects: List[SubjectStats]
    
    # 学校排名（每个科目的学校排名列表）
    school_rankings: Dict[str, List[SchoolRanking]]  # key: subject_code
    
    # 问卷统计
    questionnaires: List[QuestionnaireStats]
    
    def to_dict(self) -> Dict:
        return {
            "batch_code": self.batch_code,
            "grade_code": self.grade_code,
            "region_code": self.region_code,
            "region_name": self.region_name,
            "subjects": [s.to_dict() for s in self.subjects],
            "school_rankings": {
                subject_code: [r.to_dict() for r in rankings]
                for subject_code, rankings in self.school_rankings.items()
            },
            "questionnaires": [q.to_dict() for q in self.questionnaires]
        }


@dataclass
class SchoolSubjectStats(SubjectStats):
    """学校科目统计（包含排名信息）"""
    region_rank: int  # 在区域中的排名
    
    def to_dict(self) -> Dict:
        result = super().to_dict()
        result["region_rank"] = self.region_rank
        return result


@dataclass  
class SchoolAggregationData:
    """学校级汇聚数据"""
    batch_code: str
    grade_code: str
    school_code: str
    school_name: str
    region_code: str
    region_name: str
    
    # 考试科目统计（包含排名）
    subjects: List[SchoolSubjectStats]
    
    # 问卷统计
    questionnaires: List[QuestionnaireStats]
    
    def to_dict(self) -> Dict:
        return {
            "batch_code": self.batch_code,
            "grade_code": self.grade_code,
            "school_code": self.school_code,
            "school_name": self.school_name,
            "region_code": self.region_code,
            "region_name": self.region_name,
            "subjects": [s.to_dict() for s in self.subjects],
            "questionnaires": [q.to_dict() for q in self.questionnaires]
        }


# 选项标签映射
OPTION_LABELS_5 = {
    1: "非常不满意",
    2: "不满意",
    3: "一般",
    4: "满意",
    5: "非常满意"
}

OPTION_LABELS_4 = {
    1: "非常不符合",
    2: "不符合", 
    3: "符合",
    4: "非常符合"
}

OPTION_LABELS_3 = {
    1: "不同意",
    2: "不确定",
    3: "同意"
}


def get_option_label(value: int, max_value: int) -> str:
    """获取选项标签"""
    if max_value == 5:
        return OPTION_LABELS_5.get(value, f"选项{value}")
    elif max_value == 4:
        return OPTION_LABELS_4.get(value, f"选项{value}")
    elif max_value == 3:
        return OPTION_LABELS_3.get(value, f"选项{value}")
    else:
        return f"选项{value}"