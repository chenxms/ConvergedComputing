# 数据库枚举定义
import enum


class AggregationLevel(enum.Enum):
    """汇聚级别枚举"""
    REGIONAL = "regional"
    SCHOOL = "school"


class CalculationStatus(enum.Enum):
    """计算状态枚举"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class MetadataType(enum.Enum):
    """元数据类型枚举"""
    CALCULATION_RULE = "calculation_rule"
    GRADE_CONFIG = "grade_config"
    DIMENSION_CONFIG = "dimension_config"
    FORMULA_CONFIG = "formula_config"


class ChangeType(enum.Enum):
    """变更类型枚举"""
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    RECALCULATED = "recalculated"


class SubjectType(enum.Enum):
    """科目类型枚举"""
    EXAM = "考试类"
    INTERACTIVE = "人机交互类"
    SURVEY = "问卷类"