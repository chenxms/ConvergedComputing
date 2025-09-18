# SQLAlchemy模型定义
from sqlalchemy import (
    Column, BigInteger, String, DateTime, Text, Float, JSON, Enum, Boolean, DECIMAL, Integer,
    ForeignKey, Index, UniqueConstraint, func
)
from sqlalchemy.orm import relationship
from .connection import Base
from .enums import AggregationLevel, CalculationStatus, MetadataType, ChangeType, SubjectType
from .types import JSONText, IntString, FloatString, DateTimeString, EnumString
from datetime import datetime
from typing import Optional


# 原有的简化模型（保持向后兼容）
class Batch(Base):
    """批次模型（原有模型，保持向后兼容）"""
    __tablename__ = "batches"
    
    id = Column(BigInteger, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    created_at = Column(DateTime, default=func.now())
    status = Column(String(50))


class Task(Base):
    """任务模型（原有模型，保持向后兼容）"""
    __tablename__ = "tasks"
    
    id = Column(BigInteger, primary_key=True, index=True)
    batch_id = Column(BigInteger)
    status = Column(String(50))
    progress = Column(Float)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    updated_at = Column(
        DateTime, 
        default=func.now(), 
        onupdate=func.now(), 
        nullable=True
    )
    error_message = Column(Text)


# 新的统计相关模型

class StatisticalAggregation(Base):
    """统计汇聚主表模型"""
    __tablename__ = "statistical_aggregations"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True, comment="id")
    batch_code = Column(String(255), nullable=False, comment="批次代码")
    aggregation_level = Column(EnumString(AggregationLevel), nullable=False, comment="汇聚级别")
    school_id = Column(String(64), nullable=True, comment="学校ID(学校级时必填)")
    school_name = Column(String(255), nullable=True, comment="学校名称")
    school_id_norm = Column(String(255), nullable=True, comment="标准化学校ID")
    statistics_data = Column(JSONText(), nullable=False, comment="统计数据JSON")
    data_version = Column(String(255), nullable=False, default="1.0", comment="数据版本号")
    calculation_status = Column(
        EnumString(CalculationStatus),
        nullable=False,
        default=CalculationStatus.PENDING,
        comment="计算状态"
    )
    total_students = Column(IntString(), default=0, comment="参与学生总数")
    total_schools = Column(IntString(), default=0, comment="参与学校总数(区域级)")
    calculation_duration = Column(FloatString(), nullable=True, comment="计算耗时(秒)")
    created_at = Column(DateTimeString(), default=datetime.utcnow, nullable=True)
    updated_at = Column(
        DateTime,
        default=func.now(),
        onupdate=func.now(),
        nullable=True
    )

    history_records = relationship(
        "StatisticalHistory",
        back_populates="aggregation",
        cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint(
            'batch_code', 'aggregation_level', 'school_id',
            name='uk_batch_level_school'
        ),
        Index('idx_batch_code', 'batch_code'),
        Index('idx_aggregation_level', 'aggregation_level'),
        Index('idx_school_id', 'school_id'),
        Index('idx_school_id_norm', 'school_id_norm'),
        Index('idx_calculation_status', 'calculation_status'),
        Index('idx_created_at', 'created_at'),
        {"comment": "统计汇聚主表"}
    )



class StatisticalMetadata(Base):
    """统计元数据表模型"""
    __tablename__ = "statistical_metadata"
    
    id = Column(BigInteger, primary_key=True, index=True)
    metadata_type = Column(Enum(MetadataType), nullable=False, comment="元数据类型")
    metadata_key = Column(String(100), nullable=False, comment="元数据键")
    metadata_value = Column(JSON, nullable=False, comment="元数据内容")
    grade_level = Column(String(20), nullable=True, comment="适用年级")
    subject_type = Column(Enum(SubjectType), nullable=True, comment="适用科目类型")
    is_active = Column(Boolean, nullable=False, default=True, comment="是否激活")
    version = Column(String(10), nullable=False, default="1.0", comment="版本号")
    description = Column(Text, nullable=True, comment="配置描述")
    created_by = Column(String(50), default="system", comment="创建者")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, 
        default=func.now(), 
        onupdate=func.now(), 
        nullable=False
    )
    
    __table_args__ = (
        UniqueConstraint(
            'metadata_type', 'metadata_key', 'version', 
            name='uk_type_key_version'
        ),
        Index('idx_metadata_type', 'metadata_type'),
        Index('idx_metadata_key', 'metadata_key'),
        Index('idx_grade_level', 'grade_level'),
        Index('idx_is_active', 'is_active'),
        {"comment": "统计元数据表"}
    )


class StatisticalHistory(Base):
    """统计历史记录表模型"""
    __tablename__ = "statistical_history"
    
    id = Column(BigInteger, primary_key=True, index=True)
    aggregation_id = Column(
        BigInteger, 
        ForeignKey('statistical_aggregations.id', ondelete='CASCADE'), 
        nullable=False,
        comment="关联statistical_aggregations.id"
    )
    change_type = Column(Enum(ChangeType), nullable=False, comment="变更类型")
    previous_data = Column(JSON, nullable=True, comment="变更前数据快照")
    current_data = Column(JSON, nullable=True, comment="变更后数据快照")
    change_summary = Column(JSON, nullable=True, comment="变更摘要")
    change_reason = Column(String(255), nullable=True, comment="变更原因")
    triggered_by = Column(String(50), default="system", comment="触发者")
    batch_code = Column(String(50), nullable=False, comment="批次代码(冗余字段便于查询)")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    
    # 关系映射
    aggregation = relationship(
        "StatisticalAggregation", 
        back_populates="history_records"
    )
    
    __table_args__ = (
        Index('idx_aggregation_id', 'aggregation_id'),
        Index('idx_change_type', 'change_type'),
        Index('idx_batch_code', 'batch_code'),
        Index('idx_created_at', 'created_at'),
        {"comment": "统计历史记录表"}
    )


# 数据源相关表模型
class StudentCleanedScore(Base):
    """学生清洗后分数数据表模型"""
    __tablename__ = "student_cleaned_scores"
    
    id = Column(BigInteger, primary_key=True, index=True)
    batch_code = Column(String(50), nullable=False, comment="批次代码")
    school_code = Column(String(50), nullable=False, comment="学校代码")
    school_name = Column(String(100), nullable=True, comment="学校名称")
    student_id = Column(String(50), nullable=False, comment="学生ID")
    subject_name = Column(String(100), nullable=False, comment="科目名称")
    subject_type = Column(String(50), nullable=False, comment="科目类型")
    total_score = Column(DECIMAL(8, 2), nullable=False, comment="总分")
    dimension_scores = Column(JSON, nullable=True, comment="维度分数JSON")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, 
        default=func.now(), 
        onupdate=func.now(), 
        nullable=False
    )
    
    __table_args__ = (
        Index('idx_batch_code', 'batch_code'),
        Index('idx_school_code', 'school_code'),
        Index('idx_subject_name', 'subject_name'),
        Index('idx_student_id', 'student_id'),
        Index('idx_batch_school_subject', 'batch_code', 'school_code', 'subject_name'),
        {"comment": "学生清洗后分数数据表"}
    )


class QuestionnaireScaleOption(Base):
    """问卷量表选项表模型"""
    __tablename__ = "questionnaire_scale_options"
    
    id = Column(BigInteger, primary_key=True, index=True)
    instrument_type = Column(String(50), nullable=False, comment="量表类型")
    scale_level = Column(String(20), nullable=False, comment="量表级别")
    option_level = Column(BigInteger, nullable=False, comment="选项级别")
    option_label = Column(String(100), nullable=False, comment="选项标签")
    display_order = Column(BigInteger, nullable=True, comment="显示顺序")
    is_active = Column(Boolean, nullable=False, default=True, comment="是否激活")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, 
        default=func.now(), 
        onupdate=func.now(), 
        nullable=False
    )
    
    __table_args__ = (
        UniqueConstraint(
            'instrument_type', 'scale_level', 'option_level',
            name='uk_instrument_scale_option'
        ),
        Index('idx_instrument_type', 'instrument_type'),
        Index('idx_scale_level', 'scale_level'),
        Index('idx_option_level', 'option_level'),
        Index('idx_is_active', 'is_active'),
        {"comment": "问卷量表选项表"}
    )


class SchoolMasterData(Base):
    """学校主数据表模型"""
    __tablename__ = "school_master_data"
    
    id = Column(BigInteger, primary_key=True, index=True)
    batch_code = Column(String(50), nullable=False, comment="批次代码")
    school_id = Column(String(50), nullable=False, comment="学校ID")
    standard_school_name = Column(String(128), nullable=False, comment="标准化学校名称")
    school_type = Column(String(50), nullable=True, default="MIDDLE_SCHOOL", comment="学校类型")
    status = Column(String(20), nullable=True, default="ACTIVE", comment="状态")
    data_source = Column(String(50), nullable=True, comment="数据来源")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, 
        default=func.now(), 
        onupdate=func.now(), 
        nullable=False
    )
    
    __table_args__ = (
        Index('idx_batch_code', 'batch_code'),
        Index('idx_school_id', 'school_id'),
        Index('idx_standard_school_name', 'standard_school_name'),
        Index('idx_status', 'status'),
        Index('idx_batch_school', 'batch_code', 'school_id'),
        {"comment": "学校主数据表"}
    )


class BatchDimensionDefinition(Base):
    """批次维度定义表模型"""
    __tablename__ = "batch_dimension_definition"
    
    id = Column(BigInteger, primary_key=True, index=True)
    batch_code = Column(String(50), nullable=False, comment="批次代码")
    subject_id = Column(String(64), nullable=False, comment="科目ID")
    subject_name = Column(String(128), nullable=False, comment="科目名称")
    dimension_code = Column(String(50), nullable=False, comment="维度代码")
    dimension_name = Column(String(100), nullable=False, comment="维度中文名称")
    secondary_dimension_code = Column(String(50), nullable=True, comment="二级维度代码")
    secondary_dimension_name = Column(String(100), nullable=True, comment="二级维度中文名称")
    weight = Column(DECIMAL(5, 4), nullable=True, default=1.0000, comment="权重")
    description = Column(Text, nullable=True, comment="描述")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, 
        default=func.now(), 
        onupdate=func.now(), 
        nullable=False
    )
    
    __table_args__ = (
        Index('idx_batch_code', 'batch_code'),
        Index('idx_subject_name', 'subject_name'),
        Index('idx_dimension_code', 'dimension_code'),
        Index('idx_batch_subject_dim', 'batch_code', 'subject_name', 'dimension_code'),
        {"comment": "批次维度定义表"}
    )


class QuestionOptionDistribution(Base):
    """题目选项分布表模型 - 独立表存储问卷题目选项分布"""
    __tablename__ = "questionnaire_option_distribution"
    
    id = Column(BigInteger, primary_key=True, index=True)
    batch_code = Column(String(50), nullable=False, comment="批次代码")
    school_id = Column(String(50), nullable=False, comment="学校ID")
    subject_name = Column(String(100), nullable=False, comment="科目名称")
    question_id = Column(String(100), nullable=False, comment="题目ID")
    option_level = Column(BigInteger, nullable=False, comment="选项等级")
    option_label = Column(String(100), nullable=True, comment="选项标签")
    count = Column(BigInteger, nullable=False, default=0, comment="选择人数")
    n_total = Column(BigInteger, nullable=False, default=0, comment="总答题人数")
    pct = Column(DECIMAL(7, 4), nullable=False, default=0, comment="百分比(0-100, 4位小数)")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, 
        default=func.now(), 
        onupdate=func.now(), 
        nullable=False
    )
    
    __table_args__ = (
        UniqueConstraint(
            'batch_code', 'school_id', 'subject_name', 'question_id', 'option_level',
            name='uk_questionnaire_option_distribution'
        ),
        Index('idx_batch_school_subject', 'batch_code', 'school_id', 'subject_name'),
        Index('idx_question_option', 'question_id', 'option_level'),
        {"comment": "问卷题目选项分布统计表"}
    )


class SubjectCoreMetric(Base):
    """科目核心统计缓存表"""
    __tablename__ = "subject_core_metrics"

    id = Column(BigInteger, primary_key=True, index=True)
    batch_code = Column(String(50), nullable=False, comment="批次代码")
    subject_name = Column(String(100), nullable=False, comment="科目名称")
    subject_type = Column(String(32), nullable=False, comment="科目类型")
    student_count = Column(Integer, nullable=False, default=0, comment="参与学生数")
    avg_score = Column(Float, nullable=False, default=0.0, comment="平均分")
    std_score = Column(Float, nullable=False, default=0.0, comment="标准差")
    max_score_achieved = Column(Float, nullable=False, default=0.0, comment="最高得分")
    min_score = Column(Float, nullable=False, default=0.0, comment="最低得分")
    max_score = Column(Float, nullable=False, default=0.0, comment="满分")
    score_rate = Column(Float, nullable=False, default=0.0, comment="得分率")
    difficulty_coefficient = Column(Float, nullable=True, comment="难度系数")
    pass_rate = Column(Float, nullable=True, comment="及格率")
    excellent_rate = Column(Float, nullable=True, comment="优秀率")
    good_rate = Column(Float, nullable=True, comment="良好率")
    fail_rate = Column(Float, nullable=True, comment="不及格率")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        default=func.now(),
        onupdate=func.now(),
        nullable=False
    )

    __table_args__ = (
        UniqueConstraint('batch_code', 'subject_name', name='uk_subject_core_metrics_batch_subject'),
        Index('idx_subject_core_metrics_lookup', 'batch_code', 'subject_name'),
        {"comment": "科目核心统计指标缓存表"}
    )


class SubjectSchoolRanking(Base):
    """科目学校排名缓存表"""
    __tablename__ = "subject_school_rankings"

    id = Column(BigInteger, primary_key=True, index=True)
    batch_code = Column(String(50), nullable=False, comment="批次代码")
    subject_name = Column(String(100), nullable=False, comment="科目名称")
    subject_type = Column(String(32), nullable=False, comment="科目类型")
    school_code = Column(String(50), nullable=False, comment="学校编码")
    school_name = Column(String(100), nullable=True, comment="学校名称")
    student_count = Column(Integer, nullable=False, default=0, comment="学校参与学生数")
    avg_score = Column(Float, nullable=False, default=0.0, comment="学校平均分")
    std_score = Column(Float, nullable=False, default=0.0, comment="学校标准差")
    max_score_achieved = Column(Float, nullable=False, default=0.0, comment="学校最高分")
    min_score = Column(Float, nullable=False, default=0.0, comment="学校最低分")
    max_score = Column(Float, nullable=False, default=0.0, comment="学校满分")
    score_rate = Column(Float, nullable=False, default=0.0, comment="学校得分率")
    difficulty_coefficient = Column(Float, nullable=True, comment="学校难度系数")
    rank = Column(Integer, nullable=False, comment="学校排名")
    total_schools = Column(Integer, nullable=False, default=0, comment="批次学校总数")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        default=func.now(),
        onupdate=func.now(),
        nullable=False
    )

    __table_args__ = (
        UniqueConstraint('batch_code', 'subject_name', 'school_code', name='uk_subject_school_rankings'),
        Index('idx_subject_school_rankings_lookup', 'batch_code', 'subject_name', 'school_code'),
        {"comment": "科目学校排名缓存表"}
    )
