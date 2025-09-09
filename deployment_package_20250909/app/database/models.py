# SQLAlchemy模型定义
from sqlalchemy import (
    Column, BigInteger, String, DateTime, Text, Float, JSON, Enum, Boolean, DECIMAL,
    ForeignKey, Index, UniqueConstraint, func
)
from sqlalchemy.orm import relationship
from .connection import Base
from .enums import AggregationLevel, CalculationStatus, MetadataType, ChangeType, SubjectType
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
    
    id = Column(BigInteger, primary_key=True, index=True)
    batch_code = Column(String(50), nullable=False, comment="批次代码")
    aggregation_level = Column(Enum(AggregationLevel), nullable=False, comment="汇聚级别")
    school_id = Column(String(50), nullable=True, comment="学校ID(学校级时必填)")
    school_name = Column(String(100), nullable=True, comment="学校名称")
    statistics_data = Column(JSON, nullable=False, comment="统计数据JSON")
    data_version = Column(String(10), nullable=False, default="1.0", comment="数据版本号")
    calculation_status = Column(
        Enum(CalculationStatus), 
        nullable=False, 
        default=CalculationStatus.PENDING,
        comment="计算状态"
    )
    total_students = Column(BigInteger, default=0, comment="参与学生总数")
    total_schools = Column(BigInteger, default=0, comment="参与学校总数(区域级)")
    calculation_duration = Column(DECIMAL(8, 2), nullable=True, comment="计算耗时(秒)")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, 
        default=func.now(), 
        onupdate=func.now(), 
        nullable=False
    )
    
    # 关系映射
    history_records = relationship(
        "StatisticalHistory", 
        back_populates="aggregation",
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        UniqueConstraint(
            'batch_code', 'aggregation_level', 'school_id', 'school_name',
            name='uk_batch_level_school_name'
        ),
        Index('idx_batch_code', 'batch_code'),
        Index('idx_aggregation_level', 'aggregation_level'),
        Index('idx_school_id', 'school_id'),
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