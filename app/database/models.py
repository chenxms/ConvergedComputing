# SQLAlchemy模型定义
from sqlalchemy import Column, Integer, String, DateTime, Text, Float
from .connection import Base

class Batch(Base):
    """批次模型"""
    __tablename__ = "batches"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    created_at = Column(DateTime)
    status = Column(String(50))

class Task(Base):
    """任务模型"""
    __tablename__ = "tasks"
    
    id = Column(Integer, primary_key=True, index=True)
    batch_id = Column(Integer)
    status = Column(String(50))
    progress = Column(Float)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    error_message = Column(Text)