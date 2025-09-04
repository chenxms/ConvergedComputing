#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化统计元数据配置
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import sessionmaker
from app.database.connection import engine
from app.database.models import (
    StatisticalMetadata, MetadataType, SubjectType
)
import json
from datetime import datetime

# 创建会话
SessionLocal = sessionmaker(bind=engine)

def create_initial_metadata():
    """创建初始元数据配置"""
    
    session = SessionLocal()
    
    try:
        # 小学等级分布配置
        primary_grade_config = StatisticalMetadata(
            metadata_type=MetadataType.GRADE_CONFIG,
            metadata_key="grade_thresholds_primary",
            metadata_value={
                "excellent": 0.90,
                "good": 0.80,
                "pass": 0.60,
                "fail": 0.0,
                "description": "小学阶段等级分布阈值"
            },
            grade_level="1-6th_grade",
            is_active=True,
            version="1.0",
            description="小学(1-6年级)等级分布阈值配置",
            created_by="system",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # 初中等级分布配置
        middle_grade_config = StatisticalMetadata(
            metadata_type=MetadataType.GRADE_CONFIG,
            metadata_key="grade_thresholds_middle",
            metadata_value={
                "A": 0.85,
                "B": 0.70,
                "C": 0.60,
                "D": 0.0,
                "description": "初中阶段等级分布阈值"
            },
            grade_level="7-9th_grade",
            is_active=True,
            version="1.0",
            description="初中(7-9年级)等级分布阈值配置",
            created_by="system",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # 百分位数计算规则
        percentile_rule = StatisticalMetadata(
            metadata_type=MetadataType.CALCULATION_RULE,
            metadata_key="percentile_algorithm",
            metadata_value={
                "formula": "floor(student_count × percentile)",
                "percentiles": [0.1, 0.25, 0.5, 0.75, 0.9],
                "sort_order": "desc",
                "description": "百分位数计算标准算法"
            },
            is_active=True,
            version="1.0",
            description="百分位数计算规则配置",
            created_by="system",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # 区分度计算规则
        discrimination_rule = StatisticalMetadata(
            metadata_type=MetadataType.CALCULATION_RULE,
            metadata_key="discrimination_algorithm",
            metadata_value={
                "top_group_ratio": 0.27,
                "bottom_group_ratio": 0.27,
                "formula": "(top_group_avg - bottom_group_avg) / total_score",
                "description": "区分度计算：前27%和后27%分组的教育统计标准"
            },
            is_active=True,
            version="1.0",
            description="区分度计算规则配置",
            created_by="system",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # 问卷量表配置 - 正向量表
        survey_positive_scale = StatisticalMetadata(
            metadata_type=MetadataType.FORMULA_CONFIG,
            metadata_key="survey_scale_positive",
            metadata_value={
                "scale_type": "likert_5",
                "mapping": {
                    "1": 1,
                    "2": 2,
                    "3": 3,
                    "4": 4,
                    "5": 5
                },
                "description": "5级李克特量表正向计分"
            },
            subject_type=SubjectType.SURVEY,
            is_active=True,
            version="1.0",
            description="问卷正向量表配置",
            created_by="system",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # 问卷量表配置 - 反向量表
        survey_negative_scale = StatisticalMetadata(
            metadata_type=MetadataType.FORMULA_CONFIG,
            metadata_key="survey_scale_negative",
            metadata_value={
                "scale_type": "likert_5",
                "mapping": {
                    "1": 5,
                    "2": 4,
                    "3": 3,
                    "4": 2,
                    "5": 1
                },
                "description": "5级李克特量表反向计分"
            },
            subject_type=SubjectType.SURVEY,
            is_active=True,
            version="1.0",
            description="问卷反向量表配置",
            created_by="system",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # 难度系数计算规则
        difficulty_rule = StatisticalMetadata(
            metadata_type=MetadataType.CALCULATION_RULE,
            metadata_key="difficulty_coefficient",
            metadata_value={
                "formula": "average_score / max_score",
                "interpretation": {
                    "easy": {"min": 0.7, "max": 1.0},
                    "medium": {"min": 0.3, "max": 0.7},
                    "difficult": {"min": 0.0, "max": 0.3}
                },
                "description": "难度系数 = 平均分 / 满分"
            },
            is_active=True,
            version="1.0",
            description="难度系数计算规则",
            created_by="system",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # 添加所有配置到会话
        metadata_configs = [
            primary_grade_config,
            middle_grade_config,
            percentile_rule,
            discrimination_rule,
            survey_positive_scale,
            survey_negative_scale,
            difficulty_rule
        ]
        
        for config in metadata_configs:
            session.add(config)
        
        # 提交事务
        session.commit()
        
        print("初始元数据配置创建成功！")
        print(f"共创建 {len(metadata_configs)} 项配置:")
        for config in metadata_configs:
            print(f"  - {config.metadata_type.value}: {config.metadata_key}")
            
    except Exception as e:
        session.rollback()
        print(f"创建元数据配置失败: {str(e)}")
        raise
    
    finally:
        session.close()

def verify_metadata():
    """验证元数据配置"""
    session = SessionLocal()
    
    try:
        configs = session.query(StatisticalMetadata).all()
        print(f"\n数据库中共有 {len(configs)} 项元数据配置:")
        
        for config in configs:
            print(f"  {config.id}: {config.metadata_type.value} - {config.metadata_key}")
            print(f"    版本: {config.version}, 激活: {config.is_active}")
            print(f"    描述: {config.description}")
            print()
            
    except Exception as e:
        print(f"验证元数据配置失败: {str(e)}")
    
    finally:
        session.close()

if __name__ == "__main__":
    print("=== 统计元数据初始化 ===")
    
    # 创建初始配置
    create_initial_metadata()
    
    # 验证配置
    verify_metadata()
    
    print("元数据初始化完成！")