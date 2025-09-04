#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试数据库模型的基本CRUD操作
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import sessionmaker
from app.database.connection import engine
from app.database.models import (
    StatisticalAggregation, StatisticalMetadata, StatisticalHistory,
    AggregationLevel, CalculationStatus, MetadataType, ChangeType
)
from datetime import datetime

def test_statistical_aggregation():
    """测试统计汇聚表操作"""
    print("=== 测试 StatisticalAggregation 模型 ===")
    
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        # 创建测试数据
        test_data = StatisticalAggregation(
            batch_code="TEST_BATCH_001",
            aggregation_level=AggregationLevel.REGIONAL,
            school_id=None,
            school_name=None,
            statistics_data={
                "batch_info": {
                    "total_students": 1000,
                    "total_schools": 50
                },
                "academic_subjects": {
                    "语文": {
                        "average_score": 78.5,
                        "difficulty_coefficient": 0.785,
                        "discrimination": 0.35
                    }
                }
            },
            data_version="1.0",
            calculation_status=CalculationStatus.COMPLETED,
            total_students=1000,
            total_schools=50,
            calculation_duration=120.50,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # 插入数据
        session.add(test_data)
        session.commit()
        print("[OK] Regional aggregation data created successfully")
        
        # 创建学校级数据
        school_data = StatisticalAggregation(
            batch_code="TEST_BATCH_001",
            aggregation_level=AggregationLevel.SCHOOL,
            school_id="SCHOOL_001",
            school_name="Test School",
            statistics_data={
                "school_info": {
                    "total_students": 200,
                    "grade_distribution": {"1st_grade": 50, "2nd_grade": 50}
                },
                "academic_subjects": {
                    "语文": {
                        "average_score": 82.3,
                        "difficulty_coefficient": 0.823,
                        "discrimination": 0.42
                    }
                }
            },
            data_version="1.0",
            calculation_status=CalculationStatus.COMPLETED,
            total_students=200,
            total_schools=0,
            calculation_duration=25.30,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        session.add(school_data)
        session.commit()
        print("[OK] School aggregation data created successfully")
        
        # 查询测试
        regional_data = session.query(StatisticalAggregation).filter(
            StatisticalAggregation.batch_code == "TEST_BATCH_001",
            StatisticalAggregation.aggregation_level == AggregationLevel.REGIONAL
        ).first()
        
        if regional_data:
            print(f"[OK] Regional data found: batch={regional_data.batch_code}, students={regional_data.total_students}")
        
        school_data_query = session.query(StatisticalAggregation).filter(
            StatisticalAggregation.batch_code == "TEST_BATCH_001",
            StatisticalAggregation.aggregation_level == AggregationLevel.SCHOOL
        ).all()
        
        print(f"[OK] Found {len(school_data_query)} school records")
        
    except Exception as e:
        print(f"[ERROR] Error testing StatisticalAggregation: {str(e)}")
        session.rollback()
    
    finally:
        session.close()

def test_statistical_history():
    """测试历史记录表操作"""
    print("\n=== 测试 StatisticalHistory 模型 ===")
    
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        # 获取已有的统计记录
        aggregation = session.query(StatisticalAggregation).filter(
            StatisticalAggregation.batch_code == "TEST_BATCH_001"
        ).first()
        
        if aggregation:
            # 创建历史记录
            history_record = StatisticalHistory(
                aggregation_id=aggregation.id,
                change_type=ChangeType.CREATED,
                previous_data=None,
                current_data=aggregation.statistics_data,
                change_summary={
                    "action": "created",
                    "description": "Initial creation of statistical data"
                },
                change_reason="System initialization",
                triggered_by="test_script",
                batch_code=aggregation.batch_code,
                created_at=datetime.now()
            )
            
            session.add(history_record)
            session.commit()
            print("[OK] History record created successfully")
            
            # 查询历史记录
            history_count = session.query(StatisticalHistory).filter(
                StatisticalHistory.batch_code == "TEST_BATCH_001"
            ).count()
            
            print(f"[OK] Found {history_count} history records for batch")
            
        else:
            print("[ERROR] No aggregation data found for history test")
    
    except Exception as e:
        print(f"[ERROR] Error testing StatisticalHistory: {str(e)}")
        session.rollback()
    
    finally:
        session.close()

def test_metadata_queries():
    """测试元数据查询操作"""
    print("\n=== 测试 StatisticalMetadata 查询 ===")
    
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        # 查询年级配置
        grade_configs = session.query(StatisticalMetadata).filter(
            StatisticalMetadata.metadata_type == MetadataType.GRADE_CONFIG,
            StatisticalMetadata.is_active == True
        ).all()
        
        print(f"[OK] Found {len(grade_configs)} active grade configurations")
        
        for config in grade_configs:
            print(f"  - {config.metadata_key}: version {config.version}")
        
        # 查询计算规则
        calc_rules = session.query(StatisticalMetadata).filter(
            StatisticalMetadata.metadata_type == MetadataType.CALCULATION_RULE
        ).all()
        
        print(f"[OK] Found {len(calc_rules)} calculation rules")
        
        for rule in calc_rules:
            print(f"  - {rule.metadata_key}")
    
    except Exception as e:
        print(f"[ERROR] Error testing metadata queries: {str(e)}")
    
    finally:
        session.close()

def cleanup_test_data():
    """清理测试数据"""
    print("\n=== 清理测试数据 ===")
    
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        # 删除测试的历史记录
        history_count = session.query(StatisticalHistory).filter(
            StatisticalHistory.batch_code == "TEST_BATCH_001"
        ).delete()
        
        # 删除测试的统计数据
        aggregation_count = session.query(StatisticalAggregation).filter(
            StatisticalAggregation.batch_code == "TEST_BATCH_001"
        ).delete()
        
        session.commit()
        print(f"[OK] Cleaned up {history_count} history records and {aggregation_count} aggregation records")
    
    except Exception as e:
        print(f"[ERROR] Error during cleanup: {str(e)}")
        session.rollback()
    
    finally:
        session.close()

def main():
    """运行所有测试"""
    print("Starting database model tests...\n")
    
    # 运行测试
    test_statistical_aggregation()
    test_statistical_history()
    test_metadata_queries()
    
    # 清理测试数据
    cleanup_test_data()
    
    print("\n" + "=" * 50)
    print("All model tests completed!")

if __name__ == "__main__":
    main()