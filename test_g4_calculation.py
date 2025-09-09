#!/usr/bin/env python3
"""
测试G4-2025批次计算
"""
import asyncio
import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.services.calculation_service import CalculationService

# 设置日志级别
logging.basicConfig(level=logging.INFO)

async def test_g4_calculation():
    """测试G4-2025计算"""
    print("=== 测试G4-2025批次计算 ===\n")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 创建计算服务
        calc_service = CalculationService(session)
        
        batch_code = "G4-2025"
        
        # 测试年级获取
        grade_level = calc_service._get_batch_grade_level(batch_code)
        print(f"批次 {batch_code} 年级: {grade_level}")
        
        # 测试单个科目的数据获取
        subject_data = await calc_service._fetch_student_scores(batch_code)
        print(f"总数据量: {len(subject_data)} 条记录")
        
        # 只测试一个有数据的科目
        test_subject = "艺术"  # 根据之前的JSON，艺术科目有数据
        
        subject_subset = subject_data[subject_data['subject_name'] == test_subject]
        if not subject_subset.empty:
            print(f"\n测试科目 {test_subject}:")
            print(f"   记录数: {len(subject_subset)}")
            print(f"   学生数: {subject_subset['student_id'].nunique()}")
            print(f"   分数范围: {subject_subset['total_score'].min():.1f} - {subject_subset['total_score'].max():.1f}")
            print(f"   平均分: {subject_subset['total_score'].mean():.1f}")
            
            # 手动测试计算引擎
            import pandas as pd
            calculation_df = pd.DataFrame({
                'score': subject_subset['total_score'].fillna(0).astype(float),
                'student_id': subject_subset['student_id'],
                'school_id': subject_subset['school_code']
            })
            
            config = {
                'max_score': 200.0,  # 艺术科目满分
                'grade_level': grade_level,
                'percentiles': [10, 25, 50, 75, 90],
                'required_columns': ['score']
            }
            
            print(f"\n直接测试计算引擎:")
            print(f"   配置: {config}")
            
            try:
                educational_metrics = calc_service.engine.calculate('educational_metrics', calculation_df, config)
                print(f"   教育指标结果: {educational_metrics}")
            except Exception as e:
                print(f"   教育指标计算失败: {e}")
                import traceback
                traceback.print_exc()
        
        session.close()
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_g4_calculation())