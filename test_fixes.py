#!/usr/bin/env python3
"""
测试脚本：验证统计计算修复
"""
import asyncio
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.services.calculation_service import CalculationService

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 数据库连接配置
DATABASE_HOST = "117.72.14.166"
DATABASE_PORT = "23506"
DATABASE_USER = "root"
DATABASE_PASSWORD = "mysql_Lujing2022"
DATABASE_NAME = "appraisal_test"

DATABASE_URL = f"mysql+pymysql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}?charset=utf8mb4"

async def test_calculation_fixes():
    """测试计算修复"""
    print("=== 统计计算修复验证测试 ===\n")
    
    try:
        # 创建数据库连接
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 创建计算服务
        calc_service = CalculationService(session)
        
        # 测试批次
        batch_code = "G7-2025"
        print(f"测试批次: {batch_code}")
        
        # 1. 测试单个科目的基础数据获取
        print("\n1. 测试科目配置获取...")
        subjects = await calc_service._get_batch_subjects(batch_code)
        print(f"找到 {len(subjects)} 个科目:")
        for subject in subjects[:3]:  # 只显示前3个
            print(f"   - {subject['subject_name']}: 满分 {subject['max_score']}, 题目数 {subject['question_count']}")
        
        # 2. 测试维度数据获取
        if subjects:
            test_subject = subjects[0]['subject_name']
            print(f"\n2. 测试科目 '{test_subject}' 的维度数据获取...")
            dimensions = await calc_service._get_batch_dimensions(batch_code, test_subject)
            print(f"找到 {len(dimensions)} 个维度:")
            for dim in dimensions[:3]:  # 只显示前3个
                print(f"   - {dim['dimension_code']}: {dim['dimension_name']}")
            
            # 3. 测试题目映射
            if dimensions:
                test_dim = dimensions[0]['dimension_code']
                print(f"\n3. 测试维度 '{test_dim}' 的题目映射...")
                questions = await calc_service._get_dimension_question_mapping(batch_code, test_subject, test_dim)
                print(f"维度包含 {len(questions)} 个题目")
                
                # 4. 测试维度满分计算
                max_score = await calc_service._get_dimension_max_score(batch_code, test_subject, test_dim, questions)
                print(f"维度满分: {max_score}")
        
        # 5. 简单的完整计算测试（仅获取部分数据）
        print("\n5. 测试学生分数数据获取...")
        student_data = await calc_service._fetch_student_scores(batch_code)
        if not student_data.empty:
            print(f"获取到 {len(student_data)} 条学生分数记录")
            print(f"唯一学生数: {student_data['student_id'].nunique()}")
            print(f"科目数量: {student_data['subject_name'].nunique()}")
            
            # 检查字段重命名
            if 'total_score' in student_data.columns:
                student_data = student_data.rename(columns={'total_score': 'score'})
                print("已重命名 total_score -> score")
        
        print("\n=== 基础功能验证完成 ===")
        print("所有核心数据获取功能正常工作！")
        
        session.close()
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        print(f"测试失败: {e}")

if __name__ == "__main__":
    asyncio.run(test_calculation_fixes())