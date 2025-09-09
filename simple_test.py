#!/usr/bin/env python3
"""
简单测试脚本
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from data_cleaning_service import DataCleaningService

async def test_single_batch():
    """测试单个批次清洗"""
    print("=== 简单测试 ===")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    print("数据库连接成功")
    
    # 创建清洗服务
    cleaning_service = DataCleaningService(session)
    
    # 测试获取科目配置
    print("\n测试获取G7-2025科目配置...")
    subjects = await cleaning_service._get_batch_subjects('G7-2025')
    print(f"找到 {len(subjects)} 个科目:")
    for subject in subjects:
        print(f"  - {subject['subject_name']}: 满分 {subject['max_score']}, {subject['question_count']} 题")
    
    session.close()
    print("\n测试完成")

if __name__ == "__main__":
    asyncio.run(test_single_batch())