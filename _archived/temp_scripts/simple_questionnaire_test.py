#!/usr/bin/env python3
"""
简单的问卷功能测试
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from data_cleaning_service import DataCleaningService

async def simple_questionnaire_test():
    """简单测试问卷功能"""
    print("=== 简单问卷功能测试 ===")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 创建清洗服务
        cleaning_service = DataCleaningService(session)
        
        # 1. 测试量表信息获取
        print("1. 测试量表信息获取...")
        scale_info = await cleaning_service._get_scale_info('LIKERT_4_POSITIV')
        print(f"  LIKERT_4_POSITIV: {scale_info}")
        
        # 2. 测试分数映射
        print("\n2. 测试分数映射...")
        option_info = await cleaning_service._map_score_to_option(4.0, scale_info, 4.0)
        print(f"  4分/4分 → {option_info}")
        
        option_info = await cleaning_service._map_score_to_option(2.0, scale_info, 4.0)
        print(f"  2分/4分 → {option_info}")
        
        # 3. 检查G4-2025心理科目的具体数据
        print("\n3. 检查G4-2025心理科目数据...")
        
        # 检查学生数据样本
        sample_query = text("""
            SELECT student_id, subject_scores
            FROM student_score_detail
            WHERE batch_code = 'G4-2025' 
            AND subject_name = '心理'
            LIMIT 3
        """)
        
        sample_result = session.execute(sample_query)
        sample_data = sample_result.fetchall()
        
        print(f"  找到 {len(sample_data)} 条样本数据:")
        for row in sample_data:
            student_id = row[0]
            subject_scores_json = row[1]
            
            print(f"    学生 {student_id}:")
            try:
                if subject_scores_json:
                    subject_scores = json.loads(subject_scores_json)
                    print(f"      题目数: {len(subject_scores)}")
                    print(f"      前5个题目分数: {dict(list(subject_scores.items())[:5])}")
                else:
                    print(f"      无分数数据")
            except (json.JSONDecodeError, TypeError):
                print(f"      分数数据解析失败")
        
        # 4. 检查问卷题目配置
        print("\n4. 检查问卷题目配置...")
        question_query = text("""
            SELECT question_id, question_number, max_score, instrument_id
            FROM subject_question_config
            WHERE batch_code = 'G4-2025' 
            AND subject_name = '心理'
            AND question_type_enum = 'questionnaire'
            ORDER BY question_number
            LIMIT 5
        """)
        
        question_result = session.execute(question_query)
        question_data = question_result.fetchall()
        
        print(f"  找到 {len(question_data)} 道题目配置:")
        for row in question_data:
            print(f"    题目ID: {row[0]}, 题号: {row[1]}, 满分: {row[2]}, 量表: {row[3]}")
        
        print("\n[SUCCESS] 简单测试完成!")
        
    except Exception as e:
        print(f"[ERROR] 测试失败: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        session.close()

if __name__ == "__main__":
    asyncio.run(simple_questionnaire_test())