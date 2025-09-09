#!/usr/bin/env python3
"""
测试问卷数据清洗功能
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from data_cleaning_service import DataCleaningService

async def test_questionnaire_cleaning():
    """测试问卷数据清洗功能"""
    print("=== 问卷数据清洗功能测试 ===")
    print(f"测试时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 1. 检查是否有问卷类型的科目
        print("1. 检查现有批次中的问卷类型科目...")
        questionnaire_query = text("""
            SELECT 
                batch_code,
                subject_name,
                question_type_enum,
                instrument_id,
                COUNT(*) as question_count
            FROM subject_question_config 
            WHERE question_type_enum = 'questionnaire'
            GROUP BY batch_code, subject_name, question_type_enum, instrument_id
            ORDER BY batch_code, subject_name
        """)
        
        questionnaire_result = session.execute(questionnaire_query)
        questionnaire_subjects = questionnaire_result.fetchall()
        
        if questionnaire_subjects:
            print(f"找到 {len(questionnaire_subjects)} 个问卷科目:")
            for row in questionnaire_subjects:
                print(f"  批次: {row[0]}, 科目: {row[1]}, 量表: {row[3]}, 题目数: {row[4]}")
        else:
            print("未找到问卷类型科目，创建模拟数据进行测试...")
            await create_test_questionnaire_data(session)
        
        # 2. 测试数据清洗服务的问卷识别功能
        print("\n2. 测试数据清洗服务的问卷识别功能...")
        cleaning_service = DataCleaningService(session)
        
        # 测试获取批次科目配置（包含问卷识别）
        test_batches = ['G4-2025', 'G7-2025', 'G8-2025']
        
        for batch_code in test_batches:
            print(f"\n测试批次: {batch_code}")
            subjects_config = await cleaning_service._get_batch_subjects(batch_code)
            
            exam_subjects = [s for s in subjects_config if not s.get('is_questionnaire', False)]
            questionnaire_subjects = [s for s in subjects_config if s.get('is_questionnaire', False)]
            
            print(f"  考试科目: {len(exam_subjects)} 个")
            for subject in exam_subjects:
                print(f"    - {subject['subject_name']} (满分: {subject['max_score']})")
            
            print(f"  问卷科目: {len(questionnaire_subjects)} 个")
            for subject in questionnaire_subjects:
                print(f"    - {subject['subject_name']} (量表: {subject['instrument_id']})")
        
        # 3. 测试量表信息获取
        print("\n3. 测试量表信息获取...")
        test_instruments = ['LIKERT_4_POSITIV', 'LIKERT_5_POSITIV', 'SATISFACTION_7']
        
        for instrument_id in test_instruments:
            scale_info = await cleaning_service._get_scale_info(instrument_id)
            print(f"  量表 {instrument_id}:")
            print(f"    类型: {scale_info.get('instrument_type')}")
            print(f"    级别: {scale_info.get('scale_level')}")
            print(f"    反向: {scale_info.get('is_reverse')}")
        
        # 4. 测试分数到选项的映射
        print("\n4. 测试分数到选项的映射...")
        test_mappings = [
            (4.0, 4.0, 'LIKERT_4_POSITIV'),  # 满分4分，得分4分
            (3.0, 4.0, 'LIKERT_4_POSITIV'),  # 满分4分，得分3分
            (2.0, 4.0, 'LIKERT_4_POSITIV'),  # 满分4分，得分2分
            (5.0, 5.0, 'LIKERT_5_POSITIV'),  # 满分5分，得分5分
            (3.0, 5.0, 'LIKERT_5_POSITIV'),  # 满分5分，得分3分
        ]
        
        for score, max_score, instrument_id in test_mappings:
            scale_info = await cleaning_service._get_scale_info(instrument_id)
            option_info = await cleaning_service._map_score_to_option(score, scale_info, max_score)
            
            if option_info:
                print(f"  {instrument_id}: {score}/{max_score} → 等级{option_info['option_level']} ({option_info['option_label']})")
            else:
                print(f"  {instrument_id}: {score}/{max_score} → 映射失败")
        
        # 5. 检查问卷相关表的状态
        print("\n5. 检查问卷相关表的状态...")
        
        # 检查量表选项表
        options_query = text("""
            SELECT instrument_type, scale_level, COUNT(*) as option_count
            FROM questionnaire_scale_options
            GROUP BY instrument_type, scale_level
            ORDER BY instrument_type, scale_level
        """)
        options_result = session.execute(options_query)
        print("量表选项配置:")
        for row in options_result.fetchall():
            print(f"  {row[0]} ({row[1]}分位): {row[2]}个选项")
        
        # 检查问卷详细分数表
        detail_query = text("SELECT COUNT(*) FROM questionnaire_question_scores")
        detail_result = session.execute(detail_query)
        detail_count = detail_result.fetchone()[0]
        print(f"问卷详细分数记录: {detail_count:,} 条")
        
        # 检查清洗数据表中的科目类型分布
        type_query = text("""
            SELECT subject_type, COUNT(*) as count
            FROM student_cleaned_scores
            GROUP BY subject_type
            ORDER BY count DESC
        """)
        type_result = session.execute(type_query)
        print("科目类型分布:")
        for row in type_result.fetchall():
            print(f"  {row[0] or 'NULL'}: {row[1]:,} 条记录")
        
        print(f"\n{'='*60}")
        print("[SUCCESS] 问卷数据清洗功能测试完成!")
        print(f"测试完成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"[ERROR] 测试过程发生错误: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        session.close()

async def create_test_questionnaire_data(session):
    """创建测试用的问卷数据"""
    print("  创建模拟问卷科目配置...")
    
    # 这里可以插入一些测试数据，但由于不确定实际数据结构，暂时跳过
    # 实际使用中，如果没有问卷数据，可以手动在数据库中添加一些测试记录
    print("  (提示: 如需完整测试，请在数据库中添加question_type_enum='questionnaire'的科目配置)")

if __name__ == "__main__":
    asyncio.run(test_questionnaire_cleaning())