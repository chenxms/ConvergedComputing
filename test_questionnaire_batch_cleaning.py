#!/usr/bin/env python3
"""
测试单个批次的问卷清洗功能
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from data_cleaning_service import DataCleaningService

async def test_batch_questionnaire_cleaning(batch_code: str):
    """测试指定批次的问卷清洗"""
    print(f"=== 测试批次 {batch_code} 问卷清洗 ===")
    print(f"测试时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 创建清洗服务
        cleaning_service = DataCleaningService(session)
        
        # 1. 获取批次科目配置
        print("1. 获取批次科目配置...")
        subjects_config = await cleaning_service._get_batch_subjects(batch_code)
        
        questionnaire_subjects = [s for s in subjects_config if s.get('is_questionnaire', False)]
        
        if not questionnaire_subjects:
            print(f"  批次 {batch_code} 没有问卷科目")
            return
        
        print(f"  找到 {len(questionnaire_subjects)} 个问卷科目:")
        for subject in questionnaire_subjects:
            print(f"    - {subject['subject_name']} (量表: {subject['instrument_id']})")
        
        # 2. 检查问卷原始数据
        for subject_config in questionnaire_subjects:
            subject_name = subject_config['subject_name']
            instrument_id = subject_config['instrument_id']
            
            print(f"\n2. 检查问卷科目 {subject_name} 的原始数据...")
            
            # 检查原始数据量
            raw_query = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT student_id) as unique_students
                FROM student_score_detail
                WHERE batch_code = :batch_code 
                AND subject_name = :subject_name
            """)
            
            raw_result = session.execute(raw_query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            raw_row = raw_result.fetchone()
            
            # 检查问卷题目数量
            question_query = text("""
                SELECT COUNT(*) as question_count
                FROM subject_question_config
                WHERE batch_code = :batch_code 
                AND subject_name = :subject_name
                AND question_type_enum = 'questionnaire'
            """)
            
            question_result = session.execute(question_query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            question_count = question_result.fetchone()[0]
            
            if raw_row and raw_row[0] > 0:
                print(f"  原始数据: {raw_row[0]:,} 条记录")
                print(f"  涉及学生: {raw_row[1]:,} 人")
                print(f"  问卷题目: {question_count} 道")
                
                # 3. 执行问卷清洗
                print(f"\n3. 执行问卷科目 {subject_name} 清洗...")
                result = await cleaning_service._clean_questionnaire_scores(
                    batch_code, subject_name, instrument_id, question_count
                )
                
                print(f"清洗结果:")
                print(f"  原始记录: {result['raw_records']:,} 条")
                print(f"  清洗记录: {result['cleaned_records']:,} 条")
                print(f"  异常记录: {result['anomalous_records']:,} 条")
                print(f"  涉及学生: {result['unique_students']:,} 人")
                
                # 4. 验证清洗结果
                print(f"\n4. 验证问卷清洗结果...")
                
                # 检查问卷详细数据表
                detail_query = text("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT student_id) as unique_students,
                        COUNT(DISTINCT question_id) as unique_questions,
                        COUNT(DISTINCT option_label) as unique_options
                    FROM questionnaire_question_scores
                    WHERE batch_code = :batch_code AND subject_name = :subject_name
                """)
                
                detail_result = session.execute(detail_query, {
                    'batch_code': batch_code,
                    'subject_name': subject_name
                })
                detail_row = detail_result.fetchone()
                
                if detail_row:
                    print(f"  问卷详细表记录: {detail_row[0]:,} 条")
                    print(f"  涉及学生: {detail_row[1]:,} 人") 
                    print(f"  涉及题目: {detail_row[2]} 道")
                    print(f"  选项标签种类: {detail_row[3]} 种")
                
                # 检查选项分布
                option_query = text("""
                    SELECT 
                        option_label,
                        option_level,
                        COUNT(*) as count,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                    FROM questionnaire_question_scores
                    WHERE batch_code = :batch_code AND subject_name = :subject_name
                    GROUP BY option_label, option_level
                    ORDER BY option_level
                """)
                
                option_result = session.execute(option_query, {
                    'batch_code': batch_code,
                    'subject_name': subject_name
                })
                
                print(f"\n  选项分布:")
                for row in option_result.fetchall():
                    print(f"    {row[0]} (等级{row[1]}): {row[2]:,} 条 ({row[3]}%)")
                
                # 检查汇总表
                summary_query = text("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT student_id) as unique_students,
                        AVG(total_score) as avg_score,
                        MIN(total_score) as min_score,
                        MAX(total_score) as max_score
                    FROM student_cleaned_scores
                    WHERE batch_code = :batch_code 
                    AND subject_name = :subject_name 
                    AND subject_type = 'questionnaire'
                """)
                
                summary_result = session.execute(summary_query, {
                    'batch_code': batch_code,
                    'subject_name': subject_name
                })
                summary_row = summary_result.fetchone()
                
                if summary_row and summary_row[0] > 0:
                    print(f"\n  汇总数据表:")
                    print(f"    记录数: {summary_row[0]:,} 条")
                    print(f"    学生数: {summary_row[1]:,} 人")
                    print(f"    平均分: {summary_row[2]:.2f}")
                    print(f"    分数范围: {summary_row[3]:.2f} ~ {summary_row[4]:.2f}")
                
            else:
                print(f"  问卷科目 {subject_name} 没有原始数据")
        
        print(f"\n{'='*60}")
        print(f"[SUCCESS] 批次 {batch_code} 问卷清洗测试完成!")
        print(f"测试完成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"[ERROR] 测试过程发生错误: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        session.close()

async def main():
    """主函数"""
    # 测试一个有问卷数据的批次
    test_batch = 'G4-2025'  # 根据前面的测试，这个批次有问卷科目
    await test_batch_questionnaire_cleaning(test_batch)

if __name__ == "__main__":
    asyncio.run(main())