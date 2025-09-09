#!/usr/bin/env python3
"""
测试单个批次的问卷清洗功能
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
import time
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from data_cleaning_service import DataCleaningService

async def test_single_questionnaire():
    """测试单个批次的问卷清洗"""
    print("=== 测试G4-2025批次问卷清洗 ===")
    print(f"开始时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 创建清洗服务
        cleaning_service = DataCleaningService(session)
        batch_code = 'G4-2025'
        
        # 1. 获取批次科目配置
        print("1. 获取批次科目配置...")
        subjects_config = await cleaning_service._get_batch_subjects(batch_code)
        
        questionnaire_subjects = [s for s in subjects_config if s.get('is_questionnaire', False)]
        exam_subjects = [s for s in subjects_config if not s.get('is_questionnaire', False)]
        
        print(f"总科目数: {len(subjects_config)}")
        print(f"考试科目: {len(exam_subjects)} 个")
        print(f"问卷科目: {len(questionnaire_subjects)} 个")
        
        if not questionnaire_subjects:
            print("没有问卷科目，结束测试")
            return
        
        # 2. 逐个处理问卷科目
        for i, subject_config in enumerate(questionnaire_subjects, 1):
            subject_name = subject_config['subject_name']
            instrument_id = subject_config['instrument_id']
            question_count = subject_config['question_count']
            
            print(f"\n[{i}/{len(questionnaire_subjects)}] 处理问卷科目: {subject_name}")
            print(f"  量表类型: {instrument_id}")
            print(f"  题目数: {question_count}")
            
            # 检查原始数据量
            print("  检查原始数据...")
            raw_query = text("""
                SELECT COUNT(*) as record_count, COUNT(DISTINCT student_id) as student_count
                FROM student_score_detail
                WHERE batch_code = :batch_code AND subject_name = :subject_name
            """)
            
            raw_result = session.execute(raw_query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            raw_row = raw_result.fetchone()
            
            if raw_row and raw_row[0] > 0:
                print(f"    原始记录: {raw_row[0]:,} 条")
                print(f"    学生数: {raw_row[1]:,} 人")
                
                # 执行清洗
                print("  开始执行问卷清洗...")
                start_time = time.time()
                
                try:
                    result = await cleaning_service._clean_questionnaire_scores(
                        batch_code, subject_name, instrument_id, question_count
                    )
                    
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    print(f"  清洗完成 (耗时: {duration:.2f}秒)")
                    print(f"    原始记录: {result['raw_records']:,} 条")
                    print(f"    清洗记录: {result['cleaned_records']:,} 条")
                    print(f"    学生数: {result['unique_students']:,} 人")
                    
                    if result['cleaned_records'] > 0:
                        print(f"    [SUCCESS] 问卷科目 {subject_name} 清洗成功")
                        
                        # 验证清洗结果
                        print("  验证清洗结果...")
                        
                        # 检查问卷详细表
                        detail_query = text("""
                            SELECT 
                                COUNT(*) as record_count,
                                COUNT(DISTINCT student_id) as student_count,
                                COUNT(DISTINCT question_id) as question_count,
                                COUNT(DISTINCT option_label) as option_count
                            FROM questionnaire_question_scores
                            WHERE batch_code = :batch_code AND subject_name = :subject_name
                        """)
                        
                        detail_result = session.execute(detail_query, {
                            'batch_code': batch_code,
                            'subject_name': subject_name
                        })
                        detail_row = detail_result.fetchone()
                        
                        if detail_row and detail_row[0] > 0:
                            print(f"    问卷详细表: {detail_row[0]:,} 条记录")
                            print(f"    涉及学生: {detail_row[1]:,} 人")
                            print(f"    涉及题目: {detail_row[2]} 道")
                            print(f"    选项种类: {detail_row[3]} 种")
                            
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
                            
                            print(f"    选项分布:")
                            for opt_row in option_result.fetchall():
                                print(f"      {opt_row[0]} (等级{opt_row[1]}): {opt_row[2]:,} 条 ({opt_row[3]}%)")
                        
                        # 检查汇总表
                        summary_query = text("""
                            SELECT COUNT(*), AVG(total_score), MIN(total_score), MAX(total_score)
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
                            print(f"    汇总表记录: {summary_row[0]:,} 条")
                            print(f"    平均分: {summary_row[1]:.2f}")
                            print(f"    分数范围: {summary_row[2]:.2f} ~ {summary_row[3]:.2f}")
                        
                    else:
                        print(f"    [WARNING] 问卷科目 {subject_name} 清洗后无数据")
                        
                except Exception as e:
                    print(f"    [ERROR] 问卷科目 {subject_name} 清洗失败: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"    [INFO] 问卷科目 {subject_name} 无原始数据")
        
        print(f"\n{'='*60}")
        print("[FINAL] G4-2025批次问卷清洗测试完成！")
        print(f"完成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"[ERROR] 测试过程发生错误: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        session.close()

if __name__ == "__main__":
    asyncio.run(test_single_questionnaire())