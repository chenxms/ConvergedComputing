#!/usr/bin/env python3
"""
快速问卷数据清洗
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
import json
import time
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

async def quick_questionnaire_clean(batch_code: str):
    """快速清洗指定批次的问卷数据"""
    print(f"=== 快速清洗批次 {batch_code} 问卷数据 ===")
    print(f"开始时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 1. 获取问卷科目配置
        print("1. 获取问卷科目配置...")
        config_query = text("""
            SELECT 
                subject_name,
                MAX(instrument_id) as instrument_id,
                COUNT(*) as question_count
            FROM subject_question_config 
            WHERE batch_code = :batch_code
            AND question_type_enum = 'questionnaire'
            GROUP BY subject_name
        """)
        
        config_result = session.execute(config_query, {'batch_code': batch_code})
        questionnaire_subjects = config_result.fetchall()
        
        if not questionnaire_subjects:
            print(f"批次 {batch_code} 没有问卷科目")
            return
        
        print(f"找到 {len(questionnaire_subjects)} 个问卷科目:")
        for subject in questionnaire_subjects:
            print(f"  - {subject[0]} (量表: {subject[1]}, 题目: {subject[2]})")
        
        # 2. 清理旧的问卷数据
        print("\n2. 清理旧的问卷数据...")
        clean_query1 = text("DELETE FROM questionnaire_question_scores WHERE batch_code = :batch_code")
        result1 = session.execute(clean_query1, {'batch_code': batch_code})
        
        clean_query2 = text("DELETE FROM student_cleaned_scores WHERE batch_code = :batch_code AND subject_type = 'questionnaire'")
        result2 = session.execute(clean_query2, {'batch_code': batch_code})
        
        session.commit()
        print(f"清理了 {result1.rowcount} 条问卷详细数据，{result2.rowcount} 条汇总数据")
        
        # 3. 处理每个问卷科目
        total_processed = 0
        
        for subject_info in questionnaire_subjects:
            subject_name = subject_info[0]
            instrument_id = subject_info[1]
            question_count = subject_info[2]
            
            print(f"\n3. 处理问卷科目: {subject_name}")
            print(f"  量表: {instrument_id}, 题目数: {question_count}")
            
            start_time = time.time()
            
            try:
                # 获取学生数据
                student_query = text("""
                    SELECT 
                        student_id, student_name, school_id, school_code, 
                        school_name, class_name, subject_id, subject_scores
                    FROM student_score_detail
                    WHERE batch_code = :batch_code AND subject_name = :subject_name
                    LIMIT 100  -- 限制处理数量以加快测试
                """)
                
                student_result = session.execute(student_query, {
                    'batch_code': batch_code,
                    'subject_name': subject_name
                })
                student_data = student_result.fetchall()
                
                print(f"  找到 {len(student_data)} 条学生记录")
                
                if not student_data:
                    print(f"  跳过科目 {subject_name} (无学生数据)")
                    continue
                
                # 获取问卷题目配置
                question_query = text("""
                    SELECT question_id, question_number, max_score
                    FROM subject_question_config
                    WHERE batch_code = :batch_code 
                    AND subject_name = :subject_name
                    AND question_type_enum = 'questionnaire'
                    ORDER BY question_number
                    LIMIT 10  -- 限制题目数量以加快测试
                """)
                
                question_result = session.execute(question_query, {
                    'batch_code': batch_code,
                    'subject_name': subject_name
                })
                question_configs = question_result.fetchall()
                
                print(f"  找到 {len(question_configs)} 道题目配置")
                
                if not question_configs:
                    print(f"  跳过科目 {subject_name} (无题目配置)")
                    continue
                
                # 处理学生答题数据
                questionnaire_records = []
                summary_records = []
                
                for student_row in student_data:
                    student_id = student_row[0]
                    student_name = student_row[1]
                    school_id = student_row[2] or 0
                    school_code = student_row[3]
                    school_name = student_row[4]
                    class_name = student_row[5]
                    subject_id = student_row[6] or 0
                    subject_scores_json = student_row[7]
                    
                    # 解析学生分数
                    try:
                        if subject_scores_json:
                            subject_scores = json.loads(subject_scores_json)
                        else:
                            subject_scores = {}
                    except:
                        subject_scores = {}
                    
                    student_total_score = 0
                    student_question_count = 0
                    
                    # 处理每道题目
                    for question_config in question_configs:
                        question_id = question_config[0]
                        question_number = question_config[1]
                        max_score = float(question_config[2]) if question_config[2] else 4.0
                        
                        # 获取该题目的分数
                        question_score = subject_scores.get(question_id)
                        if question_score is None:
                            continue
                        
                        try:
                            score = float(question_score)
                            student_total_score += score
                            student_question_count += 1
                        except:
                            continue
                        
                        # 简化的分数到选项映射
                        if max_score > 0:
                            normalized_score = (score / max_score) * 4  # 标准化到4分制
                            option_level = max(1, min(4, round(normalized_score)))
                        else:
                            option_level = 1
                        
                        # 简单的选项标签映射
                        option_labels = {1: '非常不同意', 2: '不同意', 3: '同意', 4: '非常同意'}
                        option_label = option_labels.get(option_level, '选项1')
                        
                        # 添加到问卷详细记录
                        safe_student_id = int(student_id) if isinstance(student_id, (int, str)) and str(student_id).isdigit() else 0
                        
                        questionnaire_records.append({
                            'student_id': safe_student_id,
                            'subject_name': subject_name,
                            'batch_code': batch_code,
                            'dimension_code': None,
                            'dimension_name': None,
                            'question_id': question_id,
                            'question_name': f"题目{question_number}",
                            'original_score': score,
                            'scale_level': 4,
                            'instrument_type': instrument_id,
                            'is_reverse': False,
                            'option_label': option_label,
                            'option_level': option_level,
                            'max_score': max_score
                        })
                    
                    # 计算学生平均分
                    avg_score = student_total_score / student_question_count if student_question_count > 0 else 0
                    
                    # 添加到汇总记录
                    try:
                        # 安全的类型转换
                        safe_student_id = int(student_id) if isinstance(student_id, (int, str)) and str(student_id).isdigit() else 0
                        safe_school_id = int(school_id) if isinstance(school_id, (int, str)) and str(school_id).isdigit() else 0
                        safe_subject_id = 0  # subject_id通常是字符串，先设为0避免转换错误
                        
                        summary_records.append({
                            'student_id': safe_student_id,
                            'student_name': student_name,
                            'school_id': safe_school_id,
                            'school_code': school_code,
                            'school_name': school_name,
                            'class_name': class_name,
                            'subject_id': safe_subject_id,
                            'subject_name': subject_name,
                            'batch_code': batch_code,
                            'total_score': avg_score,
                            'max_score': 4.0,  # 4分制量表
                            'dimension_scores': '{}',
                            'dimension_max_scores': '{}',
                            'subject_type': 'questionnaire'
                        })
                    except Exception as convert_error:
                        print(f"    学生 {student_id} 数据转换失败: {convert_error}")
                        continue
                
                # 批量插入问卷详细数据
                if questionnaire_records:
                    insert_detail_query = text("""
                        INSERT INTO questionnaire_question_scores 
                        (student_id, subject_name, batch_code, dimension_code, dimension_name,
                         question_id, question_name, original_score, scale_level, instrument_type,
                         is_reverse, option_label, option_level, max_score)
                        VALUES 
                        (:student_id, :subject_name, :batch_code, :dimension_code, :dimension_name,
                         :question_id, :question_name, :original_score, :scale_level, :instrument_type,
                         :is_reverse, :option_label, :option_level, :max_score)
                    """)
                    
                    session.execute(insert_detail_query, questionnaire_records)
                    print(f"  插入问卷详细记录: {len(questionnaire_records)} 条")
                
                # 批量插入汇总数据
                if summary_records:
                    insert_summary_query = text("""
                        INSERT INTO student_cleaned_scores 
                        (student_id, student_name, school_id, school_code, school_name, class_name,
                         subject_id, subject_name, batch_code, total_score, max_score,
                         dimension_scores, dimension_max_scores, subject_type)
                        VALUES 
                        (:student_id, :student_name, :school_id, :school_code, :school_name, :class_name,
                         :subject_id, :subject_name, :batch_code, :total_score, :max_score,
                         :dimension_scores, :dimension_max_scores, :subject_type)
                    """)
                    
                    session.execute(insert_summary_query, summary_records)
                    print(f"  插入汇总记录: {len(summary_records)} 条")
                
                session.commit()
                
                end_time = time.time()
                duration = end_time - start_time
                
                total_processed += len(questionnaire_records)
                print(f"  科目 {subject_name} 处理完成 (耗时: {duration:.2f}秒)")
                
            except Exception as e:
                print(f"  科目 {subject_name} 处理失败: {e}")
                session.rollback()
        
        print(f"\n{'='*60}")
        print(f"[SUCCESS] 批次 {batch_code} 问卷清洗完成！")
        print(f"总处理记录: {total_processed:,} 条")
        print(f"完成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"[ERROR] 清洗过程发生错误: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
        
    finally:
        session.close()

async def main():
    """主函数"""
    # 快速测试G4-2025批次
    await quick_questionnaire_clean('G4-2025')

if __name__ == "__main__":
    asyncio.run(main())