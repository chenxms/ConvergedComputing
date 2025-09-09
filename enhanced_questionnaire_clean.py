#!/usr/bin/env python3
"""
增强版问卷数据清洗脚本 - 包含维度信息
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import json
import time
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from collections import defaultdict

def clean_questionnaire_with_dimensions(batch_code: str):
    """清洗指定批次的问卷数据，包含维度信息"""
    print(f"=== 清洗批次 {batch_code} 问卷数据（含维度） ===")
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
        
        # 2. 获取维度映射数据
        print("\n2. 获取维度映射数据...")
        dimension_mapping_query = text("""
            SELECT 
                qdm.question_id,
                qdm.dimension_code,
                bdd.dimension_name
            FROM question_dimension_mapping qdm
            JOIN batch_dimension_definition bdd ON 
                qdm.batch_code = bdd.batch_code 
                AND qdm.dimension_code = bdd.dimension_code
                AND qdm.subject_name = bdd.subject_name
            WHERE qdm.batch_code = :batch_code
            AND qdm.subject_name = '问卷'
        """)
        
        dimension_result = session.execute(dimension_mapping_query, {'batch_code': batch_code})
        dimension_map = {row[0]: {'code': row[1], 'name': row[2]} for row in dimension_result.fetchall()}
        print(f"找到 {len(dimension_map)} 个题目的维度映射")
        
        # 3. 清理旧数据
        print("\n3. 清理旧的问卷数据...")
        clean_query1 = text("DELETE FROM questionnaire_question_scores WHERE batch_code = :batch_code")
        result1 = session.execute(clean_query1, {'batch_code': batch_code})
        
        clean_query2 = text("DELETE FROM student_cleaned_scores WHERE batch_code = :batch_code AND subject_name = '问卷'")
        result2 = session.execute(clean_query2, {'batch_code': batch_code})
        
        session.commit()
        print(f"清理了 {result1.rowcount} 条问卷详细数据，{result2.rowcount} 条汇总数据")
        
        # 4. 处理每个问卷科目
        total_processed = 0
        total_students = 0
        
        for subject_info in questionnaire_subjects:
            subject_name = subject_info[0]
            instrument_id = subject_info[1]
            
            print(f"\n4. 处理问卷科目: {subject_name}")
            print(f"   量表: {instrument_id}")
            
            start_time = time.time()
            
            # 获取问卷题目配置
            question_query = text("""
                SELECT question_id, question_number, max_score
                FROM subject_question_config
                WHERE batch_code = :batch_code 
                AND subject_name = :subject_name
                AND question_type_enum = 'questionnaire'
                ORDER BY question_number
            """)
            
            question_result = session.execute(question_query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            question_configs = question_result.fetchall()
            
            print(f"   找到 {len(question_configs)} 道题目配置")
            
            if not question_configs:
                print(f"   跳过科目 {subject_name} (无题目配置)")
                continue
            
            # 获取学生数据
            student_query = text("""
                SELECT 
                    student_id, student_name, school_id, school_code, 
                    school_name, class_name, subject_id, subject_scores
                FROM student_score_detail
                WHERE batch_code = :batch_code AND subject_name = :subject_name
            """)
            
            student_result = session.execute(student_query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            student_data = student_result.fetchall()
            
            print(f"   找到 {len(student_data)} 条学生记录")
            
            if not student_data:
                print(f"   跳过科目 {subject_name} (无学生数据)")
                continue
            
            # 处理每个学生的数据
            student_count = 0
            skipped_students = 0
            
            for student_row in student_data:
                try:
                    student_id = student_row[0]
                    student_name = student_row[1]
                    school_id = student_row[2]
                    school_code = student_row[3]
                    school_name = student_row[4]
                    class_name = student_row[5]
                    subject_id = student_row[6]
                    subject_scores_json = student_row[7]
                    
                    # 验证student_id
                    if not student_id:
                        skipped_students += 1
                        continue
                    
                    # 解析学生分数
                    try:
                        if subject_scores_json:
                            subject_scores = json.loads(subject_scores_json)
                        else:
                            subject_scores = {}
                    except Exception as e:
                        print(f"      学生 {student_id} JSON解析失败: {e}")
                        skipped_students += 1
                        continue
                    
                    questionnaire_records = []
                    student_total_score = 0
                    student_question_count = 0
                    
                    # 维度分数统计
                    dimension_scores = defaultdict(float)
                    dimension_counts = defaultdict(int)
                    dimension_max_scores = defaultdict(float)
                    
                    # 处理每道题目
                    for question_config in question_configs:
                        question_id = str(question_config[0])  # 确保是字符串
                        question_number = question_config[1]
                        max_score = float(question_config[2]) if question_config[2] else 4.0
                        
                        # 使用字符串键查找分数
                        question_score = subject_scores.get(question_id)
                        if question_score is None:
                            # 尝试数字键
                            if question_id.isdigit():
                                question_score = subject_scores.get(int(question_id))
                        
                        if question_score is None:
                            continue
                        
                        try:
                            score = float(question_score)
                            student_total_score += score
                            student_question_count += 1
                        except:
                            continue
                        
                        # 获取维度信息
                        dimension_info = dimension_map.get(question_id, {})
                        dimension_code = dimension_info.get('code')
                        dimension_name = dimension_info.get('name')
                        
                        # 统计维度分数
                        if dimension_code:
                            dimension_scores[dimension_code] += score
                            dimension_counts[dimension_code] += 1
                            dimension_max_scores[dimension_code] += max_score
                        
                        # 根据量表类型映射选项
                        option_level = int(score) if score > 0 else 1
                        option_label = get_option_label(instrument_id, option_level)
                        
                        questionnaire_records.append({
                            'student_id': str(student_id),
                            'subject_name': subject_name,
                            'batch_code': batch_code,
                            'dimension_code': dimension_code,
                            'dimension_name': dimension_name,
                            'question_id': question_id,
                            'question_name': f"题目{question_number}",
                            'original_score': score,
                            'scale_level': get_scale_level(instrument_id),
                            'instrument_type': instrument_id,
                            'is_reverse': 'NEGATIVE' in instrument_id,
                            'option_label': option_label,
                            'option_level': option_level,
                            'max_score': max_score
                        })
                    
                    # 如果有数据，插入数据库
                    if questionnaire_records:
                        # 插入问卷详细记录
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
                        
                        # 计算维度平均分
                        dimension_avg_scores = {}
                        for dim_code, dim_score in dimension_scores.items():
                            if dimension_counts[dim_code] > 0:
                                dimension_avg_scores[dim_code] = dim_score / dimension_counts[dim_code]
                        
                        # 计算总平均分
                        avg_score = student_total_score / student_question_count if student_question_count > 0 else 0
                        
                        # 准备汇总记录
                        summary_record = {
                            'student_id': str(student_id),
                            'student_name': student_name,
                            'school_id': int(school_id) if school_id and str(school_id).isdigit() else 0,
                            'school_code': school_code,
                            'school_name': school_name,
                            'class_name': class_name,
                            'subject_id': 0,  # 问卷科目通常没有subject_id
                            'subject_name': subject_name,
                            'batch_code': batch_code,
                            'total_score': avg_score,
                            'max_score': 4.0,  # 标准化为4分制
                            'dimension_scores': json.dumps(dimension_avg_scores, ensure_ascii=False),
                            'dimension_max_scores': json.dumps({k: v/dimension_counts[k] if dimension_counts[k] > 0 else 0 
                                                               for k, v in dimension_max_scores.items()}, ensure_ascii=False),
                            'subject_type': 'questionnaire'
                        }
                        
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
                        
                        session.execute(insert_summary_query, [summary_record])
                        
                        # 每个学生提交一次
                        session.commit()
                        
                        student_count += 1
                        total_processed += len(questionnaire_records)
                        
                        if student_count % 100 == 0:
                            print(f"      已处理 {student_count} 个学生")
                    
                except Exception as e:
                    print(f"      学生 {student_id} 处理失败: {e}")
                    session.rollback()
                    skipped_students += 1
                    continue
            
            end_time = time.time()
            duration = end_time - start_time
            
            total_students += student_count
            print(f"   科目 {subject_name} 处理完成:")
            print(f"      成功: {student_count} 个学生")
            print(f"      跳过: {skipped_students} 个学生")
            print(f"      耗时: {duration:.2f}秒")
        
        # 5. 验证维度数据
        print("\n5. 验证维度数据...")
        verify_query = text("""
            SELECT 
                COUNT(DISTINCT dimension_code) as dimension_count,
                COUNT(CASE WHEN dimension_code IS NOT NULL THEN 1 END) as with_dimension,
                COUNT(*) as total_records
            FROM questionnaire_question_scores
            WHERE batch_code = :batch_code
        """)
        
        verify_result = session.execute(verify_query, {'batch_code': batch_code})
        verify_row = verify_result.fetchone()
        
        if verify_row:
            dimension_count = verify_row[0]
            with_dimension = verify_row[1]
            total_records = verify_row[2]
            
            print(f"   维度数量: {dimension_count}")
            print(f"   有维度的记录: {with_dimension:,}")
            print(f"   总记录数: {total_records:,}")
            
            if with_dimension > 0:
                coverage = (with_dimension / total_records) * 100
                print(f"   维度覆盖率: {coverage:.1f}%")
        
        print(f"\n{'='*60}")
        print(f"[SUCCESS] 批次 {batch_code} 问卷清洗完成（含维度）！")
        print(f"总处理学生: {total_students} 人")
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

def get_scale_level(instrument_id: str) -> int:
    """获取量表等级"""
    if 'LIKERT_4' in instrument_id:
        return 4
    elif 'LIKERT_5' in instrument_id:
        return 5
    elif 'LIKERT_7' in instrument_id:
        return 7
    elif '10_POINT' in instrument_id:
        return 10
    else:
        return 4

def get_option_label(instrument_id: str, option_level: int) -> str:
    """根据量表类型和等级获取选项标签"""
    if 'LIKERT_4' in instrument_id:
        labels = {1: '非常不同意', 2: '不同意', 3: '同意', 4: '非常同意'}
        return labels.get(option_level, f'选项{option_level}')
    elif 'LIKERT_5' in instrument_id:
        labels = {1: '非常不同意', 2: '不同意', 3: '中立', 4: '同意', 5: '非常同意'}
        return labels.get(option_level, f'选项{option_level}')
    elif 'LIKERT_7' in instrument_id:
        labels = {1: '强烈不同意', 2: '不同意', 3: '稍微不同意', 4: '中立', 
                 5: '稍微同意', 6: '同意', 7: '强烈同意'}
        return labels.get(option_level, f'选项{option_level}')
    elif 'SATISFACTION_10_POINT' in instrument_id:
        return f'{option_level}分'
    else:
        return f'选项{option_level}'

if __name__ == "__main__":
    # 测试G4-2025批次
    clean_questionnaire_with_dimensions('G4-2025')