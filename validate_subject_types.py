#!/usr/bin/env python3
"""
验证三种学科类型的数据源完整性
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def validate_subject_data_sources():
    """验证三种学科类型的数据源"""
    print("=== 验证多学科类型数据源 ===\n")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        print("1. 检查student_cleaned_scores表中的学科类型...")
        subject_type_query = text("""
            SELECT 
                batch_code,
                subject_type,
                subject_name,
                COUNT(*) as student_count,
                AVG(total_score) as avg_score,
                MAX(max_score) as max_score
            FROM student_cleaned_scores
            GROUP BY batch_code, subject_type, subject_name
            ORDER BY batch_code, subject_type, subject_name
        """)
        
        subject_results = session.execute(subject_type_query).fetchall()
        
        exam_subjects = []
        questionnaire_subjects = []
        
        for row in subject_results:
            batch_code = row[0]
            subject_type = row[1] 
            subject_name = row[2]
            student_count = row[3]
            avg_score = row[4]
            max_score = row[5]
            
            print(f"   {batch_code} - {subject_type} - {subject_name}: {student_count}人, 平均{avg_score:.2f}/{max_score}")
            
            if subject_type == 'exam':
                exam_subjects.append((batch_code, subject_name))
            elif subject_type == 'questionnaire':
                questionnaire_subjects.append((batch_code, subject_name))
        
        print(f"\n   发现考试学科: {len(exam_subjects)}个")
        print(f"   发现问卷学科: {len(questionnaire_subjects)}个")
        
        print("\n2. 检查subject_question_config表中的题目类型...")
        question_type_query = text("""
            SELECT 
                batch_code,
                question_type_enum,
                COUNT(DISTINCT subject_name) as subject_count,
                COUNT(*) as question_count
            FROM subject_question_config
            GROUP BY batch_code, question_type_enum
            ORDER BY batch_code, question_type_enum
        """)
        
        question_results = session.execute(question_type_query).fetchall()
        
        exam_questions = 0
        interaction_questions = 0
        questionnaire_questions = 0
        
        for row in question_results:
            batch_code = row[0]
            question_type = row[1]
            subject_count = row[2]
            question_count = row[3]
            
            print(f"   {batch_code} - {question_type}: {subject_count}个学科, {question_count}道题目")
            
            if question_type == 'exam':
                exam_questions += question_count
            elif question_type == 'interaction':
                interaction_questions += question_count
            elif question_type == 'questionnaire':
                questionnaire_questions += question_count
        
        print(f"\n   总计：")
        print(f"   考试题目: {exam_questions}道")
        print(f"   交互题目: {interaction_questions}道")
        print(f"   问卷题目: {questionnaire_questions}道")
        
        print("\n3. 检查考试学科的维度数据...")
        exam_dimension_query = text("""
            SELECT 
                batch_code,
                subject_name,
                COUNT(*) as student_count,
                COUNT(CASE WHEN dimension_scores IS NOT NULL AND dimension_scores != '{}' THEN 1 END) as with_dimensions
            FROM student_cleaned_scores
            WHERE subject_type = 'exam'
            GROUP BY batch_code, subject_name
        """)
        
        exam_dim_results = session.execute(exam_dimension_query).fetchall()
        
        for row in exam_dim_results:
            batch_code = row[0]
            subject_name = row[1]
            student_count = row[2]
            with_dimensions = row[3]
            
            coverage = (with_dimensions / student_count * 100) if student_count > 0 else 0
            print(f"   {batch_code} - {subject_name}: 维度覆盖率 {coverage:.1f}% ({with_dimensions}/{student_count})")
        
        print("\n4. 检查学生原始分数数据...")
        detail_query = text("""
            SELECT 
                batch_code,
                subject_name,
                COUNT(*) as record_count,
                COUNT(DISTINCT student_id) as student_count,
                COUNT(CASE WHEN subject_scores IS NOT NULL THEN 1 END) as with_scores
            FROM student_score_detail
            WHERE subject_name != '问卷'
            GROUP BY batch_code, subject_name
            ORDER BY batch_code, subject_name
            LIMIT 10
        """)
        
        detail_results = session.execute(detail_query).fetchall()
        
        print("   原始分数数据样本:")
        for row in detail_results:
            batch_code = row[0]
            subject_name = row[1]
            record_count = row[2]
            student_count = row[3]
            with_scores = row[4]
            
            print(f"   {batch_code} - {subject_name}: {record_count}条记录, {student_count}个学生, {with_scores}条有分数")
        
        print("\n5. 数据源可用性评估...")
        
        # 评估数据完整性
        data_availability = {
            'exam_subjects': len(exam_subjects) > 0,
            'questionnaire_subjects': len(questionnaire_subjects) > 0,
            'interaction_questions': interaction_questions > 0,
            'exam_dimension_data': any(row[3] > 0 for row in exam_dim_results),
            'original_score_data': len(detail_results) > 0
        }
        
        print("   数据源可用性:")
        for source, available in data_availability.items():
            status = "[OK]" if available else "[MISSING]"
            print(f"   {status} {source}")
        
        # 生成建议
        print("\n6. 汇聚计算建议...")
        
        if data_availability['exam_subjects']:
            print("   [建议] 考试学科可进行完整统计分析")
        else:
            print("   [警告] 缺少考试学科数据")
            
        if data_availability['questionnaire_subjects']:
            print("   [建议] 问卷学科可进行选项分布分析")
        else:
            print("   [警告] 缺少问卷学科数据")
            
        if data_availability['interaction_questions']:
            print("   [建议] 人机交互题目可按考试学科方式处理")
        else:
            print("   [提示] 暂无人机交互题目数据")
        
        print("\n=== 验证完成 ===")
        
    except Exception as e:
        print(f"验证过程发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    validate_subject_data_sources()