#!/usr/bin/env python3
"""
检查问卷数据状态
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json

def check_questionnaire_status():
    """检查问卷数据状态"""
    print("=== 检查问卷数据状态 ===\n")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 1. 检查问卷详细表
        print("1. 问卷详细表状态:")
        result = session.execute(text("SELECT COUNT(*) FROM questionnaire_question_scores"))
        count = result.fetchone()[0]
        print(f"   总记录数: {count}")
        
        if count > 0:
            result = session.execute(text("""
                SELECT batch_code, subject_name, COUNT(*) as cnt
                FROM questionnaire_question_scores
                GROUP BY batch_code, subject_name
            """))
            for row in result.fetchall():
                print(f"   {row[0]} - {row[1]}: {row[2]}条")
        
        # 2. 检查原始数据中的心理科目
        print("\n2. 原始数据中的心理科目:")
        result = session.execute(text("""
            SELECT batch_code, 
                   COUNT(DISTINCT student_id) as students, 
                   COUNT(*) as records
            FROM student_score_detail 
            WHERE subject_name = '心理'
            GROUP BY batch_code
            ORDER BY batch_code
        """))
        for row in result.fetchall():
            print(f"   {row[0]}: {row[1]}个学生, {row[2]}条记录")
        
        # 3. 检查问卷科目配置
        print("\n3. 问卷科目配置:")
        result = session.execute(text("""
            SELECT batch_code, subject_name, 
                   COUNT(*) as question_count,
                   GROUP_CONCAT(DISTINCT instrument_id) as instruments
            FROM subject_question_config 
            WHERE question_type_enum = 'questionnaire'
            GROUP BY batch_code, subject_name
            ORDER BY batch_code, subject_name
        """))
        for row in result.fetchall():
            print(f"   {row[0]} - {row[1]}: {row[2]}道题目, 量表: {row[3]}")
        
        # 4. 检查G4-2025批次心理科目的具体数据
        print("\n4. G4-2025批次心理科目详细分析:")
        
        # 检查是否有学生数据
        result = session.execute(text("""
            SELECT COUNT(DISTINCT student_id) as student_count,
                   COUNT(*) as record_count
            FROM student_score_detail
            WHERE batch_code = 'G4-2025' AND subject_name = '心理'
        """))
        row = result.fetchone()
        if row and row[0] > 0:
            print(f"   学生数: {row[0]}, 记录数: {row[1]}")
            
            # 抽样检查学生的subject_scores数据
            result = session.execute(text("""
                SELECT student_id, subject_scores
                FROM student_score_detail
                WHERE batch_code = 'G4-2025' AND subject_name = '心理'
                LIMIT 3
            """))
            
            print("\n   学生数据样本:")
            for row in result.fetchall():
                student_id = row[0]
                subject_scores = row[1]
                print(f"   学生 {student_id}:")
                if subject_scores:
                    try:
                        scores = json.loads(subject_scores)
                        print(f"     题目数: {len(scores)}")
                        # 显示前3个题目
                        items = list(scores.items())[:3]
                        for qid, score in items:
                            print(f"     题目{qid}: {score}分")
                    except:
                        print(f"     JSON解析失败")
                else:
                    print(f"     无分数数据")
        else:
            print("   无学生数据")
        
        # 5. 检查问卷题目配置详情
        print("\n5. G4-2025批次心理科目题目配置:")
        result = session.execute(text("""
            SELECT question_id, question_no, question_number, 
                   max_score, instrument_id
            FROM subject_question_config
            WHERE batch_code = 'G4-2025' 
            AND subject_name = '心理'
            AND question_type_enum = 'questionnaire'
            ORDER BY question_number
            LIMIT 5
        """))
        
        configs = result.fetchall()
        if configs:
            print(f"   找到 {len(configs)} 道题目配置（显示前5个）:")
            for row in configs:
                print(f"   题目ID: {row[0]}, 题号: {row[1]}, 序号: {row[2]}, 满分: {row[3]}, 量表: {row[4]}")
        else:
            print("   未找到题目配置")
        
        # 6. 分析问题
        print("\n6. 问题分析:")
        
        # 检查是否有重复数据
        result = session.execute(text("""
            SELECT batch_code, student_id, subject_name, COUNT(*) as cnt
            FROM student_cleaned_scores
            WHERE subject_type = 'questionnaire' OR subject_name = '心理'
            GROUP BY batch_code, student_id, subject_name
            HAVING COUNT(*) > 1
            LIMIT 5
        """))
        
        duplicates = result.fetchall()
        if duplicates:
            print("   发现重复数据:")
            for row in duplicates:
                print(f"     {row[0]} - 学生{row[1]} - {row[2]}: {row[3]}条")
        else:
            print("   无重复数据")
        
        print("\n=== 检查完成 ===")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    check_questionnaire_status()