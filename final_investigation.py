#!/usr/bin/env python3
"""
最终调查 - 检查数学科目数据
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json


def investigate_math_data():
    """调查数学科目数据"""
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    
    try:
        engine = create_engine(DATABASE_URL, echo=False)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        print("=== 数学科目数据深度分析 ===\n")
        
        # 1. 查看数学科目的原始数据统计
        print("1. 数学科目原始数据统计:")
        query = text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT student_id) as unique_students,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score,
                AVG(total_score) as avg_score,
                COUNT(CASE WHEN total_score = 0 THEN 1 END) as zero_count,
                COUNT(CASE WHEN total_score > 0 THEN 1 END) as non_zero_count
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025' AND subject_name = '数学'
        """)
        result = session.execute(query)
        row = result.fetchone()
        
        if row:
            print(f"  总记录数: {row[0]}")
            print(f"  唯一学生数: {row[1]}")
            print(f"  分数范围: {row[2]:.2f} - {row[3]:.2f}")
            print(f"  平均分: {row[4]:.2f}")
            print(f"  零分记录: {row[5]} ({row[5]/row[0]*100:.1f}%)")
            print(f"  非零记录: {row[6]} ({row[6]/row[0]*100:.1f}%)")
        
        # 2. 查看零分学生的详细信息
        print("\n2. 零分学生详细信息 (前10个):")
        query = text("""
            SELECT 
                student_id, student_name, school_name, total_score, subject_scores
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025' 
            AND subject_name = '数学' 
            AND total_score = 0
            LIMIT 10
        """)
        result = session.execute(query)
        rows = result.fetchall()
        
        if rows:
            print(f"{'学生ID':<15} {'姓名':<10} {'学校':<20} {'总分':<8} {'详细分数'}")
            print("-" * 100)
            for row in rows:
                student_id, student_name, school_name, total_score, subject_scores = row
                # 解析JSON分数
                try:
                    scores_dict = json.loads(subject_scores) if subject_scores else {}
                    score_summary = f"{len(scores_dict)} 题" if scores_dict else "无详细分数"
                except:
                    score_summary = "解析失败"
                
                print(f"{student_id:<15} {str(student_name):<10} {str(school_name):<20} {total_score:<8.2f} {score_summary}")
        
        # 3. 查看非零分学生的详细信息
        print("\n3. 非零分学生详细信息 (前5个):")
        query = text("""
            SELECT 
                student_id, student_name, school_name, total_score, subject_scores
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025' 
            AND subject_name = '数学' 
            AND total_score > 0
            LIMIT 5
        """)
        result = session.execute(query)
        rows = result.fetchall()
        
        if rows:
            print(f"{'学生ID':<15} {'姓名':<10} {'学校':<20} {'总分':<8} {'详细分数'}")
            print("-" * 100)
            for row in rows:
                student_id, student_name, school_name, total_score, subject_scores = row
                # 解析JSON分数
                try:
                    scores_dict = json.loads(subject_scores) if subject_scores else {}
                    if scores_dict:
                        # 计算JSON中分数的总和来验证
                        json_total = sum(float(v) for v in scores_dict.values())
                        score_summary = f"{len(scores_dict)} 题, JSON总分: {json_total:.2f}"
                    else:
                        score_summary = "无详细分数"
                except Exception as e:
                    score_summary = f"解析失败: {e}"
                
                print(f"{student_id:<15} {str(student_name):<10} {str(school_name):<20} {total_score:<8.2f} {score_summary}")
        
        # 4. 检查是否存在每个学生多条数学记录的情况
        print("\n4. 检查学生重复记录:")
        query = text("""
            SELECT student_id, COUNT(*) as record_count
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025' AND subject_name = '数学'
            GROUP BY student_id
            HAVING COUNT(*) > 1
            LIMIT 10
        """)
        result = session.execute(query)
        rows = result.fetchall()
        
        if rows:
            print("发现重复记录的学生:")
            for row in rows:
                print(f"  学生 {row[0]}: {row[1]} 条记录")
        else:
            print("  每个学生只有一条数学记录 [OK]")
        
        # 5. 检查清洗后的数据
        print("\n5. 清洗后数据检查:")
        query = text("""
            SELECT 
                COUNT(*) as cleaned_records,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score,
                AVG(total_score) as avg_score
            FROM student_cleaned_scores 
            WHERE batch_code = 'G4-2025' AND subject_name = '数学'
        """)
        result = session.execute(query)
        row = result.fetchone()
        
        if row and row[0] > 0:
            print(f"  清洗后记录数: {row[0]}")
            print(f"  分数范围: {row[1]:.2f} - {row[2]:.2f}")
            print(f"  平均分: {row[3]:.2f}")
        else:
            print("  没有清洗后的数学数据")
        
        session.close()
        
    except Exception as e:
        print(f"调查失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    investigate_math_data()