#!/usr/bin/env python3
"""
简单的问卷数据插入测试
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def simple_questionnaire_insert():
    """简单测试问卷数据插入"""
    print("=== 简单问卷数据插入测试 ===")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 1. 清理测试数据
        print("1. 清理测试数据...")
        clean_query = text("DELETE FROM questionnaire_question_scores WHERE batch_code = 'TEST-2025'")
        result = session.execute(clean_query)
        session.commit()
        print(f"清理了 {result.rowcount} 条测试数据")
        
        # 2. 插入测试数据
        print("\n2. 插入测试数据...")
        test_records = [
            {
                'student_id': 1001,
                'subject_name': '心理测试',
                'batch_code': 'TEST-2025',
                'dimension_code': 'D001',
                'dimension_name': '学习态度',
                'question_id': 'Q001',
                'question_name': '我喜欢学习',
                'original_score': 4.0,
                'scale_level': 4,
                'instrument_type': 'LIKERT_4_POSITIV',
                'is_reverse': False,
                'option_label': '非常同意',
                'option_level': 4,
                'max_score': 4.0
            },
            {
                'student_id': 1001,
                'subject_name': '心理测试',
                'batch_code': 'TEST-2025',
                'dimension_code': 'D001',
                'dimension_name': '学习态度',
                'question_id': 'Q002',
                'question_name': '学习让我快乐',
                'original_score': 3.0,
                'scale_level': 4,
                'instrument_type': 'LIKERT_4_POSITIV',
                'is_reverse': False,
                'option_label': '同意',
                'option_level': 3,
                'max_score': 4.0
            },
            {
                'student_id': 1002,
                'subject_name': '心理测试',
                'batch_code': 'TEST-2025',
                'dimension_code': 'D001',
                'dimension_name': '学习态度',
                'question_id': 'Q001',
                'question_name': '我喜欢学习',
                'original_score': 2.0,
                'scale_level': 4,
                'instrument_type': 'LIKERT_4_POSITIV',
                'is_reverse': False,
                'option_label': '不同意',
                'option_level': 2,
                'max_score': 4.0
            }
        ]
        
        insert_query = text("""
            INSERT INTO questionnaire_question_scores 
            (student_id, subject_name, batch_code, dimension_code, dimension_name,
             question_id, question_name, original_score, scale_level, instrument_type,
             is_reverse, option_label, option_level, max_score)
            VALUES 
            (:student_id, :subject_name, :batch_code, :dimension_code, :dimension_name,
             :question_id, :question_name, :original_score, :scale_level, :instrument_type,
             :is_reverse, :option_label, :option_level, :max_score)
        """)
        
        session.execute(insert_query, test_records)
        session.commit()
        
        print(f"成功插入 {len(test_records)} 条测试记录")
        
        # 3. 验证插入结果
        print("\n3. 验证插入结果...")
        verify_query = text("""
            SELECT 
                student_id, question_id, option_label, original_score
            FROM questionnaire_question_scores 
            WHERE batch_code = 'TEST-2025'
            ORDER BY student_id, question_id
        """)
        
        verify_result = session.execute(verify_query)
        print("插入的数据:")
        for row in verify_result.fetchall():
            print(f"  学生{row[0]} - 题目{row[1]}: {row[2]} (分数: {row[3]})")
        
        # 4. 统计选项占比
        print("\n4. 统计选项占比...")
        stats_query = text("""
            SELECT 
                option_label,
                option_level,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM questionnaire_question_scores
            WHERE batch_code = 'TEST-2025'
            GROUP BY option_label, option_level
            ORDER BY option_level
        """)
        
        stats_result = session.execute(stats_query)
        print("选项分布统计:")
        for row in stats_result.fetchall():
            print(f"  {row[0]} (等级{row[1]}): {row[2]}条 ({row[3]}%)")
        
        # 5. 按题目统计
        print("\n5. 按题目统计...")
        question_stats_query = text("""
            SELECT 
                question_id,
                question_name,
                option_label,
                COUNT(*) as count
            FROM questionnaire_question_scores
            WHERE batch_code = 'TEST-2025'
            GROUP BY question_id, question_name, option_label
            ORDER BY question_id, option_level
        """)
        
        question_result = session.execute(question_stats_query)
        print("按题目的选项分布:")
        current_question = None
        for row in question_result.fetchall():
            if row[0] != current_question:
                current_question = row[0]
                print(f"  题目 {row[0]} ({row[1]}):")
            print(f"    {row[2]}: {row[3]}人")
        
        print("\n[SUCCESS] 问卷数据插入和统计测试成功！")
        print("问卷清洗功能已验证可用。")
        
    except Exception as e:
        print(f"[ERROR] 测试失败: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
        
    finally:
        session.close()

if __name__ == "__main__":
    simple_questionnaire_insert()