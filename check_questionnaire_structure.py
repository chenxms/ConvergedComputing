#!/usr/bin/env python3
"""
检查问卷数据结构
"""

from app.database.connection import get_db_context
from sqlalchemy import text

def check_questionnaire_structure():
    """检查问卷数据结构"""
    
    try:
        with get_db_context() as session:
            print("=== 检查questionnaire_question_scores表结构 ===")
            
            # 1. 检查表结构
            result = session.execute(text("DESCRIBE questionnaire_question_scores"))
            columns = result.fetchall()
            
            print("\n表字段：")
            for column in columns:
                print(f"  {column[0]} - {column[1]}")
            
            # 2. 检查量表类型分布
            print("\n=== 量表类型分布 ===")
            result = session.execute(text("""
                SELECT DISTINCT instrument_type, scale_level, option_label, COUNT(*) as count
                FROM questionnaire_question_scores
                WHERE batch_code = 'G7-2025'
                GROUP BY instrument_type, scale_level, option_label
                ORDER BY scale_level, instrument_type
            """))
            scale_types = result.fetchall()
            
            for scale in scale_types:
                print(f"量表类型: {scale.instrument_type}, 等级: {scale.scale_level}, "
                      f"选项: {scale.option_label}, 记录数: {scale.count}")
            
            # 3. 检查样本数据
            print("\n=== 样本数据 ===")
            result = session.execute(text("""
                SELECT student_id, dimension_code, question_id, 
                       original_score, option_label, option_level, 
                       instrument_type, scale_level, is_reverse
                FROM questionnaire_question_scores
                WHERE batch_code = 'G7-2025'
                LIMIT 10
            """))
            samples = result.fetchall()
            
            for sample in samples:
                print(f"学生: {sample.student_id}, 维度: {sample.dimension_code}, "
                      f"题目: {sample.question_id}, 原始分: {sample.original_score}, "
                      f"选项: {sample.option_label}(等级{sample.option_level}), "
                      f"量表: {sample.instrument_type}({sample.scale_level}级), "
                      f"反向: {'是' if sample.is_reverse else '否'}")
            
            # 4. 检查维度分布
            print("\n=== 维度分布 ===")
            result = session.execute(text("""
                SELECT dimension_code, COUNT(DISTINCT question_id) as question_count,
                       COUNT(DISTINCT student_id) as student_count
                FROM questionnaire_question_scores
                WHERE batch_code = 'G7-2025'
                GROUP BY dimension_code
                ORDER BY dimension_code
            """))
            dimensions = result.fetchall()
            
            for dim in dimensions:
                print(f"维度 {dim.dimension_code}: {dim.question_count}个题目, {dim.student_count}名学生")
                
        print("\n=== 检查完成 ===")
            
    except Exception as e:
        print(f"检查过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_questionnaire_structure()