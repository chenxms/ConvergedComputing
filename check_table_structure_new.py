#!/usr/bin/env python3
"""
检查数据库表结构
"""

from app.database.connection import get_db_context
from sqlalchemy import text

def check_table_structure():
    """检查表结构"""
    
    try:
        with get_db_context() as session:
            print("=== 检查student_score_detail表结构 ===")
            
            # 检查表结构
            result = session.execute(text("DESCRIBE student_score_detail"))
            columns = result.fetchall()
            
            print("表字段：")
            for column in columns:
                print(f"  {column[0]} - {column[1]} - {column[2]} - {column[3]}")
            
            print("\n=== 检查grade_aggregation_main表结构 ===")
            result = session.execute(text("DESCRIBE grade_aggregation_main"))
            columns = result.fetchall()
            
            print("表字段：")
            for column in columns:
                print(f"  {column[0]} - {column[1]} - {column[2]} - {column[3]}")
            
            print("\n=== 检查statistical_analysis_main表结构 ===")
            result = session.execute(text("DESCRIBE statistical_analysis_main"))
            columns = result.fetchall()
            
            print("表字段：")
            for column in columns:
                print(f"  {column[0]} - {column[1]} - {column[2]} - {column[3]}")
        
    except Exception as e:
        print(f"检查过程中出现错误: {e}")

if __name__ == "__main__":
    check_table_structure()