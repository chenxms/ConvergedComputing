#!/usr/bin/env python3
"""
测试数据库连接
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def test_connection():
    """测试数据库连接"""
    print("测试数据库连接...")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        print("数据库连接成功！")
        
        # 测试查询批次数据
        query = text("""
            SELECT 
                batch_code,
                COUNT(*) as record_count,
                COUNT(DISTINCT subject_name) as subject_count,
                COUNT(DISTINCT student_id) as student_count
            FROM student_score_detail
            WHERE batch_code IN ('G7-2025', 'G8-2025')
            GROUP BY batch_code
            ORDER BY batch_code
        """)
        
        result = session.execute(query)
        rows = result.fetchall()
        
        print(f"\n待清洗批次数据统计:")
        for row in rows:
            print(f"批次 {row[0]}: {row[1]} 条记录, {row[2]} 个科目, {row[3]} 个学生")
        
        # 检查已清洗的数据
        cleaned_query = text("""
            SELECT 
                batch_code,
                COUNT(*) as record_count,
                COUNT(DISTINCT subject_name) as subject_count,
                COUNT(DISTINCT student_id) as student_count
            FROM student_cleaned_scores
            WHERE batch_code IN ('G4-2025', 'G7-2025', 'G8-2025')
            GROUP BY batch_code
            ORDER BY batch_code
        """)
        
        cleaned_result = session.execute(cleaned_query)
        cleaned_rows = cleaned_result.fetchall()
        
        print(f"\n已清洗批次数据统计:")
        for row in cleaned_rows:
            print(f"批次 {row[0]}: {row[1]} 条记录, {row[2]} 个科目, {row[3]} 个学生")
        
        session.close()
        print("\n数据库连接测试完成！")
        
    except Exception as e:
        print(f"数据库连接失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_connection()