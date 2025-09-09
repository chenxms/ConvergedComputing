#!/usr/bin/env python3
"""
检查表结构
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def check_table_structure():
    """检查表结构"""
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    
    try:
        engine = create_engine(DATABASE_URL, echo=False)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 检查student_score_detail表结构
        print("=== student_score_detail 表结构 ===")
        query = text("DESCRIBE student_score_detail")
        result = session.execute(query)
        rows = result.fetchall()
        
        print(f"{'字段名':<20} {'类型':<20} {'允许NULL':<10} {'键':<10} {'默认值':<15} {'备注'}")
        print("-" * 90)
        for row in rows:
            field, type_info, null, key, default, extra = row
            print(f"{field:<20} {type_info:<20} {null:<10} {key:<10} {str(default):<15} {extra}")
        
        # 查看前几条数据样本
        print("\n=== 数据样本 ===")
        query = text("SELECT * FROM student_score_detail WHERE batch_code = 'G4-2025' LIMIT 3")
        result = session.execute(query)
        rows = result.fetchall()
        
        if rows:
            # 获取列名
            columns = result.keys()
            print("列名:", list(columns))
            print()
            for i, row in enumerate(rows):
                print(f"记录 {i+1}:")
                for j, col in enumerate(columns):
                    print(f"  {col}: {row[j]}")
                print()
        
        session.close()
        
    except Exception as e:
        print(f"检查表结构失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    check_table_structure()