#!/usr/bin/env python3
"""
检查数据库表结构脚本
"""
from sqlalchemy import create_engine, text
import os

# 数据库连接配置
DATABASE_HOST = os.getenv("DATABASE_HOST", "117.72.14.166")
DATABASE_PORT = os.getenv("DATABASE_PORT", "23506") 
DATABASE_USER = os.getenv("DATABASE_USER", "root")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "mysql_Lujing2022")
DATABASE_NAME = os.getenv("DATABASE_NAME", "appraisal_test")

DATABASE_URL = f"mysql+pymysql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}?charset=utf8mb4"

def check_tables():
    engine = create_engine(DATABASE_URL)
    
    tables_to_check = [
        "student_score_detail",
        "subject_question_config", 
        "batch_dimension_definition",
        "question_dimension_mapping"
    ]
    
    print("=== 数据库表结构检查 ===\n")
    
    with engine.connect() as conn:
        for table_name in tables_to_check:
            print(f"Checking table: {table_name}")
            try:
                # 检查表是否存在
                result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
                if result.fetchone():
                    print(f"FOUND table {table_name}")
                    
                    # 显示表结构
                    desc_result = conn.execute(text(f"DESCRIBE {table_name}"))
                    columns = desc_result.fetchall()
                    print(f"   字段数: {len(columns)}")
                    for col in columns[:5]:  # 显示前5个字段
                        print(f"   - {col[0]} ({col[1]})")
                    if len(columns) > 5:
                        print(f"   ... 还有 {len(columns)-5} 个字段")
                    
                    # 检查样本数据
                    count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    count = count_result.fetchone()[0]
                    print(f"   记录数: {count}")
                    
                else:
                    print(f"NOT FOUND table {table_name}")
                    
            except Exception as e:
                print(f"ERROR checking table {table_name}: {e}")
            print()

if __name__ == "__main__":
    check_tables()