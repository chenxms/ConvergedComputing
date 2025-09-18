#!/usr/bin/env python3
"""
检查相关表结构
"""

import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"

def main():
    engine = create_engine(DATABASE_URL, echo=False)
    
    tables_to_check = [
        'school_master_data',
        'grade_aggregation_main',
        'school_aggregation_main', 
        'student_score_detail'
    ]
    
    for table in tables_to_check:
        print(f"\n{'='*60}")
        print(f"检查表: {table}")
        print('='*60)
        
        try:
            # 检查表结构
            structure_query = f"DESCRIBE {table}"
            structure_df = pd.read_sql(structure_query, engine)
            print("表结构:")
            print(structure_df.to_string(index=False))
            
            # 检查是否有G4-2025数据
            count_query = f"SELECT COUNT(*) as count FROM {table} WHERE batch_code = 'G4-2025'"
            count_result = pd.read_sql(count_query, engine)
            print(f"\nG4-2025批次记录数: {count_result['count'].iloc[0]}")
            
            # 如果有数据，显示几条样本
            if count_result['count'].iloc[0] > 0:
                sample_query = f"SELECT * FROM {table} WHERE batch_code = 'G4-2025' LIMIT 3"
                sample_data = pd.read_sql(sample_query, engine)
                print(f"\n样本数据 (前3条):")
                print(sample_data.to_string(index=False))
            
        except Exception as e:
            print(f"查询表 {table} 时出错: {e}")

if __name__ == "__main__":
    main()