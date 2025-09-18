#!/usr/bin/env python3
"""
查找所有汇聚相关的表
"""

import pandas as pd
from sqlalchemy import create_engine

DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"

def main():
    engine = create_engine(DATABASE_URL, echo=False)
    
    # 查找所有表
    tables_query = "SHOW TABLES"
    all_tables = pd.read_sql(tables_query, engine)
    table_names = all_tables.iloc[:, 0].tolist()
    
    print("数据库中的所有表:")
    for table in sorted(table_names):
        print(f"  - {table}")
    
    # 过滤相关的表
    relevant_keywords = ['school', 'aggregation', 'statistics', 'subjects', 'grade']
    relevant_tables = []
    
    for table in table_names:
        if any(keyword in table.lower() for keyword in relevant_keywords):
            relevant_tables.append(table)
    
    print(f"\n找到相关的表 ({len(relevant_tables)}个):")
    for table in sorted(relevant_tables):
        print(f"  - {table}")
    
    # 检查每个相关表是否有G4数据
    print(f"\n检查各表中的G4-2025数据:")
    for table in sorted(relevant_tables):
        try:
            count_query = f"SELECT COUNT(*) as count FROM {table} WHERE batch_code = 'G4-2025'"
            result = pd.read_sql(count_query, engine)
            count = result['count'].iloc[0]
            
            if count > 0:
                print(f"[+] {table}: {count}条记录")
                
                # 查看表结构
                desc_query = f"DESCRIBE {table}"
                structure = pd.read_sql(desc_query, engine)
                print(f"  表结构:")
                for _, row in structure.iterrows():
                    print(f"    {row['Field']} ({row['Type']})")
                
                # 查看样本数据
                if count <= 10:
                    sample_query = f"SELECT * FROM {table} WHERE batch_code = 'G4-2025'"
                else:
                    sample_query = f"SELECT * FROM {table} WHERE batch_code = 'G4-2025' LIMIT 3"
                
                sample = pd.read_sql(sample_query, engine)
                print(f"  样本数据:")
                print(f"    {sample.to_string(index=False, max_colwidth=50)}")
                print()
            else:
                print(f"[-] {table}: 无G4数据")
                
        except Exception as e:
            print(f"[x] {table}: 查询出错 - {e}")

if __name__ == "__main__":
    main()