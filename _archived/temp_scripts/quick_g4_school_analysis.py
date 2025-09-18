#!/usr/bin/env python3
"""
快速分析G4批次学校数据问题
"""

import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"

def analyze_g4_schools():
    engine = create_engine(DATABASE_URL, echo=False)
    
    print("="*80)
    print("G4批次学校数据分析")
    print("="*80)
    
    # 1. school_master_data中G4的学校
    print("\n1. school_master_data中的G4学校:")
    master_query = """
    SELECT 
        COUNT(*) as total_count,
        COUNT(DISTINCT school_id) as unique_schools,
        MIN(school_id) as min_school_id,
        MAX(school_id) as max_school_id
    FROM school_master_data 
    WHERE batch_code = 'G4-2025' AND status = 'ACTIVE'
    """
    master_df = pd.read_sql(master_query, engine)
    print(master_df.to_string(index=False))
    
    # 2. student_cleaned_scores中G4的学校
    print("\n2. student_cleaned_scores中的G4学校:")
    cleaned_query = """
    SELECT 
        COUNT(DISTINCT school_code) as unique_schools,
        MIN(school_code) as min_school_code,
        MAX(school_code) as max_school_code,
        COUNT(*) as total_records
    FROM student_cleaned_scores 
    WHERE batch_code = 'G4-2025'
    """
    cleaned_df = pd.read_sql(cleaned_query, engine)
    print(cleaned_df.to_string(index=False))
    
    # 3. 匹配的学校数量（使用COLLATE解决字符集问题）
    print("\n3. 能够匹配的学校数量:")
    match_query = """
    SELECT COUNT(DISTINCT scs.school_code) as matched_schools
    FROM student_cleaned_scores scs
    INNER JOIN school_master_data smd 
        ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
        AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
        AND smd.status = 'ACTIVE'
    WHERE scs.batch_code = 'G4-2025'
    """
    match_df = pd.read_sql(match_query, engine)
    print(match_df.to_string(index=False))
    
    # 4. 不匹配的学校样本（前20个）
    print("\n4. 不匹配的学校样本 (前20个):")
    orphaned_query = """
    SELECT DISTINCT 
        scs.school_code,
        scs.school_name,
        COUNT(*) as student_count
    FROM student_cleaned_scores scs
    LEFT JOIN school_master_data smd 
        ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
        AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
        AND smd.status = 'ACTIVE'
    WHERE scs.batch_code = 'G4-2025'
        AND smd.school_id IS NULL
    GROUP BY scs.school_code, scs.school_name
    ORDER BY student_count DESC
    LIMIT 20
    """
    orphaned_df = pd.read_sql(orphaned_query, engine)
    print(orphaned_df.to_string(index=False))
    
    # 5. school_master_data中的学校ID范围
    print("\n5. school_master_data中的学校ID样本:")
    sample_query = """
    SELECT school_id, standard_school_name
    FROM school_master_data 
    WHERE batch_code = 'G4-2025' AND status = 'ACTIVE'
    ORDER BY school_id
    LIMIT 20
    """
    sample_df = pd.read_sql(sample_query, engine)
    print(sample_df.to_string(index=False))

if __name__ == "__main__":
    analyze_g4_schools()