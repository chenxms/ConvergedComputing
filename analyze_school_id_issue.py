#!/usr/bin/env python3
"""
深度分析G4批次学校ID问题
"""

import pandas as pd
from sqlalchemy import create_engine, text
import json

DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"

def main():
    engine = create_engine(DATABASE_URL, echo=False)
    
    print("="*80)
    print("深度分析G4批次学校ID数据问题")
    print("="*80)
    
    # 1. 分析G4批次中的subjects JSON数据
    print("\n1. 分析G4批次的subjects JSON数据结构")
    subjects_query = """
    SELECT subjects 
    FROM grade_aggregation_main 
    WHERE batch_code = 'G4-2025'
    """
    
    subjects_result = pd.read_sql(subjects_query, engine)
    if not subjects_result.empty:
        subjects_json = json.loads(subjects_result['subjects'].iloc[0])
        print(f"G4批次包含的科目数: {len(subjects_json)}")
        for subject in subjects_json:
            print(f"  - {subject.get('subjectName')} (代码: {subject.get('subjectCode')})")
    
    # 2. 分析student_score_detail中的学校信息
    print("\n2. 分析student_score_detail中G4批次的学校信息")
    school_stats_query = """
    SELECT 
        school_id,
        school_name,
        COUNT(DISTINCT student_id) as student_count,
        COUNT(*) as record_count
    FROM student_score_detail 
    WHERE batch_code = 'G4-2025'
    GROUP BY school_id, school_name
    ORDER BY school_id
    """
    
    school_stats = pd.read_sql(school_stats_query, engine)
    print(f"G4批次中的学校统计 (总计{len(school_stats)}所学校):")
    print(school_stats.to_string(index=False))
    
    # 3. 对比student_score_detail和school_master_data的学校信息
    print("\n3. 对比student_score_detail和school_master_data的学校信息差异")
    
    # 获取master表中G4批次的学校信息  
    master_g4_query = """
    SELECT 
        school_id,
        standard_school_name as school_name
    FROM school_master_data
    WHERE batch_code = 'G4-2025'
    ORDER BY school_id
    """
    
    master_g4_schools = pd.read_sql(master_g4_query, engine)
    print(f"\nschool_master_data中G4批次学校 (总计{len(master_g4_schools)}所):")
    print(master_g4_schools.to_string(index=False))
    
    # 数据差异分析
    student_school_ids = set(school_stats['school_id'].astype(str))
    master_school_ids = set(master_g4_schools['school_id'].astype(str))
    
    print(f"\n=== 数据差异分析 ===")
    print(f"student_score_detail中的学校数: {len(student_school_ids)}")
    print(f"school_master_data中的学校数: {len(master_school_ids)}")
    
    common_ids = student_school_ids & master_school_ids
    student_only = student_school_ids - master_school_ids
    master_only = master_school_ids - student_school_ids
    
    print(f"共同学校数: {len(common_ids)}")
    print(f"仅在student表中: {len(student_only)}")
    print(f"仅在master表中: {len(master_only)}")
    
    if student_only:
        print(f"\n仅在student表中的school_id: {sorted(list(student_only))}")
    
    if master_only:
        print(f"\n仅在master表中的school_id: {sorted(list(master_only))}")
    
    # 4. 检查学校名称一致性
    if len(common_ids) > 0:
        print(f"\n4. 检查共同学校的名称一致性")
        name_comparison_data = []
        
        student_school_dict = dict(zip(school_stats['school_id'].astype(str), school_stats['school_name']))
        master_school_dict = dict(zip(master_g4_schools['school_id'].astype(str), master_g4_schools['school_name']))
        
        for school_id in common_ids:
            student_name = student_school_dict.get(school_id, 'N/A')
            master_name = master_school_dict.get(school_id, 'N/A')
            match = '一致' if student_name == master_name else '不一致'
            name_comparison_data.append({
                'school_id': school_id,
                'student_name': student_name,
                'master_name': master_name,
                'match': match
            })
        
        name_comparison_df = pd.DataFrame(name_comparison_data)
        match_stats = name_comparison_df['match'].value_counts()
        print(f"名称匹配统计:")
        print(match_stats)
        
        mismatches = name_comparison_df[name_comparison_df['match'] == '不一致']
        if not mismatches.empty:
            print(f"\n名称不匹配的学校 ({len(mismatches)}所):")
            print(mismatches.to_string(index=False))
    
    # 5. 分析其他相关汇聚结果表
    print(f"\n5. 查看其他可能的汇聚结果表")
    
    # 查找所有可能包含学校汇聚数据的表
    potential_tables = ['school_statistics', 'school_aggregation', 'subjects_school_statistics']
    
    for table in potential_tables:
        try:
            check_query = f"SELECT COUNT(*) as count FROM {table} WHERE batch_code = 'G4-2025'"
            result = pd.read_sql(check_query, engine)
            if result['count'].iloc[0] > 0:
                print(f"\n表 {table} 中有G4数据: {result['count'].iloc[0]}条记录")
                
                # 显示表结构
                desc_query = f"DESCRIBE {table}"
                structure = pd.read_sql(desc_query, engine)
                print(f"表结构:")
                print(structure.to_string(index=False))
                
                # 显示样本数据
                sample_query = f"SELECT * FROM {table} WHERE batch_code = 'G4-2025' LIMIT 3"
                sample = pd.read_sql(sample_query, engine)
                print(f"样本数据:")
                print(sample.to_string(index=False))
            
        except Exception as e:
            print(f"检查表 {table} 时出错: {e}")
    
    print(f"\n" + "="*80)
    print("分析完成")

if __name__ == "__main__":
    main()