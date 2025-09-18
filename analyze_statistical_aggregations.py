#!/usr/bin/env python3
"""
深入分析statistical_aggregations表中的G4数据问题
"""

import pandas as pd
from sqlalchemy import create_engine
import json

DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"

def main():
    engine = create_engine(DATABASE_URL, echo=False)
    
    print("="*80)
    print("深入分析statistical_aggregations表中的G4数据问题")
    print("="*80)
    
    # 1. 分析G4批次的汇聚数据概览
    print("\n1. G4批次汇聚数据概览")
    overview_query = """
    SELECT 
        aggregation_level,
        COUNT(*) as record_count,
        COUNT(DISTINCT school_id) as unique_schools,
        SUM(total_students) as total_students,
        calculation_status,
        data_version
    FROM statistical_aggregations
    WHERE batch_code = 'G4-2025'
    GROUP BY aggregation_level, calculation_status, data_version
    ORDER BY aggregation_level, calculation_status, data_version
    """
    
    overview = pd.read_sql(overview_query, engine)
    print("G4批次汇聚数据概览:")
    print(overview.to_string(index=False))
    
    # 2. 分析学校级别的数据
    print("\n2. 学校级别汇聚数据分析")
    school_level_query = """
    SELECT 
        school_id,
        school_name,
        calculation_status,
        data_version,
        total_students,
        created_at,
        updated_at
    FROM statistical_aggregations
    WHERE batch_code = 'G4-2025' 
      AND aggregation_level = 'SCHOOL'
    ORDER BY school_id
    """
    
    school_data = pd.read_sql(school_level_query, engine)
    print(f"学校级别数据 (总计{len(school_data)}条记录):")
    print(school_data.to_string(index=False, max_colwidth=30))
    
    # 3. 对比school_id与school_master_data
    print("\n3. 对比statistical_aggregations与school_master_data的学校信息")
    
    # 获取master表中G4的学校信息
    master_query = """
    SELECT 
        school_id,
        standard_school_name as school_name
    FROM school_master_data
    WHERE batch_code = 'G4-2025'
    ORDER BY school_id
    """
    
    master_schools = pd.read_sql(master_query, engine)
    
    # 数据差异分析
    agg_school_ids = set(school_data['school_id'].dropna().astype(str))
    master_school_ids = set(master_schools['school_id'].astype(str))
    
    print(f"statistical_aggregations中的学校数: {len(agg_school_ids)}")
    print(f"school_master_data中的学校数: {len(master_school_ids)}")
    
    common_ids = agg_school_ids & master_school_ids
    agg_only = agg_school_ids - master_school_ids
    master_only = master_school_ids - agg_school_ids
    
    print(f"共同学校数: {len(common_ids)}")
    print(f"仅在aggregations中: {len(agg_only)}")
    print(f"仅在master中: {len(master_only)}")
    
    if agg_only:
        print(f"\n仅在aggregations中的school_id: {sorted(list(agg_only))}")
    
    if master_only:
        print(f"\n仅在master中的school_id (前10个): {sorted(list(master_only))[:10]}")
    
    # 4. 分析学校名称匹配情况
    print("\n4. 学校名称匹配分析")
    if len(common_ids) > 0:
        # 创建字典便于比较
        agg_school_dict = dict(zip(school_data['school_id'].fillna('').astype(str), 
                                  school_data['school_name'].fillna('')))
        master_school_dict = dict(zip(master_schools['school_id'].astype(str), 
                                     master_schools['school_name']))
        
        name_comparison = []
        for school_id in common_ids:
            agg_name = agg_school_dict.get(school_id, 'N/A')
            master_name = master_school_dict.get(school_id, 'N/A')
            match = '一致' if agg_name == master_name else '不一致'
            name_comparison.append({
                'school_id': school_id,
                'agg_name': agg_name,
                'master_name': master_name,
                'match': match
            })
        
        comparison_df = pd.DataFrame(name_comparison)
        match_stats = comparison_df['match'].value_counts()
        print("名称匹配统计:")
        print(match_stats)
        
        # 显示不匹配的案例
        mismatches = comparison_df[comparison_df['match'] == '不一致']
        if not mismatches.empty:
            print(f"\n名称不匹配的学校 ({len(mismatches)}所):")
            print(mismatches.to_string(index=False))
    
    # 5. 检查statistics_data JSON结构
    print("\n5. statistics_data JSON结构分析")
    sample_query = """
    SELECT 
        school_id,
        school_name,
        statistics_data
    FROM statistical_aggregations
    WHERE batch_code = 'G4-2025' 
      AND aggregation_level = 'SCHOOL'
      AND calculation_status = 'COMPLETED'
      AND school_id IS NOT NULL 
      AND school_id != 'INVALID-SCHOOL'
      AND school_id != 'SCH_0001'
    LIMIT 3
    """
    
    sample_data = pd.read_sql(sample_query, engine)
    
    if not sample_data.empty:
        print("样本statistics_data结构:")
        for idx, row in sample_data.iterrows():
            try:
                stats_data = json.loads(row['statistics_data']) if row['statistics_data'] else {}
                print(f"\n学校 {row['school_id']} ({row['school_name']}):")
                print(f"  JSON数据键: {list(stats_data.keys()) if stats_data else '无数据'}")
                
                if 'subjects' in stats_data and stats_data['subjects']:
                    print(f"  科目数量: {len(stats_data['subjects'])}")
                    for subject in stats_data['subjects'][:2]:  # 只显示前2个科目
                        subject_name = subject.get('name', subject.get('subjectName', '未知'))
                        subject_type = subject.get('type', '未知')
                        print(f"    - {subject_name} (类型: {subject_type})")
                        
                        # 检查是否有学校信息
                        if 'school' in subject:
                            school_info = subject['school']
                            print(f"      学校信息: ID={school_info.get('id', 'N/A')}, NAME={school_info.get('name', 'N/A')}")
                        
            except Exception as e:
                print(f"解析JSON失败: {e}")
    
    # 6. 检查区域级别的数据
    print("\n6. 区域级别汇聚数据分析")
    regional_query = """
    SELECT 
        school_id,
        school_name,
        calculation_status,
        data_version,
        total_students,
        total_schools
    FROM statistical_aggregations
    WHERE batch_code = 'G4-2025' 
      AND aggregation_level = 'REGIONAL'
    """
    
    regional_data = pd.read_sql(regional_query, engine)
    print(f"区域级别数据:")
    print(regional_data.to_string(index=False))
    
    print(f"\n" + "="*80)
    print("分析完成")

if __name__ == "__main__":
    main()