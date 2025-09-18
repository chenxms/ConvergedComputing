#!/usr/bin/env python3
"""
紧急学校ID映射创建脚本
解决G4批次中school_master_data和student_cleaned_scores的ID格式不一致问题
"""

import pandas as pd
from sqlalchemy import create_engine, text
import json

DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"

def create_school_id_mapping():
    """基于学校名称相似性创建ID映射"""
    engine = create_engine(DATABASE_URL, echo=False)
    
    print("="*80)
    print("G4批次学校ID映射创建")
    print("="*80)
    
    # 1. 获取master数据
    print("\n1. 获取school_master_data中的学校...")
    master_query = """
    SELECT school_id, standard_school_name
    FROM school_master_data 
    WHERE batch_code = 'G4-2025' AND status = 'ACTIVE'
    ORDER BY school_id
    """
    master_df = pd.read_sql(master_query, engine)
    print(f"找到 {len(master_df)} 所标准学校")
    
    # 2. 获取cleaned数据
    print("\n2. 获取student_cleaned_scores中的学校...")
    cleaned_query = """
    SELECT DISTINCT school_code, school_name, COUNT(*) as student_count
    FROM student_cleaned_scores 
    WHERE batch_code = 'G4-2025'
    GROUP BY school_code, school_name
    ORDER BY student_count DESC
    """
    cleaned_df = pd.read_sql(cleaned_query, engine)
    print(f"找到 {len(cleaned_df)} 所待映射学校")
    
    # 3. 基于名称相似性进行映射
    print("\n3. 基于名称相似性创建映射...")
    mappings = []
    unmapped_cleaned = []
    unmapped_master = set(master_df['school_id'].tolist())
    
    for _, cleaned_row in cleaned_df.iterrows():
        cleaned_code = cleaned_row['school_code']
        cleaned_name = str(cleaned_row['school_name'])
        student_count = cleaned_row['student_count']
        
        # 清理名称进行匹配
        cleaned_base_name = cleaned_name.replace('(小学)', '').replace('小学', '').replace('学校', '').strip()
        
        best_match = None
        best_score = 0
        
        for _, master_row in master_df.iterrows():
            master_id = master_row['school_id']
            master_name = str(master_row['standard_school_name'])
            master_base_name = master_name.replace('(小学)', '').replace('小学', '').replace('学校', '').strip()
            
            # 简单的名称匹配算法
            if cleaned_base_name == master_base_name:
                score = 1.0  # 完全匹配
            elif master_base_name in cleaned_base_name or cleaned_base_name in master_base_name:
                score = 0.8  # 包含匹配
            elif len(cleaned_base_name) >= 2 and len(master_base_name) >= 2:
                # 计算共同字符比例
                common_chars = sum(1 for c in cleaned_base_name if c in master_base_name)
                score = common_chars / max(len(cleaned_base_name), len(master_base_name))
            else:
                score = 0
            
            if score > best_score and score >= 0.6:  # 相似度阈值
                best_match = master_id
                best_score = score
        
        if best_match:
            mappings.append({
                'cleaned_school_code': cleaned_code,
                'cleaned_school_name': cleaned_name,
                'master_school_id': best_match,
                'master_school_name': master_df[master_df['school_id'] == best_match]['standard_school_name'].iloc[0],
                'similarity_score': best_score,
                'student_count': student_count
            })
            unmapped_master.discard(best_match)
            print(f"  映射: {cleaned_code} ({cleaned_name}) -> {best_match} (相似度: {best_score:.2f})")
        else:
            unmapped_cleaned.append({
                'school_code': cleaned_code,
                'school_name': cleaned_name,
                'student_count': student_count
            })
    
    print(f"\n映射结果统计:")
    print(f"  成功映射: {len(mappings)} 对")
    print(f"  未映射的cleaned学校: {len(unmapped_cleaned)} 所")
    print(f"  未映射的master学校: {len(unmapped_master)} 所")
    
    # 4. 显示映射结果
    if mappings:
        print(f"\n成功的映射 (前20个):")
        mapping_df = pd.DataFrame(mappings)
        print(mapping_df.head(20)[['cleaned_school_code', 'master_school_id', 'similarity_score', 'student_count']].to_string(index=False))
        
        # 保存映射到文件  
        mapping_df.to_json('g4_school_id_mapping.json', orient='records', indent=2)
        print(f"\n映射已保存到: g4_school_id_mapping.json")
    
    # 5. 显示未映射的学校
    if unmapped_cleaned:
        print(f"\n未映射的cleaned学校 (前20个, 按学生数排序):")
        unmapped_df = pd.DataFrame(unmapped_cleaned)
        print(unmapped_df.head(20).to_string(index=False))
    
    if unmapped_master:
        print(f"\n未映射的master学校:")
        unmapped_master_list = list(unmapped_master)
        unmapped_master_df = master_df[master_df['school_id'].isin(unmapped_master_list)]
        print(unmapped_master_df.to_string(index=False))
    
    return mappings


def apply_id_mapping_to_cleaned_scores(mappings):
    """将ID映射应用到student_cleaned_scores表"""
    engine = create_engine(DATABASE_URL, echo=False)
    
    print("\n" + "="*80)
    print("应用ID映射到student_cleaned_scores")
    print("="*80)
    
    if not mappings:
        print("没有映射可以应用")
        return
    
    print(f"准备应用 {len(mappings)} 个映射...")
    
    with engine.begin() as conn:
        updated_count = 0
        for mapping in mappings:
            cleaned_code = mapping['cleaned_school_code']
            master_id = mapping['master_school_id']
            master_name = mapping['master_school_name']
            
            # 更新school_code和school_name
            update_query = text("""
                UPDATE student_cleaned_scores 
                SET school_code = :new_school_code,
                    school_name = :new_school_name
                WHERE batch_code = 'G4-2025' 
                    AND school_code = :old_school_code
            """)
            
            result = conn.execute(update_query, {
                'new_school_code': master_id,
                'new_school_name': master_name,
                'old_school_code': cleaned_code
            })
            
            rows_updated = result.rowcount
            updated_count += rows_updated
            
            if rows_updated > 0:
                print(f"  更新 {cleaned_code} -> {master_id}: {rows_updated} 条记录")
    
    print(f"\n总计更新了 {updated_count} 条记录")


if __name__ == "__main__":
    # 1. 创建映射
    mappings = create_school_id_mapping()
    
    # 2. 询问是否应用映射
    if mappings:
        print(f"\n{'='*80}")
        response = input(f"是否应用这 {len(mappings)} 个映射到student_cleaned_scores表? (y/N): ").strip().lower()
        
        if response == 'y' or response == 'yes':
            apply_id_mapping_to_cleaned_scores(mappings)
            print("✅ ID映射应用完成!")
        else:
            print("跳过应用映射，映射已保存到文件")
    else:
        print("❌ 没有生成有效的映射")