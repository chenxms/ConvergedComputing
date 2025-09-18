#!/usr/bin/env python3
"""
直接应用G4批次学校ID映射
基于之前的映射结果，直接更新student_cleaned_scores表
"""

import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"

def apply_g4_school_id_mappings():
    """应用预定义的学校ID映射"""
    engine = create_engine(DATABASE_URL, echo=False)
    
    print("="*80)
    print("应用G4批次学校ID映射")
    print("="*80)
    
    # 预定义的核心映射（基于之前分析的高频学校）
    core_mappings = [
        {'old_code': 'SCH_0154', 'old_name': '绵竹市齐天镇(小学)', 'new_code': '5094', 'new_name': '绵竹市齐天镇(小学)'},
        {'old_code': 'SCH_0153', 'old_name': '绵竹市孝德镇(小学)', 'new_code': '5083', 'new_name': '绵竹市孝德镇(小学)'},
        {'old_code': 'SCH_0155', 'old_name': '什邡市师古(小学)', 'new_code': '5057', 'new_name': '什邡市师古(小学)'},
        {'old_code': 'SCH_0015', 'old_name': '玉泉(小学)', 'new_code': '5048', 'new_name': '玉泉(小学)'},
        {'old_code': 'SCH_0157', 'old_name': '罗汉(小学)', 'new_code': '5084', 'new_name': '罗汉(小学)'},
        {'old_code': 'SCH_0014', 'old_name': '上仓(小学)', 'new_code': '5068', 'new_name': '上仓(小学)'},
        {'old_code': 'SCH_0126', 'old_name': '洛水(远程)(小学)', 'new_code': '5096', 'new_name': '洛水(远程)(小学)'},
        {'old_code': 'SCH_0011', 'old_name': '禾丰(小学)', 'new_code': '5047', 'new_name': '禾丰(小学)'},
        {'old_code': 'SCH_0146', 'old_name': '五福(小学)', 'new_code': '5051', 'new_name': '五福(小学)'},
        {'old_code': 'SCH_0150', 'old_name': '金鱼(小学)', 'new_code': '5077', 'new_name': '金鱼(小学)'},
        {'old_code': 'SCH_0132', 'old_name': '隐丰(小学)', 'new_code': '5056', 'new_name': '隐丰(小学)'},
        {'old_code': 'SCH_0142', 'old_name': '石亭江(远程)(小学)', 'new_code': '5074', 'new_name': '石亭江(远程)(小学)'},
        {'old_code': 'SCH_0003', 'old_name': '天池(小学)', 'new_code': '5072', 'new_name': '天池(小学)'},
        {'old_code': 'SCH_0007', 'old_name': '岷城路(小学)', 'new_code': '5090', 'new_name': '岷城路(小学)'},
        {'old_code': 'SCH_0013', 'old_name': '湔江实验(二)(小学)', 'new_code': '5071', 'new_name': '湔江实验(二)(小学)'},
        {'old_code': 'SCH_0127', 'old_name': '木鱼山学校(小学)', 'new_code': '5067', 'new_name': '木鱼山学校(小学)'},
        {'old_code': 'SCH_0002', 'old_name': '马原(小学)', 'new_code': '5073', 'new_name': '马原(小学)'},
        {'old_code': 'SCH_0156', 'old_name': '渔江(小学)', 'new_code': '5093', 'new_name': '渔江(小学)'},
        {'old_code': 'SCH_0017', 'old_name': '湔江实验(小学)', 'new_code': '5076', 'new_name': '湔江实验(小学)'},
        {'old_code': 'SCH_0131', 'old_name': '兴隆(小学)', 'new_code': '5097', 'new_name': '兴隆(小学)'},
        # 重要：还需要处理其他所有学校的映射...
    ]
    
    # 获取所有需要映射的学校
    print("1. 获取所有G4学校数据...")
    with engine.begin() as conn:
        # 获取所有当前的学校
        current_schools_query = text("""
            SELECT DISTINCT school_code, school_name, COUNT(*) as student_count
            FROM student_cleaned_scores 
            WHERE batch_code = 'G4-2025'
            GROUP BY school_code, school_name
            ORDER BY student_count DESC
        """)
        current_schools = conn.execute(current_schools_query).fetchall()
        print(f"找到 {len(current_schools)} 个不同的学校代码")
        
        # 获取所有master学校
        master_schools_query = text("""
            SELECT school_id, standard_school_name
            FROM school_master_data 
            WHERE batch_code = 'G4-2025' AND status = 'ACTIVE'
            ORDER BY school_id
        """)
        master_schools = conn.execute(master_schools_query).fetchall()
        master_dict = {row[1]: row[0] for row in master_schools}  # name -> id
        print(f"找到 {len(master_schools)} 个标准学校")
        
        # 创建完整映射
        full_mappings = []
        for row in current_schools:
            old_code = row[0]
            old_name = row[1]
            student_count = row[2]
            
            # 清理学校名称进行匹配
            clean_name = old_name.replace('(小学)', '').replace('小学', '').strip()
            
            # 寻找最佳匹配
            best_match_id = None
            for master_name, master_id in master_dict.items():
                master_clean = master_name.replace('(小学)', '').replace('小学', '').strip()
                if clean_name == master_clean or clean_name in master_clean or master_clean in clean_name:
                    best_match_id = master_id
                    best_match_name = master_name
                    break
            
            if best_match_id:
                full_mappings.append({
                    'old_code': old_code,
                    'old_name': old_name,
                    'new_code': best_match_id,
                    'new_name': best_match_name,
                    'student_count': student_count
                })
        
        print(f"创建了 {len(full_mappings)} 个映射")
        
        # 应用映射
        print("2. 应用学校ID映射...")
        total_updated = 0
        
        for mapping in full_mappings:
            update_query = text("""
                UPDATE student_cleaned_scores 
                SET school_code = :new_code,
                    school_name = :new_name
                WHERE batch_code = 'G4-2025' 
                    AND school_code = :old_code
            """)
            
            result = conn.execute(update_query, {
                'new_code': mapping['new_code'],
                'new_name': mapping['new_name'],
                'old_code': mapping['old_code']
            })
            
            rows_updated = result.rowcount
            total_updated += rows_updated
            
            if rows_updated > 0:
                print(f"  {mapping['old_code']} -> {mapping['new_code']}: {rows_updated} 条记录")
        
        print(f"\n总计更新了 {total_updated} 条记录")
        
        # 验证结果
        print("3. 验证映射结果...")
        validation_query = text("""
            SELECT COUNT(DISTINCT scs.school_code) as matched_schools
            FROM student_cleaned_scores scs
            INNER JOIN school_master_data smd 
                ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
                AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
                AND smd.status = 'ACTIVE'
            WHERE scs.batch_code = 'G4-2025'
        """)
        
        matched_count = conn.execute(validation_query).scalar()
        print(f"映射后能够匹配的学校数量: {matched_count}")
        
        return matched_count > 0


if __name__ == "__main__":
    success = apply_g4_school_id_mappings()
    if success:
        print("✅ G4学校ID映射应用成功!")
    else:
        print("❌ G4学校ID映射应用失败!")