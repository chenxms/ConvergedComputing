#!/usr/bin/env python3
"""
School Master Data Analysis Script
分析school_master_data表结构和G4批次数据问题
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import json
from typing import Dict, Any, List

# 设置数据库连接
DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"

def create_db_engine() -> Engine:
    """创建数据库引擎"""
    return create_engine(DATABASE_URL, echo=False)

def analyze_table_structure(engine: Engine, table_name: str) -> Dict[str, Any]:
    """分析表结构"""
    print(f"\n=== 分析 {table_name} 表结构 ===")
    
    # 获取表结构
    structure_query = f"DESCRIBE {table_name}"
    structure_df = pd.read_sql(structure_query, engine)
    
    print("表结构:")
    print(structure_df.to_string(index=False))
    
    # 获取表统计信息
    count_query = f"SELECT COUNT(*) as total_count FROM {table_name}"
    count_result = pd.read_sql(count_query, engine)
    total_count = count_result['total_count'].iloc[0]
    
    print(f"\n总记录数: {total_count}")
    
    # 检查重复性
    if 'school_id' in structure_df['Field'].values:
        duplicate_query = f"""
        SELECT school_id, COUNT(*) as count 
        FROM {table_name} 
        GROUP BY school_id 
        HAVING COUNT(*) > 1
        """
        duplicate_df = pd.read_sql(duplicate_query, engine)
        
        if not duplicate_df.empty:
            print(f"\n发现重复的school_id ({len(duplicate_df)}个):")
            print(duplicate_df.to_string(index=False))
        else:
            print("\n✓ school_id字段无重复值")
    
    return {
        'structure': structure_df,
        'total_count': total_count,
        'duplicates': duplicate_df if 'school_id' in structure_df['Field'].values else pd.DataFrame()
    }

def get_sample_data(engine: Engine, table_name: str, limit: int = 10) -> pd.DataFrame:
    """获取样本数据"""
    query = f"SELECT * FROM {table_name} LIMIT {limit}"
    return pd.read_sql(query, engine)

def analyze_school_master_data(engine: Engine) -> Dict[str, Any]:
    """分析school_master_data表的详细信息"""
    print("\n" + "="*60)
    print("分析 school_master_data 表")
    print("="*60)
    
    # 基本结构分析
    table_info = analyze_table_structure(engine, 'school_master_data')
    
    # 获取样本数据
    sample_data = get_sample_data(engine, 'school_master_data', 20)
    print(f"\n前20条样本数据:")
    print(sample_data.to_string(index=False))
    
    # 分析school_id字段特征
    school_id_analysis = f"""
    SELECT 
        COUNT(DISTINCT school_id) as unique_school_ids,
        MIN(LENGTH(school_id)) as min_length,
        MAX(LENGTH(school_id)) as max_length,
        AVG(LENGTH(school_id)) as avg_length
    FROM school_master_data
    WHERE school_id IS NOT NULL
    """
    
    school_id_stats = pd.read_sql(school_id_analysis, engine)
    print(f"\nschool_id字段统计:")
    print(school_id_stats.to_string(index=False))
    
    # 分析standard_school_name完整性
    school_name_analysis = f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(standard_school_name) as non_null_names,
        COUNT(DISTINCT standard_school_name) as unique_names,
        COUNT(*) - COUNT(standard_school_name) as null_names
    FROM school_master_data
    """
    
    name_stats = pd.read_sql(school_name_analysis, engine)
    print(f"\nstandard_school_name字段统计:")
    print(name_stats.to_string(index=False))
    
    return {
        'table_info': table_info,
        'sample_data': sample_data,
        'school_id_stats': school_id_stats,
        'name_stats': name_stats
    }

def analyze_g4_batch_data(engine: Engine) -> Dict[str, Any]:
    """分析当前G4批次数据问题"""
    print("\n" + "="*60)
    print("分析 G4 批次数据问题")
    print("="*60)
    
    # 检查G4批次汇聚表中的学校信息
    g4_school_query = """
    SELECT DISTINCT 
        school_id,
        school_name,
        COUNT(*) as record_count
    FROM grade_aggregation_main 
    WHERE batch_code = 'G4-2025'
    GROUP BY school_id, school_name
    ORDER BY school_id
    """
    
    g4_schools = pd.read_sql(g4_school_query, engine)
    print(f"G4批次汇聚表中的学校信息 (总计{len(g4_schools)}所学校):")
    print(g4_schools.to_string(index=False))
    
    # 对比school_master_data中的学校信息
    master_schools_query = """
    SELECT 
        school_id,
        standard_school_name as school_name
    FROM school_master_data
    ORDER BY school_id
    """
    
    master_schools = pd.read_sql(master_schools_query, engine)
    print(f"\nschool_master_data中的学校信息 (总计{len(master_schools)}所学校):")
    print(master_schools.head(20).to_string(index=False))
    print(f"... (显示前20条，总计{len(master_schools)}条)")
    
    # 找出差异
    g4_school_ids = set(g4_schools['school_id'].astype(str))
    master_school_ids = set(master_schools['school_id'].astype(str))
    
    # G4中有但master中没有的
    g4_only = g4_school_ids - master_school_ids
    # Master中有但G4中没有的  
    master_only = master_school_ids - g4_school_ids
    # 共同的
    common = g4_school_ids & master_school_ids
    
    print(f"\n=== 数据差异分析 ===")
    print(f"G4批次中的学校数量: {len(g4_school_ids)}")
    print(f"Master表中的学校数量: {len(master_school_ids)}")
    print(f"共同的学校数量: {len(common)}")
    print(f"仅在G4中的学校数量: {len(g4_only)}")
    print(f"仅在Master中的学校数量: {len(master_only)}")
    
    if g4_only:
        print(f"\n仅在G4中的school_id: {sorted(list(g4_only))}")
    
    if master_only:
        print(f"\n仅在Master中的school_id (前20个): {sorted(list(master_only))[:20]}")
    
    # 检查school_name的差异
    if len(common) > 0:
        name_comparison_query = """
        SELECT 
            g.school_id,
            g.school_name as g4_name,
            m.standard_school_name as master_name,
            CASE WHEN g.school_name = m.standard_school_name THEN '一致' ELSE '不一致' END as name_match
        FROM (
            SELECT DISTINCT school_id, school_name 
            FROM grade_aggregation_main 
            WHERE batch_code = 'G4-2025'
        ) g
        JOIN school_master_data m ON CAST(g.school_id AS CHAR) = CAST(m.school_id AS CHAR)
        ORDER BY g.school_id
        """
        
        name_comparison = pd.read_sql(name_comparison_query, engine)
        
        # 统计name匹配情况
        name_match_stats = name_comparison['name_match'].value_counts()
        print(f"\n=== 学校名称匹配情况 ===")
        print(name_match_stats)
        
        # 显示不匹配的案例
        name_mismatches = name_comparison[name_comparison['name_match'] == '不一致']
        if not name_mismatches.empty:
            print(f"\n学校名称不匹配的案例 (前10个):")
            print(name_mismatches.head(10).to_string(index=False))
    
    return {
        'g4_schools': g4_schools,
        'master_schools': master_schools,
        'g4_only': list(g4_only),
        'master_only': list(master_only),
        'common': list(common),
        'name_comparison': name_comparison if 'name_comparison' in locals() else pd.DataFrame()
    }

def analyze_data_processing_flow(engine: Engine) -> Dict[str, Any]:
    """分析数据处理流程中的学校信息来源"""
    print("\n" + "="*60)
    print("分析数据处理流程")
    print("="*60)
    
    # 检查学生成绩详情表中的学校信息来源
    student_score_query = """
    SELECT DISTINCT 
        school_id,
        COUNT(*) as student_count
    FROM student_score_detail
    WHERE batch_code = 'G4-2025'
    GROUP BY school_id
    ORDER BY school_id
    LIMIT 20
    """
    
    student_schools = pd.read_sql(student_score_query, engine)
    print(f"student_score_detail表中G4批次的学校信息 (前20个):")
    print(student_schools.to_string(index=False))
    
    # 检查是否有其他相关表使用了学校信息
    tables_with_school_id = [
        'student_score_detail',
        'grade_aggregation_main', 
        'school_aggregation_main',
        'regional_aggregation_main'
    ]
    
    school_id_sources = {}
    for table in tables_with_school_id:
        try:
            query = f"""
            SELECT DISTINCT school_id, COUNT(*) as count
            FROM {table}
            WHERE batch_code = 'G4-2025'
            GROUP BY school_id
            ORDER BY school_id
            LIMIT 5
            """
            result = pd.read_sql(query, engine)
            school_id_sources[table] = result
            print(f"\n{table}表中的school_id样本:")
            print(result.to_string(index=False))
        except Exception as e:
            print(f"\n{table}表查询失败: {e}")
            school_id_sources[table] = None
    
    return {
        'student_schools': student_schools,
        'school_id_sources': school_id_sources
    }

def main():
    """主函数"""
    try:
        engine = create_db_engine()
        print("成功连接到数据库")
        
        # 1. 分析school_master_data表
        master_data_analysis = analyze_school_master_data(engine)
        
        # 2. 分析G4批次数据问题
        g4_data_analysis = analyze_g4_batch_data(engine)
        
        # 3. 分析数据处理流程
        flow_analysis = analyze_data_processing_flow(engine)
        
        # 生成分析报告
        report = {
            'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
            'summary': {
                'school_master_data_count': master_data_analysis['table_info']['total_count'],
                'g4_schools_count': len(g4_data_analysis['g4_schools']),
                'common_schools': len(g4_data_analysis['common']),
                'g4_only_schools': len(g4_data_analysis['g4_only']),
                'master_only_schools': len(g4_data_analysis['master_only'])
            },
            'master_data_analysis': master_data_analysis,
            'g4_data_analysis': g4_data_analysis,
            'flow_analysis': flow_analysis
        }
        
        print("\n" + "="*60)
        print("分析报告摘要")
        print("="*60)
        print(f"分析时间: {report['timestamp']}")
        print(f"school_master_data总记录数: {report['summary']['school_master_data_count']}")
        print(f"G4批次学校数量: {report['summary']['g4_schools_count']}")
        print(f"共同学校数量: {report['summary']['common_schools']}")
        print(f"仅在G4中的学校: {report['summary']['g4_only_schools']}")
        print(f"仅在Master中的学校: {report['summary']['master_only_schools']}")
        
        # 保存详细报告到文件
        with open('school_data_analysis_report.json', 'w', encoding='utf-8') as f:
            # 转换DataFrame为可序列化的格式
            serializable_report = {}
            for key, value in report.items():
                if isinstance(value, dict):
                    serializable_report[key] = {}
                    for sub_key, sub_value in value.items():
                        if isinstance(sub_value, pd.DataFrame):
                            serializable_report[key][sub_key] = sub_value.to_dict('records')
                        else:
                            serializable_report[key][sub_value] = sub_value
                else:
                    serializable_report[key] = value
            
            json.dump(serializable_report, f, ensure_ascii=False, indent=2)
        
        print(f"\n详细报告已保存到: school_data_analysis_report.json")
        
    except Exception as e:
        print(f"分析过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()