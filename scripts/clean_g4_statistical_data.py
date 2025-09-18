#!/usr/bin/env python3
"""
清理G4批次statistical_aggregations表中的错误记录

问题记录包括：
1. school_name为NULL的学校级记录
2. 使用错误school_id格式的记录 
3. 重复的区域级记录
4. 不在school_master_data中的学校记录

执行步骤：
1. 备份当前数据
2. 删除错误记录
3. 验证清理结果
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import json
from datetime import datetime
from typing import Dict, Any, List

# 设置数据库连接
DATABASE_URL = os.getenv("DATABASE_URL", 
    "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4")

def create_db_engine() -> Engine:
    """创建数据库引擎"""
    return create_engine(DATABASE_URL, echo=False)

def backup_g4_data(engine: Engine) -> Dict[str, Any]:
    """备份G4批次当前数据"""
    print("=== 备份G4批次数据 ===")
    
    backup_query = """
    SELECT * FROM statistical_aggregations 
    WHERE batch_code = 'G4-2025'
    ORDER BY id
    """
    
    backup_data = pd.read_sql(backup_query, engine)
    
    # 保存备份文件
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_filename = f"statistical_aggregations_g4_backup_{timestamp}.json"
    
    backup_data.to_json(backup_filename, orient='records', ensure_ascii=False, indent=2)
    
    print(f"备份文件已保存: {backup_filename}")
    print(f"备份记录数量: {len(backup_data)}")
    print(f"学校级记录: {len(backup_data[backup_data['aggregation_level'] == 'SCHOOL'])}")
    print(f"区域级记录: {len(backup_data[backup_data['aggregation_level'] == 'REGIONAL'])}")
    
    return {
        'backup_filename': backup_filename,
        'total_records': len(backup_data),
        'backup_data': backup_data
    }

def analyze_problem_records(engine: Engine) -> Dict[str, Any]:
    """分析问题记录"""
    print("\n=== 分析问题记录 ===")
    
    # 分析学校级记录问题
    school_analysis_query = """
    SELECT 
        school_id,
        school_name,
        COUNT(*) as record_count,
        CASE 
            WHEN school_name IS NULL THEN 'NULL名称'
            WHEN school_id LIKE 'SCH_%' THEN '自动编号ID'
            WHEN school_id LIKE 'INVALID%' THEN '无效ID'
            WHEN school_id = 'UNKNOWN' THEN '未知ID'
            ELSE '其他'
        END as problem_type
    FROM statistical_aggregations 
    WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
    GROUP BY school_id, school_name
    ORDER BY record_count DESC, school_id
    """
    
    school_problems = pd.read_sql(school_analysis_query, engine)
    print(f"学校级问题记录分析:")
    print(school_problems.to_string(index=False))
    
    # 分析区域级记录问题
    regional_analysis_query = """
    SELECT 
        school_id,
        school_name,
        COUNT(*) as record_count
    FROM statistical_aggregations 
    WHERE batch_code = 'G4-2025' AND aggregation_level = 'REGIONAL'
    GROUP BY school_id, school_name
    ORDER BY record_count DESC
    """
    
    regional_problems = pd.read_sql(regional_analysis_query, engine)
    print(f"\n区域级记录分析:")
    print(regional_problems.to_string(index=False))
    
    # 检查与school_master_data的匹配情况
    master_check_query = """
    SELECT 
        sa.school_id,
        sa.school_name as agg_name,
        smd.school_id as master_school_id,
        smd.standard_school_name as master_name,
        CASE 
            WHEN smd.school_id IS NOT NULL THEN '在主数据中'
            ELSE '不在主数据中'
        END as master_status
    FROM statistical_aggregations sa
    LEFT JOIN school_master_data smd 
        ON sa.batch_code = smd.batch_code 
        AND sa.school_id = smd.school_id
        AND smd.status = 'ACTIVE'
    WHERE sa.batch_code = 'G4-2025' 
        AND sa.aggregation_level = 'SCHOOL'
        AND sa.school_id IS NOT NULL
    ORDER BY master_status, sa.school_id
    """
    
    master_check = pd.read_sql(master_check_query, engine)
    print(f"\n与school_master_data匹配情况:")
    print(master_check.to_string(index=False))
    
    return {
        'school_problems': school_problems,
        'regional_problems': regional_problems,
        'master_check': master_check
    }

def clean_problem_records(engine: Engine, dry_run: bool = True) -> Dict[str, Any]:
    """清理问题记录"""
    print(f"\n=== 清理问题记录 {'(预演模式)' if dry_run else '(执行模式)'} ===")
    
    cleanup_results = {}
    
    # 1. 删除school_name为NULL的学校级记录
    null_name_query = """
    SELECT id, school_id, school_name 
    FROM statistical_aggregations 
    WHERE batch_code = 'G4-2025' 
        AND aggregation_level = 'SCHOOL' 
        AND school_name IS NULL
    """
    
    null_name_records = pd.read_sql(null_name_query, engine)
    print(f"将要删除的NULL名称记录数: {len(null_name_records)}")
    
    if not dry_run and len(null_name_records) > 0:
        delete_null_name = text("""
        DELETE FROM statistical_aggregations 
        WHERE batch_code = 'G4-2025' 
            AND aggregation_level = 'SCHOOL' 
            AND school_name IS NULL
        """)
        result = engine.execute(delete_null_name)
        print(f"已删除NULL名称记录: {result.rowcount}条")
        cleanup_results['null_name_deleted'] = result.rowcount
    
    # 2. 删除使用自动编号格式的记录
    auto_id_query = """
    SELECT id, school_id, school_name 
    FROM statistical_aggregations 
    WHERE batch_code = 'G4-2025' 
        AND aggregation_level = 'SCHOOL'
        AND (school_id LIKE 'SCH_%' 
             OR school_id LIKE 'INVALID%' 
             OR school_id = 'UNKNOWN')
    """
    
    auto_id_records = pd.read_sql(auto_id_query, engine)
    print(f"将要删除的自动编号记录数: {len(auto_id_records)}")
    
    if not dry_run and len(auto_id_records) > 0:
        delete_auto_id = text("""
        DELETE FROM statistical_aggregations 
        WHERE batch_code = 'G4-2025' 
            AND aggregation_level = 'SCHOOL'
            AND (school_id LIKE 'SCH_%' 
                 OR school_id LIKE 'INVALID%' 
                 OR school_id = 'UNKNOWN')
        """)
        result = engine.execute(delete_auto_id)
        print(f"已删除自动编号记录: {result.rowcount}条")
        cleanup_results['auto_id_deleted'] = result.rowcount
    
    # 3. 删除不在school_master_data中的学校记录
    orphan_query = """
    SELECT sa.id, sa.school_id, sa.school_name
    FROM statistical_aggregations sa
    LEFT JOIN school_master_data smd 
        ON sa.batch_code = smd.batch_code 
        AND sa.school_id = smd.school_id
        AND smd.status = 'ACTIVE'
    WHERE sa.batch_code = 'G4-2025' 
        AND sa.aggregation_level = 'SCHOOL'
        AND sa.school_id IS NOT NULL
        AND smd.school_id IS NULL
    """
    
    orphan_records = pd.read_sql(orphan_query, engine)
    print(f"将要删除的孤儿记录数: {len(orphan_records)}")
    
    if not dry_run and len(orphan_records) > 0:
        delete_orphan = text("""
        DELETE sa FROM statistical_aggregations sa
        LEFT JOIN school_master_data smd 
            ON sa.batch_code = smd.batch_code 
            AND sa.school_id = smd.school_id
            AND smd.status = 'ACTIVE'
        WHERE sa.batch_code = 'G4-2025' 
            AND sa.aggregation_level = 'SCHOOL'
            AND sa.school_id IS NOT NULL
            AND smd.school_id IS NULL
        """)
        result = engine.execute(delete_orphan)
        print(f"已删除孤儿记录: {result.rowcount}条")
        cleanup_results['orphan_deleted'] = result.rowcount
    
    # 4. 清理重复的区域级记录，保留最新的一条
    regional_cleanup_query = """
    SELECT id, created_at 
    FROM statistical_aggregations 
    WHERE batch_code = 'G4-2025' 
        AND aggregation_level = 'REGIONAL'
    ORDER BY created_at DESC
    """
    
    regional_records = pd.read_sql(regional_cleanup_query, engine)
    print(f"区域级记录总数: {len(regional_records)}")
    
    if len(regional_records) > 1:
        # 保留最新的一条，删除其他的
        keep_id = regional_records.iloc[0]['id']
        delete_ids = regional_records.iloc[1:]['id'].tolist()
        
        print(f"将保留区域级记录ID: {keep_id}")
        print(f"将删除区域级记录ID: {delete_ids}")
        
        if not dry_run and delete_ids:
            delete_regional = text(f"""
            DELETE FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' 
                AND aggregation_level = 'REGIONAL'
                AND id IN ({','.join(map(str, delete_ids))})
            """)
            result = engine.execute(delete_regional)
            print(f"已删除多余区域级记录: {result.rowcount}条")
            cleanup_results['regional_duplicates_deleted'] = result.rowcount
    
    return cleanup_results

def verify_cleanup_results(engine: Engine) -> Dict[str, Any]:
    """验证清理结果"""
    print("\n=== 验证清理结果 ===")
    
    # 检查剩余记录
    remaining_query = """
    SELECT 
        aggregation_level,
        COUNT(*) as record_count,
        COUNT(CASE WHEN school_name IS NOT NULL THEN 1 END) as with_name_count,
        COUNT(CASE WHEN school_name IS NULL THEN 1 END) as null_name_count
    FROM statistical_aggregations 
    WHERE batch_code = 'G4-2025'
    GROUP BY aggregation_level
    """
    
    remaining_stats = pd.read_sql(remaining_query, engine)
    print(f"清理后剩余记录统计:")
    print(remaining_stats.to_string(index=False))
    
    # 检查学校级记录是否都有标准名称
    school_check_query = """
    SELECT 
        sa.school_id,
        sa.school_name,
        smd.standard_school_name as master_name,
        CASE 
            WHEN smd.school_id IS NOT NULL THEN '✓'
            ELSE '✗'
        END as in_master_data
    FROM statistical_aggregations sa
    LEFT JOIN school_master_data smd 
        ON sa.batch_code = smd.batch_code 
        AND sa.school_id = smd.school_id
        AND smd.status = 'ACTIVE'
    WHERE sa.batch_code = 'G4-2025' 
        AND sa.aggregation_level = 'SCHOOL'
    ORDER BY sa.school_id
    """
    
    school_check = pd.read_sql(school_check_query, engine)
    print(f"\n剩余学校级记录验证:")
    print(school_check.to_string(index=False))
    
    return {
        'remaining_stats': remaining_stats,
        'school_check': school_check
    }

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='清理G4批次statistical_aggregations表中的错误记录')
    parser.add_argument('--dry-run', action='store_true', default=True, 
                       help='预演模式，不实际删除数据（默认）')
    parser.add_argument('--execute', action='store_true', 
                       help='执行模式，实际删除数据')
    
    args = parser.parse_args()
    
    # 如果指定了 --execute，则关闭dry_run
    dry_run = not args.execute
    
    if not dry_run:
        confirm = input("警告：即将实际删除数据！请输入 'DELETE' 确认继续: ")
        if confirm != 'DELETE':
            print("操作已取消")
            return
    
    try:
        engine = create_db_engine()
        print("成功连接到数据库")
        
        # 1. 备份数据
        backup_info = backup_g4_data(engine)
        
        # 2. 分析问题记录
        problem_analysis = analyze_problem_records(engine)
        
        # 3. 清理问题记录
        cleanup_results = clean_problem_records(engine, dry_run)
        
        # 4. 验证清理结果
        verification_results = verify_cleanup_results(engine)
        
        # 生成报告
        report = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'mode': 'dry_run' if dry_run else 'execute',
            'backup_info': backup_info,
            'problem_analysis': problem_analysis,
            'cleanup_results': cleanup_results,
            'verification_results': verification_results
        }
        
        # 保存报告
        report_filename = f"g4_cleanup_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # 转换DataFrame为可序列化格式
        serializable_report = {}
        for key, value in report.items():
            if isinstance(value, dict):
                serializable_report[key] = {}
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, pd.DataFrame):
                        serializable_report[key][sub_key] = sub_value.to_dict('records')
                    else:
                        serializable_report[key][sub_key] = sub_value
            else:
                serializable_report[key] = value
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(serializable_report, f, ensure_ascii=False, indent=2)
        
        print(f"\n清理报告已保存: {report_filename}")
        
        if dry_run:
            print("\n*** 这是预演模式，没有实际删除数据 ***")
            print("要实际执行删除，请使用: python clean_g4_statistical_data.py --execute")
        else:
            print("\n*** 数据清理已完成 ***")
            print("请重新运行数据汇聚以生成正确的统计数据")
        
    except Exception as e:
        print(f"清理过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()