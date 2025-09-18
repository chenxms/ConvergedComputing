#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重新执行G4批次汇聚计算脚本

修复StatisticalAggregationRepository后，重新生成正确的G4批次统计数据

执行步骤：
1. 验证数据清洗是否完成
2. 清理已有的错误汇聚数据
3. 使用修复后的代码重新生成汇聚数据
4. 验证结果正确性
"""

import os
import sys
import time
from typing import Tuple, Dict, Any
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database.connection import get_db
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus
from app.services.subjects_builder import SubjectsBuilder
from app.utils.precision import round2_json


def _get_db_url() -> str:
    """获取数据库连接URL"""
    url = os.getenv("DATABASE_URL")
    if url and url.strip():
        return url
    return "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"


def verify_data_cleaning(batch_code: str) -> Dict[str, Any]:
    """验证数据清洗是否完成"""
    print(f"=== 验证 {batch_code} 批次数据清洗状态 ===")
    
    db = next(get_db())
    try:
        # 检查student_cleaned_scores表
        cleaned_count = db.execute(text("""
            SELECT COUNT(*) 
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
        """), {"batch_code": batch_code}).scalar() or 0
        
        # 检查原始数据
        raw_count = db.execute(text("""
            SELECT COUNT(*) 
            FROM student_score_detail 
            WHERE batch_code = :batch_code
        """), {"batch_code": batch_code}).scalar() or 0
        
        # 检查学校信息完整性
        school_info_check = db.execute(text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT school_code) as unique_schools,
                COUNT(CASE WHEN school_name IS NOT NULL THEN 1 END) as with_names,
                COUNT(CASE WHEN school_name IS NULL THEN 1 END) as null_names
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
        """), {"batch_code": batch_code}).fetchone()
        
        # 检查与school_master_data的匹配情况
        master_match_check = db.execute(text("""
            SELECT 
                COUNT(DISTINCT scs.school_code) as cleaned_schools,
                COUNT(DISTINCT smd.school_id) as master_schools,
                COUNT(DISTINCT CASE 
                    WHEN smd.school_id IS NOT NULL THEN scs.school_code 
                END) as matched_schools
            FROM student_cleaned_scores scs
            LEFT JOIN school_master_data smd 
                ON scs.batch_code COLLATE utf8mb4_unicode_ci = smd.batch_code COLLATE utf8mb4_unicode_ci
                AND scs.school_code COLLATE utf8mb4_unicode_ci = smd.school_id COLLATE utf8mb4_unicode_ci
                AND smd.status = 'ACTIVE'
            WHERE scs.batch_code = :batch_code
        """), {"batch_code": batch_code}).fetchone()
        
        print(f"原始数据记录数: {raw_count}")
        print(f"清洗后数据记录数: {cleaned_count}")
        print(f"数据清洗覆盖率: {(cleaned_count/raw_count*100):.2f}%" if raw_count > 0 else "0%")
        print(f"学校信息完整性: {school_info_check.with_names}/{school_info_check.total_records} 有名称")
        print(f"与主数据匹配: {master_match_check.matched_schools}/{master_match_check.cleaned_schools} 学校匹配")
        
        # 检查数据清洗质量
        is_ready = (
            cleaned_count > 0 and
            cleaned_count >= raw_count * 0.95 and  # 至少95%的数据被清洗
            school_info_check.null_names == 0 and  # 没有空的学校名称
            master_match_check.matched_schools == master_match_check.cleaned_schools  # 所有学校都在主数据中
        )
        
        return {
            'is_ready': is_ready,
            'raw_count': raw_count,
            'cleaned_count': cleaned_count,
            'school_info': {
                'total_records': school_info_check.total_records,
                'unique_schools': school_info_check.unique_schools,
                'with_names': school_info_check.with_names,
                'null_names': school_info_check.null_names
            },
            'master_match': {
                'cleaned_schools': master_match_check.cleaned_schools,
                'master_schools': master_match_check.master_schools,
                'matched_schools': master_match_check.matched_schools
            },
            'coverage_rate': cleaned_count/raw_count if raw_count > 0 else 0
        }
        
    finally:
        db.close()


def clean_existing_aggregation(batch_code: str) -> int:
    """清理已有的汇聚数据"""
    print(f"=== 清理 {batch_code} 批次已有汇聚数据 ===")
    
    db = next(get_db())
    try:
        # 查询要删除的记录数
        existing_count = db.execute(text("""
            SELECT COUNT(*) 
            FROM statistical_aggregations 
            WHERE batch_code = :batch_code
        """), {"batch_code": batch_code}).scalar() or 0
        
        if existing_count > 0:
            print(f"发现 {existing_count} 条已有记录，准备清理...")
            
            # 删除已有记录
            result = db.execute(text("""
                DELETE FROM statistical_aggregations 
                WHERE batch_code = :batch_code
            """), {"batch_code": batch_code})
            
            db.commit()
            deleted_count = result.rowcount
            print(f"已清理 {deleted_count} 条记录")
            return deleted_count
        else:
            print("没有发现已有记录")
            return 0
            
    finally:
        db.close()


def rebuild_aggregation(batch_code: str) -> Tuple[int, int, Dict[str, Any]]:
    """重新构建汇聚数据"""
    print(f"=== 重新构建 {batch_code} 批次汇聚数据 ===")
    
    db = next(get_db())
    try:
        repo = StatisticalAggregationRepository(db)
        builder = SubjectsBuilder()
        
        start_time = time.time()
        
        # 统计主数据学校总数
        total_schools_active = db.execute(text("""
            SELECT COUNT(*) 
            FROM school_master_data 
            WHERE batch_code = :batch_code AND status = 'ACTIVE'
        """), {"batch_code": batch_code}).scalar() or 0
        
        print(f"主数据中活跃学校数: {total_schools_active}")
        
        # 1. 构建区域级数据
        print("构建区域级统计数据...")
        regional_subjects = builder.build_regional_subjects(batch_code)
        regional_payload = {
            'schema_version': 'v1.2',
            'batch_code': batch_code,
            'aggregation_level': 'REGIONAL',
            'subjects': regional_subjects,
        }
        
        # 使用修复后的upsert_statistics方法
        repo.upsert_statistics({
            'batch_code': batch_code,
            'aggregation_level': DBAggregationLevel.REGIONAL,
            'school_id': None,  # 区域级应该为None，不是'REGION'
            'school_name': '区域汇总',
            'statistics_data': round2_json(regional_payload),
            'data_version': 'v1.2',
            'calculation_status': CalculationStatus.COMPLETED,
            'total_schools': total_schools_active,
        })
        print("区域级统计数据已生成")
        
        # 2. 构建学校级数据（基于school_master_data）
        print("构建学校级统计数据...")
        school_rows = db.execute(text("""
            SELECT school_id, standard_school_name 
            FROM school_master_data 
            WHERE batch_code = :batch_code AND status = 'ACTIVE' 
            ORDER BY school_id
        """), {"batch_code": batch_code}).fetchall()
        
        total_schools = len(school_rows)
        successful_schools = 0
        failed_schools = []
        
        for school_row in school_rows:
            school_id = school_row[0]
            school_name = school_row[1]
            
            try:
                # 检查该学校是否有清洗后的数据
                student_count = db.execute(text("""
                    SELECT COUNT(DISTINCT student_id)
                    FROM student_cleaned_scores
                    WHERE batch_code = :batch_code 
                        AND school_code = :school_id
                        AND subject_type IN ('exam', 'questionnaire')
                """), {"batch_code": batch_code, "school_id": school_id}).scalar() or 0
                
                if student_count == 0:
                    print(f"跳过学校 {school_id}({school_name}): 无学生数据")
                    continue
                
                # 构建学校级subjects
                school_subjects = builder.build_school_subjects(batch_code, school_id)
                school_payload = {
                    'schema_version': 'v1.2',
                    'batch_code': batch_code,
                    'aggregation_level': 'SCHOOL',
                    'school_code': school_id,
                    'subjects': school_subjects,
                }
                
                # 使用修复后的upsert_statistics方法，确保正确传递学校信息
                repo.upsert_statistics({
                    'batch_code': batch_code,
                    'aggregation_level': DBAggregationLevel.SCHOOL,
                    'school_id': school_id,
                    'school_name': school_name,  # 预传递学校名称，即使resolve失败也有fallback
                    'statistics_data': round2_json(school_payload),
                    'data_version': 'v1.2',
                    'calculation_status': CalculationStatus.COMPLETED,
                    'total_students': student_count,
                    'total_schools': total_schools_active,
                })
                
                successful_schools += 1
                
                if successful_schools % 10 == 0:
                    print(f"进度: {successful_schools}/{total_schools} 学校已完成")
                    
            except Exception as e:
                print(f"学校 {school_id}({school_name}) 处理失败: {str(e)}")
                failed_schools.append((school_id, school_name, str(e)))
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"学校级统计数据构建完成")
        print(f"成功: {successful_schools}/{total_schools} 学校")
        print(f"耗时: {duration:.2f} 秒")
        
        return successful_schools, total_schools, {
            'total_schools_active': total_schools_active,
            'successful_schools': successful_schools,
            'failed_schools': failed_schools,
            'duration': duration
        }
        
    finally:
        db.close()


def verify_aggregation_results(batch_code: str) -> Dict[str, Any]:
    """验证汇聚结果"""
    print(f"=== 验证 {batch_code} 批次汇聚结果 ===")
    
    db = next(get_db())
    try:
        # 检查汇聚记录统计
        aggregation_stats = db.execute(text("""
            SELECT 
                aggregation_level,
                COUNT(*) as record_count,
                COUNT(CASE WHEN school_name IS NOT NULL THEN 1 END) as with_name_count,
                COUNT(CASE WHEN school_name IS NULL THEN 1 END) as null_name_count,
                SUM(total_students) as total_students
            FROM statistical_aggregations 
            WHERE batch_code = :batch_code
            GROUP BY aggregation_level
            ORDER BY aggregation_level
        """), {"batch_code": batch_code}).fetchall()
        
        print("汇聚结果统计:")
        for row in aggregation_stats:
            print(f"  {row.aggregation_level}: {row.record_count}条记录, "
                  f"{row.with_name_count}有名称, {row.null_name_count}无名称, "
                  f"学生总数: {row.total_students}")
        
        # 检查学校ID格式
        school_id_check = db.execute(text("""
            SELECT 
                school_id,
                school_name,
                CASE 
                    WHEN school_id LIKE 'SCH_%' THEN '自动编号'
                    WHEN school_id LIKE 'INVALID%' THEN '无效ID'
                    WHEN school_id REGEXP '^[0-9]+$' THEN '数字ID'
                    WHEN school_id IS NULL THEN 'NULL'
                    ELSE '其他格式'
                END as id_type
            FROM statistical_aggregations 
            WHERE batch_code = :batch_code AND aggregation_level = 'SCHOOL'
            ORDER BY school_id
        """), {"batch_code": batch_code}).fetchall()
        
        # 统计ID格式分布
        id_type_stats = {}
        for row in school_id_check:
            id_type = row.id_type
            id_type_stats[id_type] = id_type_stats.get(id_type, 0) + 1
        
        print("\n学校ID格式分布:")
        for id_type, count in id_type_stats.items():
            print(f"  {id_type}: {count}个")
        
        # 检查与school_master_data的匹配
        master_match = db.execute(text("""
            SELECT 
                COUNT(*) as agg_school_count,
                COUNT(CASE WHEN smd.school_id IS NOT NULL THEN 1 END) as matched_count,
                COUNT(CASE WHEN smd.school_id IS NULL THEN 1 END) as unmatched_count
            FROM statistical_aggregations sa
            LEFT JOIN school_master_data smd 
                ON sa.batch_code COLLATE utf8mb4_unicode_ci = smd.batch_code COLLATE utf8mb4_unicode_ci
                AND sa.school_id COLLATE utf8mb4_unicode_ci = smd.school_id COLLATE utf8mb4_unicode_ci
                AND smd.status = 'ACTIVE'
            WHERE sa.batch_code = :batch_code 
                AND sa.aggregation_level = 'SCHOOL'
        """), {"batch_code": batch_code}).fetchone()
        
        print(f"\n与主数据匹配情况:")
        print(f"  汇聚学校总数: {master_match.agg_school_count}")
        print(f"  匹配成功: {master_match.matched_count}")
        print(f"  未匹配: {master_match.unmatched_count}")
        
        # 检查数据质量
        quality_issues = []
        
        # 检查是否有NULL名称的学校级记录
        null_names = sum(1 for row in aggregation_stats 
                        if row.aggregation_level == 'SCHOOL' and row.null_name_count > 0)
        if null_names > 0:
            quality_issues.append(f"学校级记录中有{null_names}条NULL名称")
        
        # 检查是否有自动编号ID
        if id_type_stats.get('自动编号', 0) > 0:
            quality_issues.append(f"发现{id_type_stats['自动编号']}个自动编号ID")
        
        # 检查是否有未匹配的学校
        if master_match.unmatched_count > 0:
            quality_issues.append(f"{master_match.unmatched_count}个学校未匹配主数据")
        
        is_valid = len(quality_issues) == 0
        
        print(f"\n数据质量评估: {'✓ 通过' if is_valid else '✗ 存在问题'}")
        if quality_issues:
            print("发现的问题:")
            for issue in quality_issues:
                print(f"  - {issue}")
        
        return {
            'is_valid': is_valid,
            'aggregation_stats': [dict(row) for row in aggregation_stats],
            'id_type_stats': id_type_stats,
            'master_match': dict(master_match),
            'quality_issues': quality_issues
        }
        
    finally:
        db.close()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='重新执行G4批次汇聚计算')
    parser.add_argument('--batch-code', default='G4-2025', 
                       help='批次代码（默认：G4-2025）')
    parser.add_argument('--skip-verification', action='store_true',
                       help='跳过数据清洗验证')
    
    args = parser.parse_args()
    batch_code = args.batch_code
    
    print(f"=== {batch_code} 批次汇聚重建工具 ===")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. 验证数据清洗状态
        if not args.skip_verification:
            cleaning_status = verify_data_cleaning(batch_code)
            if not cleaning_status['is_ready']:
                print("数据清洗未完成或存在问题，请先完成数据清洗")
                print("主要问题:")
                if cleaning_status['coverage_rate'] < 0.95:
                    print(f"  - 数据清洗覆盖率过低: {cleaning_status['coverage_rate']*100:.1f}%")
                if cleaning_status['school_info']['null_names'] > 0:
                    print(f"  - 存在{cleaning_status['school_info']['null_names']}条空学校名称记录")
                if (cleaning_status['master_match']['matched_schools'] != 
                    cleaning_status['master_match']['cleaned_schools']):
                    unmatched = (cleaning_status['master_match']['cleaned_schools'] - 
                               cleaning_status['master_match']['matched_schools'])
                    print(f"  - {unmatched}个学校未匹配主数据")
                sys.exit(1)
            print("数据清洗状态正常")
        else:
            print("跳过数据清洗验证")
        
        # 2. 清理已有汇聚数据
        cleaned_count = clean_existing_aggregation(batch_code)
        
        # 3. 重新构建汇聚数据
        successful_schools, total_schools, rebuild_info = rebuild_aggregation(batch_code)
        
        # 4. 验证汇聚结果
        verification_results = verify_aggregation_results(batch_code)
        
        # 生成报告
        print(f"\n=== 重建完成报告 ===")
        print(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"清理记录数: {cleaned_count}")
        print(f"重建学校数: {successful_schools}/{total_schools}")
        print(f"重建耗时: {rebuild_info['duration']:.2f}秒")
        print(f"数据质量: {'通过' if verification_results['is_valid'] else '存在问题'}")
        
        if not verification_results['is_valid']:
            print("\n质量问题:")
            for issue in verification_results['quality_issues']:
                print(f"  - {issue}")
        
        # 保存详细报告
        report = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'batch_code': batch_code,
            'cleaned_count': cleaned_count,
            'rebuild_info': rebuild_info,
            'verification_results': verification_results
        }
        
        report_filename = f"g4_aggregation_rebuild_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        import json
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n详细报告已保存: {report_filename}")
        
        if verification_results['is_valid']:
            print("\nG4批次汇聚重建成功完成！")
        else:
            print("\nG4批次汇聚重建完成，但存在数据质量问题需要进一步检查")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n重建过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
