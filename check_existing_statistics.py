#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查G7-2025现有统计数据状态
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import json


def check_existing_statistics():
    """检查现有统计数据"""
    print("=" * 60)
    print("G7-2025 现有统计数据检查")
    print("=" * 60)
    print()
    
    db = next(get_db())
    batch_code = "G7-2025"
    
    try:
        # 检查statistical_aggregations表
        print("1. 检查statistical_aggregations表")
        print("-" * 40)
        
        result = db.execute(text("SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = :batch"), 
                           {'batch': batch_code})
        count = result.fetchone()[0]
        print(f"G7-2025统计记录数: {count}")
        
        if count > 0:
            # 获取详细信息
            result = db.execute(text("""
                SELECT aggregation_level, school_id, school_name, 
                       calculation_status, total_students, created_at
                FROM statistical_aggregations 
                WHERE batch_code = :batch
                ORDER BY aggregation_level, school_id
            """), {'batch': batch_code})
            
            records = result.fetchall()
            for record in records:
                level = record.aggregation_level
                school = record.school_id or "区域级"
                school_name = record.school_name or "全区域"
                status = record.calculation_status
                students = record.total_students
                created = record.created_at
                print(f"  {level}: {school} ({school_name}) - {status} - {students}学生 - {created}")
                
                # 检查一个样例的statistics_data结构
                if school == "区域级":
                    result2 = db.execute(text("""
                        SELECT statistics_data 
                        FROM statistical_aggregations 
                        WHERE batch_code = :batch AND aggregation_level = :level AND school_id IS NULL
                        LIMIT 1
                    """), {'batch': batch_code, 'level': level})
                    
                    data_row = result2.fetchone()
                    if data_row and data_row.statistics_data:
                        try:
                            stats_data = json.loads(data_row.statistics_data) if isinstance(data_row.statistics_data, str) else data_row.statistics_data
                            if 'academic_subjects' in stats_data:
                                subjects = list(stats_data['academic_subjects'].keys())
                                print(f"    包含科目: {', '.join(subjects)}")
                                
                                # 检查第一个科目的数据结构
                                if subjects:
                                    first_subject = stats_data['academic_subjects'][subjects[0]]
                                    has_percentiles = 'percentiles' in first_subject
                                    has_grade_dist = 'grade_distribution' in first_subject
                                    print(f"    数据结构: 百分位数={has_percentiles}, 等级分布={has_grade_dist}")
                        except Exception as e:
                            print(f"    数据解析错误: {e}")
        
        print()
        
        # 检查regional_statistics_summary表
        print("2. 检查regional_statistics_summary表")
        print("-" * 40)
        
        result = db.execute(text("SELECT COUNT(*) FROM regional_statistics_summary WHERE batch_code = :batch"), 
                           {'batch': batch_code})
        count = result.fetchone()[0]
        print(f"G7-2025区域统计记录数: {count}")
        
        if count > 0:
            result = db.execute(text("""
                SELECT subject_name, total_students, average_score, calculated_at
                FROM regional_statistics_summary 
                WHERE batch_code = :batch
                ORDER BY subject_name
            """), {'batch': batch_code})
            
            for record in result.fetchall():
                subject = record.subject_name
                students = record.total_students
                avg_score = record.average_score
                calc_time = record.calculated_at
                print(f"  {subject}: {students}学生, 平均分{avg_score:.2f}, 计算于{calc_time}")
        
        print()
        
        # 检查school_statistics_summary表
        print("3. 检查school_statistics_summary表")
        print("-" * 40)
        
        result = db.execute(text("SELECT COUNT(*) FROM school_statistics_summary WHERE batch_code = :batch"), 
                           {'batch': batch_code})
        count = result.fetchone()[0]
        print(f"G7-2025学校统计记录数: {count}")
        
        if count > 0:
            result = db.execute(text("""
                SELECT COUNT(DISTINCT school_id) as unique_schools,
                       MIN(calculated_at) as first_calc,
                       MAX(calculated_at) as last_calc
                FROM school_statistics_summary 
                WHERE batch_code = :batch
            """), {'batch': batch_code})
            
            summary = result.fetchone()
            schools = summary.unique_schools
            first_calc = summary.first_calc
            last_calc = summary.last_calc
            print(f"  涉及学校数: {schools}")
            print(f"  计算时间范围: {first_calc} 到 {last_calc}")
            
            # 列出前几个学校
            result = db.execute(text("""
                SELECT school_code, school_name, total_students, calculated_at
                FROM school_statistics_summary 
                WHERE batch_code = :batch
                ORDER BY school_code
                LIMIT 5
            """), {'batch': batch_code})
            
            print("  学校样例:")
            for record in result.fetchall():
                code = record.school_code
                name = record.school_name
                students = record.total_students
                calc_time = record.calculated_at
                print(f"    {code} ({name}): {students}学生, {calc_time}")
        
        print()
        
        # 检查基础数据 - 学生数和科目数
        print("4. 基础数据验证")
        print("-" * 40)
        
        result = db.execute(text("""
            SELECT 
                COUNT(DISTINCT student_id) as unique_students,
                COUNT(DISTINCT school_id) as unique_schools,
                COUNT(DISTINCT subject_name) as unique_subjects,
                COUNT(*) as total_records
            FROM student_score_detail 
            WHERE batch_code = :batch
        """), {'batch': batch_code})
        
        base_stats = result.fetchone()
        print(f"基础数据统计:")
        print(f"  唯一学生数: {base_stats.unique_students}")
        print(f"  唯一学校数: {base_stats.unique_schools}")
        print(f"  唯一科目数: {base_stats.unique_subjects}")
        print(f"  总记录数: {base_stats.total_records}")
        
        # 列出所有科目
        result = db.execute(text("""
            SELECT DISTINCT subject_name 
            FROM student_score_detail 
            WHERE batch_code = :batch
            ORDER BY subject_name
        """), {'batch': batch_code})
        
        subjects = [row[0] for row in result.fetchall()]
        print(f"  科目列表: {', '.join(subjects)}")
        
        # 检查科目配置
        result = db.execute(text("""
            SELECT subject_name, max_score
            FROM subject_question_config 
            WHERE batch_code = :batch
            GROUP BY subject_name, max_score
            ORDER BY subject_name
        """), {'batch': batch_code})
        
        print("  科目配置:")
        for record in result.fetchall():
            subject = record.subject_name
            max_score = record.max_score
            print(f"    {subject}: 满分{max_score}")
        
        print()
        
        # 总结当前状态
        print("5. 状态总结")
        print("-" * 40)
        
        has_aggregations = False
        needs_calculation = True
        
        # 检查是否有完整的区域级数据
        result = db.execute(text("""
            SELECT COUNT(*) FROM statistical_aggregations 
            WHERE batch_code = :batch AND aggregation_level = 'REGIONAL'
        """), {'batch': batch_code})
        regional_count = result.fetchone()[0]
        
        if regional_count > 0:
            print("✓ 发现区域级统计数据")
            has_aggregations = True
            
            # 检查数据是否包含多科目
            result = db.execute(text("""
                SELECT statistics_data 
                FROM statistical_aggregations 
                WHERE batch_code = :batch AND aggregation_level = 'REGIONAL'
                LIMIT 1
            """), {'batch': batch_code})
            
            data_row = result.fetchone()
            if data_row and data_row.statistics_data:
                try:
                    stats_data = json.loads(data_row.statistics_data) if isinstance(data_row.statistics_data, str) else data_row.statistics_data
                    if 'academic_subjects' in stats_data:
                        subject_count = len(stats_data['academic_subjects'])
                        if subject_count >= 10:  # 预期11个科目
                            print(f"✓ 区域级数据包含{subject_count}个科目，数据较完整")
                            needs_calculation = False
                        else:
                            print(f"⚠ 区域级数据只有{subject_count}个科目，可能不完整")
                except Exception as e:
                    print(f"✗ 区域级数据解析失败: {e}")
        else:
            print("✗ 没有区域级统计数据")
        
        # 检查学校级数据
        result = db.execute(text("""
            SELECT COUNT(*) FROM statistical_aggregations 
            WHERE batch_code = :batch AND aggregation_level = 'SCHOOL'
        """), {'batch': batch_code})
        school_agg_count = result.fetchone()[0]
        
        if school_agg_count > 0:
            print(f"✓ 发现{school_agg_count}个学校级统计数据")
        else:
            print("✗ 没有学校级统计数据")
            
        print()
        
        if needs_calculation:
            print("建议: 需要执行完整的多科目统计计算")
        else:
            print("建议: 现有数据较完整，可以进行验证测试")
            
        return {
            'has_data': base_stats.total_records > 0,
            'student_count': base_stats.unique_students,
            'school_count': base_stats.unique_schools, 
            'subject_count': base_stats.unique_subjects,
            'has_regional_stats': regional_count > 0,
            'has_school_stats': school_agg_count > 0,
            'needs_calculation': needs_calculation
        }
        
    except Exception as e:
        print(f"检查失败: {e}")
        return {'error': str(e)}
    finally:
        db.close()


if __name__ == "__main__":
    result = check_existing_statistics()
    print(f"\n检查结果: {result}")