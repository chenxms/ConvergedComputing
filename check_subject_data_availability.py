#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查G7-2025各科目数据可用性
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text


def check_subject_data_availability():
    """检查各科目数据可用性"""
    print("=" * 60)
    print("G7-2025 科目数据可用性检查")
    print("=" * 60)
    print()
    
    db = next(get_db())
    batch_code = "G7-2025"
    
    try:
        # 获取所有科目及其数据量
        query = text("""
            SELECT 
                subject_name,
                COUNT(*) as record_count,
                COUNT(DISTINCT student_id) as unique_students,
                COUNT(DISTINCT school_id) as unique_schools,
                AVG(total_score) as avg_score,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score
            FROM student_score_detail 
            WHERE batch_code = :batch_code
            GROUP BY subject_name
            ORDER BY record_count DESC
        """)
        
        result = db.execute(query, {'batch_code': batch_code})
        subjects_data = result.fetchall()
        
        print(f"发现 {len(subjects_data)} 个科目:")
        print()
        
        total_expected_students = 0
        subjects_with_data = []
        
        for subject in subjects_data:
            subject_name = subject.subject_name
            record_count = subject.record_count
            unique_students = subject.unique_students
            unique_schools = subject.unique_schools  
            avg_score = subject.avg_score
            min_score = subject.min_score
            max_score = subject.max_score
            
            # 设定期望的最小学生数（基于最大科目的学生数）
            if total_expected_students < unique_students:
                total_expected_students = unique_students
            
            # 获取该科目的满分信息
            config_query = text("""
                SELECT max_score 
                FROM subject_question_config 
                WHERE batch_code = :batch_code AND subject_name = :subject_name
                LIMIT 1
            """)
            
            config_result = db.execute(config_query, {
                'batch_code': batch_code,
                'subject_name': subject_name
            })
            config_row = config_result.fetchone()
            max_possible_score = config_row.max_score if config_row else "未知"
            
            # 计算完整性
            completeness = "完整" if unique_students >= total_expected_students * 0.9 else "不完整"
            
            subjects_with_data.append({
                'name': subject_name,
                'students': unique_students, 
                'records': record_count,
                'complete': completeness == "完整"
            })
            
            print(f"{subject_name}:")
            print(f"  记录数: {record_count:,}")
            print(f"  学生数: {unique_students:,}")
            print(f"  学校数: {unique_schools}")
            print(f"  分数范围: {min_score:.1f} - {max_score:.1f} (满分{max_possible_score})")
            print(f"  平均分: {avg_score:.2f}")
            print(f"  完整性: {completeness}")
            print()
        
        print("=" * 40)
        print("数据完整性总结")
        print("=" * 40)
        
        complete_subjects = [s for s in subjects_with_data if s['complete']]
        incomplete_subjects = [s for s in subjects_with_data if not s['complete']]
        
        print(f"数据完整的科目 ({len(complete_subjects)}个):")
        for subject in complete_subjects:
            print(f"  ✓ {subject['name']}: {subject['students']:,}学生")
        
        if incomplete_subjects:
            print(f"\n数据不完整的科目 ({len(incomplete_subjects)}个):")
            for subject in incomplete_subjects:
                print(f"  ⚠ {subject['name']}: {subject['students']:,}学生")
        
        print(f"\n建议:")
        if len(complete_subjects) >= 8:
            print(f"✓ 可以进行多科目计算，使用 {len(complete_subjects)} 个完整科目")
            print(f"✓ 预期学生数: {total_expected_students:,}")
        else:
            print(f"⚠ 数据完整科目较少，建议检查数据导入")
        
        return {
            'complete_subjects': [s['name'] for s in complete_subjects],
            'incomplete_subjects': [s['name'] for s in incomplete_subjects],
            'total_students': total_expected_students,
            'can_proceed': len(complete_subjects) >= 5
        }
        
    except Exception as e:
        print(f"检查失败: {e}")
        import traceback
        traceback.print_exc()
        return {'error': str(e)}
    finally:
        db.close()


if __name__ == "__main__":
    result = check_subject_data_availability()
    print(f"\n检查结果: {result}")