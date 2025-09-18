#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
为G4-2025批次创建测试数据，用于演示汇聚功能
"""

import sys
import os
import random
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db


def create_g4_test_data():
    """创建G4-2025测试数据"""
    
    with next(get_db()) as db:
        print("=== 创建G4-2025测试数据 ===")
        
        try:
            # 1. 创建学校主数据
            print("1. 创建学校主数据...")
            schools = [
                ('G4001', '实验小学'),
                ('G4002', '中心小学'), 
                ('G4003', '育才小学'),
                ('G4004', '希望小学'),
                ('G4005', '阳光小学')
            ]
            
            for school_id, school_name in schools:
                db.execute(text("""
                    INSERT INTO school_master_data 
                    (batch_code, school_id, school_name, status, created_at, updated_at)
                    VALUES ('G4-2025', :school_id, :school_name, 'ACTIVE', NOW(), NOW())
                    ON DUPLICATE KEY UPDATE 
                        school_name = VALUES(school_name),
                        status = VALUES(status),
                        updated_at = VALUES(updated_at)
                """), {"school_id": school_id, "school_name": school_name})
            
            print(f"   已创建 {len(schools)} 所学校")
            
            # 2. 创建学生成绩数据
            print("2. 创建学生成绩数据...")
            subjects = ['数学', '语文', '英语', '科学']
            
            record_count = 0
            for school_id, school_name in schools:
                # 每个学校40-60个学生
                student_count = random.randint(40, 60)
                
                for student_num in range(1, student_count + 1):
                    student_id = f"{school_id}S{student_num:03d}"
                    
                    for subject in subjects:
                        # 生成合理的成绩分布
                        if subject == '数学':
                            total_score = random.randint(65, 95)  # 数学稍难
                        elif subject == '语文': 
                            total_score = random.randint(70, 98)  # 语文中等
                        elif subject == '英语':
                            total_score = random.randint(60, 90)  # 英语较难
                        else:  # 科学
                            total_score = random.randint(75, 100)  # 科学较容易
                        
                        # 创建维度分数（模拟4个维度）
                        dim_scores = {}
                        remaining = total_score
                        for i in range(3):
                            dim_score = random.randint(int(remaining * 0.1), int(remaining * 0.4))
                            dim_scores[f"维度{i+1}"] = dim_score
                            remaining -= dim_score
                        dim_scores["维度4"] = max(0, remaining)
                        
                        db.execute(text("""
                            INSERT INTO student_cleaned_scores
                            (batch_code, school_code, school_name, student_id, subject_name, 
                             subject_type, total_score, dimension_scores, created_at, updated_at)
                            VALUES 
                            ('G4-2025', :school_code, :school_name, :student_id, :subject_name,
                             'exam', :total_score, :dim_scores, NOW(), NOW())
                            ON DUPLICATE KEY UPDATE
                                total_score = VALUES(total_score),
                                dimension_scores = VALUES(dimension_scores),
                                updated_at = VALUES(updated_at)
                        """), {
                            "school_code": school_id,
                            "school_name": school_name, 
                            "student_id": student_id,
                            "subject_name": subject,
                            "total_score": total_score,
                            "dim_scores": str(dim_scores).replace("'", '"')
                        })
                        
                        record_count += 1
            
            print(f"   已创建 {record_count} 条学生成绩记录")
            
            # 3. 创建题目配置数据（如果不存在）
            print("3. 创建题目配置数据...")
            for subject in subjects:
                # 每个科目假设100分满分
                db.execute(text("""
                    INSERT INTO subject_question_config
                    (batch_code, subject_name, subject_type, max_score, created_at, updated_at)
                    VALUES ('G4-2025', :subject, 'exam', 100, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        max_score = VALUES(max_score),
                        updated_at = VALUES(updated_at)
                """), {"subject": subject})
            
            print(f"   已创建 {len(subjects)} 个科目配置")
            
            db.commit()
            
            # 4. 验证创建的数据
            print("\n4. 验证创建的数据:")
            
            school_count = db.execute(text("""
                SELECT COUNT(*) FROM school_master_data WHERE batch_code = 'G4-2025'
            """)).scalar()
            
            student_stats = db.execute(text("""
                SELECT COUNT(*) as total_records,
                       COUNT(DISTINCT student_id) as students,
                       COUNT(DISTINCT school_code) as schools,
                       COUNT(DISTINCT subject_name) as subjects
                FROM student_cleaned_scores 
                WHERE batch_code = 'G4-2025'
            """)).fetchone()
            
            print(f"   学校数据: {school_count} 所学校")
            print(f"   学生数据: {student_stats[0]} 条记录")
            print(f"             {student_stats[1]} 个学生")
            print(f"             {student_stats[2]} 所学校有数据") 
            print(f"             {student_stats[3]} 个科目")
            
            print("\n✅ G4-2025测试数据创建完成!")
            return True
            
        except Exception as e:
            db.rollback()
            print(f"❌ 测试数据创建失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


if __name__ == '__main__':
    create_g4_test_data()