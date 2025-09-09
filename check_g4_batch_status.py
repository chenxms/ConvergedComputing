#!/usr/bin/env python3
"""
检查批次G4-2025的当前状态和数据完整性
"""

import asyncio
from app.database.connection import get_db_context
from app.database.repositories import BatchRepository
from sqlalchemy import text

def check_batch_status():
    """检查G4-2025批次的状态"""
    
    try:
        with get_db_context() as session:
            print("=== 批次G4-2025状态检查 ===")
            
            # 2. 检查学生数据量
            result = session.execute(text("""
                SELECT COUNT(DISTINCT student_id) as student_count,
                       COUNT(*) as total_records
                FROM student_score_detail 
                WHERE batch_code = 'G4-2025'
            """))
            data_stats = result.fetchone()
            print(f"学生总数: {data_stats.student_count}")
            print(f"答题记录总数: {data_stats.total_records}")
            
            # 3. 检查科目分布
            result = session.execute(text("""
                SELECT subject_id, subject_name, COUNT(DISTINCT student_id) as student_count
                FROM student_score_detail 
                WHERE batch_code = 'G4-2025'
                GROUP BY subject_id, subject_name
                ORDER BY subject_id
            """))
            subjects = result.fetchall()
            
            print("\n=== 科目分布 ===")
            for subject in subjects:
                print(f"科目 {subject.subject_id} ({subject.subject_name}): {subject.student_count} 名学生")
            
            # 4. 检查学校分布
            result = session.execute(text("""
                SELECT school_id, COUNT(DISTINCT student_id) as student_count
                FROM student_score_detail 
                WHERE batch_code = 'G4-2025'
                GROUP BY school_id
                ORDER BY student_count DESC
                LIMIT 10
            """))
            schools = result.fetchall()
            
            print("\n=== 学校分布（前10所） ===")
            for school in schools:
                print(f"学校 {school.school_id}: {school.student_count} 名学生")
            
            # 5. 检查现有汇聚数据
            result = session.execute(text("""
                SELECT status, total_schools, total_students, created_at
                FROM grade_aggregation_main 
                WHERE batch_code = 'G4-2025'
            """))
            agg_result = result.fetchone()
            if agg_result:
                print(f"\n现有汇聚记录:")
                print(f"  状态: {agg_result.status}")
                print(f"  学校总数: {agg_result.total_schools}")
                print(f"  学生总数: {agg_result.total_students}")
                print(f"  创建时间: {agg_result.created_at}")
            else:
                print(f"\n未找到现有汇聚记录")
            
            print("\n=== 状态检查完成 ===")
        
    except Exception as e:
        print(f"检查过程中出现错误: {e}")

if __name__ == "__main__":
    check_batch_status()