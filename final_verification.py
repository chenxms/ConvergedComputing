#!/usr/bin/env python3
"""
最终验证所有批次的清洗结果
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

async def main():
    """验证所有批次清洗结果"""
    print("=== 最终验证报告 ===")
    print(f"验证时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 验证所有批次
        all_batches = ['G4-2025', 'G7-2025', 'G8-2025']
        
        print("\n【批次清洗状态总览】")
        
        total_cleaned_records = 0
        success_batches = 0
        
        for batch_code in all_batches:
            print(f"\n批次: {batch_code}")
            print("-" * 40)
            
            # 检查原始数据
            raw_query = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT subject_name) as unique_subjects,
                    COUNT(DISTINCT student_id) as unique_students
                FROM student_score_detail
                WHERE batch_code = :batch_code
            """)
            raw_result = session.execute(raw_query, {'batch_code': batch_code})
            raw_row = raw_result.fetchone()
            
            raw_records = raw_row[0] if raw_row else 0
            raw_subjects = raw_row[1] if raw_row else 0 
            raw_students = raw_row[2] if raw_row else 0
            
            # 检查清洗数据
            cleaned_query = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT subject_name) as unique_subjects,
                    COUNT(DISTINCT student_id) as unique_students,
                    MIN(total_score) as min_score,
                    MAX(total_score) as max_score,
                    AVG(total_score) as avg_score
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code
            """)
            cleaned_result = session.execute(cleaned_query, {'batch_code': batch_code})
            cleaned_row = cleaned_result.fetchone()
            
            if cleaned_row and cleaned_row[0] > 0:
                cleaned_records = cleaned_row[0]
                cleaned_subjects = cleaned_row[1]
                cleaned_students = cleaned_row[2]
                min_score = cleaned_row[3]
                max_score = cleaned_row[4]
                avg_score = cleaned_row[5]
                
                success_batches += 1
                total_cleaned_records += cleaned_records
                
                print(f"状态: [SUCCESS] 清洗完成")
                print(f"原始数据: {raw_records:,} 条记录, {raw_subjects} 个科目, {raw_students:,} 个学生")
                print(f"清洗数据: {cleaned_records:,} 条记录, {cleaned_subjects} 个科目, {cleaned_students:,} 个学生")
                print(f"数据覆盖率: {(cleaned_records/raw_records*100):.2f}%" if raw_records > 0 else "数据覆盖率: N/A")
                print(f"分数范围: {min_score:.2f} ~ {max_score:.2f}")
                print(f"平均分: {avg_score:.2f}")
                
                # 检查维度数据
                dimension_query = text("""
                    SELECT COUNT(*) as count
                    FROM student_cleaned_scores
                    WHERE batch_code = :batch_code 
                    AND dimension_scores IS NOT NULL 
                    AND dimension_scores != '{}'
                    AND dimension_scores != ''
                """)
                dim_result = session.execute(dimension_query, {'batch_code': batch_code})
                dim_count = dim_result.fetchone()[0]
                
                print(f"维度数据: {dim_count:,} 条记录包含维度分数 ({(dim_count/cleaned_records*100):.2f}%)" if cleaned_records > 0 else "维度数据: N/A")
                
            else:
                print(f"状态: [FAILED] 未找到清洗数据")
                print(f"原始数据: {raw_records:,} 条记录")
                print("清洗数据: 0 条记录")
        
        # 总体统计
        print(f"\n{'='*80}")
        print("【总体清洗统计】")
        print(f"{'='*80}")
        print(f"总批次数: {len(all_batches)} 个")
        print(f"成功批次: {success_batches} 个")
        print(f"失败批次: {len(all_batches) - success_batches} 个")
        print(f"成功率: {(success_batches/len(all_batches)*100):.2f}%")
        print(f"总清洗记录数: {total_cleaned_records:,} 条")
        
        # 按科目统计
        print(f"\n【按科目清洗统计】")
        subject_query = text("""
            SELECT 
                subject_name,
                COUNT(*) as record_count,
                COUNT(DISTINCT batch_code) as batch_count,
                COUNT(DISTINCT student_id) as student_count,
                AVG(total_score) as avg_score,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score
            FROM student_cleaned_scores
            WHERE batch_code IN ('G4-2025', 'G7-2025', 'G8-2025')
            GROUP BY subject_name
            ORDER BY record_count DESC
        """)
        subject_result = session.execute(subject_query)
        subject_rows = subject_result.fetchall()
        
        for row in subject_rows:
            subject_name = row[0]
            record_count = row[1]
            batch_count = row[2]
            student_count = row[3]
            avg_score = row[4]
            min_score = row[5]
            max_score = row[6]
            
            print(f"\n科目: {subject_name}")
            print(f"  记录数: {record_count:,} 条")
            print(f"  涉及批次: {batch_count} 个")
            print(f"  学生数: {student_count:,} 人")
            print(f"  平均分: {avg_score:.2f}")
            print(f"  分数范围: {min_score:.2f} ~ {max_score:.2f}")
        
        print(f"\n{'='*80}")
        print("[FINAL] 所有批次数据清洗验证完成！")
        print(f"验证完成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"[ERROR] 验证过程发生错误: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        session.close()

if __name__ == "__main__":
    asyncio.run(main())
