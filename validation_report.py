#!/usr/bin/env python3
"""
第一阶段数据清洗验证报告
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def generate_validation_report():
    """生成验证报告"""
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    
    try:
        engine = create_engine(DATABASE_URL, echo=False)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        print("=" * 60)
        print("       第一阶段数据清洗验证报告")
        print("=" * 60)
        
        batch_code = 'G4-2025'
        
        # 1. 表创建验证
        print("\n1. 表创建验证")
        print("-" * 30)
        query = text("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name = 'student_cleaned_scores'
        """)
        result = session.execute(query)
        if result.scalar() > 0:
            print("[OK] student_cleaned_scores 表已成功创建")
        else:
            print("[ERROR] student_cleaned_scores 表不存在")
            return
        
        # 2. 数据清洗执行验证
        print("\n2. 数据清洗执行验证")
        print("-" * 30)
        query = text("""
            SELECT 
                COUNT(*) as total_cleaned,
                COUNT(DISTINCT student_id) as unique_students,
                COUNT(DISTINCT subject_name) as subjects_count
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
        """)
        result = session.execute(query, {'batch_code': batch_code})
        row = result.fetchone()
        
        if row[0] > 0:
            print(f"[OK] 数据清洗成功执行")
            print(f"     清洗后记录总数: {row[0]:,}")
            print(f"     唯一学生数: {row[1]:,}")
            print(f"     科目数量: {row[2]}")
        else:
            print("[ERROR] 没有清洗后的数据")
            return
        
        # 3. 数据质量验证
        print("\n3. 数据质量验证")
        print("-" * 30)
        
        # 3.1 检查重复记录
        query = text("""
            SELECT COUNT(*) 
            FROM (
                SELECT student_id, subject_name, COUNT(*) as cnt
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
                GROUP BY student_id, subject_name
                HAVING COUNT(*) > 1
            ) duplicates
        """)
        result = session.execute(query, {'batch_code': batch_code})
        duplicate_count = result.scalar()
        
        if duplicate_count == 0:
            print("[OK] 无重复记录 (学生-科目唯一性)")
        else:
            print(f"[ERROR] 发现 {duplicate_count} 组重复记录")
        
        # 3.2 检查分数范围
        query = text("""
            SELECT COUNT(*)
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code 
            AND (total_score < 0 OR total_score > max_score)
        """)
        result = session.execute(query, {'batch_code': batch_code})
        invalid_scores = result.scalar()
        
        if invalid_scores == 0:
            print("[OK] 所有分数在有效范围内")
        else:
            print(f"[ERROR] 发现 {invalid_scores} 条无效分数")
        
        # 3.3 检查必填字段
        query = text("""
            SELECT 
                SUM(CASE WHEN student_id IS NULL OR student_id = '' THEN 1 ELSE 0 END) as missing_student,
                SUM(CASE WHEN subject_name IS NULL OR subject_name = '' THEN 1 ELSE 0 END) as missing_subject,
                SUM(CASE WHEN total_score IS NULL THEN 1 ELSE 0 END) as missing_score
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
        """)
        result = session.execute(query, {'batch_code': batch_code})
        row = result.fetchone()
        
        missing_issues = row[0] + row[1] + row[2]
        if missing_issues == 0:
            print("[OK] 必填字段完整")
        else:
            print(f"[ERROR] 必填字段缺失: 学生ID({row[0]}) 科目名({row[1]}) 分数({row[2]})")
        
        # 4. 各科目统计
        print("\n4. 各科目数据统计")
        print("-" * 30)
        query = text("""
            SELECT 
                subject_name,
                max_score,
                COUNT(*) as student_count,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score,
                AVG(total_score) as avg_score,
                COUNT(CASE WHEN total_score = 0 THEN 1 END) as zero_count
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
            GROUP BY subject_name, max_score
            ORDER BY subject_name
        """)
        result = session.execute(query, {'batch_code': batch_code})
        rows = result.fetchall()
        
        print(f"{'科目':<12} {'满分':<8} {'人数':<8} {'最低分':<8} {'最高分':<8} {'平均分':<8} {'零分人数':<10} {'得分率':<8}")
        print("-" * 85)
        
        for row in rows:
            subject, max_score, count, min_s, max_s, avg_s, zero_count = row
            score_rate = (avg_s / max_score * 100) if max_score > 0 else 0
            zero_rate = (zero_count / count * 100) if count > 0 else 0
            print(f"{subject:<12} {max_score:<8.0f} {count:<8} {min_s:<8.2f} {max_s:<8.2f} {avg_s:<8.2f} {zero_count:<4}({zero_rate:<4.1f}%) {score_rate:<7.1f}%")
        
        # 5. 与原始数据对比验证
        print("\n5. 与原始数据对比验证")
        print("-" * 30)
        query = text("""
            SELECT 
                'cleaned' as data_type,
                COUNT(*) as record_count,
                COUNT(DISTINCT student_id) as student_count,
                COUNT(DISTINCT subject_name) as subject_count
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
            
            UNION ALL
            
            SELECT 
                'original' as data_type,
                COUNT(*) as record_count,
                COUNT(DISTINCT student_id) as student_count,
                COUNT(DISTINCT subject_name) as subject_count
            FROM student_score_detail 
            WHERE batch_code = :batch_code
        """)
        result = session.execute(query, {'batch_code': batch_code})
        rows = result.fetchall()
        
        original_data = None
        cleaned_data = None
        
        for row in rows:
            if row[0] == 'original':
                original_data = row
            else:
                cleaned_data = row
        
        if original_data and cleaned_data:
            print(f"原始数据: {original_data[1]:,} 条记录, {original_data[2]:,} 学生, {original_data[3]} 科目")
            print(f"清洗数据: {cleaned_data[1]:,} 条记录, {cleaned_data[2]:,} 学生, {cleaned_data[3]} 科目")
            
            if cleaned_data[2] == original_data[2]:
                print("[OK] 学生数量一致")
            else:
                print(f"[WARNING] 学生数量不一致 (原始: {original_data[2]}, 清洗: {cleaned_data[2]})")
            
            if cleaned_data[3] == original_data[3]:
                print("[OK] 科目数量一致")
            else:
                print(f"[WARNING] 科目数量不一致 (原始: {original_data[3]}, 清洗: {cleaned_data[3]})")
        
        # 6. 总结
        print("\n6. 验证总结")
        print("-" * 30)
        
        all_checks_passed = (
            duplicate_count == 0 and 
            invalid_scores == 0 and 
            missing_issues == 0
        )
        
        if all_checks_passed:
            print("[SUCCESS] 第一阶段数据清洗验证通过!")
            print("         数据质量良好，可以进行下一阶段的统计分析")
        else:
            print("[WARNING] 数据清洗验证发现问题，请检查上述错误信息")
        
        print(f"\n清洗数据存储位置: student_cleaned_scores 表")
        print(f"批次代码: {batch_code}")
        print(f"验证时间: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        session.close()
        
    except Exception as e:
        print(f"生成验证报告失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    generate_validation_report()