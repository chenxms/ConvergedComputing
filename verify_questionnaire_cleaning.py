#!/usr/bin/env python3
"""
验证问卷数据清洗结果
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def verify_questionnaire_cleaning():
    """验证问卷数据清洗结果"""
    print("=== 验证问卷数据清洗结果 ===")
    print(f"验证时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 1. 检查问卷详细分数表的数据
        print("1. 检查问卷详细分数表...")
        detail_query = text("""
            SELECT 
                batch_code,
                subject_name,
                instrument_type,
                scale_level,
                COUNT(*) as record_count,
                COUNT(DISTINCT student_id) as student_count,
                COUNT(DISTINCT question_id) as question_count,
                COUNT(DISTINCT option_label) as option_count
            FROM questionnaire_question_scores
            GROUP BY batch_code, subject_name, instrument_type, scale_level
            ORDER BY batch_code, subject_name
        """)
        
        detail_result = session.execute(detail_query)
        detail_rows = detail_result.fetchall()
        
        if detail_rows:
            print(f"找到 {len(detail_rows)} 个问卷科目的详细数据:")
            total_records = 0
            for row in detail_rows:
                batch_code = row[0]
                subject_name = row[1]
                instrument_type = row[2]
                scale_level = row[3]
                record_count = row[4]
                student_count = row[5]
                question_count = row[6]
                option_count = row[7]
                
                total_records += record_count
                print(f"  批次: {batch_code}, 科目: {subject_name}")
                print(f"    量表: {instrument_type} ({scale_level}分位)")
                print(f"    记录数: {record_count:,} 条")
                print(f"    学生数: {student_count:,} 人")
                print(f"    题目数: {question_count} 道")
                print(f"    选项种类: {option_count} 种")
            
            print(f"\n问卷详细数据总计: {total_records:,} 条记录")
        else:
            print("问卷详细分数表中暂无数据")
        
        # 2. 检查选项分布统计
        if detail_rows:
            print(f"\n2. 检查选项分布统计...")
            for row in detail_rows:
                batch_code = row[0]
                subject_name = row[1]
                
                print(f"\n批次 {batch_code} - 科目 {subject_name} 的选项分布:")
                
                option_query = text("""
                    SELECT 
                        option_label,
                        option_level,
                        COUNT(*) as count,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                    FROM questionnaire_question_scores
                    WHERE batch_code = :batch_code AND subject_name = :subject_name
                    GROUP BY option_label, option_level
                    ORDER BY option_level
                """)
                
                option_result = session.execute(option_query, {
                    'batch_code': batch_code,
                    'subject_name': subject_name
                })
                
                for opt_row in option_result.fetchall():
                    print(f"  {opt_row[0]} (等级{opt_row[1]}): {opt_row[2]:,} 条 ({opt_row[3]}%)")
        
        # 3. 检查汇总表中的问卷数据
        print(f"\n3. 检查汇总表中的问卷数据...")
        summary_query = text("""
            SELECT 
                batch_code,
                subject_name,
                COUNT(*) as record_count,
                COUNT(DISTINCT student_id) as student_count,
                AVG(total_score) as avg_score,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score,
                MAX(max_score) as scale_max_score
            FROM student_cleaned_scores
            WHERE subject_type = 'questionnaire'
            GROUP BY batch_code, subject_name
            ORDER BY batch_code, subject_name
        """)
        
        summary_result = session.execute(summary_query)
        summary_rows = summary_result.fetchall()
        
        if summary_rows:
            print(f"汇总表中问卷数据:")
            for row in summary_rows:
                print(f"  批次: {row[0]}, 科目: {row[1]}")
                print(f"    记录数: {row[2]:,} 条")
                print(f"    学生数: {row[3]:,} 人")
                print(f"    平均分: {row[4]:.2f}")
                print(f"    分数范围: {row[5]:.2f} ~ {row[6]:.2f}")
                print(f"    量表满分: {row[7]:.0f}")
        else:
            print("汇总表中暂无问卷数据")
        
        # 4. 数据一致性检查
        print(f"\n4. 数据一致性检查...")
        if detail_rows and summary_rows:
            print("检查详细表和汇总表的数据一致性:")
            
            # 按批次和科目对比学生数
            for detail_row in detail_rows:
                batch_code = detail_row[0]
                subject_name = detail_row[1]
                detail_students = detail_row[5]
                
                # 在汇总表中查找对应数据
                summary_students = None
                for summary_row in summary_rows:
                    if summary_row[0] == batch_code and summary_row[1] == subject_name:
                        summary_students = summary_row[3]
                        break
                
                if summary_students is not None:
                    if detail_students == summary_students:
                        print(f"  ✅ {batch_code}-{subject_name}: 学生数一致 ({detail_students:,} 人)")
                    else:
                        print(f"  ❌ {batch_code}-{subject_name}: 学生数不一致 (详细表:{detail_students:,}, 汇总表:{summary_students:,})")
                else:
                    print(f"  ⚠️ {batch_code}-{subject_name}: 汇总表中未找到对应数据")
        
        # 5. 总体统计
        print(f"\n5. 总体统计...")
        
        # 统计问卷科目配置
        config_query = text("""
            SELECT 
                batch_code,
                COUNT(DISTINCT subject_name) as questionnaire_subjects,
                COUNT(*) as questionnaire_questions
            FROM subject_question_config
            WHERE question_type_enum = 'questionnaire'
            GROUP BY batch_code
            ORDER BY batch_code
        """)
        
        config_result = session.execute(config_query)
        config_rows = config_result.fetchall()
        
        print("问卷科目配置统计:")
        total_subjects = 0
        total_questions = 0
        for row in config_rows:
            total_subjects += row[1]
            total_questions += row[2]
            print(f"  批次 {row[0]}: {row[1]} 个科目, {row[2]} 道题目")
        
        print(f"总计: {total_subjects} 个问卷科目, {total_questions} 道问卷题目")
        
        # 统计清洗结果
        if detail_rows:
            cleaned_subjects = len(detail_rows)
            cleaned_records = sum(row[4] for row in detail_rows)
            cleaned_students = sum(row[5] for row in detail_rows)
            
            print(f"\n清洗结果统计:")
            print(f"  已清洗科目: {cleaned_subjects} 个")
            print(f"  清洗记录数: {cleaned_records:,} 条")
            print(f"  涉及学生: {cleaned_students:,} 人次")
            
            if total_subjects > 0:
                coverage_rate = (cleaned_subjects / total_subjects) * 100
                print(f"  清洗覆盖率: {coverage_rate:.2f}%")
        
        print(f"\n{'='*60}")
        print("[SUCCESS] 问卷数据清洗结果验证完成！")
        print(f"验证完成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"[ERROR] 验证过程发生错误: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        session.close()

if __name__ == "__main__":
    verify_questionnaire_cleaning()