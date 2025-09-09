#!/usr/bin/env python3
"""
最终问卷数据清洗状态检查
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def check_final_status():
    """检查最终问卷数据清洗状态"""
    print("=== 最终问卷数据清洗状态 ===\n")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 1. 问卷配置统计
        print("1. 问卷科目配置:")
        config_query = text("""
            SELECT 
                batch_code,
                subject_name,
                COUNT(*) as question_count,
                GROUP_CONCAT(DISTINCT instrument_id) as instruments
            FROM subject_question_config
            WHERE question_type_enum = 'questionnaire'
            GROUP BY batch_code, subject_name
            ORDER BY batch_code
        """)
        
        config_result = session.execute(config_query)
        configs = config_result.fetchall()
        
        for row in configs:
            print(f"   {row[0]} - {row[1]}: {row[2]}道题目")
        
        # 2. 问卷详细数据统计
        print("\n2. 问卷详细数据清洗结果:")
        detail_query = text("""
            SELECT 
                batch_code,
                subject_name,
                COUNT(*) as record_count,
                COUNT(DISTINCT student_id) as student_count,
                COUNT(DISTINCT question_id) as question_count
            FROM questionnaire_question_scores
            GROUP BY batch_code, subject_name
            ORDER BY batch_code
        """)
        
        detail_result = session.execute(detail_query)
        details = detail_result.fetchall()
        
        if details:
            for row in details:
                batch_code = row[0]
                subject_name = row[1]
                record_count = row[2]
                student_count = row[3]
                question_count = row[4]
                
                print(f"   {batch_code} - {subject_name}:")
                print(f"      记录数: {record_count:,}")
                print(f"      学生数: {student_count:,}")
                print(f"      题目数: {question_count}")
                print(f"      理论记录数: {student_count * question_count:,}")
                
                if record_count == student_count * question_count:
                    print(f"      状态: 完整")
                else:
                    print(f"      状态: 部分缺失")
        else:
            print("   暂无清洗数据")
        
        # 3. 汇总表统计
        print("\n3. 学生汇总表中的问卷数据:")
        summary_query = text("""
            SELECT 
                batch_code,
                subject_name,
                COUNT(*) as student_count,
                AVG(total_score) as avg_score,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score
            FROM student_cleaned_scores
            WHERE subject_type = 'questionnaire' OR subject_name = '问卷'
            GROUP BY batch_code, subject_name
            ORDER BY batch_code
        """)
        
        summary_result = session.execute(summary_query)
        summaries = summary_result.fetchall()
        
        if summaries:
            for row in summaries:
                print(f"   {row[0]} - {row[1]}:")
                print(f"      学生数: {row[2]:,}")
                print(f"      平均分: {row[3]:.2f}")
                print(f"      分数范围: {row[4]:.2f} - {row[5]:.2f}")
        else:
            print("   暂无汇总数据")
        
        # 4. 选项分布统计
        print("\n4. 选项分布概览:")
        option_query = text("""
            SELECT 
                batch_code,
                option_label,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY batch_code), 2) as percentage
            FROM questionnaire_question_scores
            GROUP BY batch_code, option_label
            ORDER BY batch_code, 
                CASE 
                    WHEN option_label = '非常不同意' THEN 1
                    WHEN option_label = '不同意' THEN 2
                    WHEN option_label = '同意' THEN 3
                    WHEN option_label = '非常同意' THEN 4
                    ELSE 5
                END
        """)
        
        option_result = session.execute(option_query)
        current_batch = None
        
        for row in option_result.fetchall():
            if row[0] != current_batch:
                current_batch = row[0]
                print(f"\n   批次 {row[0]}:")
            print(f"      {row[1]}: {row[2]:,}条 ({row[3]}%)")
        
        # 5. 总体完成情况
        print("\n5. 总体完成情况:")
        
        # 统计已配置的批次科目
        config_count_query = text("""
            SELECT COUNT(DISTINCT CONCAT(batch_code, '-', subject_name)) 
            FROM subject_question_config
            WHERE question_type_enum = 'questionnaire'
        """)
        config_count = session.execute(config_count_query).scalar()
        
        # 统计已清洗的批次科目
        cleaned_count_query = text("""
            SELECT COUNT(DISTINCT CONCAT(batch_code, '-', subject_name))
            FROM questionnaire_question_scores
        """)
        cleaned_count = session.execute(cleaned_count_query).scalar()
        
        print(f"   配置的问卷科目: {config_count}个")
        print(f"   已清洗的科目: {cleaned_count}个")
        
        if config_count > 0:
            completion_rate = (cleaned_count / config_count) * 100
            print(f"   完成率: {completion_rate:.1f}%")
            
            if completion_rate == 100:
                print("\n   [SUCCESS] 所有问卷数据清洗已完成！")
            else:
                print(f"\n   [PENDING] 还有 {config_count - cleaned_count} 个科目待清洗")
        
        print("\n=== 检查完成 ===")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    check_final_status()