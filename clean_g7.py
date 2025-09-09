#!/usr/bin/env python3
"""
专门清洗G7-2025批次
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
import time
import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from data_cleaning_service import DataCleaningService

async def main():
    """清洗G7-2025"""
    print(f"=== 清洗批次 G7-2025 ===")
    print(f"开始时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 创建清洗服务
        cleaning_service = DataCleaningService(session)
        batch_code = 'G7-2025'
        
        # 检查当前状态
        print("检查当前数据状态...")
        raw_query = text("SELECT COUNT(*) FROM student_score_detail WHERE batch_code = :batch_code")
        raw_result = session.execute(raw_query, {'batch_code': batch_code})
        raw_count = raw_result.fetchone()[0]
        
        cleaned_query = text("SELECT COUNT(*) FROM student_cleaned_scores WHERE batch_code = :batch_code")  
        cleaned_result = session.execute(cleaned_query, {'batch_code': batch_code})
        cleaned_count = cleaned_result.fetchone()[0]
        
        print(f"原始数据: {raw_count:,} 条")
        print(f"已清洗: {cleaned_count:,} 条")
        
        if cleaned_count > 0:
            print("发现已有清洗数据，将重新清洗...")
        
        # 执行清洗
        start_time = time.time()
        print(f"\n开始清洗批次 {batch_code}...")
        
        result = await cleaning_service.clean_batch_scores(batch_code)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 输出结果
        print(f"\n=== G7-2025 清洗完成 ===")
        print(f"处理科目: {result['subjects_processed']} 个")
        print(f"原始记录: {result['total_raw_records']:,} 条")
        print(f"清洗记录: {result['total_cleaned_records']:,} 条")
        print(f"异常记录: {result['anomalous_records']:,} 条")
        print(f"处理时间: {duration/60:.2f} 分钟")
        
        # 详细科目信息
        print(f"\n科目详情:")
        for subject_name, subject_result in result['subjects'].items():
            print(f"  {subject_name}:")
            print(f"    原始记录: {subject_result['raw_records']:,} 条")
            print(f"    清洗记录: {subject_result['cleaned_records']:,} 条")
            print(f"    异常记录: {subject_result['anomalous_records']:,} 条")
            print(f"    学生数: {subject_result['unique_students']:,} 人")
        
        # 最终验证
        final_verify_query = text("""
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
        final_result = session.execute(final_verify_query, {'batch_code': batch_code})
        final_row = final_result.fetchone()
        
        if final_row and final_row[0] > 0:
            print(f"\n✅ 最终验证通过")
            print(f"  清洗后总记录: {final_row[0]:,} 条")
            print(f"  科目数: {final_row[1]} 个")
            print(f"  学生数: {final_row[2]:,} 个")
            print(f"  分数范围: {final_row[3]:.2f} ~ {final_row[4]:.2f}")
            print(f"  平均分: {final_row[5]:.2f}")
        else:
            print(f"❌ 最终验证失败")
        
        print(f"\n完成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"❌ 清洗失败: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        session.close()

if __name__ == "__main__":
    asyncio.run(main())