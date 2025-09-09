#!/usr/bin/env python3
"""
单批次数据清洗器 - 逐个处理批次
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

async def clean_single_batch(batch_code: str):
    """清洗单个批次"""
    print(f"=== 清洗批次 {batch_code} ===")
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
        
        # 执行清洗
        start_time = time.time()
        print(f"\n开始清洗批次 {batch_code}...")
        
        result = await cleaning_service.clean_batch_scores(batch_code)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 输出结果
        print(f"\n=== 清洗完成 ===")
        print(f"批次: {result['batch_code']}")
        print(f"处理科目: {result['subjects_processed']} 个")
        print(f"原始记录: {result['total_raw_records']:,} 条")
        print(f"清洗记录: {result['total_cleaned_records']:,} 条")
        print(f"异常记录: {result['anomalous_records']:,} 条")
        print(f"处理时间: {duration/60:.2f} 分钟")
        
        # 验证结果
        print(f"\n验证清洗结果...")
        verify_query = text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT subject_name) as unique_subjects,
                COUNT(DISTINCT student_id) as unique_students
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
        """)
        verify_result = session.execute(verify_query, {'batch_code': batch_code})
        verify_row = verify_result.fetchone()
        
        if verify_row and verify_row[0] > 0:
            print(f"✅ 验证通过")
            print(f"  清洗后记录: {verify_row[0]:,} 条")
            print(f"  科目数: {verify_row[1]} 个")
            print(f"  学生数: {verify_row[2]:,} 个")
        else:
            print(f"❌ 验证失败 - 清洗表中无数据")
        
        print(f"\n完成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"❌ 清洗失败: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        session.close()
        print("数据库连接已关闭")

async def main():
    """主函数"""
    # 先清洗 G7-2025
    await clean_single_batch('G7-2025')
    
    print("\n" + "="*80 + "\n")
    
    # 再清洗 G8-2025
    await clean_single_batch('G8-2025')

if __name__ == "__main__":
    asyncio.run(main())