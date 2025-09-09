#!/usr/bin/env python3
"""
批量数据清洗运行器 - 专门处理G7-2025和G8-2025批次
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
    """主函数 - 批量清洗指定批次"""
    print("=== 批量数据清洗运行器 ===")
    print(f"开始时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # 创建清洗服务
    cleaning_service = DataCleaningService(session)
    
    # 待清洗的批次列表
    batches_to_clean = ['G7-2025', 'G8-2025']
    
    total_start_time = time.time()
    all_results = {}
    
    try:
        for i, batch_code in enumerate(batches_to_clean, 1):
            print(f"\n[{i}/{len(batches_to_clean)}] 开始处理批次: {batch_code}")
            print("-" * 50)
            
            batch_start_time = time.time()
            
            # 先检查当前状态
            current_status = await check_batch_status(session, batch_code)
            print(f"当前状态:")
            print(f"  原始数据: {current_status['raw_records']} 条")
            print(f"  已清洗: {current_status['cleaned_records']} 条")
            
            # 执行清洗
            print(f"\n开始清洗批次 {batch_code}...")
            result = await cleaning_service.clean_batch_scores(batch_code)
            
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            
            # 记录处理时间
            result['processing_time_seconds'] = round(batch_duration, 2)
            result['processing_time_minutes'] = round(batch_duration / 60, 2)
            
            all_results[batch_code] = result
            
            # 输出单批次结果
            print(f"\n批次 {batch_code} 清洗完成!")
            print(f"  处理科目: {result['subjects_processed']} 个")
            print(f"  原始记录: {result['total_raw_records']} 条")
            print(f"  清洗记录: {result['total_cleaned_records']} 条")
            print(f"  异常记录: {result['anomalous_records']} 条")
            print(f"  处理时间: {result['processing_time_minutes']} 分钟")
            
            # 验证清洗结果
            print(f"\n验证批次 {batch_code} 清洗结果...")
            verification_result = await verify_cleaning_result(session, batch_code)
            result['verification'] = verification_result
            
            if verification_result['success']:
                print(f"✅ 批次 {batch_code} 验证通过")
                print(f"  清洗后记录数: {verification_result['total_records']}")
                print(f"  科目数: {verification_result['unique_subjects']}")
                print(f"  学生数: {verification_result['unique_students']}")
            else:
                print(f"❌ 批次 {batch_code} 验证失败: {verification_result.get('error', '未知错误')}")
        
        total_end_time = time.time()
        total_duration = total_end_time - total_start_time
        
        # 输出总体结果汇总
        print(f"\n{'='*80}")
        print(f"=== 批量清洗总结果汇总 ===")
        print(f"{'='*80}")
        print(f"完成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"总处理时间: {round(total_duration / 60, 2)} 分钟")
        
        total_subjects = 0
        total_raw_records = 0
        total_cleaned_records = 0
        total_anomalous_records = 0
        success_count = 0
        
        for batch_code, result in all_results.items():
            verification_success = result.get('verification', {}).get('success', False)
            if verification_success:
                success_count += 1
            
            print(f"\n批次 {batch_code}:")
            print(f"  - 状态: {'✅ 成功' if verification_success else '❌ 失败'}")
            print(f"  - 处理科目: {result['subjects_processed']} 个")
            print(f"  - 原始记录: {result['total_raw_records']} 条")
            print(f"  - 清洗记录: {result['total_cleaned_records']} 条")
            print(f"  - 异常记录: {result['anomalous_records']} 条")
            print(f"  - 处理时间: {result['processing_time_minutes']} 分钟")
            
            total_subjects += result['subjects_processed']
            total_raw_records += result['total_raw_records']
            total_cleaned_records += result['total_cleaned_records']
            total_anomalous_records += result['anomalous_records']
        
        print(f"\n【最终统计】")
        print(f"  - 处理批次: {len(all_results)} 个")
        print(f"  - 成功批次: {success_count} 个")
        print(f"  - 处理科目: {total_subjects} 个")
        print(f"  - 原始记录: {total_raw_records} 条")
        print(f"  - 清洗记录: {total_cleaned_records} 条")
        print(f"  - 异常记录: {total_anomalous_records} 条")
        print(f"  - 总处理时间: {round(total_duration / 60, 2)} 分钟")
        
        # 生成最终验证报告
        await generate_final_report(session, all_results)
        
    except Exception as e:
        print(f"\n❌ 批量清洗过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        session.close()
        print(f"\n数据库连接已关闭")

async def check_batch_status(session, batch_code: str):
    """检查批次当前状态"""
    try:
        # 检查原始数据
        raw_query = text("""
            SELECT COUNT(*) as count
            FROM student_score_detail
            WHERE batch_code = :batch_code
        """)
        raw_result = session.execute(raw_query, {'batch_code': batch_code})
        raw_count = raw_result.fetchone()[0]
        
        # 检查已清洗数据
        cleaned_query = text("""
            SELECT COUNT(*) as count
            FROM student_cleaned_scores
            WHERE batch_code = :batch_code
        """)
        cleaned_result = session.execute(cleaned_query, {'batch_code': batch_code})
        cleaned_count = cleaned_result.fetchone()[0]
        
        return {
            'raw_records': raw_count,
            'cleaned_records': cleaned_count
        }
        
    except Exception as e:
        print(f"检查批次状态失败: {e}")
        return {'raw_records': 0, 'cleaned_records': 0}

async def verify_cleaning_result(session, batch_code: str):
    """验证清洗结果的完整性和正确性"""
    try:
        # 检查清洗表中是否有数据
        query = text("""
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
        
        result = session.execute(query, {'batch_code': batch_code})
        row = result.fetchone()
        
        if not row or row[0] == 0:
            return {
                'success': False,
                'error': f'清洗表中未找到批次 {batch_code} 的数据'
            }
        
        return {
            'success': True,
            'total_records': row[0],
            'unique_subjects': row[1],
            'unique_students': row[2],
            'score_range': {
                'min': float(row[3]) if row[3] else 0,
                'max': float(row[4]) if row[4] else 0,
                'avg': round(float(row[5]), 2) if row[5] else 0
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': f'验证过程发生错误: {str(e)}'
        }

async def generate_final_report(session, all_results):
    """生成最终清洗报告"""
    print(f"\n{'='*80}")
    print(f"=== 最终清洗报告 ===")
    print(f"{'='*80}")
    
    try:
        # 查询所有批次的最终状态
        final_query = text("""
            SELECT 
                batch_code,
                COUNT(*) as total_records,
                COUNT(DISTINCT subject_name) as unique_subjects,
                COUNT(DISTINCT student_id) as unique_students,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score,
                AVG(total_score) as avg_score
            FROM student_cleaned_scores 
            WHERE batch_code IN ('G4-2025', 'G7-2025', 'G8-2025')
            GROUP BY batch_code
            ORDER BY batch_code
        """)
        
        result = session.execute(final_query)
        rows = result.fetchall()
        
        print(f"\n【所有批次清洗状态】")
        total_all_records = 0
        for row in rows:
            batch_code = row[0]
            record_count = row[1]
            subject_count = row[2]
            student_count = row[3]
            min_score = float(row[4]) if row[4] else 0
            max_score = float(row[5]) if row[5] else 0
            avg_score = round(float(row[6]), 2) if row[6] else 0
            
            status = "✅ 已完成" if batch_code in ['G4-2025', 'G7-2025', 'G8-2025'] else "❌ 未处理"
            
            print(f"\n批次 {batch_code}: {status}")
            print(f"  记录数: {record_count:,} 条")
            print(f"  科目数: {subject_count} 个")
            print(f"  学生数: {student_count:,} 个")
            print(f"  分数范围: {min_score:.2f} ~ {max_score:.2f}")
            print(f"  平均分: {avg_score:.2f}")
            
            total_all_records += record_count
        
        print(f"\n【总计】")
        print(f"  已清洗总记录数: {total_all_records:,} 条")
        print(f"  涵盖批次: {len(rows)} 个")
        
    except Exception as e:
        print(f"生成最终报告失败: {e}")

if __name__ == "__main__":
    asyncio.run(main())