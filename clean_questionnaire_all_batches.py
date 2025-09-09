#!/usr/bin/env python3
"""
清洗所有批次的问卷数据
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

async def clean_questionnaire_all_batches():
    """清洗所有批次的问卷数据"""
    print("=== 清洗所有批次的问卷数据 ===")
    print(f"开始时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 创建清洗服务
        cleaning_service = DataCleaningService(session)
        
        # 处理的批次列表
        batches = ['G4-2025', 'G7-2025', 'G8-2025']
        
        total_questionnaire_subjects = 0
        total_cleaned_records = 0
        batch_results = {}
        
        for batch_code in batches:
            print(f"\n{'='*60}")
            print(f"处理批次: {batch_code}")
            print(f"{'='*60}")
            
            batch_start_time = time.time()
            
            # 1. 获取批次科目配置
            subjects_config = await cleaning_service._get_batch_subjects(batch_code)
            questionnaire_subjects = [s for s in subjects_config if s.get('is_questionnaire', False)]
            
            print(f"找到科目总数: {len(subjects_config)} 个")
            print(f"其中问卷科目: {len(questionnaire_subjects)} 个")
            
            batch_result = {
                'batch_code': batch_code,
                'total_subjects': len(subjects_config),
                'questionnaire_subjects': len(questionnaire_subjects),
                'subjects_detail': [],
                'total_cleaned_records': 0,
                'processing_time': 0
            }
            
            if not questionnaire_subjects:
                print(f"批次 {batch_code} 没有问卷科目，跳过")
                batch_results[batch_code] = batch_result
                continue
            
            # 2. 逐个处理问卷科目
            for i, subject_config in enumerate(questionnaire_subjects, 1):
                subject_name = subject_config['subject_name']
                instrument_id = subject_config['instrument_id']
                question_count = subject_config['question_count']
                
                print(f"\n[{i}/{len(questionnaire_subjects)}] 处理问卷科目: {subject_name}")
                print(f"  量表类型: {instrument_id}")
                print(f"  题目数: {question_count}")
                
                subject_start_time = time.time()
                
                try:
                    # 执行问卷清洗
                    result = await cleaning_service._clean_questionnaire_scores(
                        batch_code, subject_name, instrument_id, question_count
                    )
                    
                    subject_end_time = time.time()
                    subject_duration = subject_end_time - subject_start_time
                    
                    # 记录结果
                    subject_result = {
                        'subject_name': subject_name,
                        'instrument_id': instrument_id,
                        'question_count': question_count,
                        'raw_records': result['raw_records'],
                        'cleaned_records': result['cleaned_records'],
                        'unique_students': result['unique_students'],
                        'processing_time': subject_duration
                    }
                    
                    batch_result['subjects_detail'].append(subject_result)
                    batch_result['total_cleaned_records'] += result['cleaned_records']
                    total_cleaned_records += result['cleaned_records']
                    
                    print(f"  清洗结果:")
                    print(f"    原始记录: {result['raw_records']:,} 条")
                    print(f"    清洗记录: {result['cleaned_records']:,} 条")
                    print(f"    学生数: {result['unique_students']:,} 人")
                    print(f"    处理耗时: {subject_duration:.2f} 秒")
                    
                    if result['cleaned_records'] > 0:
                        print(f"    [SUCCESS] 科目 {subject_name} 清洗成功")
                    else:
                        print(f"    [WARNING] 科目 {subject_name} 清洗后无数据")
                        
                except Exception as e:
                    print(f"    [ERROR] 科目 {subject_name} 清洗失败: {e}")
                    subject_result = {
                        'subject_name': subject_name,
                        'instrument_id': instrument_id,
                        'question_count': question_count,
                        'raw_records': 0,
                        'cleaned_records': 0,
                        'unique_students': 0,
                        'processing_time': 0,
                        'error': str(e)
                    }
                    batch_result['subjects_detail'].append(subject_result)
            
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            batch_result['processing_time'] = batch_duration
            
            total_questionnaire_subjects += len(questionnaire_subjects)
            batch_results[batch_code] = batch_result
            
            print(f"\n批次 {batch_code} 处理完成:")
            print(f"  问卷科目: {len(questionnaire_subjects)} 个")
            print(f"  清洗记录: {batch_result['total_cleaned_records']:,} 条")
            print(f"  处理耗时: {batch_duration/60:.2f} 分钟")
        
        # 3. 汇总统计
        print(f"\n{'='*60}")
        print("总体清洗统计")
        print(f"{'='*60}")
        
        total_processing_time = sum(br['processing_time'] for br in batch_results.values())
        
        print(f"处理批次数: {len(batches)} 个")
        print(f"问卷科目总数: {total_questionnaire_subjects} 个")
        print(f"清洗记录总数: {total_cleaned_records:,} 条")
        print(f"总处理时间: {total_processing_time/60:.2f} 分钟")
        
        # 按批次详细统计
        print(f"\n按批次详细统计:")
        for batch_code, batch_result in batch_results.items():
            print(f"\n批次 {batch_code}:")
            print(f"  问卷科目: {batch_result['questionnaire_subjects']} 个")
            print(f"  清洗记录: {batch_result['total_cleaned_records']:,} 条")
            print(f"  处理时间: {batch_result['processing_time']/60:.2f} 分钟")
            
            if batch_result['subjects_detail']:
                print(f"  科目明细:")
                for subject in batch_result['subjects_detail']:
                    status = "成功" if subject['cleaned_records'] > 0 else ("失败" if 'error' in subject else "无数据")
                    print(f"    - {subject['subject_name']}: {subject['cleaned_records']:,} 条记录 [{status}]")
        
        print(f"\n{'='*60}")
        print("[FINAL] 所有批次问卷数据清洗完成！")
        print(f"完成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"[ERROR] 清洗过程发生错误: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        session.close()

if __name__ == "__main__":
    asyncio.run(clean_questionnaire_all_batches())