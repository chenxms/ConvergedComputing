#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化的批量汇聚执行器
安全地执行G4-2025、G7-2025、G8-2025的数据汇聚
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.services.calculation_service import CalculationService
from app.database.repositories import DataAdapterRepository

async def test_single_batch_aggregation(batch_code: str):
    """测试单个批次的汇聚功能"""
    print(f"\n{'='*50}")
    print(f"测试批次汇聚: {batch_code}")
    print(f"{'='*50}")
    
    start_time = datetime.now()
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 1. 检查数据准备状态
        print("步骤1: 检查数据准备状态...")
        data_adapter = DataAdapterRepository(session)
        readiness = data_adapter.check_data_readiness(batch_code)
        
        print(f"   数据状态: {readiness['overall_status']}")
        print(f"   学生数量: {readiness['cleaned_students']:,}")
        print(f"   数据完整度: {readiness['completeness_ratio']:.1%}")
        print(f"   数据源: {readiness['data_sources']['primary_source']}")
        
        if not readiness['data_sources']['has_cleaned_data']:
            print(f"[警告] 批次 {batch_code} 缺少清洗数据")
            return {'status': 'skipped', 'reason': '缺少清洗数据'}
        
        # 2. 获取科目配置
        print("\n步骤2: 获取科目配置...")
        subjects = data_adapter.get_subject_configurations(batch_code)
        exam_subjects = [s for s in subjects if s.get('question_type_enum') != 'questionnaire']
        questionnaire_subjects = [s for s in subjects if s.get('question_type_enum') == 'questionnaire']
        
        print(f"   总科目数: {len(subjects)}")
        print(f"   考试科目: {len(exam_subjects)}")  
        print(f"   问卷科目: {len(questionnaire_subjects)}")
        
        # 3. 创建计算服务
        print("\n步骤3: 初始化计算服务...")
        calc_service = CalculationService(session)
        
        # 4. 测试区域级汇聚
        print("\n步骤4: 执行区域级汇聚...")
        try:
            regional_result = await calc_service.calculate_batch_statistics(batch_code)
            print(f"   区域级汇聚: 成功")
            regional_success = True
        except Exception as e:
            print(f"   区域级汇聚: 失败 - {str(e)[:100]}")
            regional_success = False
            regional_result = None
        
        # 5. 测试学校级汇聚（限制学校数量）
        print("\n步骤5: 执行学校级汇聚测试...")
        schools = await calc_service._get_batch_schools(batch_code)
        print(f"   批次中学校数: {len(schools)}")
        
        # 只测试前3个学校
        test_schools = schools[:3]
        school_success = 0
        school_fail = 0
        
        for i, school_id in enumerate(test_schools, 1):
            try:
                print(f"   测试学校 {i}/{len(test_schools)}: {school_id}")
                school_result = await calc_service.calculate_school_statistics(batch_code, school_id)
                school_success += 1
                print(f"   学校 {school_id}: 成功")
            except Exception as e:
                school_fail += 1
                print(f"   学校 {school_id}: 失败 - {str(e)[:80]}")
        
        # 6. 汇总结果
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n步骤6: 测试结果汇总...")
        print(f"   耗时: {int(duration//60)}分{int(duration%60)}秒")
        print(f"   区域级: {'成功' if regional_success else '失败'}")
        print(f"   学校级: 成功{school_success}/{len(test_schools)}")
        
        overall_success = regional_success and school_success > 0
        
        session.close()
        
        return {
            'batch_code': batch_code,
            'status': 'success' if overall_success else 'partial',
            'readiness': readiness,
            'subjects': {
                'total': len(subjects),
                'exam': len(exam_subjects),
                'questionnaire': len(questionnaire_subjects)
            },
            'regional_success': regional_success,
            'school_test': {
                'tested': len(test_schools),
                'success': school_success,
                'failed': school_fail
            },
            'duration_seconds': duration
        }
        
    except Exception as e:
        error_msg = f"批次 {batch_code} 测试异常: {str(e)}"
        print(f"[错误] {error_msg}")
        return {
            'batch_code': batch_code,
            'status': 'error',
            'error': error_msg
        }

async def run_all_batches():
    """运行所有批次的汇聚测试"""
    print("="*80)
    print("批量数据汇聚测试执行器")
    print("="*80)
    
    target_batches = ['G4-2025', 'G7-2025', 'G8-2025']
    results = {}
    
    overall_start = datetime.now()
    
    # 逐个测试批次
    for batch_code in target_batches:
        result = await test_single_batch_aggregation(batch_code)
        results[batch_code] = result
    
    # 生成汇总报告
    overall_end = datetime.now()
    overall_duration = (overall_end - overall_start).total_seconds()
    
    print(f"\n{'='*80}")
    print("批量汇聚测试汇总报告")
    print(f"{'='*80}")
    
    print(f"总执行时间: {int(overall_duration//60)}分{int(overall_duration%60)}秒")
    print(f"测试批次数: {len(target_batches)}")
    print()
    
    success_count = 0
    partial_count = 0
    error_count = 0
    
    for batch_code, result in results.items():
        print(f"批次 {batch_code}:")
        
        if result['status'] == 'success':
            success_count += 1
            print("   状态: 完全成功")
            print(f"   学生数: {result['readiness']['cleaned_students']:,}")
            print(f"   科目数: {result['subjects']['total']} (考试:{result['subjects']['exam']}, 问卷:{result['subjects']['questionnaire']})")
            print(f"   区域级汇聚: {'成功' if result['regional_success'] else '失败'}")
            print(f"   学校级汇聚测试: {result['school_test']['success']}/{result['school_test']['tested']}")
            print(f"   耗时: {int(result['duration_seconds']//60)}分{int(result['duration_seconds']%60)}秒")
            
        elif result['status'] == 'partial':
            partial_count += 1
            print("   状态: 部分成功")
            if 'readiness' in result:
                print(f"   学生数: {result['readiness']['cleaned_students']:,}")
            if 'regional_success' in result:
                print(f"   区域级汇聚: {'成功' if result['regional_success'] else '失败'}")
            if 'school_test' in result:
                print(f"   学校级汇聚测试: {result['school_test']['success']}/{result['school_test']['tested']}")
                
        elif result['status'] == 'skipped':
            print("   状态: 跳过")
            print(f"   原因: {result.get('reason', '未知')}")
            
        else:
            error_count += 1
            print("   状态: 错误")
            if 'error' in result:
                print(f"   错误: {result['error']}")
        
        print()
    
    # 最终统计
    print("="*50)
    print("最终统计:")
    print(f"   成功批次: {success_count}/{len(target_batches)}")
    print(f"   部分成功: {partial_count}/{len(target_batches)}")
    print(f"   失败批次: {error_count}/{len(target_batches)}")
    
    if success_count == len(target_batches):
        print("\n[SUCCESS] 所有批次汇聚测试完全成功！")
        print("汇聚系统工作正常，可以进行生产环境部署")
    elif success_count + partial_count == len(target_batches):
        print("\n[WARNING] 汇聚测试基本成功，建议检查部分成功的批次")
    else:
        print("\n[CAUTION] 部分批次汇聚测试失败，建议检查系统配置")
    
    print("="*80)

if __name__ == "__main__":
    asyncio.run(run_all_batches())