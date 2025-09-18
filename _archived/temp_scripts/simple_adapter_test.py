#!/usr/bin/env python3
"""
简化的数据适配器集成测试
验证数据清洗与汇聚对接的核心功能
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

async def simple_adapter_test():
    """简化的数据适配器集成测试"""
    print("=== 数据适配器集成测试 ===\n")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 创建数据适配器和计算服务
        data_adapter = DataAdapterRepository(session)
        calc_service = CalculationService(session)
        
        print("[OK] 数据库连接成功")
        
        # 测试批次
        test_batch = 'G7-2025'
        print(f"\n[TEST] 测试批次: {test_batch}")
        print("-" * 40)
        
        # 1. 测试数据源自动选择
        print("1. 测试数据源自动选择...")
        readiness = data_adapter.check_data_readiness(test_batch)
        print(f"   数据源状态: {readiness['overall_status']}")
        print(f"   学生数量: {readiness['student_count']}")
        print(f"   学校数量: {readiness['school_count']}")
        print(f"   科目数量: {readiness['subject_count']}")
        
        if readiness['overall_status'] == 'NO_DATA':
            print("[WARN] 批次无可用数据，跳过测试")
            return
        
        # 2. 测试科目配置获取
        print("\n2. 测试科目配置获取...")
        subjects = data_adapter.get_subject_configurations(test_batch)
        print(f"   找到 {len(subjects)} 个科目配置")
        
        exam_count = 0
        questionnaire_count = 0
        
        for subject in subjects[:5]:  # 显示前5个
            subject_type = subject.get('subject_type', '')
            question_type = subject.get('question_type_enum', '')
            
            if question_type == 'questionnaire' or subject_type == 'questionnaire':
                questionnaire_count += 1
                print(f"   [Q] 问卷: {subject['subject_name']}")
            else:
                exam_count += 1
                print(f"   [E] 考试: {subject['subject_name']}")
        
        total_questionnaire = len([s for s in subjects if s.get('question_type_enum') == 'questionnaire'])
        print(f"   考试科目: {len(subjects) - total_questionnaire}, 问卷科目: {total_questionnaire}")
        
        # 3. 测试学生分数数据获取
        print("\n3. 测试学生分数数据获取...")
        scores = data_adapter.get_student_scores(test_batch)
        print(f"   获取到 {len(scores)} 条学生分数记录")
        
        if scores:
            sample = scores[0]
            print(f"   数据字段: {list(sample.keys())}")
            print(f"   示例数据: 学生ID={sample.get('student_id')}, 科目={sample.get('subject_name')}, 分数={sample.get('score')}")
        
        # 4. 测试问卷数据处理（如果有问卷科目）
        if questionnaire_count > 0:
            print("\n4. 测试问卷数据处理...")
            questionnaire_subjects = [s for s in subjects 
                                    if s.get('question_type_enum') == 'questionnaire']
            test_questionnaire = questionnaire_subjects[0]
            subject_name = test_questionnaire['subject_name']
            
            print(f"   测试问卷: {subject_name}")
            
            # 获取问卷明细数据
            questionnaire_details = data_adapter.get_questionnaire_details(test_batch, subject_name)
            print(f"   问卷明细记录: {len(questionnaire_details)}")
            
            if questionnaire_details:
                # 检查量表类型
                sample_detail = questionnaire_details[0]
                print(f"   问卷字段: {list(sample_detail.keys())}")
                
                # 获取选项分布
                distribution = data_adapter.get_questionnaire_distribution(test_batch, subject_name)
                print(f"   选项分布记录: {len(distribution)}")
        
        # 5. 测试维度统计
        print("\n5. 测试维度统计...")
        dimension_config = data_adapter.get_dimension_configurations(test_batch)
        
        if dimension_config:
            print(f"   找到 {len(dimension_config)} 个维度配置")
            
            # 测试一个维度
            if dimension_config:
                test_config = dimension_config[0]
                dimension_name = test_config.get('dimension_name', 'Unknown')
                subject_name = test_config.get('subject_name', 'Unknown')
                
                print(f"   测试维度: {dimension_name} (科目: {subject_name})")
                
                dimension_stats = data_adapter.get_dimension_statistics(test_batch, subject_name, dimension_name)
                print(f"   维度统计记录: {len(dimension_stats)}")
        else:
            print("   该批次没有维度配置")
        
        # 6. 性能测试
        print("\n6. 性能测试...")
        start_time = datetime.now()
        
        # 重新获取数据测试性能
        scores = data_adapter.get_student_scores(test_batch)
        data_fetch_time = (datetime.now() - start_time).total_seconds()
        
        print(f"   数据获取性能: {len(scores)} 条记录，耗时 {data_fetch_time:.2f} 秒")
        if len(scores) > 0:
            print(f"   平均处理速度: {len(scores)/data_fetch_time:.0f} 记录/秒")
        
        if data_fetch_time > 10:
            print("   [WARN] 数据获取性能可能需要优化")
        else:
            print("   [OK] 数据获取性能良好")
        
        session.close()
        print("\n[SUCCESS] 数据适配器集成测试完成！")
        
        # 返回测试结果摘要
        return {
            'status': 'success',
            'batch_code': test_batch,
            'data_status': readiness['overall_status'],
            'student_count': len(scores),
            'subject_count': len(subjects),
            'questionnaire_count': questionnaire_count,
            'exam_count': len(subjects) - questionnaire_count,
            'performance_seconds': data_fetch_time
        }
        
    except Exception as e:
        print(f"[ERROR] 集成测试失败: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'error': str(e)}

async def test_calculation_service_integration():
    """测试计算服务集成"""
    print("\n=== 计算服务集成测试 ===")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        calc_service = CalculationService(session)
        test_batch = 'G7-2025'
        
        print(f"[TEST] 测试批次: {test_batch}")
        
        # 1. 测试批次科目获取
        print("\n1. 测试批次科目获取...")
        subjects = await calc_service._get_batch_subjects(test_batch)
        print(f"   通过计算服务获取到 {len(subjects)} 个科目")
        
        # 2. 测试学校列表获取
        print("\n2. 测试学校列表获取...")
        schools = await calc_service._get_batch_schools(test_batch)
        print(f"   通过计算服务获取到 {len(schools)} 个学校")
        
        if schools:
            print(f"   示例学校ID: {schools[0]}")
        
        # 3. 测试计算配置获取
        print("\n3. 测试计算配置获取...")
        config = await calc_service._get_calculation_config(test_batch)
        print(f"   年级级别: {config.get('grade_level')}")
        print(f"   默认满分: {config.get('max_score')}")
        
        session.close()
        print("\n[SUCCESS] 计算服务集成测试完成！")
        return True
        
    except Exception as e:
        print(f"[ERROR] 计算服务集成测试失败: {e}")
        return False

if __name__ == "__main__":
    # 运行适配器测试
    result = asyncio.run(simple_adapter_test())
    
    if result and result.get('status') == 'success':
        # 如果适配器测试成功，运行计算服务测试
        asyncio.run(test_calculation_service_integration())
    
    print("\n" + "="*50)
    print("测试总结:")
    if result:
        if result.get('status') == 'success':
            print(f"- 测试状态: 成功")
            print(f"- 测试批次: {result.get('batch_code')}")
            print(f"- 数据状态: {result.get('data_status')}")
            print(f"- 学生记录: {result.get('student_count')}")
            print(f"- 科目数量: {result.get('subject_count')}")
            print(f"- 考试科目: {result.get('exam_count')}")
            print(f"- 问卷科目: {result.get('questionnaire_count')}")
            print(f"- 性能耗时: {result.get('performance_seconds'):.2f}秒")
        else:
            print(f"- 测试状态: 失败")
            print(f"- 错误信息: {result.get('error')}")
    print("="*50)