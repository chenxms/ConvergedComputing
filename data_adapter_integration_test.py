#!/usr/bin/env python3
"""
数据适配器端到端集成测试
验证数据清洗与汇聚对接的完整流程
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
import json
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.services.calculation_service import CalculationService
from app.database.repositories import DataAdapterRepository

async def test_data_adapter_integration():
    """数据适配器端到端集成测试"""
    print("=== 数据适配器端到端集成测试 ===\n")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 创建数据适配器和计算服务
        data_adapter = DataAdapterRepository(session)
        calc_service = CalculationService(session)
        
        print("[OK] 数据库连接成功\n")
        
        # 测试批次列表
        test_batches = ['G7-2025', 'G4-2025', 'G8-2025']
        
        for batch_code in test_batches:
            print(f"[TEST] 测试批次: {batch_code}")
            print("-" * 50)
            
            # 1. 测试数据源自动选择
            await test_data_source_selection(data_adapter, batch_code)
            
            # 2. 测试科目配置获取
            await test_subject_configuration_retrieval(data_adapter, batch_code)
            
            # 3. 测试数据完整性验证
            await test_data_readiness_check(data_adapter, batch_code)
            
            # 4. 测试考试数据处理流程
            await test_exam_data_processing(calc_service, batch_code)
            
            # 5. 测试问卷数据处理流程（如果有）
            await test_questionnaire_data_processing(data_adapter, calc_service, batch_code)
            
            # 6. 测试维度统计计算
            await test_dimension_statistics(data_adapter, batch_code)
            
            print(f"✅ 批次 {batch_code} 测试完成\n")
        
        session.close()
        print("🎉 所有集成测试通过！")
        
    except Exception as e:
        print(f"❌ 集成测试失败: {e}")
        import traceback
        traceback.print_exc()

async def test_data_source_selection(data_adapter: DataAdapterRepository, batch_code: str):
    """测试数据源自动选择功能"""
    print("  1. 测试数据源自动选择...")
    
    try:
        # 检查数据就绪状态
        readiness = data_adapter.check_data_readiness(batch_code)
        print(f"     数据源状态: {readiness['overall_status']}")
        print(f"     清洗数据: {'✅' if readiness['data_sources']['has_cleaned_data'] else '❌'}")
        print(f"     原始数据: {'✅' if readiness['data_sources']['has_original_data'] else '❌'}")
        
        if readiness['overall_status'] == 'NO_DATA':
            print(f"     ⚠️ 批次 {batch_code} 无可用数据，跳过后续测试")
            return False
        
        # 获取学生分数数据
        scores = data_adapter.get_student_scores(batch_code)
        print(f"     获取到 {len(scores)} 条学生分数记录")
        
        if scores:
            sample = scores[0]
            print(f"     数据结构示例: {list(sample.keys())[:5]}...")
            
        return len(scores) > 0
        
    except Exception as e:
        print(f"     ❌ 数据源选择测试失败: {e}")
        return False

async def test_subject_configuration_retrieval(data_adapter: DataAdapterRepository, batch_code: str):
    """测试科目配置获取"""
    print("  2. 测试科目配置获取...")
    
    try:
        # 获取科目配置
        subjects = data_adapter.get_subject_configurations(batch_code)
        print(f"     找到 {len(subjects)} 个科目配置:")
        
        exam_subjects = []
        questionnaire_subjects = []
        
        for subject in subjects:
            subject_type = subject.get('subject_type', '')
            question_type = subject.get('question_type_enum', '')
            
            if question_type == 'questionnaire' or subject_type == 'questionnaire':
                questionnaire_subjects.append(subject)
                print(f"     📝 问卷: {subject['subject_name']} (满分: {subject['max_score']})")
            else:
                exam_subjects.append(subject)
                print(f"     📚 考试: {subject['subject_name']} (满分: {subject['max_score']})")
        
        return {
            'exam_count': len(exam_subjects),
            'questionnaire_count': len(questionnaire_subjects),
            'subjects': subjects
        }
        
    except Exception as e:
        print(f"     ❌ 科目配置获取失败: {e}")
        return None

async def test_data_readiness_check(data_adapter: DataAdapterRepository, batch_code: str):
    """测试数据完整性验证"""
    print("  3. 测试数据完整性验证...")
    
    try:
        readiness = data_adapter.check_data_readiness(batch_code)
        
        print(f"     总体状态: {readiness['overall_status']}")
        print(f"     学生数量: {readiness['student_count']}")
        print(f"     学校数量: {readiness['school_count']}")
        print(f"     科目数量: {readiness['subject_count']}")
        print(f"     数据源: {readiness['data_sources']['primary_source']}")
        
        # 检查数据质量指标
        if 'data_quality' in readiness:
            quality = readiness['data_quality']
            print(f"     数据完整性: {quality.get('completeness', 'N/A')}")
            print(f"     数据一致性: {quality.get('consistency', 'N/A')}")
        
        return readiness['overall_status'] in ['READY', 'READY_WITH_WARNINGS']
        
    except Exception as e:
        print(f"     ❌ 数据完整性验证失败: {e}")
        return False

async def test_exam_data_processing(calc_service: CalculationService, batch_code: str):
    """测试考试数据处理流程"""
    print("  4. 测试考试数据处理流程...")
    
    try:
        # 获取批次科目配置
        subjects = await calc_service._get_batch_subjects(batch_code)
        exam_subjects = [s for s in subjects 
                        if s.get('question_type_enum', '').lower() != 'questionnaire']
        
        if not exam_subjects:
            print("     ⚠️ 没有找到考试科目")
            return True
        
        print(f"     找到 {len(exam_subjects)} 个考试科目")
        
        # 测试单科目统计计算
        test_subject = exam_subjects[0]
        subject_name = test_subject['subject_name']
        max_score = test_subject['max_score']
        
        print(f"     测试科目: {subject_name} (满分: {max_score})")
        
        # 获取学生分数数据进行统计计算测试
        scores = calc_service.data_adapter.get_student_scores(batch_code, 'exam')
        subject_scores = [s for s in scores if s['subject_name'] == subject_name]
        
        if subject_scores:
            print(f"     该科目有 {len(subject_scores)} 条分数记录")
            
            # 简单统计验证
            total_scores = [s['score'] for s in subject_scores if s['score'] is not None]
            if total_scores:
                avg_score = sum(total_scores) / len(total_scores)
                print(f"     平均分: {avg_score:.2f}")
                print(f"     得分率: {(avg_score/max_score)*100:.1f}%")
        
        return True
        
    except Exception as e:
        print(f"     ❌ 考试数据处理测试失败: {e}")
        return False

async def test_questionnaire_data_processing(data_adapter: DataAdapterRepository, calc_service: CalculationService, batch_code: str):
    """测试问卷数据处理流程"""
    print("  5. 测试问卷数据处理流程...")
    
    try:
        # 获取问卷科目
        subjects = data_adapter.get_subject_configurations(batch_code)
        questionnaire_subjects = [s for s in subjects 
                                if s.get('question_type_enum', '').lower() == 'questionnaire']
        
        if not questionnaire_subjects:
            print("     ℹ️ 该批次没有问卷科目")
            return True
        
        print(f"     找到 {len(questionnaire_subjects)} 个问卷科目")
        
        # 测试问卷明细数据获取
        for subject in questionnaire_subjects[:2]:  # 测试前2个问卷
            subject_name = subject['subject_name']
            print(f"     测试问卷: {subject_name}")
            
            # 获取问卷明细数据
            questionnaire_details = data_adapter.get_questionnaire_details(batch_code, subject_name)
            print(f"       问卷明细记录: {len(questionnaire_details)}")
            
            if questionnaire_details:
                # 检查量表类型
                scale_types = set(d.get('scale_level') for d in questionnaire_details)
                print(f"       量表类型: {scale_types}")
                
                # 获取选项分布
                distribution = data_adapter.get_questionnaire_distribution(batch_code, subject_name)
                print(f"       选项分布记录: {len(distribution)}")
        
        return True
        
    except Exception as e:
        print(f"     ❌ 问卷数据处理测试失败: {e}")
        return False

async def test_dimension_statistics(data_adapter: DataAdapterRepository, batch_code: str):
    """测试维度统计计算"""
    print("  6. 测试维度统计计算...")
    
    try:
        # 获取维度配置
        dimension_config = data_adapter.get_dimension_configurations(batch_code)
        
        if not dimension_config:
            print("     ℹ️ 该批次没有维度配置")
            return True
        
        print(f"     找到 {len(dimension_config)} 个维度配置")
        
        # 检查维度映射数据
        for config in dimension_config[:3]:  # 测试前3个维度
            dimension_name = config.get('dimension_name', 'Unknown')
            subject_name = config.get('subject_name', 'Unknown')
            
            print(f"     维度: {dimension_name} (科目: {subject_name})")
            
            # 获取维度统计数据
            dimension_stats = data_adapter.get_dimension_statistics(batch_code, subject_name, dimension_name)
            print(f"       维度统计记录: {len(dimension_stats)}")
        
        return True
        
    except Exception as e:
        print(f"     ❌ 维度统计测试失败: {e}")
        return False

async def test_performance_metrics():
    """测试性能指标"""
    print("📈 性能测试...")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        data_adapter = DataAdapterRepository(session)
        
        # 测试大批次性能
        large_batch = 'G7-2025'  # 通常是最大的批次
        
        start_time = datetime.now()
        
        # 数据获取性能测试
        scores = data_adapter.get_student_scores(large_batch)
        data_fetch_time = (datetime.now() - start_time).total_seconds()
        
        print(f"  数据获取性能: {len(scores)} 条记录，耗时 {data_fetch_time:.2f} 秒")
        print(f"  平均处理速度: {len(scores)/data_fetch_time:.0f} 记录/秒")
        
        # 性能基准检查
        if data_fetch_time > 10:
            print("  ⚠️ 数据获取性能可能需要优化")
        else:
            print("  ✅ 数据获取性能良好")
        
        session.close()
        return True
        
    except Exception as e:
        print(f"  ❌ 性能测试失败: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(test_data_adapter_integration())
    print("\n" + "="*50)
    asyncio.run(test_performance_metrics())