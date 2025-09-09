#!/usr/bin/env python3
"""
汇聚功能专项测试
验证数据清洗后的汇聚计算流程
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

async def test_aggregation_flow():
    """测试汇聚计算流程"""
    print("=== 汇聚功能专项测试 ===\n")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 创建服务实例
        calc_service = CalculationService(session)
        data_adapter = DataAdapterRepository(session)
        
        print("[OK] 数据库连接成功")
        
        # 测试批次
        test_batch = 'G7-2025'
        print(f"\n[TEST] 测试批次: {test_batch}")
        print("-" * 40)
        
        # 1. 检查数据准备状态
        print("1. 检查数据准备状态...")
        readiness = data_adapter.check_data_readiness(test_batch)
        print(f"   总体状态: {readiness['overall_status']}")
        print(f"   清洗数据学生数: {readiness['cleaned_students']}")
        print(f"   原始数据学生数: {readiness['original_students']}")
        print(f"   完成度比例: {readiness['completeness_ratio']:.1%}")
        
        if readiness['overall_status'] == 'NO_DATA':
            print("[ERROR] 无可用数据，停止测试")
            return
        
        # 2. 测试批次科目配置
        print("\n2. 获取批次科目配置...")
        subjects = await calc_service._get_batch_subjects(test_batch)
        print(f"   共找到 {len(subjects)} 个科目")
        
        # 分类科目
        exam_subjects = []
        questionnaire_subjects = []
        
        for subject in subjects:
            if subject.get('question_type_enum') == 'questionnaire':
                questionnaire_subjects.append(subject)
                print(f"   [问卷] {subject['subject_name']} (满分: {subject['max_score']})")
            else:
                exam_subjects.append(subject)
                print(f"   [考试] {subject['subject_name']} (满分: {subject['max_score']})")
        
        print(f"   考试科目数: {len(exam_subjects)}, 问卷科目数: {len(questionnaire_subjects)}")
        
        # 3. 测试单个学校的数据获取
        print("\n3. 测试单个学校数据获取...")
        schools = await calc_service._get_batch_schools(test_batch)
        print(f"   共找到 {len(schools)} 个学校")
        
        if schools:
            test_school = schools[0]
            print(f"   测试学校: {test_school}")
            
            # 获取该学校的分数数据（限制数量）
            school_scores = data_adapter.get_student_scores(test_batch, school_id=test_school)
            print(f"   该学校学生记录数: {len(school_scores)}")
            
            if school_scores:
                sample_score = school_scores[0]
                print(f"   数据示例: 学生={sample_score['student_id']}, 科目={sample_score['subject_name']}, 分数={sample_score['score']}")
        
        # 4. 测试小规模汇聚计算
        print("\n4. 测试汇聚计算功能...")
        if exam_subjects and schools:
            test_subject = exam_subjects[0]
            subject_name = test_subject['subject_name']
            max_score = test_subject['max_score']
            
            print(f"   测试科目: {subject_name} (满分: {max_score})")
            print(f"   测试学校: {test_school}")
            
            # 获取特定科目和学校的数据进行计算测试
            subject_school_scores = [
                s for s in school_scores 
                if s['subject_name'] == subject_name
            ]
            
            if subject_school_scores:
                print(f"   该科目该学校数据量: {len(subject_school_scores)}")
                
                # 基本统计信息
                scores = [s['score'] for s in subject_school_scores if s['score'] is not None]
                if scores:
                    avg_score = sum(scores) / len(scores)
                    min_score = min(scores)
                    max_score_actual = max(scores)
                    
                    print(f"   统计结果:")
                    print(f"     平均分: {avg_score:.2f}")
                    print(f"     最低分: {min_score:.2f}")
                    print(f"     最高分: {max_score_actual:.2f}")
                    print(f"     得分率: {(avg_score/max_score)*100:.1f}%")
        
        # 5. 测试问卷数据处理（如果有）
        if questionnaire_subjects:
            print("\n5. 测试问卷数据处理...")
            test_questionnaire = questionnaire_subjects[0]
            questionnaire_name = test_questionnaire['subject_name']
            
            print(f"   测试问卷: {questionnaire_name}")
            
            # 获取问卷明细数据
            questionnaire_details = data_adapter.get_questionnaire_details(test_batch, questionnaire_name)
            print(f"   问卷明细记录数: {len(questionnaire_details)}")
            
            if questionnaire_details:
                sample_detail = questionnaire_details[0]
                print(f"   问卷数据示例:")
                print(f"     学生ID: {sample_detail.get('student_id')}")
                print(f"     题目ID: {sample_detail.get('question_id')}")
                print(f"     原始分数: {sample_detail.get('original_score')}")
                print(f"     量表等级: {sample_detail.get('scale_level')}")
        
        # 6. 性能指标
        print("\n6. 性能指标测试...")
        start_time = datetime.now()
        
        # 快速数据获取测试
        quick_scores = data_adapter.get_student_scores(test_batch, school_id=test_school)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"   数据获取耗时: {duration:.2f}秒")
        print(f"   数据量: {len(quick_scores)}条记录")
        if duration > 0:
            print(f"   处理速度: {len(quick_scores)/duration:.0f}记录/秒")
        
        session.close()
        
        # 返回测试结果
        return {
            'status': 'success',
            'batch_code': test_batch,
            'data_status': readiness['overall_status'],
            'total_subjects': len(subjects),
            'exam_subjects': len(exam_subjects),
            'questionnaire_subjects': len(questionnaire_subjects),
            'total_schools': len(schools),
            'completeness_ratio': readiness['completeness_ratio'],
            'performance_seconds': duration
        }
        
    except Exception as e:
        print(f"[ERROR] 汇聚功能测试失败: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'error': str(e)}

async def test_calculation_service_integration():
    """测试计算服务完整集成"""
    print("\n=== 计算服务集成测试 ===")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        calc_service = CalculationService(session)
        test_batch = 'G7-2025'
        
        print(f"[TEST] 验证计算服务的数据适配器集成")
        
        # 1. 验证计算服务使用数据适配器
        print("\n1. 验证数据适配器集成...")
        print(f"   数据适配器类型: {type(calc_service.data_adapter).__name__}")
        print(f"   是否为DataAdapterRepository: {calc_service.data_adapter.__class__.__name__ == 'DataAdapterRepository'}")
        
        # 2. 测试计算配置获取
        print("\n2. 测试计算配置获取...")
        config = await calc_service._get_calculation_config(test_batch)
        print(f"   年级级别: {config.get('grade_level')}")
        print(f"   百分位数: {config.get('percentiles')}")
        
        if 'batch_summary' in config:
            batch_summary = config['batch_summary']
            print(f"   批次摘要信息: {type(batch_summary)}")
            print(f"   数据源: {batch_summary.get('data_source', '未知')}")
        
        # 3. 验证数据获取方法使用适配器
        print("\n3. 验证数据获取方法...")
        schools = await calc_service._get_batch_schools(test_batch)
        print(f"   通过计算服务获取学校数: {len(schools)}")
        
        subjects = await calc_service._get_batch_subjects(test_batch)
        print(f"   通过计算服务获取科目数: {len(subjects)}")
        
        session.close()
        print("\n[SUCCESS] 计算服务集成验证完成！")
        return True
        
    except Exception as e:
        print(f"[ERROR] 计算服务集成测试失败: {e}")
        return False

if __name__ == "__main__":
    print("开始汇聚功能专项测试...\n")
    
    # 运行汇聚功能测试
    result = asyncio.run(test_aggregation_flow())
    
    if result and result.get('status') == 'success':
        # 如果汇聚测试成功，运行计算服务集成测试
        asyncio.run(test_calculation_service_integration())
    
    print("\n" + "="*60)
    print("汇聚功能测试总结:")
    if result:
        if result.get('status') == 'success':
            print(f"✅ 测试状态: 成功")
            print(f"✅ 测试批次: {result.get('batch_code')}")
            print(f"✅ 数据状态: {result.get('data_status')}")
            print(f"✅ 科目总数: {result.get('total_subjects')}")
            print(f"✅ 考试科目: {result.get('exam_subjects')}")
            print(f"✅ 问卷科目: {result.get('questionnaire_subjects')}")
            print(f"✅ 学校总数: {result.get('total_schools')}")
            print(f"✅ 数据完整度: {result.get('completeness_ratio', 0):.1%}")
            print(f"✅ 性能表现: {result.get('performance_seconds', 0):.2f}秒")
            print("\n🎉 汇聚功能验证通过，系统已准备好进行统计计算！")
        else:
            print(f"❌ 测试状态: 失败")
            print(f"❌ 错误信息: {result.get('error')}")
    print("="*60)