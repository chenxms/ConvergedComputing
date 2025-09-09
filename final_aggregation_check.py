#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
汇聚系统最终状态检查
验证数据清洗完成后的系统就绪状态
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.database.repositories import DataAdapterRepository

async def final_aggregation_check():
    """最终汇聚状态检查"""
    print("=== 汇聚系统最终状态检查 ===\n")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 创建数据适配器
        data_adapter = DataAdapterRepository(session)
        
        print("[OK] 数据库连接成功")
        
        # 测试批次
        test_batch = 'G7-2025'
        print(f"\n[CHECK] 检查批次: {test_batch}")
        print("-" * 40)
        
        # 1. 数据准备状态检查
        print("1. 数据准备状态...")
        readiness = data_adapter.check_data_readiness(test_batch)
        
        print(f"   总体状态: {readiness['overall_status']}")
        print(f"   清洗数据学生数: {readiness['cleaned_students']:,}")
        print(f"   原始数据学生数: {readiness['original_students']:,}")
        print(f"   问卷数据学生数: {readiness['questionnaire_students']:,}")
        print(f"   数据完整度: {readiness['completeness_ratio']:.1%}")
        print(f"   主要数据源: {readiness['data_sources']['primary_source']}")
        
        # 2. 科目配置状态
        print("\n2. 科目配置状态...")
        subjects = data_adapter.get_subject_configurations(test_batch)
        
        exam_subjects = [s for s in subjects if s.get('question_type_enum') != 'questionnaire']
        questionnaire_subjects = [s for s in subjects if s.get('question_type_enum') == 'questionnaire']
        
        print(f"   总科目数: {len(subjects)}")
        print(f"   考试科目: {len(exam_subjects)}")
        print(f"   问卷科目: {len(questionnaire_subjects)}")
        
        # 3. 数据库统计
        print("\n3. 数据库统计...")
        
        with engine.connect() as conn:
            # 清洗数据统计
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT student_id) as unique_students,
                    COUNT(DISTINCT school_id) as unique_schools,
                    COUNT(DISTINCT subject_name) as unique_subjects
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
            """), {'batch_code': test_batch})
            
            cleaned_stats = result.fetchone()
            
            # 问卷数据统计
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT student_id) as unique_students,
                    COUNT(DISTINCT subject_name) as unique_subjects
                FROM questionnaire_question_scores 
                WHERE batch_code = :batch_code
            """), {'batch_code': test_batch})
            
            questionnaire_stats = result.fetchone()
        
        print(f"   清洗数据记录: {cleaned_stats.total_records:,}")
        print(f"   清洗数据学生: {cleaned_stats.unique_students:,}")
        print(f"   清洗数据学校: {cleaned_stats.unique_schools:,}")
        print(f"   问卷数据记录: {questionnaire_stats.total_records:,}")
        print(f"   问卷数据学生: {questionnaire_stats.unique_students:,}")
        
        # 4. 系统就绪评估
        print("\n4. 系统就绪评估...")
        
        is_ready = True
        readiness_score = 0
        max_score = 6
        
        # 检查项目
        checks = [
            ("数据状态", readiness['overall_status'] in ['READY', 'READY_WITH_WARNINGS']),
            ("清洗数据", cleaned_stats.total_records > 0),
            ("学生数据", cleaned_stats.unique_students > 0),
            ("学校数据", cleaned_stats.unique_schools > 0),
            ("科目配置", len(subjects) > 0),
            ("考试科目", len(exam_subjects) > 0)
        ]
        
        for check_name, passed in checks:
            if passed:
                print(f"   [PASS] {check_name}")
                readiness_score += 1
            else:
                print(f"   [FAIL] {check_name}")
                is_ready = False
        
        # 可选检查项目
        optional_checks = [
            ("问卷数据", questionnaire_stats.total_records > 0),
            ("数据完整度", readiness['completeness_ratio'] >= 0.95)
        ]
        
        for check_name, passed in optional_checks:
            if passed:
                print(f"   [BONUS] {check_name}")
            else:
                print(f"   [SKIP] {check_name}")
        
        # 5. 最终结论
        print(f"\n5. 最终结论...")
        print(f"   就绪评分: {readiness_score}/{max_score}")
        print(f"   数据完整度: {readiness['completeness_ratio']:.1%}")
        
        if is_ready and readiness_score >= 5:
            status = "READY"
            message = "汇聚系统完全就绪，可以开始统计计算"
        elif readiness_score >= 4:
            status = "READY_WITH_WARNINGS"  
            message = "汇聚系统基本就绪，建议检查警告项目"
        else:
            status = "NOT_READY"
            message = "汇聚系统未就绪，请修复失败项目"
        
        print(f"   系统状态: {status}")
        print(f"   状态说明: {message}")
        
        session.close()
        
        return {
            'status': status,
            'is_ready': is_ready,
            'readiness_score': readiness_score,
            'max_score': max_score,
            'batch_code': test_batch,
            'cleaned_records': cleaned_stats.total_records,
            'cleaned_students': cleaned_stats.unique_students,
            'unique_schools': cleaned_stats.unique_schools,
            'questionnaire_records': questionnaire_stats.total_records,
            'total_subjects': len(subjects),
            'exam_subjects': len(exam_subjects),
            'questionnaire_subjects': len(questionnaire_subjects),
            'completeness_ratio': readiness['completeness_ratio'],
            'data_status': readiness['overall_status']
        }
        
    except Exception as e:
        print(f"[ERROR] 系统状态检查失败: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'ERROR', 'is_ready': False, 'error': str(e)}

if __name__ == "__main__":
    result = asyncio.run(final_aggregation_check())
    
    print("\n" + "="*60)
    print("汇聚系统状态报告:")
    print("="*60)
    
    if result.get('status') == 'READY':
        print("[SUCCESS] 汇聚系统完全就绪！")
        print(f"批次代码: {result.get('batch_code')}")
        print(f"清洗数据: {result.get('cleaned_records', 0):,}条记录")
        print(f"学生数量: {result.get('cleaned_students', 0):,}人")
        print(f"学校数量: {result.get('unique_schools', 0):,}所")
        print(f"考试科目: {result.get('exam_subjects', 0)}个")
        print(f"问卷科目: {result.get('questionnaire_subjects', 0)}个")
        print(f"问卷记录: {result.get('questionnaire_records', 0):,}条")
        print(f"数据完整度: {result.get('completeness_ratio', 0):.1%}")
        print(f"就绪评分: {result.get('readiness_score', 0)}/{result.get('max_score', 6)}")
        print("\n[READY] 系统准备完毕，可以执行汇聚计算！")
        
    elif result.get('status') == 'READY_WITH_WARNINGS':
        print("[WARNING] 汇聚系统基本就绪（有警告）")
        print(f"就绪评分: {result.get('readiness_score', 0)}/{result.get('max_score', 6)}")
        print("建议检查上述警告项目后再进行汇聚计算")
        
    elif result.get('status') == 'NOT_READY':
        print("[FAILED] 汇聚系统未就绪")
        print(f"就绪评分: {result.get('readiness_score', 0)}/{result.get('max_score', 6)}")
        print("请修复上述失败项目后再试")
        
    else:
        print("[ERROR] 系统检查失败")
        if 'error' in result:
            print(f"错误信息: {result['error']}")
    
    print("="*60)