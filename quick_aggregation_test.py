#!/usr/bin/env python3
"""
快速汇聚状态检查
验证数据清洗完成后的系统状态
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.database.repositories import DataAdapterRepository

async def quick_aggregation_check():
    """快速检查汇聚准备状态"""
    print("=== 快速汇聚状态检查 ===\n")
    
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
        
        # 2. 快速科目配置检查
        print("\n2. 科目配置检查...")
        subjects = data_adapter.get_subject_configurations(test_batch)
        
        exam_count = 0
        questionnaire_count = 0
        
        for subject in subjects[:5]:  # 只显示前5个
            if subject.get('question_type_enum') == 'questionnaire':
                questionnaire_count += 1
                subject_type_display = '问卷'
            else:
                exam_count += 1
                subject_type_display = '考试'
            
            print(f"   [{subject_type_display}] {subject['subject_name']} (满分: {subject['max_score']})")
        
        total_questionnaire = len([s for s in subjects if s.get('question_type_enum') == 'questionnaire'])
        total_exam = len(subjects) - total_questionnaire
        
        print(f"   总计: {len(subjects)}个科目 (考试:{total_exam}, 问卷:{total_questionnaire})")
        
        # 3. 数据库表状态直接检查
        print("\n3. 数据库表状态检查...")
        
        with engine.connect() as conn:
            # 检查清洗数据表
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
            
            print(f"   清洗数据表统计:")
            print(f"     总记录数: {cleaned_stats.total_records:,}")
            print(f"     学生数: {cleaned_stats.unique_students:,}")
            print(f"     学校数: {cleaned_stats.unique_schools:,}")
            print(f"     科目数: {cleaned_stats.unique_subjects:,}")
            
            # 检查问卷数据表
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT student_id) as unique_students,
                    COUNT(DISTINCT subject_name) as unique_subjects
                FROM questionnaire_question_scores 
                WHERE batch_code = :batch_code
            """), {'batch_code': test_batch})
            
            questionnaire_stats = result.fetchone()
            
            print(f"   问卷数据表统计:")
            print(f"     总记录数: {questionnaire_stats.total_records:,}")
            print(f"     学生数: {questionnaire_stats.unique_students:,}")
            print(f"     科目数: {questionnaire_stats.unique_subjects:,}")
        
        # 4. 汇聚准备状态评估
        print("\n4. 汇聚准备状态评估...")
        
        is_ready = True
        issues = []
        
        if readiness['overall_status'] == 'NO_DATA':
            is_ready = False
            issues.append("无可用数据")
        elif readiness['overall_status'] == 'ORIGINAL_DATA_ONLY':
            issues.append("仅有原始数据，建议使用清洗数据")
        
        if cleaned_stats.total_records == 0:
            is_ready = False
            issues.append("清洗数据表为空")
        
        if len(subjects) == 0:
            is_ready = False
            issues.append("无科目配置")
        
        if cleaned_stats.unique_schools == 0:
            is_ready = False
            issues.append("无学校数据")
        
        # 5. 系统状态总结
        print("\n5. 系统状态总结...")
        
        if is_ready:
            print("   ✅ 汇聚系统准备就绪！")
            print("   ✅ 数据清洗完成")
            print("   ✅ 科目配置完整")
            print("   ✅ 学校数据可用")
            if questionnaire_stats.total_records > 0:
                print("   ✅ 问卷数据可用")
        else:
            print("   ❌ 汇聚系统未就绪")
            for issue in issues:
                print(f"   ❌ {issue}")
        
        if issues and readiness['overall_status'] in ['READY_WITH_WARNINGS', 'ORIGINAL_DATA_ONLY']:
            print("\n   ⚠️ 警告:")
            for issue in issues:
                print(f"   ⚠️ {issue}")
            print("   ⚠️ 系统可运行但建议修复上述问题")
        
        session.close()
        
        return {
            'is_ready': is_ready,
            'overall_status': readiness['overall_status'],
            'cleaned_records': cleaned_stats.total_records,
            'cleaned_students': cleaned_stats.unique_students,
            'questionnaire_records': questionnaire_stats.total_records,
            'total_subjects': len(subjects),
            'exam_subjects': total_exam,
            'questionnaire_subjects': total_questionnaire,
            'unique_schools': cleaned_stats.unique_schools,
            'completeness_ratio': readiness['completeness_ratio'],
            'issues': issues
        }
        
    except Exception as e:
        print(f"[ERROR] 快速状态检查失败: {e}")
        import traceback
        traceback.print_exc()
        return {'is_ready': False, 'error': str(e)}

if __name__ == "__main__":
    result = asyncio.run(quick_aggregation_check())
    
    print("\n" + "="*60)
    print("汇聚系统状态报告:")
    if result.get('is_ready'):
        print("🚀 状态: 系统就绪，可以执行汇聚计算")
        print(f"📊 清洗数据: {result.get('cleaned_records', 0):,}条记录")
        print(f"👥 学生数量: {result.get('cleaned_students', 0):,}人")
        print(f"🏫 学校数量: {result.get('unique_schools', 0):,}所")
        print(f"📚 考试科目: {result.get('exam_subjects', 0)}个")
        print(f"📝 问卷科目: {result.get('questionnaire_subjects', 0)}个")
        print(f"📈 数据完整度: {result.get('completeness_ratio', 0):.1%}")
        
        if result.get('questionnaire_records', 0) > 0:
            print(f"📋 问卷记录: {result.get('questionnaire_records'):,}条")
            
    else:
        print("❌ 状态: 系统未就绪")
        if 'error' in result:
            print(f"❌ 错误: {result['error']}")
        if 'issues' in result:
            for issue in result['issues']:
                print(f"❌ 问题: {issue}")
    
    print("="*60)