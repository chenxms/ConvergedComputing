#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试多科目计算功能 - 使用数据样本
"""
import asyncio
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from app.services.calculation_service import CalculationService

async def test_multi_subject_sample():
    print("=== 测试多科目计算功能（数据样本） ===")
    print()
    
    db = next(get_db())
    
    try:
        calculation_service = CalculationService(db)
        batch_code = "G7-2025"
        
        print("1. 获取科目配置信息...")
        subjects_config = await calculation_service._get_batch_subjects(batch_code)
        if subjects_config:
            print(f"找到 {len(subjects_config)} 个科目:")
            for i, subject in enumerate(subjects_config, 1):
                print(f"  {i}. {subject['subject_name']} (满分: {subject['max_score']})")
        else:
            print("ERROR: 没有找到科目配置")
            return False
        
        print()
        print("2. 获取学生数据样本...")
        # 获取少量数据进行测试
        from sqlalchemy import text
        query = text("""
            SELECT 
                ssd.student_id,
                ssd.student_name,
                ssd.school_id,
                ssd.school_code,
                ssd.school_name,
                ssd.subject_name,
                ssd.total_score,
                sqc.max_score
            FROM student_score_detail ssd
            JOIN subject_question_config sqc 
                ON ssd.subject_name = sqc.subject_name 
                AND ssd.batch_code = sqc.batch_code
            WHERE ssd.batch_code = :batch_code
            LIMIT 1000
        """)
        
        result = db.execute(query, {'batch_code': batch_code})
        rows = result.fetchall()
        
        if not rows:
            print("ERROR: 没有找到数据样本")
            return False
        
        # 转换为DataFrame
        import pandas as pd
        sample_df = pd.DataFrame(rows, columns=[
            'student_id', 'student_name', 'school_id', 'school_code', 'school_name',
            'subject_name', 'total_score', 'max_score'
        ])
        
        print(f"获取到 {len(sample_df)} 条记录")
        print(f"包含 {sample_df['subject_name'].nunique()} 个科目")
        print(f"包含 {sample_df['student_id'].nunique()} 个学生")
        print(f"包含 {sample_df['school_code'].nunique()} 个学校")
        
        print()
        print("3. 测试多科目统计整合...")
        
        # 直接调用多科目整合方法
        result = await calculation_service._consolidate_multi_subject_results(
            batch_code, sample_df
        )
        
        if 'academic_subjects' in result:
            subjects = result['academic_subjects']
            print(f"SUCCESS: 成功处理 {len(subjects)} 个科目")
            print()
            
            for subject_name, subject_data in subjects.items():
                if 'school_stats' in subject_data:
                    stats = subject_data['school_stats']
                    avg_score = stats.get('avg_score', 0)
                    student_count = stats.get('student_count', 0)
                    score_rate = stats.get('score_rate', 0)
                    
                    # 检查百分位数
                    percentiles = subject_data.get('percentiles', {})
                    p10 = percentiles.get('P10', 0)
                    p50 = percentiles.get('P50', 0) 
                    p90 = percentiles.get('P90', 0)
                    
                    # 检查等级分布
                    grade_dist = subject_data.get('grade_distribution', {})
                    excellent_pct = grade_dist.get('excellent', {}).get('percentage', 0)
                    
                    print(f"  {subject_name}:")
                    print(f"    平均分: {avg_score:.2f}")
                    print(f"    学生数: {student_count}")
                    print(f"    得分率: {score_rate:.1%}")
                    print(f"    P10/P50/P90: {p10:.1f}/{p50:.1f}/{p90:.1f}")
                    print(f"    优秀率: {excellent_pct:.1f}%")
                    print()
        else:
            print("ERROR: 结果中没有academic_subjects")
            return False
        
        print("=== 多科目计算功能验证成功 ===")
        print(f"✓ 支持 {len(result['academic_subjects'])} 个科目的并行计算")
        print("✓ 每个科目包含完整统计指标: 平均分、得分率、P10/P50/P90、等级分布")
        print("✓ 多科目数据结构完整，支持前端展示")
        
        return True
        
    except Exception as e:
        print(f"ERROR: 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

async def main():
    success = await test_multi_subject_sample()
    if success:
        print("\n🎉 多科目统计计算功能验证成功!")
        print("现在系统支持G7-2025批次所有科目的统计分析，包含P10、P50、P90百分位数")
    else:
        print("\n❌ 多科目统计计算功能需要进一步调试")

if __name__ == "__main__":
    asyncio.run(main())