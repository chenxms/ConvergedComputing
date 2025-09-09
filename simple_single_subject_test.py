#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单的单科目计算测试
验证计算管道是否工作
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from app.services.calculation_service import CalculationService
from sqlalchemy import text
import pandas as pd
import asyncio


async def simple_single_subject_test():
    """简单的单科目计算测试"""
    print("=" * 50)
    print("单科目计算验证测试")  
    print("=" * 50)
    print()
    
    db = next(get_db())
    
    try:
        calculation_service = CalculationService(db)
        batch_code = "G7-2025"
        
        print("1. 获取数学科目数据样本...")
        
        # 只取数学科目的少量数据进行测试
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
                AND ssd.subject_name = '数学'
            LIMIT 1000
        """)
        
        result = db.execute(query, {'batch_code': batch_code})
        rows = result.fetchall()
        
        if not rows:
            print("ERROR: 没有找到数学科目数据")
            return False
        
        # 转换为DataFrame
        sample_df = pd.DataFrame(rows, columns=[
            'student_id', 'student_name', 'school_id', 'school_code', 'school_name',
            'subject_name', 'total_score', 'max_score'
        ])
        
        print(f"获取到数学科目 {len(sample_df)} 条记录")
        print(f"涉及 {sample_df['student_id'].nunique()} 个学生")
        print(f"涉及 {sample_df['school_code'].nunique()} 个学校")
        
        max_score = sample_df['max_score'].iloc[0]
        avg_score = sample_df['total_score'].mean()
        print(f"数学满分: {max_score}, 平均分: {avg_score:.2f}")
        
        print()
        print("2. 测试多科目整合方法...")
        
        # 调用多科目整合方法
        result = await calculation_service._consolidate_multi_subject_results(
            batch_code, sample_df
        )
        
        if 'academic_subjects' in result:
            subjects = result['academic_subjects']
            print(f"SUCCESS: 成功处理 {len(subjects)} 个科目")
            
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
                    
        else:
            print("ERROR: 结果中没有academic_subjects")
            return False
        
        print()
        print("SUCCESS: 单科目计算验证成功!")
        return True
        
    except Exception as e:
        print(f"ERROR: 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()


async def main():
    success = await simple_single_subject_test()
    if success:
        print("\n单科目计算管道验证成功! 可以继续全科目计算")
    else:
        print("\n单科目计算需要调试")


if __name__ == "__main__":
    asyncio.run(main())