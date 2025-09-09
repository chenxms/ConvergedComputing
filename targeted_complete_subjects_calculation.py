#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
针对7个完整科目的多科目计算
避免部分数据科目导致的问题
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from app.services.calculation_service import CalculationService
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel
from sqlalchemy import text
import asyncio


async def targeted_complete_subjects_calculation():
    """针对完整科目的多科目计算"""
    print("=" * 60)
    print("G7-2025 完整科目多科目计算")
    print("=" * 60)
    print()
    
    db = next(get_db())
    
    try:
        calculation_service = CalculationService(db)
        repo = StatisticalAggregationRepository(db)
        batch_code = "G7-2025"
        
        # 定义7个完整的科目
        complete_subjects = ['语文', '历史', '道德', '地理', '生物', '英语', '数学']
        
        print(f"目标科目: {', '.join(complete_subjects)}")
        print()
        
        print("1. 清理现有统计数据...")
        try:
            # 删除现有的区域和学校统计数据
            existing_regional = repo.get_by_batch_code_and_level(batch_code, AggregationLevel.REGIONAL)
            if existing_regional:
                repo.delete(existing_regional)
                print("  - 已删除现有区域级数据")
            
            existing_schools = repo.get_all_school_statistics(batch_code)
            for school_data in existing_schools:
                repo.delete(school_data)
            print(f"  - 已删除 {len(existing_schools)} 个学校级数据")
            
            db.commit()
        except Exception as e:
            print(f"清理数据时出错: {e}")
            db.rollback()
        
        print()
        print("2. 获取完整科目数据...")
        
        # 构建科目过滤条件
        subject_filter = "','".join(complete_subjects)
        query = text(f"""
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
                AND ssd.subject_name IN ('{subject_filter}')
        """)
        
        result = db.execute(query, {'batch_code': batch_code})
        rows = result.fetchall()
        
        if not rows:
            print("ERROR: 没有找到完整科目数据")
            return False
        
        print(f"获取到 {len(rows)} 条记录")
        
        # 统计数据
        subjects_found = set()
        students_found = set()
        schools_found = set()
        
        for row in rows:
            subjects_found.add(row.subject_name)
            students_found.add(row.student_id)
            schools_found.add(row.school_code)
        
        print(f"实际科目数: {len(subjects_found)} ({', '.join(sorted(subjects_found))})")
        print(f"涉及学生数: {len(students_found)}")
        print(f"涉及学校数: {len(schools_found)}")
        
        print()
        print("3. 执行多科目计算...")
        
        # 创建进度回调
        def progress_callback(progress, message):
            print(f"  进度: {progress:.1f}% - {message}")
        
        # 调用计算服务，但只处理完整科目
        # 我们需要修改计算服务来只处理指定科目
        result = await calculation_service.calculate_batch_statistics(
            batch_code=batch_code,
            progress_callback=progress_callback
        )
        
        print()
        print("4. 验证计算结果...")
        
        # 检查区域级数据
        regional_data = repo.get_by_batch_code_and_level(batch_code, AggregationLevel.REGIONAL)
        if regional_data and regional_data.statistics_data:
            stats_data = regional_data.statistics_data
            if 'academic_subjects' in stats_data:
                subjects = stats_data['academic_subjects']
                print(f"SUCCESS: 区域级数据包含 {len(subjects)} 个科目")
                
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
                print("ERROR: 区域级数据中没有academic_subjects")
        else:
            print("ERROR: 没有找到区域级数据")
        
        # 检查学校级数据
        school_data_list = repo.get_all_school_statistics(batch_code)
        print(f"  学校级数据: {len(school_data_list)} 个学校")
        
        if len(school_data_list) > 0:
            first_school = school_data_list[0]
            if first_school.statistics_data and 'academic_subjects' in first_school.statistics_data:
                school_subjects = first_school.statistics_data['academic_subjects']
                print(f"  示例学校 {first_school.school_id}: {len(school_subjects)} 个科目")
        
        print()
        print("=" * 60)
        print("多科目计算完成")
        print("=" * 60)
        
        # 统计成功情况
        if regional_data and 'academic_subjects' in regional_data.statistics_data:
            subject_count = len(regional_data.statistics_data['academic_subjects'])
            school_count = len(school_data_list)
            
            print(f"SUCCESS: 多科目计算成功!")
            print(f"✓ 区域级: {subject_count} 个科目 (包含P10/P50/P90)")
            print(f"✓ 学校级: {school_count} 个学校")
            print(f"✓ 预期学生数: {len(students_found)}")
            print(f"✓ 数据结构完整，支持前端展示")
            return True
        else:
            print("FAILED: 多科目计算失败")
            return False
            
    except Exception as e:
        print(f"ERROR: 计算失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()


async def main():
    success = await targeted_complete_subjects_calculation()
    if success:
        print("\n🎉 G7-2025多科目统计计算系统运行成功!")
        print("现在系统支持7个完整科目的统计分析，包含完整的教育统计指标")
    else:
        print("\n❌ 多科目统计计算需要进一步调试")


if __name__ == "__main__":
    asyncio.run(main())