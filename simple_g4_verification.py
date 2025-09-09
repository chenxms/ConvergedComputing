#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化的G4-2025汇聚结果验证
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import asyncio
import json

async def simple_verify_g4():
    """简化验证G4-2025汇聚结果"""
    print("=== G4-2025汇聚结果验证报告 ===")
    print()
    
    db = next(get_db())
    
    try:
        # 1. 汇聚数据概览
        print("1. 汇聚数据概览:")
        result = db.execute(text("""
            SELECT aggregation_level, COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025'
            GROUP BY aggregation_level
            ORDER BY aggregation_level
        """))
        
        for row in result:
            print(f"   {row.aggregation_level}: {row.count}条记录")
        print()
        
        # 2. 数据完整性验证
        print("2. 数据完整性验证:")
        total_check = db.execute(text("""
            SELECT COUNT(*) as total_schools,
                   SUM(total_students) as total_students,
                   MIN(created_at) as earliest_time,
                   MAX(created_at) as latest_time
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
        """))
        
        stats = total_check.fetchone()
        print(f"   学校总数: {stats.total_schools}")
        print(f"   学生总数: {stats.total_students}")
        print(f"   创建时间范围: {stats.earliest_time} ~ {stats.latest_time}")
        print()
        
        # 3. 区域数据验证
        print("3. 区域级汇聚数据:")
        region_check = db.execute(text("""
            SELECT total_students, total_schools, created_at
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'REGIONAL'
        """))
        
        region = region_check.fetchone()
        if region:
            print(f"   区域学生总数: {region.total_students}")
            print(f"   区域学校总数: {region.total_schools}")
            print(f"   区域数据创建时间: {region.created_at}")
        else:
            print("   未找到区域级数据!")
        print()
        
        # 4. 学校数据样例
        print("4. 学校数据样例 (前10所学校):")
        school_check = db.execute(text("""
            SELECT school_id, school_name, total_students
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            ORDER BY CAST(school_id AS UNSIGNED)
            LIMIT 10
        """))
        
        for row in school_check:
            school_id = row.school_id if row.school_id else "空ID"
            print(f"   学校ID: {school_id}, 名称: {row.school_name}, 学生数: {row.total_students}")
        print()
        
        # 5. 验证科目数据结构
        print("5. 科目数据结构验证:")
        subject_check = db.execute(text("""
            SELECT statistics_data
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' 
                AND aggregation_level = 'SCHOOL'
                AND school_id = '5044'
        """))
        
        school_data = subject_check.fetchone()
        if school_data and school_data.statistics_data:
            try:
                stats_json = json.loads(school_data.statistics_data)
                
                if 'subjects' in stats_json:
                    print(f"   发现科目数: {len(stats_json['subjects'])}")
                    for subject_name, subject_data in stats_json['subjects'].items():
                        student_count = subject_data.get('basic_statistics', {}).get('count', 0)
                        avg_score = subject_data.get('educational_metrics', {}).get('average_score', 0)
                        pass_rate = subject_data.get('educational_metrics', {}).get('pass_rate', 0) * 100
                        print(f"   - 科目: {subject_name}, 学生: {student_count}人, 平均分: {avg_score:.1f}, 及格率: {pass_rate:.1f}%")
                        
                        # 检查P10/P50/P90
                        percentiles = subject_data.get('percentiles', {})
                        if 'P10' in percentiles and 'P50' in percentiles and 'P90' in percentiles:
                            print(f"     P10: {percentiles['P10']:.1f}, P50: {percentiles['P50']:.1f}, P90: {percentiles['P90']:.1f}")
                        
                        # 检查等级分布
                        grade_dist = subject_data.get('educational_metrics', {}).get('grade_distribution', {})
                        if grade_dist:
                            excellent = grade_dist.get('excellent_rate', 0) * 100
                            good = grade_dist.get('good_rate', 0) * 100
                            print(f"     等级分布 - 优秀: {excellent:.1f}%, 良好: {good:.1f}%")
                else:
                    print("   数据结构中未找到科目信息")
            except json.JSONDecodeError as e:
                print(f"   JSON解析失败: {e}")
        else:
            print("   无法获取学校5044的统计数据")
        print()
        
        # 6. 检查学校ID分布
        print("6. 学校ID分布验证:")
        id_dist = db.execute(text("""
            SELECT 
                CASE 
                    WHEN school_id = '' THEN 'Empty ID'
                    WHEN school_id REGEXP '^[0-9]+$' THEN 'Numeric ID'
                    ELSE 'Other Format'
                END as id_type,
                COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            GROUP BY id_type
        """))
        
        for row in id_dist:
            print(f"   {row.id_type}: {row.count}个学校")
        print()
        
        # 7. 最终验证结果
        print("7. 最终验证结果:")
        if stats.total_schools == 57:
            print(f"   学校数量正确: {stats.total_schools}所学校")
        else:
            print(f"   学校数量异常: 期望57所，实际{stats.total_schools}所")
            
        if stats.total_students > 300000:
            print(f"   学生总数合理: {stats.total_students}名学生")
        else:
            print(f"   学生总数偏低: {stats.total_students}名学生")
            
        if region and region.total_schools > 0:
            print(f"   区域数据完整: 包含{region.total_schools}所学校")
        else:
            print(f"   区域数据缺失")
        
        print(f"\n汇聚时间: {stats.latest_time}")
        print("G4-2025批次汇聚验证完成，数据已准备好供审查！")
            
    except Exception as e:
        print(f"验证失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(simple_verify_g4())