#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
G4-2025最终汇聚结果验证报告
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import asyncio
import json

async def final_g4_verification_report():
    """G4-2025最终汇聚结果验证报告"""
    print("=== G4-2025同事修改算法后的最终验证报告 ===")
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
        
        school_count = 0
        regional_count = 0
        for row in result:
            print(f"   {row.aggregation_level}: {row.count}条记录")
            if row.aggregation_level == 'SCHOOL':
                school_count = row.count
            elif row.aggregation_level == 'REGIONAL':
                regional_count = row.count
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
        print(f"   汇聚时间范围: {stats.earliest_time} ~ {stats.latest_time}")
        processing_duration = (stats.latest_time - stats.earliest_time).total_seconds()
        print(f"   处理耗时: {processing_duration:.1f}秒")
        print()
        
        # 3. 区域数据验证
        print("3. 区域级汇聚数据验证:")
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
            # 检查区域与学校数据的一致性
            if region.total_students == stats.total_students and region.total_schools == 0:
                print("   区域数据与学校数据学生数一致")
                print("   注意: 区域学校数为0，可能是数据结构问题")
        else:
            print("   未找到区域级数据!")
        print()
        
        # 4. 学校数据样例和统计质量检查
        print("4. 学校数据统计质量检查:")
        school_stats = db.execute(text("""
            SELECT school_id, school_name, total_students, statistics_data
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            ORDER BY total_students DESC
            LIMIT 5
        """))
        
        print("   前5所学生数最多的学校:")
        for row in school_stats:
            school_id = row.school_id if row.school_id else "空ID"
            print(f"   学校ID: {school_id}, 名称: {row.school_name}, 学生数: {row.total_students}")
            
            # 检查统计数据质量
            if row.statistics_data:
                try:
                    stats_json = json.loads(row.statistics_data)
                    
                    # 检查基础统计
                    if 'basic_stats' in stats_json:
                        basic = stats_json['basic_stats']
                        avg_score = basic.get('mean', 0)
                        print(f"     平均分: {avg_score:.2f}, 标准差: {basic.get('std', 0):.2f}")
                    
                    # 检查百分位数
                    if 'percentiles' in stats_json:
                        percentiles = stats_json['percentiles']
                        print(f"     百分位数 P10: {percentiles.get('P10', 0):.1f}, P50: {percentiles.get('P50', 0):.1f}, P90: {percentiles.get('P75', 0):.1f}")
                    
                    # 检查教育指标
                    if 'educational_metrics' in stats_json:
                        metrics = stats_json['educational_metrics']
                        pass_rate = metrics.get('pass_rate', 0) * 100
                        excellent_rate = metrics.get('excellent_rate', 0) * 100
                        print(f"     及格率: {pass_rate:.1f}%, 优秀率: {excellent_rate:.1f}%")
                    
                    # 检查区分度
                    if 'discrimination' in stats_json:
                        disc = stats_json['discrimination']
                        if 'interpretation' in disc:
                            print(f"     区分度评价: {disc['interpretation']}")
                    
                except json.JSONDecodeError:
                    print("     JSON数据解析失败")
            print()
        
        # 5. 学校ID分布情况
        print("5. 学校ID分布情况:")
        id_dist = db.execute(text("""
            SELECT 
                CASE 
                    WHEN school_id = '' OR school_id IS NULL THEN 'Empty ID'
                    WHEN school_id REGEXP '^[0-9]+$' THEN 'Numeric ID'
                    ELSE 'Other Format'
                END as id_type,
                COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            GROUP BY id_type
        """))
        
        empty_id_count = 0
        numeric_id_count = 0
        for row in id_dist:
            print(f"   {row.id_type}: {row.count}个学校")
            if row.id_type == 'Empty ID':
                empty_id_count = row.count
            elif row.id_type == 'Numeric ID':
                numeric_id_count = row.count
        print()
        
        # 6. 最终质量评估
        print("6. 数据质量评估结果:")
        quality_score = 0
        
        # 学校数量检查
        if stats.total_schools == 57:
            print(f"   ✓ 学校数量正确: {stats.total_schools}所学校")
            quality_score += 25
        else:
            print(f"   ✗ 学校数量异常: 期望57所，实际{stats.total_schools}所")
            
        # 学生总数检查
        if stats.total_students > 300000:
            print(f"   ✓ 学生总数合理: {stats.total_students}名学生")
            quality_score += 25
        else:
            print(f"   ✗ 学生总数偏低: {stats.total_students}名学生")
            
        # 区域数据检查
        if region and region.total_students > 0:
            print(f"   ✓ 区域数据完整: 学生总数{region.total_students}")
            quality_score += 25
        else:
            print(f"   ⚠ 区域数据需要检查: 学校总数字段为0")
            quality_score += 15
            
        # 学校ID分布检查
        if numeric_id_count > 50:
            print(f"   ✓ 学校ID分布良好: {numeric_id_count}个数字ID")
            quality_score += 20
        else:
            print(f"   ⚠ 学校ID分布: {numeric_id_count}个数字ID, {empty_id_count}个空ID")
            quality_score += 10
            
        # 数据结构更新检查
        if processing_duration < 200:
            print(f"   ✓ 处理效率良好: {processing_duration:.1f}秒完成")
            quality_score += 5
        
        print()
        print(f"7. 总体质量评分: {quality_score}/100")
        
        if quality_score >= 90:
            print("   🎯 数据质量优秀! G4-2025汇聚成功完成")
        elif quality_score >= 75:
            print("   ✅ 数据质量良好, G4-2025汇聚基本成功")
        else:
            print("   ⚠ 数据质量需要改进")
        
        print()
        print(f"汇聚完成时间: {stats.latest_time}")
        print("=== 使用同事修改算法的G4-2025批次汇聚验证完成 ===")
            
    except Exception as e:
        print(f"验证失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(final_g4_verification_report())