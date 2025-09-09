#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试修复后的学校ID查询逻辑
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.services.calculation_service import CalculationService
from app.database.connection import get_db
from sqlalchemy import text
import asyncio

async def test_school_id_fix():
    """测试修复后的学校ID查询"""
    print("=== 测试修复后的学校ID查询逻辑 ===")
    print()
    
    db = next(get_db())
    calc_service = CalculationService(db)
    
    try:
        # 1. 获取一个真实的学校ID
        print("1. 获取G4-2025批次的第一个学校ID:")
        result = db.execute(text("""
            SELECT DISTINCT school_id, school_name 
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025'
            ORDER BY school_id 
            LIMIT 1
        """))
        
        school_data = result.fetchone()
        if not school_data:
            print("  未找到G4-2025批次的学校数据")
            return
        
        school_id = school_data.school_id
        school_name = school_data.school_name
        print(f"  测试学校: {school_id} - {school_name}")
        print()
        
        # 2. 直接测试数据库查询
        print("2. 测试修复后的数据库查询:")
        result = db.execute(text("""
            SELECT COUNT(*) as count,
                   COUNT(DISTINCT student_id) as student_count,
                   COUNT(DISTINCT subject_name) as subject_count
            FROM student_score_detail ssd
            JOIN subject_question_config sqc 
                ON ssd.subject_name = sqc.subject_name 
                AND ssd.batch_code = sqc.batch_code
            WHERE ssd.batch_code = :batch_code 
                AND ssd.school_id = :school_id
        """), {'batch_code': 'G4-2025', 'school_id': school_id})
        
        query_result = result.fetchone()
        if query_result.count == 0:
            print(f"  ❌ 仍然无法获取学校 {school_id} 的数据")
        else:
            print(f"  ✅ 成功获取学校数据，共 {query_result.count} 条记录")
            print(f"     学生数: {query_result.student_count}")
            print(f"     科目数: {query_result.subject_count}")
        
        print()
        
        # 3. 测试完整的学校级计算
        print("3. 测试完整的学校级统计计算:")
        try:
            result = await calc_service.calculate_school_statistics('G4-2025', school_id)
            
            if result and 'success' in result and result['success']:
                print(f"  ✅ 学校级统计计算成功")
                print(f"     学校ID: {result.get('school_id', 'N/A')}")
                print(f"     学校名: {result.get('school_name', 'N/A')}")
                if 'statistics' in result:
                    stats = result['statistics']
                    print(f"     学生数: {stats.get('student_count', 'N/A')}")
                    print(f"     科目数: {len(stats.get('subjects', {}))}")
            else:
                print(f"  ❌ 学校级统计计算失败")
                if result:
                    print(f"     错误: {result.get('error', 'Unknown error')}")
        
        except Exception as e:
            print(f"  ❌ 学校级统计计算异常: {e}")
            import traceback
            traceback.print_exc()
        
        # 4. 验证数据库中的学校ID
        print(f"\n4. 验证数据库存储的学校ID:")
        result = db.execute(text("""
            SELECT school_id, school_name 
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' 
                AND aggregation_level = 'SCHOOL'
                AND school_id = :school_id
        """), {'school_id': school_id})
        
        saved_data = result.fetchone()
        if saved_data:
            print(f"  ✅ 数据库中找到正确的学校ID: {saved_data.school_id}")
            print(f"     学校名: {saved_data.school_name}")
        else:
            print(f"  ❌ 数据库中未找到学校ID: {school_id}")
            
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(test_school_id_fix())