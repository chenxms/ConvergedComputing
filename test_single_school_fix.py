#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试单个学校的ID修复 - 使用具体的学校ID
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.services.calculation_service import CalculationService
from app.database.connection import get_db
from sqlalchemy import text
import asyncio

async def test_single_school():
    """测试具体学校的计算"""
    print("=== 测试单个学校ID修复 ===")
    print()
    
    db = next(get_db())
    calc_service = CalculationService(db)
    
    try:
        # 直接使用已知的学校ID (从之前的对比中看到的)
        test_school_id = "5044"
        
        print(f"1. 测试学校ID: {test_school_id}")
        
        # 检查该学校是否存在数据
        result = db.execute(text("""
            SELECT school_id, school_name, COUNT(*) as record_count,
                   COUNT(DISTINCT student_id) as student_count,
                   GROUP_CONCAT(DISTINCT subject_name) as subjects
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025' AND school_id = :school_id
            GROUP BY school_id, school_name
        """), {'school_id': test_school_id})
        
        school_info = result.fetchone()
        if not school_info:
            print(f"  ❌ 学校 {test_school_id} 不存在数据")
            return
        
        print(f"  学校名: {school_info.school_name}")
        print(f"  记录数: {school_info.record_count}")  
        print(f"  学生数: {school_info.student_count}")
        print(f"  科目: {school_info.subjects}")
        print()
        
        # 测试学校级统计计算
        print("2. 执行学校级统计计算:")
        try:
            result = await calc_service.calculate_school_statistics('G4-2025', test_school_id)
            
            if result and result.get('success'):
                print(f"  ✅ 计算成功")
                print(f"  学校ID: {result.get('school_id')}")
                print(f"  学校名: {result.get('school_name')}")
            else:
                print(f"  ❌ 计算失败")
                if result and 'error' in result:
                    print(f"  错误: {result['error']}")
        
        except Exception as e:
            print(f"  ❌ 计算异常: {e}")
        
        print()
        
        # 验证数据库中的数据
        print("3. 验证数据库存储:")
        result = db.execute(text("""
            SELECT school_id, school_name 
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' 
                AND aggregation_level = 'SCHOOL'
                AND school_id = :school_id
        """), {'school_id': test_school_id})
        
        saved_school = result.fetchone()
        if saved_school:
            print(f"  ✅ 数据库中找到学校: {saved_school.school_id} - {saved_school.school_name}")
        else:
            print(f"  ❌ 数据库中未找到学校: {test_school_id}")
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(test_single_school())