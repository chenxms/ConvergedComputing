#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试单个学校计算是否修复成功
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.services.calculation_service import CalculationService
from app.database.connection import get_db
from sqlalchemy import text
import asyncio

async def test_single_school_calculation():
    """测试单个学校计算"""
    print("=== 测试单个学校计算修复结果 ===")
    
    db = next(get_db())
    batch_code = "G7-2025"
    
    try:
        # 1. 获取一个学校ID进行测试
        result = db.execute(text("""
            SELECT DISTINCT school_id
            FROM student_score_detail 
            WHERE batch_code = :batch_code
            LIMIT 1
        """), {'batch_code': batch_code})
        
        school_record = result.fetchone()
        if not school_record:
            print("ERROR: 找不到任何学校数据")
            return
            
        test_school_id = school_record.school_id
        print(f"测试学校ID: {test_school_id}")
        
        # 2. 测试单个学校计算
        calc_service = CalculationService(db)
        
        print("正在测试学校级计算...")
        result = await calc_service.calculate_school_statistics(
            batch_code=batch_code,
            school_id=test_school_id
        )
        
        print("SUCCESS: 单个学校计算成功!")
        print(f"计算结果结构: {list(result.keys())}")
        
        # 3. 检查是否写入了数据库
        result = db.execute(text("""
            SELECT COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code = :batch_code AND school_id = :school_id
        """), {'batch_code': batch_code, 'school_id': test_school_id})
        
        db_count = result.fetchone().count
        print(f"数据库记录数: {db_count}")
        
        if db_count > 0:
            print("✓ 学校数据已成功写入statistical_aggregations表")
            
            # 显示数据样例
            result = db.execute(text("""
                SELECT school_name, statistics_data
                FROM statistical_aggregations 
                WHERE batch_code = :batch_code AND school_id = :school_id
                LIMIT 1
            """), {'batch_code': batch_code, 'school_id': test_school_id})
            
            record = result.fetchone()
            if record:
                import json
                stats_data = json.loads(record.statistics_data) if isinstance(record.statistics_data, str) else record.statistics_data
                subjects_count = len(stats_data.get('academic_subjects', {}))
                print(f"学校名称: {record.school_name}")
                print(f"包含科目数: {subjects_count}")
        else:
            print("× 数据未写入数据库")
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(test_single_school_calculation())