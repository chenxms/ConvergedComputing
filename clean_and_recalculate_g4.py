#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清理G4-2025数据并重新计算
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.services.calculation_service import CalculationService
from app.database.connection import get_db
from sqlalchemy import text
import asyncio

async def clean_and_recalculate_g4():
    """清理G4-2025数据并重新计算"""
    print("=== 清理G4-2025数据并重新计算 ===")
    print()
    
    db = next(get_db())
    calc_service = CalculationService(db)
    
    try:
        # 1. 清理现有的G4-2025数据
        print("1. 清理现有的G4-2025统计数据:")
        result = db.execute(text("""
            DELETE FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025'
        """))
        db.commit()
        deleted_count = result.rowcount
        print(f"   已删除 {deleted_count} 条记录")
        print()
        
        # 2. 重新计算G4-2025批次数据
        print("2. 重新计算G4-2025批次统计数据:")
        
        # 定义进度回调函数
        def progress_callback(progress: float, message: str = ""):
            percentage = progress * 100
            print(f"   {percentage:.1f}% - {message}")
        
        # 执行批次计算
        result = await calc_service.calculate_batch_statistics(
            batch_code='G4-2025',
            progress_callback=progress_callback
        )
        
        if result and result.get('success'):
            print(f"\n✅ G4-2025 计算完成!")
            print(f"   总耗时: {result.get('duration', 0):.1f}秒")
            print(f"   成功学校: {result.get('successful_schools', 0)}")
            print(f"   失败学校: {result.get('failed_schools', 0)}")
        else:
            print(f"❌ G4-2025 计算失败")
            if result and 'error' in result:
                print(f"   错误: {result['error']}")
        
        print()
        
        # 3. 验证计算结果
        print("3. 验证计算结果:")
        result = db.execute(text("""
            SELECT aggregation_level, COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025'
            GROUP BY aggregation_level
            ORDER BY aggregation_level
        """))
        
        for row in result:
            print(f"   {row.aggregation_level}: {row.count}条记录")
        
        # 4. 显示学校数据样例
        result = db.execute(text("""
            SELECT school_id, school_name, total_students
            FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            ORDER BY CAST(school_id AS UNSIGNED)
            LIMIT 10
        """))
        
        print(f"\n4. 学校数据样例（前10个）:")
        for row in result:
            print(f"   {row.school_id}: {row.school_name} ({row.total_students}学生)")
            
    except Exception as e:
        print(f"处理失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(clean_and_recalculate_g4())