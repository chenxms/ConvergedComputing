#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于同事修复后的代码重新汇聚G4-2025批次数据
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.services.calculation_service import CalculationService
from app.database.connection import get_db
from sqlalchemy import text
import asyncio

async def final_reaggregate_g4():
    """基于修复后的代码重新汇聚G4-2025数据"""
    print("=== 基于修复后的代码重新汇聚G4-2025批次数据 ===")
    print()
    
    db = next(get_db())
    calc_service = CalculationService(db)
    
    try:
        # 1. 清理现有数据
        print("1. 清理现有G4-2025汇聚数据:")
        result = db.execute(text("""
            DELETE FROM statistical_aggregations 
            WHERE batch_code = 'G4-2025'
        """))
        db.commit()
        deleted_count = result.rowcount
        print(f"   删除了 {deleted_count} 条历史记录")
        print()
        
        # 2. 使用修复后的代码重新计算
        print("2. 使用修复后的代码重新汇聚G4-2025:")
        
        def progress_callback(progress: float, message: str = ""):
            percentage = progress * 100
            print(f"   进度 {percentage:.1f}% - {message}")
        
        # 执行完整的批次计算
        result = await calc_service.calculate_batch_statistics(
            batch_code='G4-2025',
            progress_callback=progress_callback
        )
        
        if result and result.get('success'):
            print(f"\n✅ G4-2025 重新汇聚完成!")
            print(f"   总耗时: {result.get('duration', 0):.1f}秒")
            print(f"   成功学校: {result.get('successful_schools', 0)}")
            print(f"   失败学校: {result.get('failed_schools', 0)}")
            
            # 验证汇聚结果
            print(f"\n3. 验证汇聚结果:")
            result_check = db.execute(text("""
                SELECT aggregation_level, COUNT(*) as count
                FROM statistical_aggregations 
                WHERE batch_code = 'G4-2025'
                GROUP BY aggregation_level
                ORDER BY aggregation_level
            """))
            
            for row in result_check:
                print(f"   {row.aggregation_level}: {row.count}条记录")
            
            # 显示学校数据样例
            print(f"\n4. 学校数据样例:")
            school_check = db.execute(text("""
                SELECT school_id, school_name, total_students, created_at
                FROM statistical_aggregations 
                WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
                ORDER BY CAST(school_id AS UNSIGNED)
                LIMIT 10
            """))
            
            for row in school_check:
                print(f"   {row.school_id}: {row.school_name} ({row.total_students}学生) - {row.created_at}")
            
            # 检查数据统计
            total_check = db.execute(text("""
                SELECT COUNT(*) as total_schools,
                       SUM(total_students) as total_students
                FROM statistical_aggregations 
                WHERE batch_code = 'G4-2025' AND aggregation_level = 'SCHOOL'
            """))
            
            stats = total_check.fetchone()
            print(f"\n5. 汇聚数据统计:")
            print(f"   学校总数: {stats.total_schools}")
            print(f"   学生总数: {stats.total_students}")
            
        else:
            print(f"❌ G4-2025 重新汇聚失败")
            if result and 'error' in result:
                print(f"   错误: {result['error']}")
            
    except Exception as e:
        print(f"重新汇聚失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(final_reaggregate_g4())