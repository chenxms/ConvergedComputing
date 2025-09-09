#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版批次重新计算脚本 - 避免Unicode问题
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.services.calculation_service import CalculationService
from app.database.connection import get_db
from sqlalchemy import text
import asyncio
import time

async def simple_recalc_batches():
    """简单重新计算主要批次"""
    print("=== 简化版批次重新计算 ===")
    
    db = next(get_db())
    calc_service = CalculationService(db)
    
    # 主要批次列表
    main_batches = ['G4-2025', 'G7-2025', 'G8-2025']
    
    try:
        print("开始处理3个主要批次:")
        for batch_code in main_batches:
            print(f"  - {batch_code}")
        print()
        
        successful = 0
        failed = []
        
        for i, batch_code in enumerate(main_batches, 1):
            print(f"[{i}/3] 处理批次: {batch_code}")
            
            start_time = time.time()
            
            try:
                def progress_callback(progress, message):
                    print(f"  {progress:.1f}% - {message}")
                
                result = await calc_service.calculate_batch_statistics(
                    batch_code=batch_code,
                    progress_callback=progress_callback
                )
                
                duration = time.time() - start_time
                print(f"批次 {batch_code} 完成，耗时: {duration:.1f}秒")
                
                # 验证结果
                result = db.execute(text("""
                    SELECT aggregation_level, COUNT(*) as count
                    FROM statistical_aggregations 
                    WHERE batch_code = :batch_code
                    GROUP BY aggregation_level
                """), {'batch_code': batch_code})
                
                level_counts = {row.aggregation_level: row.count for row in result.fetchall()}
                regional_count = level_counts.get('REGIONAL', 0)
                school_count = level_counts.get('SCHOOL', 0)
                
                print(f"结果: 区域级={regional_count}, 学校级={school_count}")
                
                if regional_count >= 1:
                    successful += 1
                    print(f"SUCCESS - {batch_code}")
                else:
                    failed.append(batch_code)
                    print(f"FAILED - {batch_code}")
                
            except Exception as e:
                failed.append(batch_code)
                duration = time.time() - start_time
                print(f"ERROR - {batch_code}: {e}")
            
            print("-" * 50)
        
        print("最终结果:")
        print(f"成功: {successful}/{len(main_batches)}")
        print(f"失败: {len(failed)}")
        
        if failed:
            print(f"失败批次: {', '.join(failed)}")
        
        # 总结
        result = db.execute(text("SELECT COUNT(*) as total FROM statistical_aggregations"))
        total_records = result.fetchone().total
        print(f"总计统计记录: {total_records}")
        
        if successful == len(main_batches):
            print("所有主要批次计算完成!")
        
    except Exception as e:
        print(f"脚本执行失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(simple_recalc_batches())