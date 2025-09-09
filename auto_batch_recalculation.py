#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动对主要批次进行重新汇聚计算
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.services.calculation_service import CalculationService
from app.database.connection import get_db
from sqlalchemy import text
import asyncio
import time

async def auto_recalculate_main_batches():
    """自动重新计算主要批次的汇聚数据"""
    print("=== 自动重新计算主要批次汇聚数据 ===")
    
    db = next(get_db())
    calc_service = CalculationService(db)
    
    try:
        # 1. 获取主要批次（排除测试批次）
        print("1. 获取主要批次...")
        result = db.execute(text("""
            SELECT DISTINCT batch_code, COUNT(DISTINCT student_id) as student_count,
                   COUNT(DISTINCT school_id) as school_count,
                   COUNT(DISTINCT subject_name) as subject_count
            FROM student_score_detail 
            WHERE batch_code NOT LIKE '%TEST%' 
                AND batch_code NOT LIKE '%DEBUG%' 
                AND batch_code NOT LIKE '%MAPPER%'
                AND student_id IS NOT NULL
            GROUP BY batch_code
            HAVING COUNT(DISTINCT student_id) > 100  -- 只处理有足够数据的批次
            ORDER BY batch_code
        """))
        
        batches = result.fetchall()
        print(f"找到 {len(batches)} 个主要批次:")
        for batch in batches:
            print(f"  - {batch.batch_code}: {batch.student_count}学生, {batch.school_count}学校, {batch.subject_count}科目")
        
        # 2. 清理现有statistical_aggregations数据
        print("\n2. 清理现有统计数据...")
        db.execute(text("DELETE FROM statistical_aggregations"))
        db.commit()
        print("✓ 清理完成")
        
        # 3. 重新计算每个主要批次
        total_batches = len(batches)
        successful_batches = 0
        failed_batches = []
        
        for i, batch in enumerate(batches, 1):
            batch_code = batch.batch_code
            print(f"\n[{i}/{total_batches}] ==================")
            print(f"处理批次: {batch_code}")
            print(f"基础数据: {batch.student_count}学生, {batch.school_count}学校, {batch.subject_count}科目")
            
            start_time = time.time()
            
            try:
                # 执行批次级汇聚计算（包含区域级和自动触发学校级）
                print("正在执行汇聚计算...")
                
                def progress_callback(progress, message):
                    print(f"  进度: {progress:.1f}% - {message}")
                
                result = await calc_service.calculate_batch_statistics(
                    batch_code=batch_code,
                    progress_callback=progress_callback
                )
                
                duration = time.time() - start_time
                print(f"✓ 批次 {batch_code} 汇聚计算完成，耗时: {duration:.1f}秒")
                
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
                
                print(f"结果验证: 区域级={regional_count}, 学校级={school_count}")
                
                if regional_count >= 1:
                    successful_batches += 1
                    print(f"✅ 批次 {batch_code} 计算成功！")
                    
                    if school_count == 0:
                        print(f"⚠️  注意: 学校级数据为0，可能需要进一步检查")
                else:
                    failed_batches.append(batch_code)
                    print(f"❌ 批次 {batch_code} 计算失败 - 无区域级数据生成")
                
            except Exception as e:
                failed_batches.append(batch_code)
                duration = time.time() - start_time
                print(f"❌ 批次 {batch_code} 计算失败，耗时: {duration:.1f}秒")
                print(f"错误: {e}")
                import traceback
                print(traceback.format_exc())
        
        print()
        print("=" * 60)
        print("最终结果汇总")
        print("=" * 60)
        
        # 4. 最终验证所有批次结果
        result = db.execute(text("""
            SELECT batch_code, aggregation_level, COUNT(*) as count
            FROM statistical_aggregations 
            GROUP BY batch_code, aggregation_level
            ORDER BY batch_code, aggregation_level
        """))
        
        print("各批次汇聚数据统计:")
        current_batch = None
        batch_totals = {}
        for row in result.fetchall():
            if current_batch != row.batch_code:
                if current_batch is not None:
                    print(f"    小计: {batch_totals.get(current_batch, 0)}条")
                    print()
                current_batch = row.batch_code
                print(f"📊 批次 {row.batch_code}:")
                batch_totals[current_batch] = 0
            print(f"   {row.aggregation_level}: {row.count}条")
            batch_totals[current_batch] += row.count
        
        if current_batch:
            print(f"    小计: {batch_totals.get(current_batch, 0)}条")
        
        # 总计
        result = db.execute(text("SELECT COUNT(*) as total FROM statistical_aggregations"))
        total_records = result.fetchone().total
        print(f"\n📈 总计: {total_records} 条汇聚统计记录")
        
        print(f"\n📊 处理结果:")
        print(f"   成功: {successful_batches}/{total_batches} 个批次")
        print(f"   失败: {len(failed_batches)} 个批次")
        
        if failed_batches:
            print(f"   失败批次: {', '.join(failed_batches)}")
        
        if successful_batches == total_batches:
            print("\n🎉 所有主要批次汇聚计算完成！")
        else:
            print(f"\n⚠️  有 {len(failed_batches)} 个批次需要进一步处理")
        
    except Exception as e:
        print(f"重新计算失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(auto_recalculate_main_batches())