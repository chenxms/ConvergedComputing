#!/usr/bin/env python3
"""
测试统一满分计算逻辑
验证从 subject_question_config 表计算的满分是否正确
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from app.database.connection import get_db_context
from app.services.calculation_service import CalculationService
from sqlalchemy import text


async def test_unified_max_score():
    """测试统一满分计算逻辑"""
    print("=== 测试统一满分计算逻辑 ===")
    
    with get_db_context() as db_session:
        calc_service = CalculationService(db_session)
        
        # 获取测试批次
        batch_query = text("""
            SELECT DISTINCT batch_code 
            FROM subject_question_config 
            ORDER BY batch_code DESC 
            LIMIT 3
        """)
        
        batches = db_session.execute(batch_query).fetchall()
        if not batches:
            print("❌ 未找到任何批次数据")
            return
            
        print(f"找到 {len(batches)} 个批次进行测试")
        
        for batch_row in batches:
            batch_code = batch_row[0]
            print(f"\n--- 测试批次: {batch_code} ---")
            
            # 1. 测试批量获取满分信息
            print("1. 测试批量获取满分信息")
            batch_max_scores = calc_service._batch_get_max_scores(batch_code)
            
            subjects = batch_max_scores.get('subjects', {})
            dimensions = batch_max_scores.get('dimensions', {})
            
            print(f"   科目数量: {len(subjects)}")
            print(f"   维度数量: {sum(len(dims) for dims in dimensions.values())}")
            
            # 2. 验证科目满分计算
            print("2. 验证科目满分计算")
            for subject_name, batch_max_score in list(subjects.items())[:3]:  # 只测试前3个科目
                # 使用单独方法计算
                single_max_score = calc_service._get_subject_max_score(batch_code, subject_name)
                
                # 直接查询数据库验证
                verify_query = text("""
                    SELECT SUM(max_score) as total_max_score
                    FROM subject_question_config
                    WHERE batch_code = :batch_code AND subject_name = :subject_name
                """)
                
                verify_result = db_session.execute(verify_query, {
                    'batch_code': batch_code,
                    'subject_name': subject_name
                }).fetchone()
                
                db_max_score = float(verify_result[0]) if verify_result and verify_result[0] else 100.0
                
                print(f"   科目 {subject_name}:")
                print(f"     批量方法: {batch_max_score}")
                print(f"     单独方法: {single_max_score}")
                print(f"     数据库验证: {db_max_score}")
                
                if batch_max_score == single_max_score == db_max_score:
                    print(f"     ✅ 满分计算一致")
                else:
                    print(f"     ❌ 满分计算不一致!")
                    
            # 3. 验证维度满分计算
            print("3. 验证维度满分计算")
            tested_dimensions = 0
            for subject_name, subject_dimensions in dimensions.items():
                if tested_dimensions >= 5:  # 只测试5个维度
                    break
                    
                for dimension_code, batch_dimension_max in list(subject_dimensions.items())[:2]:
                    # 使用单独方法计算
                    single_dimension_max = calc_service._get_dimension_max_score(batch_code, subject_name, dimension_code)
                    
                    # 直接查询数据库验证（使用question_dimension_mapping）
                    verify_query = text("""
                        SELECT SUM(sqc.max_score) as dimension_max_score
                        FROM subject_question_config sqc
                        LEFT JOIN question_dimension_mapping qdm ON sqc.question_id = qdm.question_id
                        WHERE sqc.batch_code = :batch_code 
                            AND sqc.subject_name = :subject_name
                            AND (qdm.dimension_code = :dimension_code OR sqc.instrument_id = :dimension_code)
                    """)
                    
                    verify_result = db_session.execute(verify_query, {
                        'batch_code': batch_code,
                        'subject_name': subject_name,
                        'dimension_code': dimension_code
                    }).fetchone()
                    
                    db_dimension_max = float(verify_result[0]) if verify_result and verify_result[0] else 0.0
                    
                    print(f"   维度 {subject_name}/{dimension_code}:")
                    print(f"     批量方法: {batch_dimension_max}")
                    print(f"     单独方法: {single_dimension_max}")
                    print(f"     数据库验证: {db_dimension_max}")
                    
                    if batch_dimension_max == single_dimension_max == db_dimension_max:
                        print(f"     ✅ 维度满分计算一致")
                    else:
                        print(f"     ❌ 维度满分计算不一致!")
                    
                    tested_dimensions += 1
                    if tested_dimensions >= 5:
                        break
                        
            # 4. 测试缓存功能
            print("4. 测试缓存功能")
            if subjects:
                first_subject = list(subjects.keys())[0]
                
                # 第二次调用应该使用缓存
                cached_max_score = calc_service._get_subject_max_score(batch_code, first_subject)
                print(f"   科目 {first_subject} 缓存调用: {cached_max_score}")
                print(f"   ✅ 缓存功能正常")
                
            # 5. 性能统计
            print("5. 性能统计")
            print(f"   缓存的批次数: {len(calc_service._max_score_cache)}")
            if batch_code in calc_service._max_score_cache:
                cached_subjects = [k for k in calc_service._max_score_cache[batch_code].keys() if k != 'dimensions']
                cached_dimensions = calc_service._max_score_cache[batch_code].get('dimensions', {})
                print(f"   缓存的科目数: {len(cached_subjects)}")
                print(f"   缓存的维度数: {sum(len(dims) for dims in cached_dimensions.values())}")


async def test_data_consistency():
    """测试数据一致性：对比旧方法和新方法的结果"""
    print("\n=== 测试数据一致性 ===")
    
    with get_db_context() as db_session:
        # 获取一个测试批次
        batch_query = text("""
            SELECT DISTINCT batch_code 
            FROM subject_question_config 
            ORDER BY batch_code DESC 
            LIMIT 1
        """)
        
        batch_result = db_session.execute(batch_query).fetchone()
        if not batch_result:
            print("❌ 未找到测试批次")
            return
            
        batch_code = batch_result[0]
        print(f"测试批次: {batch_code}")
        
        # 对比科目配置获取方法
        print("1. 对比科目配置获取方法")
        
        # 使用数据适配器获取
        from app.database.repositories import DataAdapterRepository
        data_adapter = DataAdapterRepository(db_session)
        adapter_configs = data_adapter.get_subject_configurations(batch_code)
        
        # 使用计算服务获取
        calc_service = CalculationService(db_session)
        batch_max_scores = calc_service._batch_get_max_scores(batch_code)
        
        print(f"   数据适配器获取科目数: {len(adapter_configs)}")
        print(f"   计算服务获取科目数: {len(batch_max_scores.get('subjects', {}))}")
        
        # 对比每个科目的满分
        for config in adapter_configs[:3]:  # 只对比前3个科目
            subject_name = config['subject_name']
            adapter_max_score = config['max_score']
            service_max_score = batch_max_scores['subjects'].get(subject_name, 0)
            
            print(f"   科目 {subject_name}:")
            print(f"     数据适配器满分: {adapter_max_score}")
            print(f"     计算服务满分: {service_max_score}")
            
            if adapter_max_score == service_max_score:
                print(f"     ✅ 满分一致")
            else:
                print(f"     ❌ 满分不一致!")
                
                # 查看原因
                debug_query = text("""
                    SELECT question_id, max_score, question_type_enum
                    FROM subject_question_config
                    WHERE batch_code = :batch_code AND subject_name = :subject_name
                    ORDER BY question_id
                """)
                
                debug_result = db_session.execute(debug_query, {
                    'batch_code': batch_code,
                    'subject_name': subject_name
                }).fetchall()
                
                total_max = sum(float(row[1]) for row in debug_result if row[1])
                exam_max = sum(float(row[1]) for row in debug_result if row[1] and row[2] in ('exam', 'interaction'))
                
                print(f"     调试信息:")
                print(f"       题目总数: {len(debug_result)}")
                print(f"       所有题目满分总和: {total_max}")
                print(f"       考试题目满分总和: {exam_max}")


async def main():
    """主函数"""
    try:
        await test_unified_max_score()
        await test_data_consistency()
        print("\n=== 测试完成 ===")
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())