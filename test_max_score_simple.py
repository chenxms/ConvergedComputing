#!/usr/bin/env python3
"""
简单测试统一满分计算逻辑
验证从 subject_question_config 表计算的满分是否正确
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from app.database.connection import get_db_context
from app.services.calculation_service import CalculationService
from sqlalchemy import text


async def test_max_score_calculation():
    """测试满分计算逻辑"""
    print("=== 测试统一满分计算逻辑 ===")
    
    with get_db_context() as db_session:
        calc_service = CalculationService(db_session)
        
        # 获取一个测试批次
        batch_query = text("""
            SELECT DISTINCT batch_code 
            FROM subject_question_config 
            ORDER BY batch_code DESC 
            LIMIT 1
        """)
        
        batch_result = db_session.execute(batch_query).fetchone()
        if not batch_result:
            print("未找到任何批次数据")
            return
            
        batch_code = batch_result[0]
        print(f"测试批次: {batch_code}")
        
        # 1. 测试批量获取满分信息
        print("\n1. 批量获取满分信息")
        batch_max_scores = calc_service._batch_get_max_scores(batch_code)
        
        subjects = batch_max_scores.get('subjects', {})
        dimensions = batch_max_scores.get('dimensions', {})
        
        print(f"科目数量: {len(subjects)}")
        print(f"维度数量: {sum(len(dims) for dims in dimensions.values())}")
        
        # 显示前几个科目的满分
        for i, (subject_name, max_score) in enumerate(list(subjects.items())[:3]):
            print(f"科目 {i+1}: {subject_name} = {max_score} 分")
        
        # 2. 验证科目满分计算一致性
        print("\n2. 验证科目满分计算一致性")
        test_count = 0
        consistent_count = 0
        
        for subject_name, batch_max_score in list(subjects.items())[:5]:  # 测试前5个科目
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
            
            test_count += 1
            if batch_max_score == single_max_score == db_max_score:
                consistent_count += 1
                print(f"科目 {subject_name}: {db_max_score} 分 [一致]")
            else:
                print(f"科目 {subject_name}: 批量={batch_max_score}, 单独={single_max_score}, 数据库={db_max_score} [不一致]")
                
        print(f"科目满分一致性: {consistent_count}/{test_count}")
        
        # 3. 测试维度满分计算
        print("\n3. 测试维度满分计算")
        dimension_test_count = 0
        dimension_consistent_count = 0
        
        for subject_name, subject_dimensions in dimensions.items():
            if dimension_test_count >= 5:  # 只测试5个维度
                break
                
            for dimension_code, batch_dimension_max in list(subject_dimensions.items())[:2]:
                # 使用单独方法计算
                single_dimension_max = calc_service._get_dimension_max_score(batch_code, subject_name, dimension_code)
                
                dimension_test_count += 1
                if batch_dimension_max == single_dimension_max:
                    dimension_consistent_count += 1
                    print(f"维度 {subject_name}/{dimension_code}: {single_dimension_max} 分 [一致]")
                else:
                    print(f"维度 {subject_name}/{dimension_code}: 批量={batch_dimension_max}, 单独={single_dimension_max} [不一致]")
                    
                if dimension_test_count >= 5:
                    break
                    
        print(f"维度满分一致性: {dimension_consistent_count}/{dimension_test_count}")
        
        # 4. 测试与数据适配器的兼容性
        print("\n4. 测试与数据适配器的兼容性")
        from app.database.repositories import DataAdapterRepository
        data_adapter = DataAdapterRepository(db_session)
        adapter_configs = data_adapter.get_subject_configurations(batch_code)
        
        adapter_consistent_count = 0
        for config in adapter_configs[:3]:  # 测试前3个科目
            subject_name = config['subject_name']
            adapter_max_score = config['max_score']
            service_max_score = subjects.get(subject_name, 0)
            
            if adapter_max_score == service_max_score:
                adapter_consistent_count += 1
                print(f"科目 {subject_name}: {adapter_max_score} 分 [数据适配器一致]")
            else:
                print(f"科目 {subject_name}: 数据适配器={adapter_max_score}, 计算服务={service_max_score} [不一致]")
        
        print(f"数据适配器一致性: {adapter_consistent_count}/{len(adapter_configs[:3])}")
        
        # 5. 缓存测试
        print("\n5. 缓存功能测试")
        if subjects:
            first_subject = list(subjects.keys())[0]
            # 第二次调用应该使用缓存
            cached_max_score = calc_service._get_subject_max_score(batch_code, first_subject)
            original_max_score = subjects[first_subject]
            
            if cached_max_score == original_max_score:
                print(f"缓存功能正常: {first_subject} = {cached_max_score} 分")
            else:
                print(f"缓存功能异常: 原始={original_max_score}, 缓存={cached_max_score}")
        
        print(f"缓存的批次数: {len(calc_service._max_score_cache)}")


async def main():
    """主函数"""
    try:
        await test_max_score_calculation()
        print("\n=== 测试完成 ===")
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())