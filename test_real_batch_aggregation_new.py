#!/usr/bin/env python3
"""
使用真实批次数据测试新的简化汇聚功能
"""

import asyncio
import json
from datetime import datetime
from app.database.connection import get_db_context
from app.services.simplified_aggregation_service import SimplifiedAggregationService
from app.utils.precision_handler import format_decimal

async def test_batch_aggregation(batch_code: str = "G7-2025"):
    """测试特定批次的汇聚功能"""
    
    print(f"\n{'='*60}")
    print(f"测试批次 {batch_code} 的简化汇聚功能")
    print(f"{'='*60}\n")
    
    try:
        with get_db_context() as session:
            # 创建汇聚服务
            service = SimplifiedAggregationService(session)
            
            # 1. 测试区域级汇聚
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始区域级汇聚...")
            regional_data = await service.aggregate_batch_regional(batch_code)
            
            if regional_data:
                print(f"\n✅ 区域级汇聚成功")
                print(f"  - 批次: {regional_data.get('batch_code')}")
                print(f"  - 学校总数: {regional_data.get('total_schools')}")
                print(f"  - 学生总数: {regional_data.get('total_students')}")
                
                subjects = regional_data.get('subjects', {})
                print(f"  - 科目数: {len(subjects)}")
                
                # 显示各科目信息
                for subject_id, subject_data in subjects.items():
                    print(f"\n  科目 {subject_id} ({subject_data.get('subject_name')})")
                    metrics = subject_data.get('metrics', {})
                    print(f"    - 学生数: {metrics.get('student_count')}")
                    print(f"    - 平均分: {metrics.get('avg_score')}")
                    print(f"    - 难度系数: {metrics.get('difficulty')}")
                    print(f"    - 标准差: {metrics.get('std_dev')}")
                    print(f"    - 区分度: {metrics.get('discrimination')}")
                    print(f"    - P10/P50/P90: {metrics.get('p10')}/{metrics.get('p50')}/{metrics.get('p90')}")
                    
                    # 显示排名信息
                    ranking = subject_data.get('ranking', {})
                    school_rankings = ranking.get('school_rankings', [])
                    if school_rankings:
                        print(f"    - 前3名学校:")
                        for school in school_rankings[:3]:
                            print(f"      {school.get('rank')}. {school.get('school_name')}: {school.get('avg_score')}")
                    
                    # 显示维度信息
                    dimensions = subject_data.get('dimensions', {})
                    if dimensions:
                        print(f"    - 维度数: {len(dimensions)}")
                        for dim_code, dim_data in list(dimensions.items())[:2]:
                            print(f"      {dim_code}: 平均分{dim_data.get('avg_score')}, 得分率{dim_data.get('score_rate')}%")
                    
                    # 显示问卷信息
                    questionnaire_dims = subject_data.get('questionnaire_dimensions', [])
                    if questionnaire_dims:
                        print(f"    - 问卷维度数: {len(questionnaire_dims)}")
                        for q_dim in questionnaire_dims[:1]:
                            print(f"      {q_dim.get('dimension_code')} ({q_dim.get('dimension_name')})")
                            print(f"        平均分: {q_dim.get('avg_score')}, 得分率: {q_dim.get('score_rate')}%")
                            
                            # 显示选项分布
                            option_dists = q_dim.get('dimension_option_distributions', [])
                            if option_dists:
                                print(f"        选项分布:")
                                for opt in option_dists[:3]:
                                    print(f"          {opt.get('option_label')}: {opt.get('percentage')}%")
                
                # 保存结果到文件
                output_file = f"test_results_{batch_code}_regional_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(regional_data, f, ensure_ascii=False, indent=2)
                print(f"\n区域级结果已保存到: {output_file}")
            
            # 2. 测试学校级汇聚（选择前3所学校）
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 开始学校级汇聚...")
            
            # 获取学校列表
            from sqlalchemy import text
            result = session.execute(text("""
                SELECT DISTINCT school_id, school_name
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code
                LIMIT 3
            """), {"batch_code": batch_code})
            schools = result.fetchall()
            
            for school in schools:
                print(f"\n处理学校: {school.school_id} ({school.school_name})")
                school_data = await service.aggregate_batch_school(batch_code, school.school_id)
                
                if school_data:
                    print(f"  ✅ 学校汇聚成功")
                    print(f"    - 学生总数: {school_data.get('total_students')}")
                    
                    # 显示科目排名
                    subjects = school_data.get('subjects', {})
                    for subject_id, subject_data in list(subjects.items())[:2]:
                        ranking = subject_data.get('ranking', {})
                        regional_rank = ranking.get('regional_rank')
                        total_schools = ranking.get('total_schools')
                        if regional_rank:
                            print(f"    - {subject_data.get('subject_name')}: 区域排名 {regional_rank}/{total_schools}")
            
            print(f"\n{'='*60}")
            print("✅ 测试完成")
            print(f"{'='*60}\n")
            
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()

async def test_all_batches():
    """测试所有批次的汇聚"""
    
    print("\n开始测试所有批次汇聚...")
    
    try:
        with get_db_context() as session:
            service = SimplifiedAggregationService(session)
            
            # 处理所有批次
            results = await service.aggregate_all_batches()
            
            print(f"\n汇聚结果汇总:")
            print(f"  - 成功批次: {results.get('successful_batches', [])}")
            print(f"  - 失败批次: {results.get('failed_batches', [])}")
            print(f"  - 总耗时: {format_decimal(results.get('total_duration', 0))} 秒")
            
    except Exception as e:
        print(f"\n❌ 批量测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 测试单个批次
    asyncio.run(test_batch_aggregation("G7-2025"))
    
    # 如需测试所有批次，取消下面的注释
    # asyncio.run(test_all_batches())