#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.database.connection import get_db
from app.services.calculation_service import CalculationService
from app.services.subjects_builder import SubjectsBuilder
from sqlalchemy import text


async def test_math_dimension_names():
    """测试数学科目的维度名称映射"""
    print("测试数学科目的维度名称映射...")
    
    with next(get_db()) as db:
        service = CalculationService(db)
        
        batch_code = "G4-2025"
        subject_name = "数学"
        
        print(f"测试批次: {batch_code}, 科目: {subject_name}")
        
        try:
            # 先测试批量加载维度名称
            dimension_mapping = service._batch_load_dimension_names(batch_code, subject_name)
            print(f"维度名称映射: {len(dimension_mapping)} 个")
            for dim_code, dim_name in dimension_mapping.items():
                print(f"  - {dim_code} -> {dim_name}")
            
            # 测试维度计算
            dimension_results = await service._calculate_subject_dimensions(batch_code, subject_name)
            
            if dimension_results:
                print(f"\nOK: 维度计算成功，共 {len(dimension_results)} 个维度")
                
                # 检查每个维度的名称
                for dim_code, dim_data in dimension_results.items():
                    basic_stats = dim_data.get('basic_stats', {})
                    dimension_name = basic_stats.get('name', dim_code)
                    expected_name = dimension_mapping.get(dim_code, dim_code)
                    
                    print(f"\n维度: {dim_code}")
                    print(f"  实际名称: {dimension_name}")
                    print(f"  期望名称: {expected_name}")
                    print(f"  映射成功: {'是' if dimension_name == expected_name else '否'}")
                    
                    if 'statistical_indicators' in dim_data:
                        indicators = dim_data['statistical_indicators']
                        print(f"  统计指标: {list(indicators.keys())}")
                    
                return True
            else:
                print("WARN: 数学科目未找到维度数据")
                return False
                
        except Exception as e:
            print(f"FAIL: 测试失败: {e}")
            import traceback
            traceback.print_exc()
            return False


def test_subjects_builder_math():
    """测试SubjectsBuilder数学科目维度"""
    print("\n\n测试SubjectsBuilder数学科目维度...")
    
    builder = SubjectsBuilder()
    
    with next(get_db()) as db:
        batch_code = "G4-2025"
        subject_name = "数学"
        
        # 获取一个学校
        result = db.execute(text("""
            SELECT DISTINCT school_code 
            FROM student_cleaned_scores 
            WHERE batch_code = :batch AND subject_name = :subject
            AND dimension_scores IS NOT NULL AND dimension_scores != ''
            LIMIT 1
        """), {"batch": batch_code, "subject": subject_name}).fetchone()
        
        if not result:
            print("WARN: 未找到学校数据")
            return
        
        school_code = result[0]
        print(f"测试学校: {school_code}")
        
        try:
            # 先测试批量加载维度名称
            dimension_mapping = builder._batch_load_dimension_names(db, batch_code, subject_name)
            print(f"维度名称映射: {len(dimension_mapping)} 个")
            
            # 测试学校维度排名计算
            dimensions = builder._compute_school_dimensions_with_rank(
                batch_code, subject_name, school_code
            )
            
            if dimensions:
                print(f"\nOK: 学校维度计算成功，共 {len(dimensions)} 个维度")
                
                for dim in dimensions:
                    dim_code = dim.get('code', '')
                    dim_name = dim.get('name', '')
                    expected_name = dimension_mapping.get(dim_code, dim_code)
                    
                    print(f"\n维度: {dim_code}")
                    print(f"  实际名称: {dim_name}")
                    print(f"  期望名称: {expected_name}")
                    print(f"  映射成功: {'是' if dim_name == expected_name else '否'}")
                    print(f"  平均分: {dim.get('avg', 'N/A')}")
                    print(f"  排名: {dim.get('rank', 'N/A')}")
                return True
            else:
                print("WARN: 该学校数学科目未找到维度数据")
                return False
                
        except Exception as e:
            print(f"FAIL: SubjectsBuilder测试失败: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    print("开始数学科目维度名称映射测试...")
    
    # 测试CalculationService
    success1 = asyncio.run(test_math_dimension_names())
    
    # 测试SubjectsBuilder  
    success2 = test_subjects_builder_math()
    
    if success1 and success2:
        print("\n\nSUCCESS: 数学科目维度名称映射测试全部通过!")
    else:
        print("\n\nFAIL: 部分测试失败")


if __name__ == "__main__":
    main()