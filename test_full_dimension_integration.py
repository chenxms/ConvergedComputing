#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import sys
import os
from pathlib import Path
import json

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.database.connection import get_db
from app.services.calculation_service import CalculationService
from app.services.subjects_builder import SubjectsBuilder
from sqlalchemy import text


async def test_dimension_calculation_with_names():
    """测试维度计算中的中文名称映射"""
    print("测试维度计算中的中文名称映射...")
    
    with next(get_db()) as db:
        service = CalculationService(db)
        
        # 获取有数据的批次
        result = db.execute(text(
            "SELECT DISTINCT batch_code FROM batch_dimension_definition LIMIT 1"
        )).fetchone()
        
        if not result:
            print("WARN: 未找到批次数据")
            return
        
        batch_code = result[0]
        print(f"测试批次: {batch_code}")
        
        # 获取科目
        subjects = db.execute(text(
            "SELECT DISTINCT subject_name FROM batch_dimension_definition WHERE batch_code = :batch LIMIT 2"
        ), {"batch": batch_code}).fetchall()
        
        for subject_row in subjects:
            subject_name = subject_row[0]
            print(f"\n测试科目: {subject_name}")
            
            try:
                # 测试维度计算
                dimension_results = await service._calculate_subject_dimensions(batch_code, subject_name)
                
                if dimension_results:
                    print(f"OK: 维度计算成功，共 {len(dimension_results)} 个维度")
                    
                    # 检查每个维度的名称是否为中文
                    for dim_code, dim_data in list(dimension_results.items())[:3]:
                        basic_stats = dim_data.get('basic_stats', {})
                        dimension_name = basic_stats.get('name', dim_code)
                        
                        print(f"  维度: {dim_code}")
                        print(f"    中文名称: {dimension_name}")
                        print(f"    是否使用中文名称: {'是' if dimension_name != dim_code else '否'}")
                        
                        # 检查维度统计数据结构
                        if 'statistical_indicators' in dim_data:
                            indicators = dim_data['statistical_indicators']
                            print(f"    统计指标: {list(indicators.keys())}")
                else:
                    print("WARN: 该科目未找到维度数据")
                    
            except Exception as e:
                print(f"FAIL: 维度计算失败: {e}")


def test_subjects_builder_with_names():
    """测试SubjectsBuilder中的维度名称"""
    print("\n\n测试SubjectsBuilder中的维度名称...")
    
    builder = SubjectsBuilder()
    
    with next(get_db()) as db:
        # 获取有数据的批次和学校
        result = db.execute(text("""
            SELECT DISTINCT bdd.batch_code, bdd.subject_name, scs.school_code 
            FROM batch_dimension_definition bdd
            JOIN student_cleaned_scores scs ON scs.batch_code = bdd.batch_code AND scs.subject_name = bdd.subject_name
            WHERE scs.dimension_scores IS NOT NULL AND scs.dimension_scores != ''
            LIMIT 1
        """)).fetchone()
        
        if not result:
            print("WARN: 未找到测试数据")
            return
        
        batch_code, subject_name, school_code = result[0], result[1], result[2]
        print(f"测试: {batch_code}, {subject_name}, {school_code}")
        
        try:
            # 测试学校维度排名计算
            dimensions = builder._compute_school_dimensions_with_rank(
                batch_code, subject_name, school_code
            )
            
            if dimensions:
                print(f"OK: 学校维度计算成功，共 {len(dimensions)} 个维度")
                
                for dim in dimensions:
                    dim_code = dim.get('code', '')
                    dim_name = dim.get('name', '')
                    print(f"  维度: {dim_code}")
                    print(f"    中文名称: {dim_name}")
                    print(f"    是否使用中文名称: {'是' if dim_name != dim_code else '否'}")
                    print(f"    平均分: {dim.get('avg', 'N/A')}")
                    print(f"    排名: {dim.get('rank', 'N/A')}")
            else:
                print("WARN: 该学校科目未找到维度数据")
                
        except Exception as e:
            print(f"FAIL: 学校维度计算失败: {e}")


def main():
    print("开始完整的维度名称映射集成测试...")
    
    # 运行异步测试
    asyncio.run(test_dimension_calculation_with_names())
    
    # 运行同步测试
    test_subjects_builder_with_names()
    
    print("\n\nSUCCESS: 完整的维度名称映射集成测试完成!")


if __name__ == "__main__":
    main()