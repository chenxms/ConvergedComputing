#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试维度名称映射功能
"""

import asyncio
import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.database.connection import get_db
from app.database.models import BatchDimensionDefinition
from app.services.calculation_service import CalculationService
from app.services.subjects_builder import SubjectsBuilder
from sqlalchemy import text


def test_batch_dimension_definition_table():
    """测试batch_dimension_definition表是否存在和可用"""
    print("=== 测试 BatchDimensionDefinition 表 ===")
    
    with next(get_db()) as db:
        # 1. 测试表是否存在
        try:
            result = db.execute(text("SHOW TABLES LIKE 'batch_dimension_definition'")).fetchone()
            if result:
                print("✓ batch_dimension_definition表存在")
            else:
                print("✗ batch_dimension_definition表不存在")
                return False
        except Exception as e:
            print(f"✗ 检查表存在性失败: {e}")
            return False
        
        # 2. 测试表结构
        try:
            result = db.execute(text("DESCRIBE batch_dimension_definition")).fetchall()
            print("✓ 表结构:")
            for row in result:
                print(f"  - {row[0]}: {row[1]}")
        except Exception as e:
            print(f"✗ 检查表结构失败: {e}")
            return False
        
        # 3. 测试数据查询
        try:
            count = db.execute(text("SELECT COUNT(*) FROM batch_dimension_definition")).fetchone()[0]
            print(f"✓ 表中数据数量: {count}")
            
            # 获取一些示例数据
            if count > 0:
                sample_data = db.execute(text(
                    "SELECT batch_code, subject_name, dimension_code, dimension_name FROM batch_dimension_definition LIMIT 5"
                )).fetchall()
                print("✓ 示例数据:")
                for row in sample_data:
                    print(f"  - {row[0]}, {row[1]}, {row[2]} -> {row[3]}")
                return True, sample_data
            else:
                print("⚠ 表中暂无数据")
                return True, []
        except Exception as e:
            print(f"✗ 查询数据失败: {e}")
            return False, []


def test_calculation_service_dimension_names():
    """测试CalculationService中的维度名称获取功能"""
    print("\n=== 测试 CalculationService 维度名称功能 ===")
    
    with next(get_db()) as db:
        service = CalculationService(db)
        
        # 1. 测试批量加载维度名称
        try:
            # 获取一个有数据的批次和科目
            result = db.execute(text(
                "SELECT DISTINCT batch_code, subject_name FROM batch_dimension_definition LIMIT 1"
            )).fetchone()
            
            if result:
                batch_code, subject_name = result[0], result[1]
                print(f"测试批次: {batch_code}, 科目: {subject_name}")
                
                # 测试批量加载
                dimension_mapping = service._batch_load_dimension_names(batch_code, subject_name)
                print(f"✓ 批量加载维度名称: {len(dimension_mapping)} 个")
                for dim_code, dim_name in list(dimension_mapping.items())[:3]:  # 显示前3个
                    print(f"  - {dim_code} -> {dim_name}")
                
                # 测试单个获取
                if dimension_mapping:
                    first_dim_code = next(iter(dimension_mapping.keys()))
                    retrieved_name = service._get_dimension_name(batch_code, subject_name, first_dim_code)
                    expected_name = dimension_mapping[first_dim_code]
                    
                    if retrieved_name == expected_name:
                        print(f"✓ 单个获取维度名称: {first_dim_code} -> {retrieved_name}")
                    else:
                        print(f"✗ 单个获取维度名称失败: 期望 {expected_name}, 实际 {retrieved_name}")
                
                return True
            else:
                print("[WARN] 未找到测试数据")
                return True
                
        except Exception as e:
            print(f"✗ 测试失败: {e}")
            return False


def test_subjects_builder_dimension_names():
    """测试SubjectsBuilder中的维度名称功能"""
    print("\n=== 测试 SubjectsBuilder 维度名称功能 ===")
    
    builder = SubjectsBuilder()
    
    with next(get_db()) as db:
        try:
            # 获取一个有数据的批次和科目
            result = db.execute(text(
                "SELECT DISTINCT batch_code, subject_name FROM batch_dimension_definition LIMIT 1"
            )).fetchone()
            
            if result:
                batch_code, subject_name = result[0], result[1]
                print(f"测试批次: {batch_code}, 科目: {subject_name}")
                
                # 测试批量加载
                dimension_mapping = builder._batch_load_dimension_names(db, batch_code, subject_name)
                print(f"✓ SubjectsBuilder批量加载维度名称: {len(dimension_mapping)} 个")
                for dim_code, dim_name in list(dimension_mapping.items())[:3]:  # 显示前3个
                    print(f"  - {dim_code} -> {dim_name}")
                
                # 测试单个获取
                if dimension_mapping:
                    first_dim_code = next(iter(dimension_mapping.keys()))
                    retrieved_name = builder._get_dimension_name(db, batch_code, subject_name, first_dim_code)
                    expected_name = dimension_mapping[first_dim_code]
                    
                    if retrieved_name == expected_name:
                        print(f"✓ SubjectsBuilder单个获取维度名称: {first_dim_code} -> {retrieved_name}")
                    else:
                        print(f"✗ SubjectsBuilder单个获取维度名称失败: 期望 {expected_name}, 实际 {retrieved_name}")
                
                return True
            else:
                print("[WARN] 未找到测试数据")
                return True
                
        except Exception as e:
            print(f"✗ 测试失败: {e}")
            return False


async def test_dimension_calculation_integration():
    """测试维度计算中的名称映射集成"""
    print("\n=== 测试维度计算名称映射集成 ===")
    
    with next(get_db()) as db:
        service = CalculationService(db)
        
        try:
            # 获取一个有维度数据的批次
            result = db.execute(text(
                "SELECT DISTINCT batch_code FROM batch_dimension_definition LIMIT 1"
            )).fetchone()
            
            if result:
                batch_code = result[0]
                print(f"测试批次: {batch_code}")
                
                # 获取科目列表
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
                            print(f"✓ 维度计算成功: {len(dimension_results)} 个维度")
                            for dim_code, dim_data in list(dimension_results.items())[:2]:  # 显示前2个
                                dimension_name = dim_data.get('basic_stats', {}).get('name', dim_code)
                                print(f"  - {dim_code}: {dimension_name}")
                        else:
                            print("⚠ 该科目未找到维度数据")
                    except Exception as e:
                        print(f"✗ 维度计算失败: {e}")
                
                return True
            else:
                print("⚠ 未找到测试批次")
                return True
                
        except Exception as e:
            print(f"✗ 集成测试失败: {e}")
            return False


def main():
    """主测试函数"""
    print("开始测试维度名称映射功能...")
    
    # 1. 测试表结构和数据
    table_ok, sample_data = test_batch_dimension_definition_table()
    if not table_ok:
        print("❌ 表结构测试失败，停止后续测试")
        return
    
    if not sample_data:
        print("⚠ 表中无数据，跳过功能测试")
        return
    
    # 2. 测试CalculationService功能
    if not test_calculation_service_dimension_names():
        print("❌ CalculationService测试失败")
        return
    
    # 3. 测试SubjectsBuilder功能
    if not test_subjects_builder_dimension_names():
        print("❌ SubjectsBuilder测试失败")
        return
    
    # 4. 测试集成功能
    asyncio.run(test_dimension_calculation_integration())
    
    print("\n✅ 维度名称映射功能测试完成！")


if __name__ == "__main__":
    main()