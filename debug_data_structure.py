#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试数据结构和字段验证问题
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.services.calculation_service import CalculationService
from app.database.connection import get_db
import asyncio

async def debug_data_structure():
    """调试数据结构和字段映射"""
    print("=== 调试数据结构和字段验证问题 ===")
    
    db = next(get_db())
    calc_service = CalculationService(db)
    batch_code = "G7-2025"
    
    try:
        # 1. 获取原始数据
        print("1. 获取原始数据...")
        data = await calc_service._fetch_student_scores(batch_code)
        print(f"原始数据行数: {len(data)}")
        print(f"原始数据字段: {list(data.columns)}")
        print("前5行数据:")
        print(data.head())
        
        # 2. 检查字段映射前后
        print("\n2. 检查字段映射...")
        if 'total_score' in data.columns:
            print("发现total_score字段，进行映射...")
            data_mapped = data.rename(columns={'total_score': 'score'})
            print(f"映射后字段: {list(data_mapped.columns)}")
            print("映射后前5行数据:")
            print(data_mapped[['student_id', 'subject_name', 'score']].head())
        else:
            print("没有找到total_score字段!")
            
        # 3. 检查配置
        print("\n3. 检查配置...")
        config = await calc_service._get_calculation_config(batch_code)
        print(f"required_columns: {config['required_columns']}")
        
        # 4. 检查验证器
        print("\n4. 测试数据验证...")
        if 'total_score' in data.columns:
            data_for_validation = data.rename(columns={'total_score': 'score'})
            validation_result = calc_service.engine.validator.validate_input_data(
                data_for_validation, config
            )
            print(f"验证结果: {validation_result}")
        
    except Exception as e:
        print(f"调试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_data_structure())