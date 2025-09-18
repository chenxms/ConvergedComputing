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
from app.database.models import BatchDimensionDefinition
from app.services.calculation_service import CalculationService
from app.services.subjects_builder import SubjectsBuilder
from sqlalchemy import text


def main():
    print("测试维度名称映射功能...")
    
    with next(get_db()) as db:
        # 1. 检查表结构
        try:
            result = db.execute(text("SHOW TABLES LIKE 'batch_dimension_definition'")).fetchone()
            if result:
                print("OK: batch_dimension_definition表存在")
                
                # 查看数据
                count = db.execute(text("SELECT COUNT(*) FROM batch_dimension_definition")).fetchone()[0]
                print(f"OK: 表中数据数量: {count}")
                
                if count > 0:
                    # 获取示例数据
                    sample_data = db.execute(text(
                        "SELECT batch_code, subject_name, dimension_code, dimension_name FROM batch_dimension_definition LIMIT 3"
                    )).fetchall()
                    print("OK: 示例数据:")
                    for row in sample_data:
                        print(f"  - {row[0]}, {row[1]}, {row[2]} -> {row[3]}")
                    
                    # 测试CalculationService
                    service = CalculationService(db)
                    batch_code, subject_name = sample_data[0][0], sample_data[0][1]
                    
                    print(f"测试批次: {batch_code}, 科目: {subject_name}")
                    
                    # 测试批量加载维度名称
                    dimension_mapping = service._batch_load_dimension_names(batch_code, subject_name)
                    print(f"OK: CalculationService批量加载维度名称: {len(dimension_mapping)} 个")
                    
                    # 测试SubjectsBuilder
                    builder = SubjectsBuilder()
                    dimension_mapping2 = builder._batch_load_dimension_names(db, batch_code, subject_name)
                    print(f"OK: SubjectsBuilder批量加载维度名称: {len(dimension_mapping2)} 个")
                    
                    # 显示映射结果
                    for dim_code, dim_name in list(dimension_mapping.items())[:3]:
                        print(f"  维度映射: {dim_code} -> {dim_name}")
                    
                    print("SUCCESS: 维度名称映射功能测试完成!")
                else:
                    print("WARN: 表中无数据")
            else:
                print("FAIL: batch_dimension_definition表不存在")
        except Exception as e:
            print(f"FAIL: 测试失败: {e}")


if __name__ == "__main__":
    main()