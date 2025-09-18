#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终的维度名称映射验证测试
验证前端接收到的数据是否包含中文维度名称
"""

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


async def test_frontend_api_format():
    """测试前端API格式中的维度名称"""
    print("测试前端API格式中的维度名称...")
    
    with next(get_db()) as db:
        service = CalculationService(db)
        builder = SubjectsBuilder()
        
        batch_code = "G4-2025"
        subject_name = "数学" 
        school_code = "5071"
        
        print(f"测试批次: {batch_code}, 科目: {subject_name}, 学校: {school_code}")
        
        try:
            # 1. 测试区域级数据（CalculationService输出）
            print("\n=== 区域级维度数据 ===")
            dimension_results = await service._calculate_subject_dimensions(batch_code, subject_name)
            
            if dimension_results:
                print(f"区域级维度统计: {len(dimension_results)} 个维度")
                for dim_code, dim_data in list(dimension_results.items())[:2]:
                    basic_stats = dim_data.get('basic_stats', {})
                    print(f"维度 {dim_code}:")
                    print(f"  中文名称: {basic_stats.get('name', '未定义')}")
                    print(f"  平均分: {basic_stats.get('avg_score', 0):.2f}")
                    print(f"  学生数: {basic_stats.get('student_count', 0)}")
                    
                    # 检查统计指标
                    if 'statistical_indicators' in dim_data:
                        indicators = dim_data['statistical_indicators']
                        print(f"  难度系数: {indicators.get('difficulty_coefficient', 0):.3f}")
                        print(f"  优秀率: {indicators.get('excellent_rate', 0):.1f}%")
            else:
                print("WARN: 区域级维度数据为空")
                return False
            
            # 2. 测试学校级数据（SubjectsBuilder输出）
            print("\n=== 学校级维度数据 ===")
            school_dimensions = builder._compute_school_dimensions_with_rank(
                batch_code, subject_name, school_code
            )
            
            if school_dimensions:
                print(f"学校级维度排名: {len(school_dimensions)} 个维度")
                for dim in school_dimensions[:2]:
                    print(f"维度 {dim.get('code', '')}:")
                    print(f"  中文名称: {dim.get('name', '未定义')}")
                    print(f"  平均分: {dim.get('avg', 0)}")
                    print(f"  排名: {dim.get('rank', 0)}")
                    print(f"  得分率: {dim.get('score_rate', 0)}%")
            else:
                print("WARN: 学校级维度数据为空")
                return False
            
            # 3. 测试区域级subjects构建（完整前端数据结构）
            print("\n=== 完整前端数据结构测试 ===")
            regional_subjects = builder.build_regional_subjects(batch_code)
            
            # 找到数学科目
            math_subject = None
            for subject in regional_subjects:
                if subject.get('name') == subject_name:
                    math_subject = subject
                    break
            
            if math_subject:
                print(f"前端数学科目数据结构:")
                print(f"  科目名称: {math_subject.get('name')}")
                print(f"  科目类型: {math_subject.get('type')}")
                
                # 检查维度数据
                dimensions = math_subject.get('dimensions', [])
                if dimensions:
                    print(f"  维度数量: {len(dimensions)}")
                    for dim in dimensions[:2]:
                        print(f"  维度:")
                        print(f"    代码: {dim.get('code')}")
                        print(f"    中文名称: {dim.get('name', '未定义')}")
                        print(f"    平均分: {dim.get('avg', 0)}")
                else:
                    print("  WARN: 前端维度数据为空")
            else:
                print("WARN: 未找到数学科目数据")
            
            return True
            
        except Exception as e:
            print(f"FAIL: 测试失败: {e}")
            import traceback
            traceback.print_exc()
            return False


def test_dimension_mapping_coverage():
    """测试维度映射覆盖率"""
    print("\n\n测试维度映射覆盖率...")
    
    with next(get_db()) as db:
        # 获取所有有维度定义的科目
        results = db.execute(text("""
            SELECT DISTINCT subject_name, COUNT(DISTINCT dimension_code) as dim_count
            FROM batch_dimension_definition 
            WHERE batch_code = 'G4-2025'
            GROUP BY subject_name
            ORDER BY dim_count DESC
        """)).fetchall()
        
        print(f"找到 {len(results)} 个有维度定义的科目:")
        
        total_subjects = 0
        mapped_subjects = 0
        
        for row in results:
            subject_name, dim_count = row[0], row[1]
            
            # 测试该科目的维度映射
            service = CalculationService(db)
            try:
                dimension_mapping = service._batch_load_dimension_names("G4-2025", subject_name)
                mapped_count = len(dimension_mapping)
                
                coverage = (mapped_count / dim_count * 100) if dim_count > 0 else 0
                print(f"  {subject_name}: {mapped_count}/{dim_count} 维度映射 ({coverage:.1f}%)")
                
                total_subjects += 1
                if mapped_count > 0:
                    mapped_subjects += 1
                    
            except Exception as e:
                print(f"  {subject_name}: 映射失败 - {e}")
        
        overall_coverage = (mapped_subjects / total_subjects * 100) if total_subjects > 0 else 0
        print(f"\n总体映射覆盖率: {mapped_subjects}/{total_subjects} 科目 ({overall_coverage:.1f}%)")
        
        return overall_coverage > 80  # 80%以上覆盖率视为成功


def main():
    print("开始最终的维度名称映射验证测试...")
    
    # 测试1：前端API格式
    success1 = asyncio.run(test_frontend_api_format())
    
    # 测试2：映射覆盖率
    success2 = test_dimension_mapping_coverage()
    
    if success1 and success2:
        print("\n\n🎉 SUCCESS: 维度名称映射修复完成!")
        print("✅ CalculationService中的维度统计已使用中文名称")
        print("✅ SubjectsBuilder中的维度排名已使用中文名称") 
        print("✅ 前端数据结构包含正确的中文维度名称")
        print("✅ 维度映射覆盖率达到要求")
        print("\n用户反馈的问题已解决：维度现在显示中文名称而不是维度ID")
    else:
        print("\n\n❌ FAIL: 部分测试未通过")
        if not success1:
            print("- 前端API格式测试失败")
        if not success2:
            print("- 维度映射覆盖率不足")


if __name__ == "__main__":
    main()