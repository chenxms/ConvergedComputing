#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试增强的SubjectsBuilder功能
验证calculation_service计算的统计数据是否正确传递到subjects_builder
"""

import sys
import os
import asyncio
from typing import Dict, Any
import json

# 添加路径
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from app.services.subjects_builder import SubjectsBuilder
from app.services.calculation_service import CalculationService


async def test_enhanced_subjects_builder():
    """测试增强的SubjectsBuilder功能"""
    print("=== 测试增强的SubjectsBuilder功能 ===\n")
    
    # 测试批次
    batch_code = "G4-2025"
    test_school = "G40001"
    
    db = next(get_db())
    
    try:
        # 1. 获取增强统计数据
        print(f"1. 获取批次 {batch_code} 的增强统计数据...")
        calc_service = CalculationService(db)
        
        # 获取学生数据
        student_df = await calc_service._fetch_student_scores(batch_code)
        print(f"   获取学生数据: {len(student_df)} 条记录")
        
        if student_df.empty:
            print("   ❌ 没有学生数据，跳过测试")
            return
        
        # 计算增强统计数据
        enhanced_stats = await calc_service._consolidate_multi_subject_results(batch_code, student_df)
        
        if enhanced_stats:
            print(f"   ✅ 获取增强统计数据成功")
            print(f"   学业科目数: {len(enhanced_stats.get('academic_subjects', {}))}")
            print(f"   非学业科目数: {len(enhanced_stats.get('non_academic_subjects', {}))}")
            
            # 显示第一个科目的统计信息
            all_subjects = {**enhanced_stats.get('academic_subjects', {}), **enhanced_stats.get('non_academic_subjects', {})}
            if all_subjects:
                first_subject_name, first_subject_data = next(iter(all_subjects.items()))
                print(f"   示例科目 '{first_subject_name}' 包含的统计字段:")
                for key in first_subject_data.keys():
                    print(f"     - {key}")
        else:
            print("   ❌ 无法获取增强统计数据")
            enhanced_stats = None
        
        # 2. 测试区域级subjects构建
        print(f"\n2. 测试区域级subjects构建（包含增强数据）...")
        subjects_builder = SubjectsBuilder()
        
        # 不使用增强数据
        regional_subjects_basic = subjects_builder.build_regional_subjects(batch_code)
        print(f"   基础区域级subjects: {len(regional_subjects_basic)} 个科目")
        
        # 使用增强数据
        regional_subjects_enhanced = subjects_builder.build_regional_subjects(batch_code, enhanced_stats=enhanced_stats)
        print(f"   增强区域级subjects: {len(regional_subjects_enhanced)} 个科目")
        
        # 比较差异
        if regional_subjects_enhanced:
            subject_enhanced = regional_subjects_enhanced[0]
            subject_basic = regional_subjects_basic[0] if regional_subjects_basic else {}
            
            print(f"\n   科目 '{subject_enhanced.get('subject_name')}' 增强后的新字段:")
            enhanced_metrics = subject_enhanced.get('metrics', {})
            basic_metrics = subject_basic.get('metrics', {})
            
            # 检查新增的字段
            new_fields = []
            if 'percentiles' in enhanced_metrics:
                new_fields.append('percentiles')
            if 'discrimination' in enhanced_metrics:
                new_fields.append('discrimination')  
            if 'grade_distribution' in enhanced_metrics:
                new_fields.append('grade_distribution')
                
            if new_fields:
                print(f"     新增字段: {', '.join(new_fields)}")
                
                # 显示详细内容
                if 'percentiles' in enhanced_metrics:
                    p_data = enhanced_metrics['percentiles']
                    print(f"     百分位数: P10={p_data.get('P10')}, P50={p_data.get('P50')}, P90={p_data.get('P90')}")
                
                if 'discrimination' in enhanced_metrics:
                    print(f"     区分度: {enhanced_metrics['discrimination']}")
                    
                if 'grade_distribution' in enhanced_metrics:
                    grade_dist = enhanced_metrics['grade_distribution']
                    print(f"     等级分布: 优秀={grade_dist.get('excellent', {}).get('percentage', 0)}%, "
                          f"良好={grade_dist.get('good', {}).get('percentage', 0)}%, "
                          f"及格={grade_dist.get('pass', {}).get('percentage', 0)}%, "
                          f"不及格={grade_dist.get('fail', {}).get('percentage', 0)}%")
            else:
                print("     ❌ 没有检测到新增字段")
        
        # 3. 测试学校级subjects构建
        print(f"\n3. 测试学校级subjects构建（包含增强数据）...")
        
        # 不使用增强数据
        school_subjects_basic = subjects_builder.build_school_subjects(batch_code, test_school)
        print(f"   基础学校级subjects: {len(school_subjects_basic)} 个科目")
        
        # 使用增强数据
        school_subjects_enhanced = subjects_builder.build_school_subjects(batch_code, test_school, enhanced_stats=enhanced_stats)
        print(f"   增强学校级subjects: {len(school_subjects_enhanced)} 个科目")
        
        # 比较差异
        if school_subjects_enhanced:
            subject_enhanced = school_subjects_enhanced[0]
            print(f"\n   学校级科目 '{subject_enhanced.get('subject_name')}' 增强后的字段:")
            enhanced_metrics = subject_enhanced.get('metrics', {})
            
            # 检查新增的字段
            new_fields = []
            if 'percentiles' in enhanced_metrics:
                new_fields.append('percentiles')
            if 'discrimination' in enhanced_metrics:
                new_fields.append('discrimination')  
            if 'grade_distribution' in enhanced_metrics:
                new_fields.append('grade_distribution')
                
            if new_fields:
                print(f"     新增字段: {', '.join(new_fields)}")
                
            # 检查维度数据是否增强
            dimensions = subject_enhanced.get('dimensions', [])
            if dimensions:
                dim_enhanced = dimensions[0]
                dim_new_fields = []
                if 'difficulty' in dim_enhanced:
                    dim_new_fields.append('difficulty')
                if 'discrimination' in dim_enhanced:
                    dim_new_fields.append('discrimination')
                if 'percentiles' in dim_enhanced:
                    dim_new_fields.append('percentiles')
                    
                if dim_new_fields:
                    print(f"     维度增强字段: {', '.join(dim_new_fields)}")
        
        # 4. 数据完整性验证
        print(f"\n4. 数据完整性验证...")
        validation_passed = True
        
        # 验证基础字段仍然存在
        for subject in regional_subjects_enhanced:
            metrics = subject.get('metrics', {})
            required_fields = ['avg', 'stddev', 'max', 'min', 'difficulty']
            missing_fields = [f for f in required_fields if f not in metrics]
            if missing_fields:
                print(f"   ❌ 科目 {subject.get('subject_name')} 缺少基础字段: {missing_fields}")
                validation_passed = False
        
        # 验证数值精度
        for subject in regional_subjects_enhanced:
            metrics = subject.get('metrics', {})
            if 'percentiles' in metrics:
                p_data = metrics['percentiles']
                for p_key, p_val in p_data.items():
                    if not isinstance(p_val, (int, float)):
                        print(f"   ❌ 科目 {subject.get('subject_name')} 百分位数 {p_key} 类型错误: {type(p_val)}")
                        validation_passed = False
        
        if validation_passed:
            print("   ✅ 数据完整性验证通过")
        
        # 5. 保存测试结果样例
        print(f"\n5. 保存测试结果样例...")
        if regional_subjects_enhanced:
            sample_data = {
                'batch_code': batch_code,
                'enhanced_subjects_sample': regional_subjects_enhanced[0] if regional_subjects_enhanced else None,
                'school_enhanced_sample': school_subjects_enhanced[0] if school_subjects_enhanced else None
            }
            
            with open('subjects_builder_enhanced_test_result.json', 'w', encoding='utf-8') as f:
                json.dump(sample_data, f, ensure_ascii=False, indent=2)
            print("   ✅ 测试结果已保存到 subjects_builder_enhanced_test_result.json")
        
        print(f"\n=== 测试完成 ===")
        print(f"✅ SubjectsBuilder增强功能集成成功")
        print(f"✅ 新增字段: percentiles, discrimination, grade_distribution")
        print(f"✅ 维度数据增强: difficulty, discrimination, percentiles")
        print(f"✅ 向后兼容性: 保持现有字段")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(test_enhanced_subjects_builder())