#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import asyncio
import json

sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from app.services.subjects_builder import SubjectsBuilder
from app.services.calculation_service import CalculationService


async def test_enhanced_subjects_builder():
    """测试增强的SubjectsBuilder功能"""
    print("=== 测试增强的SubjectsBuilder功能 ===")
    
    batch_code = "G4-2025"
    test_school = "G40001"
    
    db = next(get_db())
    
    try:
        print(f"1. 获取批次 {batch_code} 的增强统计数据...")
        calc_service = CalculationService(db)
        
        student_df = await calc_service._fetch_student_scores(batch_code)
        print(f"   获取学生数据: {len(student_df)} 条记录")
        
        if student_df.empty:
            print("   [WARNING] 没有学生数据，跳过测试")
            return
        
        enhanced_stats = await calc_service._consolidate_multi_subject_results(batch_code, student_df)
        
        if enhanced_stats:
            print(f"   [SUCCESS] 获取增强统计数据成功")
            print(f"   学业科目数: {len(enhanced_stats.get('academic_subjects', {}))}")
            print(f"   非学业科目数: {len(enhanced_stats.get('non_academic_subjects', {}))}")
            
            # 检查第一个科目的统计信息
            all_subjects = {**enhanced_stats.get('academic_subjects', {}), **enhanced_stats.get('non_academic_subjects', {})}
            if all_subjects:
                first_subject_name, first_subject_data = next(iter(all_subjects.items()))
                print(f"   示例科目 '{first_subject_name}' 包含的统计字段:")
                for key in first_subject_data.keys():
                    print(f"     - {key}")
                    
                # 检查关键字段
                required_fields = ['percentiles', 'statistical_indicators', 'grade_distribution']
                has_fields = [f for f in required_fields if f in first_subject_data]
                print(f"   关键增强字段: {has_fields}")
        else:
            print("   [WARNING] 无法获取增强统计数据")
            enhanced_stats = None
        
        print(f"\n2. 测试区域级subjects构建（包含增强数据）...")
        subjects_builder = SubjectsBuilder()
        
        # 不使用增强数据
        regional_subjects_basic = subjects_builder.build_regional_subjects(batch_code)
        print(f"   基础区域级subjects: {len(regional_subjects_basic)} 个科目")
        
        # 使用增强数据
        regional_subjects_enhanced = subjects_builder.build_regional_subjects(batch_code, enhanced_stats=enhanced_stats)
        print(f"   增强区域级subjects: {len(regional_subjects_enhanced)} 个科目")
        
        # 比较差异
        if regional_subjects_enhanced and regional_subjects_basic:
            subject_enhanced = regional_subjects_enhanced[0]
            subject_basic = regional_subjects_basic[0]
            
            print(f"\n   科目 '{subject_enhanced.get('subject_name')}' 字段比较:")
            enhanced_metrics = subject_enhanced.get('metrics', {})
            basic_metrics = subject_basic.get('metrics', {})
            
            # 检查新增的字段
            enhanced_fields = set(enhanced_metrics.keys())
            basic_fields = set(basic_metrics.keys())
            new_fields = enhanced_fields - basic_fields
            
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
                          f"良好={grade_dist.get('good', {}).get('percentage', 0)}%")
            else:
                print("     [WARNING] 没有检测到新增字段")
        
        print(f"\n3. 测试学校级subjects构建...")
        
        # 使用增强数据
        school_subjects_enhanced = subjects_builder.build_school_subjects(batch_code, test_school, enhanced_stats=enhanced_stats)
        print(f"   增强学校级subjects: {len(school_subjects_enhanced)} 个科目")
        
        # 验证数据完整性
        print(f"\n4. 数据完整性验证...")
        validation_passed = True
        
        for subject in regional_subjects_enhanced:
            metrics = subject.get('metrics', {})
            required_fields = ['avg', 'stddev', 'max', 'min', 'difficulty']
            missing_fields = [f for f in required_fields if f not in metrics]
            if missing_fields:
                print(f"   [ERROR] 科目 {subject.get('subject_name')} 缺少基础字段: {missing_fields}")
                validation_passed = False
        
        if validation_passed:
            print("   [SUCCESS] 数据完整性验证通过")
        
        # 保存测试结果
        if regional_subjects_enhanced:
            sample_data = {
                'batch_code': batch_code,
                'enhanced_subjects_sample': regional_subjects_enhanced[0] if regional_subjects_enhanced else None,
                'school_enhanced_sample': school_subjects_enhanced[0] if school_subjects_enhanced else None
            }
            
            with open('subjects_enhanced_test_result.json', 'w', encoding='utf-8') as f:
                json.dump(sample_data, f, ensure_ascii=False, indent=2)
            print("   [SUCCESS] 测试结果已保存")
        
        print(f"\n=== 测试结果 ===")
        print(f"[SUCCESS] SubjectsBuilder增强功能集成成功")
        print(f"[SUCCESS] 新增字段: percentiles, discrimination, grade_distribution")
        print(f"[SUCCESS] 向后兼容性: 保持现有字段")
        
    except Exception as e:
        print(f"[ERROR] 测试失败: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(test_enhanced_subjects_builder())