#!/usr/bin/env python3
"""
紧急G4修复测试脚本
验证SubjectsBuilder修复后是否解决了NULL学校名称问题
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.subjects_builder import SubjectsBuilder
from app.utils.data_validation import DataValidator
from app.database.connection import get_db
import json

async def test_subjects_builder_fix():
    """测试SubjectsBuilder修复效果"""
    print("="*60)
    print("G4批次SubjectsBuilder修复效果测试")
    print("="*60)
    
    batch_code = "G4-2025"
    builder = SubjectsBuilder()
    
    try:
        # 1. 测试科目列表
        print("\n1. 获取科目列表...")
        subjects_list = builder.list_subjects(batch_code)
        print(f"找到 {len(subjects_list)} 个科目:")
        for subject in subjects_list:
            print(f"  - {subject.name} ({subject.type})")
        
        if not subjects_list:
            print("❌ 没有找到科目数据")
            return False
        
        # 2. 测试区域级学校排名（这里最容易出现NULL学校名称）
        print(f"\n2. 测试区域级学校排名...")
        test_subject = subjects_list[0].name
        print(f"使用科目: {test_subject}")
        
        school_rankings = builder._compute_school_rankings(batch_code, test_subject)
        print(f"获得 {len(school_rankings)} 所学校排名:")
        
        null_schools = 0
        valid_schools = 0
        
        for i, school in enumerate(school_rankings[:10]):  # 只显示前10个
            school_name = school.get('school_name')
            if school_name is None or school_name == 'None':
                null_schools += 1
                print(f"  ❌ {i+1}. {school['school_code']} - NULL学校名称")
            else:
                valid_schools += 1
                print(f"  ✓ {i+1}. {school['school_code']} - {school_name} (平均分: {school['avg']}, 排名: {school['rank']})")
        
        print(f"\n学校名称统计:")
        print(f"  有效学校名称: {valid_schools}")
        print(f"  NULL学校名称: {null_schools}")
        
        # 3. 测试完整的区域级subjects构建
        print(f"\n3. 测试完整区域级subjects构建...")
        regional_subjects = builder.build_regional_subjects(batch_code)
        print(f"构建了 {len(regional_subjects)} 个区域级科目数据")
        
        # 检查第一个科目的school_rankings中是否还有NULL
        if regional_subjects:
            first_subject = regional_subjects[0]
            school_rankings_in_subject = first_subject.get('school_rankings', [])
            null_count_in_subject = sum(1 for school in school_rankings_in_subject 
                                      if school.get('school_name') is None or school.get('school_name') == 'None')
            
            print(f"第一个科目 '{first_subject['subject_name']}' 中:")
            print(f"  总学校数: {len(school_rankings_in_subject)}")
            print(f"  NULL学校名称数: {null_count_in_subject}")
            
            if null_count_in_subject == 0:
                print("✅ 修复成功! 没有发现NULL学校名称")
                return True
            else:
                print("❌ 修复失败! 仍有NULL学校名称")
                return False
        
        return null_schools == 0
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_data_validation():
    """测试数据验证"""
    print("\n" + "="*60)
    print("G4批次数据验证测试")
    print("="*60)
    
    with next(get_db()) as db:
        validator = DataValidator(db)
        validation_result = validator.validate_batch_school_consistency("G4-2025")
        
        print(f"验证结果: {'通过' if validation_result['is_valid'] else '失败'}")
        print(f"统计信息:")
        for key, value in validation_result['statistics'].items():
            print(f"  {key}: {value}")
        
        if validation_result['errors']:
            print(f"\n错误 ({len(validation_result['errors'])}个):")
            for error in validation_result['errors']:
                print(f"  - {error['type']}: {error['message']}")
        
        if validation_result['warnings']:
            print(f"\n警告 ({len(validation_result['warnings'])}个):")
            for warning in validation_result['warnings']:
                print(f"  - {warning['type']}: {warning['message']}")
        
        return validation_result['is_valid']


async def main():
    """主测试流程"""
    print("开始G4批次紧急修复验证...")
    
    # 1. 数据验证
    validation_success = await test_data_validation()
    
    # 2. SubjectsBuilder测试
    builder_success = await test_subjects_builder_fix()
    
    # 总结
    print("\n" + "="*60)
    print("修复验证总结")
    print("="*60)
    print(f"数据验证: {'通过' if validation_success else '失败'}")
    print(f"SubjectsBuilder修复: {'成功' if builder_success else '失败'}")
    
    overall_success = validation_success and builder_success
    print(f"整体修复状态: {'成功' if overall_success else '失败'}")
    
    if not overall_success:
        print("\n下一步行动:")
        print("1. 检查school_master_data表是否包含G4-2025批次的所有学校")
        print("2. 运行数据清洗服务重新处理G4数据")
        print("3. 验证school_id格式是否符合5044-5099范围")
    else:
        print("\n✅ 修复验证成功! 可以开始数据重建流程")


if __name__ == "__main__":
    asyncio.run(main())