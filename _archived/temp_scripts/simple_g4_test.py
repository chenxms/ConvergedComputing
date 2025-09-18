#!/usr/bin/env python3
"""
简化的G4修复验证
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.subjects_builder import SubjectsBuilder
import asyncio

async def simple_verification():
    """简化验证G4修复效果"""
    print("="*60)
    print("G4批次修复验证")
    print("="*60)
    
    batch_code = "G4-2025"
    builder = SubjectsBuilder()
    
    try:
        # 1. 测试科目列表
        print("1. 获取科目列表...")
        subjects_list = builder.list_subjects(batch_code)
        print(f"找到 {len(subjects_list)} 个科目")
        for subject in subjects_list:
            print(f"  - {subject.name} ({subject.type})")
        
        if not subjects_list:
            print("错误: 没有找到科目数据")
            return False
        
        # 2. 测试学校排名
        print(f"\n2. 测试学校排名...")
        test_subject = subjects_list[0].name
        print(f"使用科目: {test_subject}")
        
        school_rankings = builder._compute_school_rankings(batch_code, test_subject)
        print(f"获得 {len(school_rankings)} 所学校排名")
        
        null_count = 0
        valid_count = 0
        
        for i, school in enumerate(school_rankings[:10]):  # 前10个
            school_name = school.get('school_name')
            if school_name is None or school_name == 'None':
                null_count += 1
                print(f"  错误 {i+1}. {school['school_code']} - NULL学校名称")
            else:
                valid_count += 1
                print(f"  正确 {i+1}. {school['school_code']} - {school_name} (平均分: {school['avg']}, 排名: {school['rank']})")
        
        print(f"\n学校名称统计:")
        print(f"  有效学校名称: {valid_count}")
        print(f"  NULL学校名称: {null_count}")
        
        # 3. 测试完整subjects构建  
        print(f"\n3. 测试完整subjects构建...")
        regional_subjects = builder.build_regional_subjects(batch_code)
        print(f"构建了 {len(regional_subjects)} 个区域级科目数据")
        
        # 检查第一个科目的NULL情况
        if regional_subjects:
            first_subject = regional_subjects[0]
            school_rankings_in_subject = first_subject.get('school_rankings', [])
            null_count_in_subject = sum(1 for school in school_rankings_in_subject 
                                      if school.get('school_name') is None or school.get('school_name') == 'None')
            
            print(f"第一个科目 '{first_subject['subject_name']}' 中:")
            print(f"  总学校数: {len(school_rankings_in_subject)}")
            print(f"  NULL学校名称数: {null_count_in_subject}")
            
            if null_count_in_subject == 0:
                print("成功: 没有发现NULL学校名称")
                return True
            else:
                print("失败: 仍有NULL学校名称")
                return False
        
        return null_count == 0
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(simple_verification())
    print(f"\n修复验证结果: {'成功' if success else '失败'}")