#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""只更新区域级数据的增强统计字段"""

import sys
import json
import asyncio
from datetime import datetime, timezone
from sqlalchemy import text

from app.database.connection import get_db
from app.services.subjects_builder import SubjectsBuilder
from app.services.calculation_service import CalculationService
from scripts.rewrite_subjects_v12_enhanced import get_enhanced_stats_for_regional

async def update_regional_only(batch_code: str):
    """只更新区域级数据"""
    
    print(f"开始更新批次 {batch_code} 的区域级数据...")
    
    sb = SubjectsBuilder()
    
    with next(get_db()) as db:
        # 获取区域级增强统计
        print("计算区域级增强统计...")
        regional_enhanced_stats = await get_enhanced_stats_for_regional(batch_code, db)
        
        if regional_enhanced_stats:
            print(f"成功计算增强统计，包含 {len(regional_enhanced_stats.get('academic_subjects', {}))} 个学业科目")
            
            # 打印第一个科目的详细信息用于调试
            for subject_name, subject_data in regional_enhanced_stats.get('academic_subjects', {}).items():
                print(f"\n科目 '{subject_name}' 的数据结构:")
                print(f"  - 包含键: {list(subject_data.keys())[:10]}")
                if 'percentiles' in subject_data:
                    print(f"  - 百分位数: {subject_data['percentiles']}")
                if 'discrimination' in subject_data:
                    print(f"  - 区分度: {subject_data['discrimination']}")
                if 'grade_distribution' in subject_data:
                    print(f"  - 等级分布键: {list(subject_data['grade_distribution'].keys())[:5]}")
                break
        else:
            print("未能计算增强统计")
            return
        
        # 构建区域级subjects（传递增强统计）
        print("构建区域级subjects...")
        regional_subjects = sb.build_regional_subjects(
            batch_code, 
            enhanced_stats=regional_enhanced_stats
        )
        
        # 检查第一个subject是否包含增强字段
        if regional_subjects and len(regional_subjects) > 0:
            first_subject = regional_subjects[0]
            print(f"\n第一个subject '{first_subject.get('subject_name')}' 包含的字段:")
            print(f"  - discrimination: {first_subject.get('discrimination', '未找到')}")
            print(f"  - p10: {first_subject.get('p10', '未找到')}")
            print(f"  - p50: {first_subject.get('p50', '未找到')}")
            print(f"  - p90: {first_subject.get('p90', '未找到')}")
            print(f"  - grade_distribution: {'找到' if 'grade_distribution' in first_subject else '未找到'}")
        
        regional_json = {
            "schema_version": "v1.2",
            "batch_code": batch_code,
            "aggregation_level": "REGIONAL",
            "subjects": regional_subjects,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        
        # 保存区域级数据
        print("保存区域级数据到数据库...")
        db.execute(
            text(
                "UPDATE statistical_aggregations "
                "SET statistics_data = :d, updated_at = NOW() "
                "WHERE batch_code = :b AND aggregation_level = 'REGIONAL'"
            ),
            {"b": batch_code, "d": json.dumps(regional_json, ensure_ascii=False)},
        )
        db.commit()
        
        print("区域级数据更新完成！")

if __name__ == "__main__":
    batch_code = sys.argv[1] if len(sys.argv) > 1 else "G4-2025"
    asyncio.run(update_regional_only(batch_code))