#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""只更新单个学校的数据"""

import sys
import json
import asyncio
from datetime import datetime, timezone
from sqlalchemy import text
from app.database.connection import get_db
from app.services.subjects_builder import SubjectsBuilder
from scripts.rewrite_subjects_v12_enhanced_fixed import get_enhanced_stats_for_school

async def update_single_school(batch_code: str, school_code: str):
    """只更新单个学校的数据"""
    
    print(f"开始更新学校 {school_code} 的数据...")
    
    sb = SubjectsBuilder()
    
    with next(get_db()) as db:
        # 获取学校级增强统计（按科目分别计算）
        print("计算增强统计...")
        school_enhanced_stats = await get_enhanced_stats_for_school(batch_code, school_code, db)
        
        if not school_enhanced_stats:
            print("未能计算增强统计")
            return
        
        # 构建学校级subjects
        print("构建subjects...")
        school_subjects = sb.build_school_subjects(
            batch_code, 
            school_code,
            enhanced_stats=school_enhanced_stats
        )
        
        school_json = {
            "schema_version": "v1.2",
            "batch_code": batch_code,
            "aggregation_level": "SCHOOL",
            "school_code": school_code,
            "subjects": school_subjects,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        
        # 保存学校级数据
        print("保存到数据库...")
        db.execute(
            text(
                "UPDATE statistical_aggregations "
                "SET statistics_data = :d, updated_at = NOW() "
                "WHERE batch_code = :b AND aggregation_level = 'SCHOOL' AND school_id = :s"
            ),
            {"b": batch_code, "s": school_code, "d": json.dumps(school_json, ensure_ascii=False)},
        )
        db.commit()
        
        print(f"学校 {school_code} 数据更新完成！")

if __name__ == "__main__":
    batch = sys.argv[1] if len(sys.argv) > 1 else "G4-2025"
    school = sys.argv[2] if len(sys.argv) > 2 else "5068"
    asyncio.run(update_single_school(batch, school))
