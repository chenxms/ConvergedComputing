#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""验证增强统计字段是否已保存到数据库"""

import json
import sys
from sqlalchemy import text
from app.database.connection import get_db

def verify_enhanced_fields(batch_code: str):
    """验证增强统计字段"""
    
    with next(get_db()) as db:
        # 1. 查询区域级数据
        print(f"\n{'='*60}")
        print(f"验证批次 {batch_code} 的区域级数据")
        print(f"{'='*60}")
        
        result = db.execute(
            text("""
            SELECT statistics_data 
            FROM statistical_aggregations 
            WHERE batch_code = :b 
            AND aggregation_level = 'REGIONAL'
            LIMIT 1
            """),
            {"b": batch_code}
        ).fetchone()
        
        if result:
            data = json.loads(result[0])
            print(f"Schema版本: {data.get('schema_version', '未知')}")
            
            if 'subjects' in data and len(data['subjects']) > 0:
                for idx, subject in enumerate(data['subjects'], 1):
                    print(f"\n科目 {idx}: {subject.get('subject_name', '未知')}")
                    print(f"  - 区分度 (discrimination): {subject.get('discrimination', '未找到')}")
                    print(f"  - P10: {subject.get('p10', '未找到')}")
                    print(f"  - P50: {subject.get('p50', '未找到')}")
                    print(f"  - P90: {subject.get('p90', '未找到')}")
                    
                    grade_dist = subject.get('grade_distribution', '未找到')
                    if isinstance(grade_dist, dict):
                        print(f"  - 等级分布:")
                        for grade, value in grade_dist.items():
                            print(f"      {grade}: {value}")
                    else:
                        print(f"  - 等级分布: {grade_dist}")
                    
                    # 检查维度
                    if 'dimensions' in subject and len(subject['dimensions']) > 0:
                        print(f"  - 维度数量: {len(subject['dimensions'])}")
                        for dim in subject['dimensions'][:2]:  # 只显示前2个
                            print(f"    * {dim.get('dimension_name', '未知')}: 得分率={dim.get('score_rate', '未找到')}")
        else:
            print("未找到区域级数据")
        
        # 2. 查询学校级数据（抽样一所）
        print(f"\n{'='*60}")
        print(f"验证批次 {batch_code} 的学校级数据（抽样）")
        print(f"{'='*60}")
        
        result = db.execute(
            text("""
            SELECT school_id, statistics_data 
            FROM statistical_aggregations 
            WHERE batch_code = :b 
            AND aggregation_level = 'SCHOOL'
            LIMIT 1
            """),
            {"b": batch_code}
        ).fetchone()
        
        if result:
            school_id, stats_data = result
            data = json.loads(stats_data)
            print(f"学校ID: {school_id}")
            print(f"Schema版本: {data.get('schema_version', '未知')}")
            
            if 'subjects' in data and len(data['subjects']) > 0:
                subject = data['subjects'][0]  # 只看第一个科目
                print(f"\n科目: {subject.get('subject_name', '未知')}")
                print(f"  - 区分度 (discrimination): {subject.get('discrimination', '未找到')}")
                print(f"  - P10: {subject.get('p10', '未找到')}")
                print(f"  - P50: {subject.get('p50', '未找到')}")
                print(f"  - P90: {subject.get('p90', '未找到')}")
                
                grade_dist = subject.get('grade_distribution', '未找到')
                if isinstance(grade_dist, dict):
                    print(f"  - 等级分布:")
                    for grade, value in grade_dist.items():
                        print(f"      {grade}: {value}")
                else:
                    print(f"  - 等级分布: {grade_dist}")
        else:
            print("未找到学校级数据")

if __name__ == "__main__":
    batch_code = sys.argv[1] if len(sys.argv) > 1 else "G4-2025"
    verify_enhanced_fields(batch_code)