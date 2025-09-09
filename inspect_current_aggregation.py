#!/usr/bin/env python3
"""
检查当前汇聚数据质量
"""
import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database.connection import get_db
from sqlalchemy import text

def inspect_aggregation_data():
    """检查汇聚数据质量"""
    print("="*60)
    print("检查当前汇聚数据质量")
    print("="*60)
    
    db = next(get_db())
    
    # 1. 检查total_students和total_schools
    print("\n1. 检查total_students和total_schools字段:")
    query = text("""
        SELECT batch_code, aggregation_level, 
               total_students, total_schools,
               school_id
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025'
        LIMIT 5
    """)
    results = db.execute(query).fetchall()
    for row in results:
        print(f"  {row[0]} {row[1]}: total_students={row[2]}, total_schools={row[3]}, school_id={row[4]}")
    
    # 2. 检查一条区域汇聚数据的内容
    print("\n2. 检查G7-2025区域汇聚数据内容:")
    query = text("""
        SELECT statistics_data
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025' 
        AND aggregation_level = 'REGIONAL'
        LIMIT 1
    """)
    result = db.execute(query).fetchone()
    
    if result:
        data = result[0] if isinstance(result[0], dict) else json.loads(result[0])
        
        # 检查第一个科目
        if 'subjects' in data and data['subjects']:
            subject = data['subjects'][0]
            print(f"\n  第一个科目: {subject.get('subject_name')}")
            print(f"  科目类型: {subject.get('type')}")
            
            # 检查metrics
            metrics = subject.get('metrics', {})
            print(f"\n  Metrics内容:")
            for key, value in metrics.items():
                print(f"    {key}: {value} (类型: {type(value).__name__})")
            
            # 检查school_rankings
            rankings = subject.get('school_rankings', [])
            print(f"\n  学校排名数量: {len(rankings)}")
            if rankings:
                print(f"  第一个学校排名:")
                for key, value in rankings[0].items():
                    print(f"    {key}: {value}")
            
            # 检查dimensions
            dimensions = subject.get('dimensions', [])
            print(f"\n  维度数量: {len(dimensions)}")
            if dimensions:
                print(f"  第一个维度:")
                for key, value in dimensions[0].items():
                    print(f"    {key}: {value}")
    
    # 3. 检查一条学校汇聚数据
    print("\n3. 检查G7-2025学校汇聚数据内容:")
    query = text("""
        SELECT statistics_data, school_id
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025' 
        AND aggregation_level = 'SCHOOL'
        LIMIT 1
    """)
    result = db.execute(query).fetchone()
    
    if result:
        data = result[0] if isinstance(result[0], dict) else json.loads(result[0])
        school_id = result[1]
        print(f"  学校: {school_id}")
        
        if 'subjects' in data and data['subjects']:
            subject = data['subjects'][0]
            print(f"  第一个科目: {subject.get('subject_name')}")
            
            # 检查是否有region_rank和total_schools
            print(f"  region_rank: {subject.get('region_rank', '缺失')}")
            print(f"  total_schools: {subject.get('total_schools', '缺失')}")
            
            # 检查metrics
            metrics = subject.get('metrics', {})
            print(f"\n  Metrics内容:")
            missing_fields = []
            required_fields = ['avg', 'difficulty', 'stddev', 'discrimination', 'max', 'min', 'p10', 'p50', 'p90']
            for field in required_fields:
                if field in metrics:
                    print(f"    {field}: {metrics[field]}")
                else:
                    missing_fields.append(field)
            
            if missing_fields:
                print(f"\n  缺失的必要字段: {', '.join(missing_fields)}")
    
    # 4. 检查数据精度问题
    print("\n4. 检查数据精度问题:")
    query = text("""
        SELECT statistics_data
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025' 
        AND aggregation_level = 'REGIONAL'
        LIMIT 1
    """)
    result = db.execute(query).fetchone()
    
    if result:
        data_str = str(result[0]) if isinstance(result[0], dict) else result[0]
        # 查找超过2位小数的数字
        import re
        float_pattern = r'\d+\.\d{3,}'
        matches = re.findall(float_pattern, data_str[:1000])  # 只检查前1000字符
        if matches:
            print(f"  发现超过2位小数的数值: {matches[:5]}")
        else:
            print(f"  未发现超过2位小数的数值")
    
    # 5. 检查问卷数据
    print("\n5. 检查问卷数据:")
    query = text("""
        SELECT COUNT(*) 
        FROM questionnaire_option_distribution
        WHERE batch_code = 'G7-2025'
    """)
    count = db.execute(query).scalar()
    print(f"  问卷选项分布记录数: {count}")
    
    db.close()
    
    print("\n" + "="*60)
    print("检查完成")

if __name__ == "__main__":
    inspect_aggregation_data()