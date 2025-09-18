#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
分析 student_score_detail 表中的学校数据

快速查看各批次的学校分布情况，分析名称冲突和数据质量问题

使用方法：
python scripts/analyze_school_data.py \
  --db "mysql+pymysql://user:pass@host:port/appraisal_test?charset=utf8mb4" \
  --batch G4-2025

或：
export DATABASE_URL="mysql+pymysql://user:pass@host:port/appraisal_test?charset=utf8mb4"
python scripts/analyze_school_data.py --all
"""

import argparse
import sys
import os
from collections import defaultdict
from sqlalchemy import create_engine, text


def parse_args():
    parser = argparse.ArgumentParser(description="分析学校数据")
    parser.add_argument("--db", help="数据库连接字符串")
    parser.add_argument("--batch", help="指定批次代码")
    parser.add_argument("--all", action="store_true", help="分析所有批次")
    return parser.parse_args()


def analyze_batch_schools(engine, batch_code=None):
    """分析学校数据"""
    
    if batch_code:
        sql = text("""
            SELECT batch_code, school_id, school_name, 
                   COUNT(*) as student_count,
                   COUNT(DISTINCT subject_name) as subject_count
            FROM student_score_detail 
            WHERE batch_code = :batch
            GROUP BY batch_code, school_id, school_name
            ORDER BY batch_code, school_id, student_count DESC
        """)
        params = {"batch": batch_code}
    else:
        sql = text("""
            SELECT batch_code, school_id, school_name, 
                   COUNT(*) as student_count,
                   COUNT(DISTINCT subject_name) as subject_count
            FROM student_score_detail 
            GROUP BY batch_code, school_id, school_name
            ORDER BY batch_code, school_id, student_count DESC
        """)
        params = {}
    
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    
    # 按批次组织数据
    batch_data = defaultdict(list)
    for row in rows:
        batch = row[0]
        school_id = row[1]
        school_name = row[2] or f"未知学校{school_id}"
        student_count = row[3]
        subject_count = row[4]
        
        batch_data[batch].append({
            'school_id': school_id,
            'school_name': school_name,
            'student_count': student_count,
            'subject_count': subject_count
        })
    
    return batch_data


def detect_name_conflicts(schools_data):
    """检测学校名称冲突"""
    conflicts = defaultdict(list)
    
    for school in schools_data:
        school_id = school['school_id']
        school_name = school['school_name']
        conflicts[school_id].append(school_name)
    
    # 找出有冲突的学校
    actual_conflicts = {}
    for school_id, names in conflicts.items():
        unique_names = list(set(names))
        if len(unique_names) > 1:
            actual_conflicts[school_id] = unique_names
    
    return actual_conflicts


def generate_school_name_abbreviation(school_name):
    """生成学校名称缩写（简化版）"""
    import re
    
    if not school_name or school_name.startswith("未知学校"):
        return school_name
    
    # 移除常见后缀
    name = school_name
    suffixes = ['实验学校', '小学', '中学', '学校', '校']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
            break
    
    # 如果名称过长，取前10个字符
    if len(name) > 10:
        name = name[:10]
    
    return name if name else school_name[:8]


def print_batch_summary(batch_code, schools_data):
    """打印批次摘要"""
    print(f"\n{'='*60}")
    print(f"批次: {batch_code}")
    print(f"{'='*60}")
    
    total_schools = len(set(s['school_id'] for s in schools_data))
    total_students = sum(s['student_count'] for s in schools_data)
    total_records = len(schools_data)
    
    print(f"唯一学校数: {total_schools}")
    print(f"学生总数: {total_students}")
    print(f"数据记录数: {total_records}")
    
    # 检测名称冲突
    conflicts = detect_name_conflicts(schools_data)
    if conflicts:
        print(f"\n名称冲突学校数: {len(conflicts)}")
        print("冲突详情:")
        for school_id, names in list(conflicts.items())[:5]:  # 只显示前5个
            print(f"  学校ID {school_id}: {names}")
        if len(conflicts) > 5:
            print(f"  ... 还有 {len(conflicts) - 5} 个冲突")
    else:
        print("无名称冲突")
    
    # 显示学校列表（前20个）
    print(f"\n学校列表 (前20个):")
    print(f"{'学校ID':<15} {'原始名称':<25} {'建议缩写':<15} {'学生数':<8} {'科目数'}")
    print("-" * 80)
    
    # 按学校ID去重，选择学生数最多的记录
    unique_schools = {}
    for school in schools_data:
        school_id = school['school_id']
        if school_id not in unique_schools or school['student_count'] > unique_schools[school_id]['student_count']:
            unique_schools[school_id] = school
    
    sorted_schools = sorted(unique_schools.values(), key=lambda x: x['student_count'], reverse=True)
    
    for i, school in enumerate(sorted_schools[:20]):
        school_id = school['school_id']
        original_name = school['school_name']
        abbreviated_name = generate_school_name_abbreviation(original_name)
        student_count = school['student_count']
        subject_count = school['subject_count']
        
        print(f"{school_id:<15} {original_name:<25} {abbreviated_name:<15} {student_count:<8} {subject_count}")
    
    if len(sorted_schools) > 20:
        print(f"... 还有 {len(sorted_schools) - 20} 所学校")


def main():
    args = parse_args()
    
    # 获取数据库连接
    db_url = args.db or os.getenv("DATABASE_URL")
    if not db_url:
        print("错误: 请提供数据库连接字符串")
        sys.exit(1)
    
    if not args.batch and not args.all:
        print("错误: 请指定 --batch <批次代码> 或 --all")
        sys.exit(1)
    
    try:
        engine = create_engine(db_url)
        print("正在连接数据库...")
        
        # 分析数据
        batch_code = args.batch if not args.all else None
        batch_data = analyze_batch_schools(engine, batch_code)
        
        if not batch_data:
            print("未找到任何数据")
            sys.exit(0)
        
        # 显示结果
        print(f"找到 {len(batch_data)} 个批次的数据")
        
        for batch, schools in batch_data.items():
            print_batch_summary(batch, schools)
        
        # 总体统计
        if len(batch_data) > 1:
            print(f"\n{'='*60}")
            print("总体统计")
            print(f"{'='*60}")
            total_batches = len(batch_data)
            total_unique_schools = len(set(
                school['school_id'] 
                for schools in batch_data.values() 
                for school in schools
            ))
            total_students = sum(
                school['student_count'] 
                for schools in batch_data.values() 
                for school in schools
            )
            
            print(f"批次总数: {total_batches}")
            print(f"全部唯一学校数: {total_unique_schools}")
            print(f"学生总数: {total_students}")
        
    except Exception as e:
        print(f"执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()