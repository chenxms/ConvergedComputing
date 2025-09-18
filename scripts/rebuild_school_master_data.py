#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
重建 school_master_data 表脚本

基于 student_score_detail 表中的 school_id 和学校名称数据，
重建 school_master_data 表，确保：
1. 一个批次下学校ID不重复
2. 整个表的学校ID可以重复（不同批次）
3. 采用简短的缩写学校名称
4. 所有学校状态设为 ACTIVE

使用方法：
python scripts/rebuild_school_master_data.py \
  --db "mysql+pymysql://user:pass@host:port/appraisal_test?charset=utf8mb4" \
  --batch G4-2025 \
  --dry-run  # 可选，仅显示结果不执行

或使用环境变量：
export DATABASE_URL="mysql+pymysql://user:pass@host:port/appraisal_test?charset=utf8mb4"
python scripts/rebuild_school_master_data.py --batch G4-2025
"""

import argparse
import sys
import os
import re
from typing import Dict, List, Tuple, Set
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def parse_args():
    parser = argparse.ArgumentParser(description="重建 school_master_data 表")
    parser.add_argument("--db", help="数据库连接字符串（缺省从 DATABASE_URL 环境变量读取）")
    parser.add_argument("--batch", help="指定批次代码（如指定则只处理该批次）")
    parser.add_argument("--all-batches", action="store_true", help="处理所有批次")
    parser.add_argument("--dry-run", action="store_true", help="仅显示结果，不执行实际操作")
    parser.add_argument("--force", action="store_true", help="强制执行，清空现有 school_master_data")
    return parser.parse_args()


def get_school_name_abbreviation(school_name: str) -> str:
    """
    生成学校名称的简短缩写
    规则：
    1. 移除常见后缀（学校、小学、中学、实验学校等）
    2. 保留关键词汇
    3. 限制长度在15个字符以内
    """
    if not school_name:
        return "未知学校"
    
    # 清理无意义的字符
    name = school_name.strip()
    name = re.sub(r'[（）\(\)\[\]【】]', '', name)
    
    # 移除常见后缀
    suffixes = [
        '实验学校', '实验小学', '实验中学', '实验校',
        '小学校', '中学校', '学校',
        '小学', '中学', '高中', '初中',
        '第一小学', '第二小学', '第三小学', '第四小学', '第五小学',
        '第一中学', '第二中学', '第三中学', '第四中学', '第五中学',
        '一小', '二小', '三小', '四小', '五小', '六小', '七小', '八小', '九小',
        '一中', '二中', '三中', '四中', '五中', '六中', '七中', '八中', '九中',
    ]
    
    for suffix in sorted(suffixes, key=len, reverse=True):
        if name.endswith(suffix):
            name = name[:-len(suffix)]
            break
    
    # 移除地区前缀（保留最后两级）
    parts = re.split(r'[市县区镇乡街道]', name)
    if len(parts) > 2:
        name = ''.join(parts[-2:])
    
    # 如果名称过长，进行进一步缩减
    if len(name) > 15:
        # 保留前8个字符 + "..."
        name = name[:8] + "..."
    
    # 如果名称为空，使用原始名称的前10个字符
    if not name or len(name.strip()) == 0:
        name = school_name[:10]
    
    return name.strip()


def analyze_school_data(engine, batch_code: str = None) -> Dict[str, List[Tuple[str, str, int]]]:
    """
    分析 student_score_detail 表中的学校数据
    返回: {batch_code: [(school_id, school_name, student_count), ...]}
    """
    print(f"正在分析学校数据...")
    
    with engine.connect() as conn:
        if batch_code:
            # 分析指定批次
            sql = text("""
                SELECT batch_code, school_id, school_name, COUNT(*) as student_count
                FROM student_score_detail 
                WHERE batch_code = :batch
                GROUP BY batch_code, school_id, school_name
                ORDER BY batch_code, school_id, student_count DESC
            """)
            rows = conn.execute(sql, {"batch": batch_code}).fetchall()
        else:
            # 分析所有批次
            sql = text("""
                SELECT batch_code, school_id, school_name, COUNT(*) as student_count
                FROM student_score_detail 
                GROUP BY batch_code, school_id, school_name
                ORDER BY batch_code, school_id, student_count DESC
            """)
            rows = conn.execute(sql).fetchall()
    
    # 按批次组织数据
    batch_data = {}
    for row in rows:
        batch = row[0]
        school_id = row[1]
        school_name = row[2] or f"学校{school_id}"
        student_count = row[3]
        
        if batch not in batch_data:
            batch_data[batch] = []
        batch_data[batch].append((school_id, school_name, student_count))
    
    return batch_data


def resolve_school_names(school_records: List[Tuple[str, str, int]]) -> Dict[str, str]:
    """
    解决同一学校ID的名称冲突问题
    选择出现频次最高的名称，如果频次相同则选择最短的
    """
    school_name_map = {}
    
    # 按 school_id 分组
    school_groups = {}
    for school_id, school_name, student_count in school_records:
        if school_id not in school_groups:
            school_groups[school_id] = []
        school_groups[school_id].append((school_name, student_count))
    
    # 为每个学校选择最佳名称
    for school_id, name_records in school_groups.items():
        if len(name_records) == 1:
            # 只有一个名称
            school_name_map[school_id] = name_records[0][0]
        else:
            # 多个名称，选择学生数最多的，如果相同则选择最短的
            name_records.sort(key=lambda x: (-x[1], len(x[0])))
            school_name_map[school_id] = name_records[0][0]
    
    return school_name_map


def generate_school_master_records(batch_data: Dict[str, List[Tuple[str, str, int]]]) -> List[Dict]:
    """
    生成 school_master_data 表的记录
    """
    records = []
    
    for batch_code, school_records in batch_data.items():
        print(f"\n处理批次: {batch_code}")
        print(f"发现 {len(school_records)} 条学校记录")
        
        # 解决同一批次内学校名称冲突
        school_name_map = resolve_school_names(school_records)
        
        # 获取唯一学校列表
        unique_schools = {}
        for school_id, school_name, student_count in school_records:
            if school_id not in unique_schools:
                unique_schools[school_id] = {
                    'school_name': school_name_map[school_id],
                    'total_students': 0
                }
            unique_schools[school_id]['total_students'] += student_count
        
        print(f"去重后: {len(unique_schools)} 所学校")
        
        # 生成记录
        for school_id, school_info in unique_schools.items():
            # 跳过空的学校ID
            if not school_id or school_id.strip() == '':
                print(f"  跳过空学校ID: {school_info['school_name']}")
                continue
                
            original_name = school_info['school_name']
            abbreviated_name = get_school_name_abbreviation(original_name)
            
            record = {
                'batch_code': batch_code,
                'school_id': school_id,
                'standard_school_name': abbreviated_name,
                'original_school_name': original_name,
                'status': 'ACTIVE',
                'total_students': school_info['total_students']
            }
            records.append(record)
            
            print(f"  {school_id}: {original_name} -> {abbreviated_name} ({school_info['total_students']} 学生)")
    
    return records


def backup_existing_data(engine) -> bool:
    """备份现有的 school_master_data 表"""
    try:
        with engine.connect() as conn:
            # 检查是否存在数据
            check_sql = text("SELECT COUNT(*) FROM school_master_data")
            result = conn.execute(check_sql).fetchone()
            record_count = result[0] if result else 0
            
            if record_count > 0:
                print(f"现有 school_master_data 表中有 {record_count} 条记录")
                
                # 创建备份表
                backup_table = "school_master_data_backup_" + str(int(__import__('time').time()))
                backup_sql = text(f"""
                    CREATE TABLE {backup_table} AS 
                    SELECT * FROM school_master_data
                """)
                conn.execute(backup_sql)
                conn.commit()
                print(f"已备份到表: {backup_table}")
                return True
            else:
                print("school_master_data 表为空，无需备份")
                return True
                
    except Exception as e:
        print(f"备份失败: {e}")
        return False


def rebuild_school_master_data(engine, records: List[Dict], dry_run: bool = False):
    """重建 school_master_data 表"""
    if dry_run:
        print(f"\n=== DRY RUN: 将要插入 {len(records)} 条记录 ===")
        for record in records[:10]:  # 只显示前10条
            print(f"批次: {record['batch_code']}, 学校: {record['school_id']}, "
                  f"名称: {record['standard_school_name']}, 学生数: {record['total_students']}")
        if len(records) > 10:
            print(f"... 还有 {len(records) - 10} 条记录")
        return True
    
    try:
        with engine.connect() as conn:
            # 清空现有数据
            print("清空现有 school_master_data 表...")
            conn.execute(text("DELETE FROM school_master_data"))
            
            # 插入新数据
            print(f"插入 {len(records)} 条新记录...")
            insert_sql = text("""
                INSERT INTO school_master_data 
                (batch_code, school_id, standard_school_name, status)
                VALUES (:batch_code, :school_id, :standard_school_name, :status)
            """)
            
            for record in records:
                conn.execute(insert_sql, {
                    'batch_code': record['batch_code'],
                    'school_id': record['school_id'],
                    'standard_school_name': record['standard_school_name'],
                    'status': record['status']
                })
            
            conn.commit()
            print("数据插入完成！")
            return True
            
    except Exception as e:
        print(f"重建失败: {e}")
        return False


def main():
    args = parse_args()
    
    # 获取数据库连接
    db_url = args.db or os.getenv("DATABASE_URL")
    if not db_url:
        print("错误: 请提供数据库连接字符串 (--db 参数或 DATABASE_URL 环境变量)")
        sys.exit(1)
    
    if not args.batch and not args.all_batches:
        print("错误: 请指定 --batch <批次代码> 或 --all-batches")
        sys.exit(1)
    
    try:
        engine = create_engine(db_url)
        print(f"已连接到数据库")
        
        # 分析学校数据
        batch_code = args.batch if not args.all_batches else None
        batch_data = analyze_school_data(engine, batch_code)
        
        if not batch_data:
            print("未找到任何学校数据")
            sys.exit(0)
        
        # 生成新的记录
        records = generate_school_master_records(batch_data)
        
        if not records:
            print("未生成任何记录")
            sys.exit(0)
        
        print(f"\n=== 总结 ===")
        print(f"处理批次数: {len(batch_data)}")
        print(f"生成记录数: {len(records)}")
        
        # 显示各批次统计
        for batch, data in batch_data.items():
            unique_schools = len(set(record[0] for record in data))
            print(f"  {batch}: {unique_schools} 所学校")
        
        if args.dry_run:
            rebuild_school_master_data(engine, records, dry_run=True)
            print("\n=== DRY RUN 完成，未执行实际操作 ===")
        else:
            # 确认操作
            if not args.force:
                confirm = input(f"\n确认要重建 school_master_data 表吗？(y/N): ")
                if confirm.lower() != 'y':
                    print("操作已取消")
                    sys.exit(0)
            
            # 备份现有数据
            if not backup_existing_data(engine):
                print("备份失败，操作中止")
                sys.exit(1)
            
            # 执行重建
            if rebuild_school_master_data(engine, records):
                print("\n=== school_master_data 表重建完成 ===")
            else:
                print("\n=== 重建失败 ===")
                sys.exit(1)
    
    except Exception as e:
        print(f"执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()