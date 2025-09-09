#!/usr/bin/env python3
"""
检查远程数据库中实际存在的表
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text, MetaData, inspect
from app.database.connection import engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def show_all_tables():
    """显示数据库中所有表"""
    print("🔍 检查数据库中的所有表...")
    
    try:
        with engine.connect() as connection:
            # 显示所有表
            result = connection.execute(text("SHOW TABLES"))
            all_tables = [row[0] for row in result.fetchall()]
            
            print(f"✓ 数据库连接成功，共发现 {len(all_tables)} 个表:")
            print("=" * 50)
            
            for i, table in enumerate(sorted(all_tables), 1):
                print(f"{i:2d}. {table}")
            
            return all_tables
                
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return []

def show_table_structure(table_name):
    """显示表结构"""
    print(f"\n📋 表 '{table_name}' 的结构:")
    
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f"DESCRIBE {table_name}"))
            columns = result.fetchall()
            
            print("字段名           | 类型           | 空值 | 键   | 默认值")
            print("-" * 60)
            
            for col in columns:
                field = col[0][:15].ljust(15)
                col_type = str(col[1])[:13].ljust(13)
                null_val = col[2][:4].ljust(4)
                key = col[3][:4].ljust(4)
                default = str(col[4] or "")[:10]
                print(f"{field} | {col_type} | {null_val} | {key} | {default}")
                
    except Exception as e:
        print(f"❌ 获取表结构失败: {e}")

def check_table_data(table_name):
    """检查表中数据量"""
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) as count FROM {table_name}"))
            count = result.fetchone()[0]
            print(f"📊 表 '{table_name}' 数据量: {count:,} 条记录")
            
            # 显示前几行数据
            if count > 0:
                result = connection.execute(text(f"SELECT * FROM {table_name} LIMIT 3"))
                rows = result.fetchall()
                columns = result.keys()
                
                print("前3行数据:")
                print(" | ".join([col[:10].ljust(10) for col in columns]))
                print("-" * (len(columns) * 13))
                
                for row in rows:
                    print(" | ".join([str(val)[:10].ljust(10) for val in row]))
            
            return count
                
    except Exception as e:
        print(f"❌ 检查数据失败: {e}")
        return 0

def find_relevant_tables(all_tables):
    """寻找可能相关的学生/统计数据表"""
    print("\n🔍 寻找可能包含学生数据的表...")
    
    # 可能的关键词
    keywords = [
        'student', 'score', 'answer', 'result', 'detail',
        'subject', 'question', 'config', 'mapping',
        'grade', 'aggregation', 'statistics', 'batch'
    ]
    
    relevant_tables = []
    
    for table in all_tables:
        table_lower = table.lower()
        for keyword in keywords:
            if keyword in table_lower:
                relevant_tables.append(table)
                break
    
    if relevant_tables:
        print("✓ 发现可能相关的表:")
        for table in relevant_tables:
            print(f"  - {table}")
    else:
        print("! 没有发现明显相关的表名")
        print("! 让我们检查所有表的结构...")
    
    return relevant_tables

def main():
    print("=" * 60)
    print("🏥 远程数据库表结构分析")
    print("=" * 60)
    
    # 1. 显示所有表
    all_tables = show_all_tables()
    
    if not all_tables:
        print("❌ 无法获取数据库表列表")
        return False
    
    # 2. 寻找相关表
    relevant_tables = find_relevant_tables(all_tables)
    
    # 3. 检查相关表的结构和数据
    tables_to_check = relevant_tables if relevant_tables else all_tables
    
    for table in tables_to_check:
        print(f"\n{'='*60}")
        print(f"🔍 分析表: {table}")
        print('='*60)
        
        # 显示表结构
        show_table_structure(table)
        
        # 检查数据量
        data_count = check_table_data(table)
        
        # 如果数据量很大，询问是否继续
        if data_count > 1000:
            print(f"! 表 {table} 有大量数据 ({data_count:,} 条)")
    
    # 4. 总结和建议
    print(f"\n{'='*60}")
    print("📋 分析总结")
    print('='*60)
    
    print("基于以上分析，请确认:")
    print("1. 哪些表包含学生答题数据？")
    print("2. 哪些表包含题目配置信息？") 
    print("3. 表名是否与代码中假设的不同？")
    print("4. 数据结构是否需要适配？")
    
    print(f"\n如果发现了正确的表，我们可以:")
    print("1. 修改验证脚本使用正确的表名")
    print("2. 调整数据访问层的映射关系")
    print("3. 创建适配器来处理不同的数据结构")
    
    return True

if __name__ == "__main__":
    main()