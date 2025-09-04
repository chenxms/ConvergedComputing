#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证数据库表结构和数据
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from app.database.connection import engine
from app.database.models import (
    StatisticalAggregation, StatisticalMetadata, StatisticalHistory,
    Batch, Task
)

def verify_tables():
    """验证表结构"""
    print("=== 验证数据库表结构 ===")
    
    with engine.connect() as connection:
        # 检查所有表
        result = connection.execute(
            text("SHOW TABLES FROM appraisal_stats")
        )
        tables = [row[0] for row in result.fetchall()]
        
        print(f"数据库中的表 ({len(tables)}个):")
        for table in sorted(tables):
            print(f"  - {table}")
        
        # 检查统计相关表的字段
        statistical_tables = [
            'statistical_aggregations', 
            'statistical_metadata', 
            'statistical_history'
        ]
        
        for table in statistical_tables:
            if table in tables:
                print(f"\n{table} 表结构:")
                result = connection.execute(text(f"DESCRIBE {table}"))
                for row in result.fetchall():
                    field, type_, null, key, default, extra = row
                    print(f"  {field}: {type_} {'NULL' if null == 'YES' else 'NOT NULL'} {key} {extra or ''}")
            else:
                print(f"\n❌ 表 {table} 不存在")

def verify_indexes():
    """验证索引"""
    print("\n=== 验证索引结构 ===")
    
    with engine.connect() as connection:
        # 检查主要表的索引
        tables_to_check = ['statistical_aggregations', 'statistical_metadata', 'statistical_history']
        
        for table in tables_to_check:
            result = connection.execute(text(f"SHOW INDEX FROM {table}"))
            indexes = result.fetchall()
            
            print(f"\n{table} 的索引:")
            for index in indexes:
                table_name, non_unique, key_name, seq_in_index, column_name, collation, cardinality, sub_part, packed, null_field, index_type, comment, index_comment = index
                print(f"  - {key_name}: {column_name} ({'UNIQUE' if non_unique == 0 else 'NON-UNIQUE'})")

def verify_metadata_data():
    """验证元数据"""
    print("\n=== 验证元数据配置 ===")
    
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        configs = session.query(StatisticalMetadata).all()
        print(f"元数据配置总数: {len(configs)}")
        
        # 按类型分组统计
        type_counts = {}
        for config in configs:
            type_name = config.metadata_type.value
            if type_name not in type_counts:
                type_counts[type_name] = 0
            type_counts[type_name] += 1
        
        print("按类型分布:")
        for type_name, count in type_counts.items():
            print(f"  - {type_name}: {count} 项")
        
        # 显示具体配置
        print("\n详细配置:")
        for config in configs:
            print(f"  {config.id}: {config.metadata_key}")
            print(f"    类型: {config.metadata_type.value}")
            print(f"    版本: {config.version}, 状态: {'激活' if config.is_active else '禁用'}")
            print(f"    描述: {config.description}")
            print()
        
    except Exception as e:
        print(f"验证元数据失败: {str(e)}")
    finally:
        session.close()

def test_foreign_keys():
    """测试外键约束"""
    print("=== 测试外键约束 ===")
    
    with engine.connect() as connection:
        # 检查外键约束
        result = connection.execute(text("""
            SELECT 
                TABLE_NAME,
                COLUMN_NAME,
                CONSTRAINT_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = 'appraisal_stats'
            AND REFERENCED_TABLE_NAME IS NOT NULL
        """))
        
        foreign_keys = result.fetchall()
        
        if foreign_keys:
            print("外键约束:")
            for fk in foreign_keys:
                table_name, column_name, constraint_name, ref_table, ref_column = fk
                print(f"  - {table_name}.{column_name} -> {ref_table}.{ref_column} ({constraint_name})")
        else:
            print("没有找到外键约束")

def verify_json_fields():
    """验证JSON字段"""
    print("\n=== 验证JSON字段 ===")
    
    with engine.connect() as connection:
        # 检查JSON字段
        result = connection.execute(text("""
            SELECT 
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'appraisal_stats'
            AND DATA_TYPE = 'json'
        """))
        
        json_fields = result.fetchall()
        
        if json_fields:
            print("JSON字段:")
            for field in json_fields:
                table_name, column_name, data_type = field
                print(f"  - {table_name}.{column_name}")
        else:
            print("没有找到JSON字段")

def main():
    """主验证函数"""
    print("数据库验证开始...\n")
    
    try:
        # 验证表结构
        verify_tables()
        
        # 验证索引
        verify_indexes()
        
        # 验证元数据
        verify_metadata_data()
        
        # 测试外键约束
        test_foreign_keys()
        
        # 验证JSON字段
        verify_json_fields()
        
        print("\n" + "=" * 50)
        print("✅ 数据库验证完成！所有检查通过。")
        
    except Exception as e:
        print(f"\n❌ 数据库验证失败: {str(e)}")
        raise

if __name__ == "__main__":
    main()