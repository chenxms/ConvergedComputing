#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查数据库表结构
确定正确的字段名称用于一致性验证
"""

import os
import mysql.connector
from mysql.connector import Error

def check_table_structures():
    """检查相关表的结构"""

    # 数据库配置
    db_config = {
        'host': os.getenv('DATABASE_HOST', '117.72.14.166'),
        'port': int(os.getenv('DATABASE_PORT', '23506')),
        'user': os.getenv('DATABASE_USER', 'root'),
        'password': os.getenv('DATABASE_PASSWORD', 'mysql_Lujing2022'),
        'database': os.getenv('DATABASE_NAME', 'appraisal_test'),
        'charset': 'utf8mb4',
        'autocommit': True
    }

    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # 检查的表列表
        tables_to_check = [
            'subject_core_metrics',
            'subject_school_rankings',
            'statistical_aggregations'
        ]

        print("数据库表结构检查")
        print("=" * 80)

        for table_name in tables_to_check:
            print(f"\n表名: {table_name}")
            print("-" * 40)

            # 检查表是否存在
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            if not cursor.fetchone():
                print(f"  ❌ 表 {table_name} 不存在")
                continue

            # 获取表结构
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()

            print("  字段列表:")
            for col in columns:
                field_name, field_type, null, key, default, extra = col
                key_info = f" ({key})" if key else ""
                null_info = " NULL" if null == "YES" else " NOT NULL"
                print(f"    {field_name}: {field_type}{null_info}{key_info}")

            # 获取记录数
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            record_count = cursor.fetchone()[0]
            print(f"  记录数: {record_count}")

            # 如果是statistical_aggregations，检查G7-2025的记录
            if table_name == 'statistical_aggregations':
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE batch_id = 'G7-2025'")
                g7_count = cursor.fetchone()[0]
                print(f"  G7-2025记录数: {g7_count}")

                if g7_count > 0:
                    # 检查aggregation_level的分布
                    cursor.execute(f"""
                        SELECT aggregation_level, COUNT(*)
                        FROM {table_name}
                        WHERE batch_id = 'G7-2025'
                        GROUP BY aggregation_level
                    """)
                    level_dist = cursor.fetchall()
                    print("  按聚合级别分布:")
                    for level, count in level_dist:
                        print(f"    {level}: {count} 条")

        # 检查是否有其他可能的批次相关表
        print(f"\n搜索包含批次信息的表")
        print("-" * 40)

        # 搜索所有表
        cursor.execute("SHOW TABLES")
        all_tables = [table[0] for table in cursor.fetchall()]

        batch_related_tables = []
        for table in all_tables:
            # 检查表中是否有batch相关字段
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()

            batch_fields = []
            for col in columns:
                field_name = col[0].lower()
                if 'batch' in field_name or 'g7' in field_name:
                    batch_fields.append(col[0])

            if batch_fields:
                batch_related_tables.append((table, batch_fields))

        if batch_related_tables:
            print("  发现的批次相关表:")
            for table, fields in batch_related_tables:
                print(f"    {table}: {', '.join(fields)}")
        else:
            print("  未发现批次相关字段")

        cursor.close()
        connection.close()
        print(f"\n检查完成!")

    except Error as e:
        print(f"数据库错误: {e}")
    except Exception as e:
        print(f"其他错误: {e}")

if __name__ == "__main__":
    check_table_structures()