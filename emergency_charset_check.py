#!/usr/bin/env python3
"""
紧急字符集检查脚本
检查问卷相关表的字符集和排序规则状态
"""
import sys
import os
sys.path.append('/app')

from sqlalchemy import text
from app.database.connection import get_db_context

def check_table_charset():
    """检查关键表的字符集和排序规则"""

    tables_to_check = [
        'school_master_data',
        'student_cleaned_scores',
        'questionnaire_option_distribution',
        'questionnaire_question_scores',
        'question_dimension_mapping',
        'questionnaire_scale_options'
    ]

    print("=== 🔍 数据库字符集和排序规则检查 ===\n")

    with get_db_context() as db:
        # 获取数据库名
        result = db.execute(text("SELECT DATABASE()"))
        database_name = result.fetchone()[0]
        print(f"📊 当前数据库: {database_name}\n")

        for table in tables_to_check:
            print(f"--- 📋 {table} ---")
            try:
                # 检查表是否存在
                check_table_sql = text("""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = :db_name
                    AND TABLE_NAME = :table_name
                """)

                result = db.execute(check_table_sql, {
                    'db_name': database_name,
                    'table_name': table
                })

                table_exists = result.fetchone()[0] > 0

                if not table_exists:
                    print(f"❌ 表不存在\n")
                    continue

                # 检查字符串列的字符集和排序规则
                charset_sql = text("""
                    SELECT
                        COLUMN_NAME,
                        DATA_TYPE,
                        CHARACTER_SET_NAME,
                        COLLATION_NAME,
                        IS_NULLABLE,
                        COLUMN_DEFAULT
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = :db_name
                    AND TABLE_NAME = :table_name
                    AND CHARACTER_SET_NAME IS NOT NULL
                    ORDER BY ORDINAL_POSITION
                """)

                result = db.execute(charset_sql, {
                    'db_name': database_name,
                    'table_name': table
                })

                columns = result.fetchall()

                if columns:
                    print("📝 字符列信息:")
                    collations_found = set()
                    for col in columns:
                        col_name, data_type, charset, collation, nullable, default = col
                        collations_found.add(collation)
                        print(f"  • {col_name} ({data_type}): {charset} / {collation}")

                    # 检查是否有冲突的排序规则
                    if len(collations_found) > 1:
                        print(f"⚠️  发现多种排序规则: {collations_found}")
                    elif 'utf8mb4_unicode_ci' in collations_found:
                        print(f"🔴 需要转换: {collations_found}")
                    elif 'utf8mb4_0900_ai_ci' in collations_found:
                        print(f"✅ 已符合标准: {collations_found}")

                else:
                    print("ℹ️  无字符串列")

                print()

            except Exception as e:
                print(f"❌ 检查失败: {e}\n")

def check_problematic_join():
    """测试问题JOIN查询"""
    print("=== 🧪 测试问题JOIN查询 ===\n")

    with get_db_context() as db:
        try:
            # 测试引起字符集冲突的JOIN
            test_sql = text("""
                SELECT COUNT(*)
                FROM student_cleaned_scores scs
                JOIN school_master_data smd ON scs.school_code = smd.school_code
                WHERE scs.batch_code = 'G7-2025'
                AND scs.subject_type = 'questionnaire'
                LIMIT 1
            """)

            result = db.execute(test_sql)
            count = result.fetchone()[0]
            print(f"✅ JOIN查询成功，匹配记录数: {count}")

        except Exception as e:
            print(f"🔴 JOIN查询失败: {e}")
            if "Illegal mix of collations" in str(e):
                print("📝 确认字符集冲突问题存在")

def check_null_labels():
    """检查问卷标签NULL问题"""
    print("\n=== 🏷️  检查问卷标签NULL问题 ===\n")

    with get_db_context() as db:
        try:
            # 检查questionnaire_option_distribution中的NULL标签
            null_check_sql = text("""
                SELECT
                    batch_code,
                    subject_name,
                    COUNT(*) as null_count
                FROM questionnaire_option_distribution
                WHERE option_label IS NULL
                GROUP BY batch_code, subject_name
                ORDER BY batch_code, subject_name
            """)

            result = db.execute(null_check_sql)
            null_records = result.fetchall()

            if null_records:
                print("🔴 发现NULL标签记录:")
                total_null = 0
                for batch, subject, count in null_records:
                    print(f"  • {batch}/{subject}: {count}条")
                    total_null += count
                print(f"📊 总计NULL标签: {total_null}条")
            else:
                print("✅ 未发现NULL标签")

        except Exception as e:
            print(f"❌ 检查NULL标签失败: {e}")

def check_existing_indexes():
    """检查现有索引"""
    print("\n=== 📊 检查现有索引 ===\n")

    with get_db_context() as db:
        try:
            # 获取数据库名
            result = db.execute(text("SELECT DATABASE()"))
            database_name = result.fetchone()[0]

            index_tables = [
                'questionnaire_option_distribution',
                'questionnaire_question_scores',
                'student_cleaned_scores'
            ]

            for table in index_tables:
                print(f"--- 📋 {table} 索引 ---")

                index_sql = text("""
                    SELECT
                        INDEX_NAME,
                        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns,
                        NON_UNIQUE,
                        INDEX_TYPE
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = :db_name
                    AND TABLE_NAME = :table_name
                    GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE
                    ORDER BY INDEX_NAME
                """)

                result = db.execute(index_sql, {
                    'db_name': database_name,
                    'table_name': table
                })

                indexes = result.fetchall()

                if indexes:
                    for idx_name, columns, non_unique, idx_type in indexes:
                        unique_str = "UNIQUE" if not non_unique else ""
                        print(f"  • {idx_name} ({idx_type} {unique_str}): {columns}")
                else:
                    print("  ℹ️  无索引信息")
                print()

        except Exception as e:
            print(f"❌ 检查索引失败: {e}")

if __name__ == "__main__":
    try:
        check_table_charset()
        check_problematic_join()
        check_null_labels()
        check_existing_indexes()
        print("🎯 检查完成！")

    except Exception as e:
        print(f"❌ 脚本执行失败: {e}")
        sys.exit(1)