#!/usr/bin/env python3
"""
检查核心表的字符集和排序规则状态
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def check_table_charset_collation():
    """检查表的字符集和排序规则"""
    
    # 需要检查的核心表
    tables_to_check = [
        'student_cleaned_scores',
        'student_score_detail', 
        'subject_question_config',
        'question_dimension_mapping',
        'grade_aggregation_main'
    ]
    
    # 需要重点关注的JOIN相关列
    key_columns = {
        'student_cleaned_scores': ['batch_code', 'subject_id', 'subject_name', 'student_id'],
        'student_score_detail': ['batch_code', 'subject_name', 'subject_id', 'student_id'],
        'subject_question_config': ['batch_code', 'subject_name', 'subject_id'],
        'question_dimension_mapping': ['batch_code', 'subject_name'],
        'grade_aggregation_main': ['batch_code']
    }
    
    try:
        with get_db_context() as session:
            print("=== 数据库表字符集和排序规则检查 ===")
            print(f"检查时间: {datetime.now()}\n")
            
            # 1. 检查数据库默认字符集
            print("1. 数据库默认设置:")
            result = session.execute(text("""
                SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME
                FROM information_schema.SCHEMATA 
                WHERE SCHEMA_NAME = DATABASE()
            """))
            db_info = result.fetchone()
            print(f"   数据库默认字符集: {db_info[0]}")
            print(f"   数据库默认排序规则: {db_info[1]}\n")
            
            # 2. 检查表级别的字符集和排序规则
            print("2. 表级别字符集和排序规则:")
            for table in tables_to_check:
                result = session.execute(text(f"""
                    SELECT TABLE_COLLATION, TABLE_COMMENT
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{table}'
                """))
                table_info = result.fetchone()
                
                if table_info:
                    print(f"   {table}:")
                    print(f"     表排序规则: {table_info[0]}")
                    if table_info[1]:
                        print(f"     注释: {table_info[1]}")
                else:
                    print(f"   {table}: [表不存在]")
            
            print()
            
            # 3. 检查关键列的字符集和排序规则
            print("3. 关键JOIN列的字符集和排序规则:")
            for table, columns in key_columns.items():
                print(f"\n   表: {table}")
                
                # 检查表是否存在
                table_exists = session.execute(text(f"""
                    SELECT COUNT(*) FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}'
                """)).fetchone()[0]
                
                if not table_exists:
                    print(f"     [表不存在]")
                    continue
                
                for column in columns:
                    result = session.execute(text(f"""
                        SELECT 
                            COLUMN_TYPE,
                            CHARACTER_SET_NAME,
                            COLLATION_NAME,
                            IS_NULLABLE,
                            COLUMN_DEFAULT,
                            COLUMN_KEY
                        FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() 
                        AND TABLE_NAME = '{table}' 
                        AND COLUMN_NAME = '{column}'
                    """))
                    col_info = result.fetchone()
                    
                    if col_info:
                        print(f"     {column}:")
                        print(f"       类型: {col_info[0]}")
                        print(f"       字符集: {col_info[1] or 'N/A'}")
                        print(f"       排序规则: {col_info[2] or 'N/A'}")
                        print(f"       索引: {col_info[5] or 'None'}")
                    else:
                        print(f"     {column}: [列不存在]")
            
            # 4. 检查现有索引
            print(f"\n4. 现有索引情况:")
            for table in tables_to_check:
                result = session.execute(text(f"""
                    SELECT 
                        INDEX_NAME,
                        COLUMN_NAME,
                        SEQ_IN_INDEX,
                        NON_UNIQUE,
                        INDEX_TYPE
                    FROM information_schema.STATISTICS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{table}'
                    ORDER BY INDEX_NAME, SEQ_IN_INDEX
                """))
                indexes = result.fetchall()
                
                if indexes:
                    print(f"\n   {table}:")
                    current_index = None
                    for idx in indexes:
                        if idx[0] != current_index:
                            if current_index is not None:
                                print()
                            current_index = idx[0]
                            unique_str = "UNIQUE" if idx[3] == 0 else "NON-UNIQUE"
                            print(f"     {idx[0]} ({unique_str}, {idx[4]}): ", end="")
                        print(f"{idx[1]}({idx[2]})", end=" ")
                    print()
                else:
                    print(f"\n   {table}: [无索引信息]")
            
            # 5. 识别潜在问题
            print(f"\n5. 潜在问题分析:")
            issues = []
            
            # 检查是否所有文本列都使用相同的排序规则
            collations_found = set()
            for table, columns in key_columns.items():
                table_exists = session.execute(text(f"""
                    SELECT COUNT(*) FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}'
                """)).fetchone()[0]
                
                if table_exists:
                    for column in columns:
                        result = session.execute(text(f"""
                            SELECT COLLATION_NAME
                            FROM information_schema.COLUMNS 
                            WHERE TABLE_SCHEMA = DATABASE() 
                            AND TABLE_NAME = '{table}' 
                            AND COLUMN_NAME = '{column}'
                            AND DATA_TYPE IN ('varchar', 'char', 'text')
                        """))
                        col_info = result.fetchone()
                        if col_info and col_info[0]:
                            collations_found.add(col_info[0])
            
            if len(collations_found) > 1:
                issues.append(f"发现多种排序规则: {', '.join(collations_found)}")
            
            # 检查建议的索引是否存在
            recommended_indexes = {
                'student_cleaned_scores': [
                    ['batch_code', 'subject_id', 'student_id'],
                    ['batch_code', 'subject_name']
                ],
                'student_score_detail': [
                    ['batch_code', 'subject_name', 'subject_id']
                ]
            }
            
            for table, recommended in recommended_indexes.items():
                for idx_cols in recommended:
                    # 检查是否存在这个组合的索引
                    idx_name_pattern = '_'.join(idx_cols)
                    result = session.execute(text(f"""
                        SELECT COUNT(DISTINCT INDEX_NAME)
                        FROM information_schema.STATISTICS 
                        WHERE TABLE_SCHEMA = DATABASE() 
                        AND TABLE_NAME = '{table}'
                        AND COLUMN_NAME IN ({','.join([f"'{col}'" for col in idx_cols])})
                        GROUP BY INDEX_NAME
                        HAVING COUNT(*) = {len(idx_cols)}
                    """))
                    
                    existing_count = result.fetchone()
                    if not existing_count or existing_count[0] == 0:
                        issues.append(f"缺少建议的复合索引: {table}({', '.join(idx_cols)})")
            
            if issues:
                print("   发现以下问题:")
                for issue in issues:
                    print(f"   - {issue}")
            else:
                print("   未发现明显问题")
            
            print(f"\n6. 优化建议:")
            print(f"   1. 统一所有JOIN相关文本列的排序规则为 utf8mb4_0900_ai_ci")
            print(f"   2. 为高频JOIN操作添加复合索引")
            print(f"   3. 确保表默认字符集为 utf8mb4")
            
    except Exception as e:
        print(f"检查失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_table_charset_collation()