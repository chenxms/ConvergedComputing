#!/usr/bin/env python3
"""
统一关键表的字符集和排序规则为 utf8mb4_0900_ai_ci
解决JOIN操作中的排序规则不一致问题
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime
import time

def unify_charset_collation():
    """统一表的字符集和排序规则"""
    
    try:
        with get_db_context() as session:
            print("=== 统一字符集和排序规则操作 ===")
            print(f"开始时间: {datetime.now()}")
            print("目标排序规则: utf8mb4_0900_ai_ci\n")
            
            # 1. 修改 student_cleaned_scores 表和列的排序规则
            print("1. 修改 student_cleaned_scores 表排序规则...")
            
            # 首先修改表的默认排序规则
            print("   修改表默认排序规则...")
            session.execute(text("""
                ALTER TABLE student_cleaned_scores 
                CONVERT TO CHARACTER SET utf8mb4 
                COLLATE utf8mb4_0900_ai_ci
            """))
            session.commit()
            print("   [OK] 表默认排序规则已更新")
            
            # 等待一下确保操作完成
            time.sleep(2)
            
            # 2. 验证修改结果
            print("\n2. 验证修改结果...")
            
            # 检查表排序规则
            result = session.execute(text("""
                SELECT TABLE_COLLATION
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'student_cleaned_scores'
            """))
            table_collation = result.fetchone()[0]
            print(f"   student_cleaned_scores 表排序规则: {table_collation}")
            
            # 检查关键列的排序规则
            key_columns = ['batch_code', 'subject_id', 'subject_name', 'student_id']
            for column in key_columns:
                result = session.execute(text(f"""
                    SELECT COLLATION_NAME
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = 'student_cleaned_scores' 
                    AND COLUMN_NAME = '{column}'
                """))
                col_info = result.fetchone()
                if col_info:
                    print(f"   {column}: {col_info[0]}")
            
            # 3. 验证所有相关表的排序规则一致性
            print(f"\n3. 验证表间排序规则一致性...")
            tables_to_check = [
                'student_cleaned_scores',
                'student_score_detail', 
                'subject_question_config',
                'question_dimension_mapping'
            ]
            
            collations_found = set()
            for table in tables_to_check:
                result = session.execute(text(f"""
                    SELECT TABLE_COLLATION
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{table}'
                """))
                collation = result.fetchone()[0]
                collations_found.add(collation)
                print(f"   {table}: {collation}")
            
            # 4. 结果评估
            print(f"\n4. 操作结果评估:")
            if len(collations_found) == 1 and 'utf8mb4_0900_ai_ci' in collations_found:
                print("   [OK] 成功！所有表都使用相同的排序规则 utf8mb4_0900_ai_ci")
                print("   [OK] JOIN操作的排序规则冲突问题已解决")
            else:
                print(f"   [WARNING] 仍有不一致的排序规则: {collations_found}")
            
            # 5. 测试JOIN操作
            print(f"\n5. 测试JOIN操作性能...")
            start_time = time.time()
            
            result = session.execute(text("""
                SELECT COUNT(*)
                FROM student_cleaned_scores scs
                JOIN student_score_detail ssd ON BINARY scs.batch_code = BINARY ssd.batch_code
                                              AND BINARY scs.student_id = BINARY ssd.student_id
                WHERE scs.batch_code = 'G4-2025'
                LIMIT 10
            """))
            
            count = result.fetchone()[0]
            elapsed_time = time.time() - start_time
            
            print(f"   测试查询结果: {count} 行")
            print(f"   执行时间: {elapsed_time:.3f} 秒")
            
            if elapsed_time < 1.0:
                print("   [OK] JOIN操作性能正常")
            else:
                print("   [WARNING] JOIN操作仍需优化（可能需要添加索引）")
            
            print(f"\n=== 操作完成 ===")
            print(f"完成时间: {datetime.now()}")
            print("下一步: 添加复合索引以进一步优化性能")
            
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("[WARNING] 此操作将修改数据库表结构")
    print("[WARNING] 请确保已备份重要数据") 
    print("[WARNING] 建议在低峰期执行")
    print()
    
    # 在生产环境中应该要求确认
    confirm = input("是否继续执行排序规则统一操作？(输入 'yes' 确认): ")
    if confirm.lower() == 'yes':
        unify_charset_collation()
    else:
        print("操作已取消")