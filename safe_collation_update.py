#!/usr/bin/env python3
"""
安全地分步修改列的排序规则，避免长时间锁表
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime
import time

def safe_collation_update():
    """安全地分步修改排序规则"""
    
    try:
        with get_db_context() as session:
            print("=== 安全分步修改排序规则 ===")
            print(f"开始时间: {datetime.now()}")
            print("策略: 只修改关键JOIN列，避免全表重建\n")
            
            # 需要修改的列（只修改JOIN相关的关键列）
            columns_to_modify = [
                'batch_code',
                'subject_id', 
                'subject_name',
                'student_id'
            ]
            
            print("1. 检查当前列的排序规则...")
            for column in columns_to_modify:
                result = session.execute(text(f"""
                    SELECT COLLATION_NAME
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = 'student_cleaned_scores' 
                    AND COLUMN_NAME = '{column}'
                """))
                current_collation = result.fetchone()[0]
                print(f"   {column}: {current_collation}")
            
            print(f"\n2. 分步修改列排序规则...")
            
            # 逐列修改，避免长时间锁定
            for i, column in enumerate(columns_to_modify, 1):
                print(f"   [{i}/{len(columns_to_modify)}] 修改 {column}...")
                
                try:
                    # 获取列的当前定义
                    result = session.execute(text(f"""
                        SELECT COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                        FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() 
                        AND TABLE_NAME = 'student_cleaned_scores' 
                        AND COLUMN_NAME = '{column}'
                    """))
                    col_info = result.fetchone()
                    
                    if col_info:
                        data_type = col_info[0]
                        is_nullable = "NULL" if col_info[1] == "YES" else "NOT NULL"
                        default_value = f"DEFAULT '{col_info[2]}'" if col_info[2] else ""
                        
                        # 只对字符类型列进行修改
                        if any(t in data_type.lower() for t in ['varchar', 'char', 'text']):
                            # 使用MODIFY而不是CONVERT，更快且影响更小
                            modify_sql = f"""
                                ALTER TABLE student_cleaned_scores 
                                MODIFY COLUMN {column} {data_type} 
                                CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci 
                                {is_nullable} {default_value}
                            """
                            
                            start_time = time.time()
                            session.execute(text(modify_sql))
                            session.commit()
                            elapsed = time.time() - start_time
                            
                            print(f"       完成 ({elapsed:.2f}秒)")
                        else:
                            print(f"       跳过 (非字符类型: {data_type})")
                    
                    # 短暂休息，释放资源
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"       失败: {e}")
                    # 继续处理其他列
                    continue
            
            print(f"\n3. 验证修改结果...")
            all_success = True
            for column in columns_to_modify:
                result = session.execute(text(f"""
                    SELECT COLLATION_NAME
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = 'student_cleaned_scores' 
                    AND COLUMN_NAME = '{column}'
                """))
                current_collation = result.fetchone()[0]
                
                if current_collation == 'utf8mb4_0900_ai_ci':
                    print(f"   {column}: [OK] {current_collation}")
                else:
                    print(f"   {column}: [FAILED] {current_collation}")
                    all_success = False
            
            # 4. 测试JOIN性能
            print(f"\n4. 测试JOIN操作...")
            start_time = time.time()
            
            # 不使用BINARY强制比较，让MySQL使用相同排序规则优化
            result = session.execute(text("""
                SELECT COUNT(*)
                FROM student_cleaned_scores scs
                JOIN student_score_detail ssd ON scs.batch_code = ssd.batch_code
                                              AND scs.student_id = ssd.student_id
                WHERE scs.batch_code = 'G4-2025'
                LIMIT 10
            """))
            
            count = result.fetchone()[0]
            elapsed_time = time.time() - start_time
            
            print(f"   查询结果: {count} 行")
            print(f"   执行时间: {elapsed_time:.3f} 秒")
            
            # 5. 结果总结
            print(f"\n=== 操作结果 ===")
            if all_success:
                print("[SUCCESS] 所有关键列排序规则已成功统一为 utf8mb4_0900_ai_ci")
                print("[SUCCESS] JOIN操作不再需要BINARY强制转换")
                print("[SUCCESS] 排序规则冲突问题已解决")
            else:
                print("[PARTIAL] 部分列修改成功，请检查失败的列")
            
            print(f"完成时间: {datetime.now()}")
            print("下一步: 添加复合索引以进一步优化JOIN性能")
            
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("[INFO] 安全分步修改排序规则")
    print("[INFO] 只修改关键JOIN列，避免全表重建")
    print("[INFO] 预计执行时间: 1-3分钟")
    print()
    
    confirm = input("是否开始执行安全修改操作？(输入 'yes' 确认): ")
    if confirm.lower() == 'yes':
        safe_collation_update()
    else:
        print("操作已取消")