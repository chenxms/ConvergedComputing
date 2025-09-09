#!/usr/bin/env python3
"""
实施PO方案：添加关键复合索引
按照PO方案添加 idx_scs_batch_subj_stu 索引优化JOIN性能
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime
import time

def implement_po_index_optimization():
    """实施PO索引优化方案"""
    
    try:
        with get_db_context() as session:
            print("=== 实施PO索引优化方案 ===")
            print(f"执行时间: {datetime.now()}")
            print("目标: 添加 idx_scs_batch_subj_stu 复合索引\n")
            
            # 1. 检查当前索引状态
            print("1. 检查现有索引状态:")
            result = session.execute(text("""
                SHOW INDEX FROM student_cleaned_scores
            """))
            
            existing_indexes = result.fetchall()
            po_index_exists = False
            
            print("   当前索引:")
            for idx in existing_indexes:
                if len(idx) >= 3:  # 确保有足够的列
                    index_name = idx[2]  # Key_name列
                    column_name = idx[4] if len(idx) > 4 else "unknown"  # Column_name列
                    print(f"     {index_name}: {column_name}")
                    
                    if index_name == 'idx_scs_batch_subj_stu':
                        po_index_exists = True
            
            if po_index_exists:
                print("\n   [INFO] PO推荐索引已存在，跳过创建")
                return
            
            # 2. 创建PO推荐的复合索引
            print(f"\n2. 创建PO推荐索引:")
            print("   索引名: idx_scs_batch_subj_stu")
            print("   列组合: (batch_code, subject_id, student_id)")
            print("   算法: INPLACE (在线DDL)")
            
            # 按PO方案使用INPLACE算法和NONE锁定
            create_index_sql = """
                ALTER TABLE student_cleaned_scores 
                ADD INDEX idx_scs_batch_subj_stu (batch_code, subject_id, student_id),
                ALGORITHM=INPLACE, LOCK=NONE
            """
            
            print("   执行DDL...")
            start_time = time.time()
            
            try:
                session.execute(text(create_index_sql))
                session.commit()
                
                creation_time = time.time() - start_time
                print(f"   [SUCCESS] 索引创建成功，耗时: {creation_time:.2f}秒")
                
            except Exception as e:
                if "Duplicate key name" in str(e):
                    print(f"   [INFO] 索引已存在，跳过创建")
                else:
                    print(f"   [ERROR] 索引创建失败: {e}")
                    # 尝试不使用INPLACE算法
                    print("   尝试使用默认算法...")
                    try:
                        simple_sql = """
                            CREATE INDEX idx_scs_batch_subj_stu 
                            ON student_cleaned_scores (batch_code, subject_id, student_id)
                        """
                        session.execute(text(simple_sql))
                        session.commit()
                        print(f"   [SUCCESS] 使用默认算法创建成功")
                    except Exception as e2:
                        print(f"   [FAILED] 默认算法也失败: {e2}")
                        return
            
            # 3. 验证索引创建结果
            print(f"\n3. 验证索引创建:")
            result = session.execute(text("""
                SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
                FROM INFORMATION_SCHEMA.STATISTICS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'student_cleaned_scores'
                AND INDEX_NAME = 'idx_scs_batch_subj_stu'
                ORDER BY SEQ_IN_INDEX
            """))
            
            index_columns = result.fetchall()
            
            if index_columns:
                print("   [SUCCESS] 索引验证通过:")
                for idx_name, col_name, seq in index_columns:
                    print(f"     位置{seq}: {col_name}")
                
                # 检查列顺序是否正确
                expected_order = ['batch_code', 'subject_id', 'student_id']
                actual_order = [col[1] for col in index_columns]
                
                if actual_order == expected_order:
                    print("   [SUCCESS] 索引列顺序正确")
                else:
                    print(f"   [WARNING] 索引列顺序: 期望{expected_order}, 实际{actual_order}")
                    
            else:
                print("   [ERROR] 索引验证失败，未找到创建的索引")
                return
            
            # 4. 测试索引性能效果
            print(f"\n4. 测试索引优化效果:")
            
            # 4.1 测试EXPLAIN计划
            print("   4.1 查询计划分析:")
            test_queries = [
                """
                SELECT COUNT(*) FROM student_cleaned_scores 
                WHERE batch_code = 'G4-2025' 
                AND subject_id = 'CHINESE'
                """,
                """
                SELECT * FROM student_cleaned_scores 
                WHERE batch_code = 'G4-2025' 
                AND subject_id = 'CHINESE' 
                AND student_id = 'STU001'
                LIMIT 1
                """
            ]
            
            for i, query in enumerate(test_queries, 1):
                print(f"\n     测试查询{i}:")
                try:
                    explain_sql = f"EXPLAIN {query}"
                    result = session.execute(text(explain_sql))
                    explain_result = result.fetchall()
                    
                    for row in explain_result:
                        if len(row) >= 5:
                            table = row[1] if len(row) > 1 else "unknown"
                            type_val = row[2] if len(row) > 2 else "unknown"
                            key_val = row[3] if len(row) > 3 else "none"
                            rows_val = row[4] if len(row) > 4 else "unknown"
                            
                            print(f"       表: {table}")
                            print(f"       类型: {type_val}")
                            print(f"       使用索引: {key_val}")
                            print(f"       扫描行数: {rows_val}")
                            
                            # 评估优化效果
                            if key_val == 'idx_scs_batch_subj_stu':
                                print("       [EXCELLENT] 使用了PO推荐索引!")
                            elif 'batch_code' in str(key_val):
                                print("       [GOOD] 使用了batch_code相关索引")
                            else:
                                print("       [WARNING] 未使用预期索引")
                                
                except Exception as e:
                    print(f"       查询计划分析失败: {e}")
            
            # 4.2 测试实际查询性能
            print(f"\n   4.2 实际查询性能测试:")
            
            performance_sql = """
                SELECT COUNT(*) as total_count,
                       COUNT(DISTINCT batch_code) as batches,
                       COUNT(DISTINCT subject_id) as subjects  
                FROM student_cleaned_scores
                WHERE batch_code IN ('G4-2025', 'G7-2025')
                AND subject_id IS NOT NULL
            """
            
            start_time = time.time()
            result = session.execute(text(performance_sql))
            perf_result = result.fetchone()
            query_time = time.time() - start_time
            
            print(f"     查询结果: {perf_result[0]}条记录, {perf_result[1]}个批次, {perf_result[2]}个科目")
            print(f"     查询时间: {query_time:.3f}秒")
            
            if query_time < 1.0:
                print("     [EXCELLENT] 查询性能优秀 (<1秒)")
            elif query_time < 3.0:
                print("     [GOOD] 查询性能良好 (<3秒)")  
            else:
                print("     [WARNING] 查询性能需要改进 (>3秒)")
            
            # 5. 生成后续优化建议
            print(f"\n5. 后续优化建议:")
            
            print("   ✅ 完成项:")
            print("     - PO推荐的复合索引已添加")
            print("     - JOIN查询性能应显著提升")
            print("     - UPDATE操作锁定范围将缩小")
            
            print(f"\n   📋 下一步建议:")
            print("     1. 统一student_cleaned_scores排序规则为utf8mb4_0900_ai_ci")
            print("     2. 移除查询中的BINARY包装，直接使用等值比较")
            print("     3. 测试大批量UPDATE JOIN操作性能")
            print("     4. 考虑添加问卷物化汇总表")
            
            print(f"\n=== PO索引优化完成 ===")
            print(f"完成时间: {datetime.now()}")
            
    except Exception as e:
        print(f"索引优化失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    implement_po_index_optimization()