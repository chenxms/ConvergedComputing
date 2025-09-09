#!/usr/bin/env python3
"""
为核心表添加复合索引以优化JOIN性能
避免长时间UPDATE操作和锁等待
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime
import time

def add_performance_indexes():
    """添加性能优化索引"""
    
    # 定义需要添加的索引
    indexes_to_add = [
        {
            'table': 'student_cleaned_scores',
            'name': 'idx_batch_subject_student',
            'columns': ['batch_code', 'subject_id', 'student_id'],
            'description': '优化UPDATE JOIN操作的主要索引'
        },
        {
            'table': 'student_cleaned_scores', 
            'name': 'idx_batch_subject_name',
            'columns': ['batch_code', 'subject_name'],
            'description': '优化按科目名称的JOIN查询'
        },
        {
            'table': 'student_score_detail',
            'name': 'idx_batch_subject_student_detail',
            'columns': ['batch_code', 'subject_name', 'subject_id'],
            'description': '优化明细表JOIN查询'
        },
        {
            'table': 'student_score_detail',
            'name': 'idx_batch_student_detail',
            'columns': ['batch_code', 'student_id'],
            'description': '优化学生数据关联查询'
        }
    ]
    
    try:
        with get_db_context() as session:
            print("=== 添加性能优化索引 ===")
            print(f"开始时间: {datetime.now()}")
            print("目标: 减少JOIN操作锁等待时间\n")
            
            # 1. 检查现有索引
            print("1. 检查现有索引...")
            for index in indexes_to_add:
                table = index['table']
                index_name = index['name']
                
                # 检查索引是否已存在
                result = session.execute(text(f"""
                    SELECT COUNT(*) as count
                    FROM information_schema.STATISTICS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{table}'
                    AND INDEX_NAME = '{index_name}'
                """))
                
                exists = result.fetchone()[0] > 0
                status = "存在" if exists else "不存在"
                print(f"   {table}.{index_name}: {status}")
            
            print()
            
            # 2. 添加索引
            print("2. 添加新索引...")
            successful_indexes = []
            failed_indexes = []
            
            for i, index in enumerate(indexes_to_add, 1):
                table = index['table']
                index_name = index['name']
                columns = index['columns']
                description = index['description']
                
                print(f"   [{i}/{len(indexes_to_add)}] {table}.{index_name}")
                print(f"       列: {', '.join(columns)}")
                print(f"       用途: {description}")
                
                try:
                    # 检查索引是否已存在
                    result = session.execute(text(f"""
                        SELECT COUNT(*) as count
                        FROM information_schema.STATISTICS 
                        WHERE TABLE_SCHEMA = DATABASE() 
                        AND TABLE_NAME = '{table}'
                        AND INDEX_NAME = '{index_name}'
                    """))
                    
                    if result.fetchone()[0] > 0:
                        print("       状态: 已存在，跳过")
                        successful_indexes.append(index_name)
                        continue
                    
                    # 创建索引
                    columns_str = ', '.join(columns)
                    create_sql = f"""
                        CREATE INDEX {index_name} 
                        ON {table} ({columns_str})
                    """
                    
                    start_time = time.time()
                    session.execute(text(create_sql))
                    session.commit()
                    elapsed = time.time() - start_time
                    
                    print(f"       状态: 创建成功 ({elapsed:.2f}秒)")
                    successful_indexes.append(index_name)
                    
                    # 短暂休息，避免过载
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"       状态: 创建失败 - {e}")
                    failed_indexes.append(index_name)
                    continue
            
            print()
            
            # 3. 验证索引创建结果
            print("3. 验证索引...")
            for index in indexes_to_add:
                table = index['table']
                index_name = index['name']
                columns = index['columns']
                
                # 检查索引详情
                result = session.execute(text(f"""
                    SELECT 
                        INDEX_NAME,
                        COLUMN_NAME,
                        SEQ_IN_INDEX,
                        CARDINALITY,
                        INDEX_TYPE
                    FROM information_schema.STATISTICS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{table}'
                    AND INDEX_NAME = '{index_name}'
                    ORDER BY SEQ_IN_INDEX
                """))
                
                index_details = result.fetchall()
                if index_details:
                    print(f"   {table}.{index_name}: [OK]")
                    for detail in index_details:
                        print(f"       {detail[1]}({detail[2]}) - 基数:{detail[3]} - 类型:{detail[4]}")
                else:
                    print(f"   {table}.{index_name}: [MISSING]")
            
            # 4. 测试JOIN性能
            print(f"\n4. 测试JOIN查询性能...")
            
            # 测试复杂UPDATE JOIN（之前导致锁定的查询类型）
            print("   测试UPDATE JOIN类型查询...")
            start_time = time.time()
            
            result = session.execute(text("""
                EXPLAIN SELECT COUNT(*)
                FROM student_cleaned_scores scs
                JOIN student_score_detail ssd ON BINARY scs.batch_code = BINARY ssd.batch_code
                                              AND BINARY scs.student_id = BINARY ssd.student_id
                WHERE scs.batch_code = 'G4-2025'
                AND scs.subject_name = '语文'
            """))
            
            explain_result = result.fetchall()
            elapsed_time = time.time() - start_time
            
            print(f"   执行时间: {elapsed_time:.3f} 秒")
            print("   查询计划:")
            for row in explain_result:
                print(f"       {row}")
            
            # 5. 操作总结
            print(f"\n=== 索引优化结果 ===")
            print(f"成功创建索引: {len(successful_indexes)}")
            for name in successful_indexes:
                print(f"  ✓ {name}")
            
            if failed_indexes:
                print(f"失败索引: {len(failed_indexes)}")
                for name in failed_indexes:
                    print(f"  ✗ {name}")
            
            print(f"\n预期效果:")
            print("  - UPDATE JOIN操作锁定时间大幅减少")
            print("  - 精确定位目标行，缩小锁范围") 
            print("  - 避免全表扫描导致的长时间锁定")
            print("  - 提高并发UPDATE操作的吞吐量")
            
            print(f"\n完成时间: {datetime.now()}")
            
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("[INFO] 添加复合索引优化JOIN性能")
    print("[INFO] 专门解决UPDATE JOIN长时间锁定问题")
    print("[INFO] 预计执行时间: 2-5分钟（取决于表大小）")
    print()
    
    confirm = input("是否开始添加性能索引？(输入 'yes' 确认): ")
    if confirm.lower() == 'yes':
        add_performance_indexes()
    else:
        print("操作已取消")