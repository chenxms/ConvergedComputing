#!/usr/bin/env python3
"""
分析数据库表大小和提供优化建议
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def analyze_database_size():
    """分析数据库大小和性能状况"""
    
    try:
        with get_db_context() as session:
            print("=== 数据库大小和性能分析 ===")
            print(f"分析时间: {datetime.now()}\n")
            
            # 1. 检查表大小
            print("1. 核心表数据量分析:")
            tables = ['student_cleaned_scores', 'student_score_detail', 'subject_question_config']
            
            for table in tables:
                try:
                    # 表行数
                    result = session.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
                    row_count = result.fetchone()[0]
                    
                    # 表大小信息
                    result = session.execute(text(f"""
                        SELECT 
                            ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS size_mb,
                            ROUND(DATA_LENGTH / 1024 / 1024, 2) AS data_mb,
                            ROUND(INDEX_LENGTH / 1024 / 1024, 2) AS index_mb,
                            TABLE_ROWS as estimated_rows
                        FROM information_schema.TABLES 
                        WHERE TABLE_SCHEMA = DATABASE() 
                        AND TABLE_NAME = '{table}'
                    """))
                    size_info = result.fetchone()
                    
                    print(f"   {table}:")
                    print(f"     实际行数: {row_count:,}")
                    print(f"     表大小: {size_info[0]} MB (数据:{size_info[1]} MB, 索引:{size_info[2]} MB)")
                    print(f"     预估行数: {size_info[3]:,}")
                    print()
                    
                except Exception as e:
                    print(f"   {table}: 检查失败 - {e}")
            
            # 2. 分析当前索引使用情况
            print("2. 当前索引分析:")
            result = session.execute(text("""
                SELECT 
                    TABLE_NAME,
                    INDEX_NAME,
                    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns,
                    INDEX_TYPE,
                    NON_UNIQUE
                FROM information_schema.STATISTICS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME IN ('student_cleaned_scores', 'student_score_detail')
                GROUP BY TABLE_NAME, INDEX_NAME, INDEX_TYPE, NON_UNIQUE
                ORDER BY TABLE_NAME, INDEX_NAME
            """))
            
            indexes = result.fetchall()
            current_table = None
            for idx in indexes:
                if idx[0] != current_table:
                    current_table = idx[0]
                    print(f"\n   {current_table}:")
                
                unique_str = "UNIQUE" if idx[4] == 0 else "NON-UNIQUE"
                print(f"     {idx[1]} ({unique_str}): {idx[2]}")
            
            # 3. 检查系统资源和配置
            print(f"\n3. MySQL配置检查:")
            config_vars = [
                'innodb_buffer_pool_size',
                'max_connections', 
                'innodb_lock_wait_timeout',
                'lock_wait_timeout',
                'wait_timeout',
                'interactive_timeout'
            ]
            
            for var in config_vars:
                try:
                    result = session.execute(text(f"SHOW VARIABLES LIKE '{var}'"))
                    var_info = result.fetchone()
                    if var_info:
                        print(f"   {var_info[0]}: {var_info[1]}")
                except:
                    print(f"   {var}: 无法获取")
            
            # 4. 检查当前进程状态
            print(f"\n4. 当前数据库活动:")
            result = session.execute(text("""
                SELECT 
                    COUNT(*) as total_connections,
                    SUM(CASE WHEN COMMAND != 'Sleep' THEN 1 ELSE 0 END) as active_connections,
                    SUM(CASE WHEN TIME > 60 THEN 1 ELSE 0 END) as long_running
                FROM information_schema.PROCESSLIST
            """))
            
            activity = result.fetchone()
            print(f"   总连接数: {activity[0]}")
            print(f"   活跃连接数: {activity[1]}")
            print(f"   长时间运行: {activity[2]}")
            
            # 5. 基于分析结果的建议
            print(f"\n=== 优化建议 ===")
            
            # 获取student_cleaned_scores的行数来判断
            result = session.execute(text("SELECT COUNT(*) FROM student_cleaned_scores"))
            scs_count = result.fetchone()[0]
            
            if scs_count > 1000000:  # 超过100万行
                print("🔥 大表优化策略 (推荐):")
                print("   1. **在维护窗口期执行DDL操作**")
                print("      - 选择低峰期（如凌晨2-4点）")
                print("      - 预留足够时间（可能需要30-60分钟）")
                print("   2. **使用MySQL命令行直接执行**")
                print("      mysql> CREATE INDEX idx_batch_subject_student ON student_cleaned_scores(batch_code, subject_id, student_id);")
                print("   3. **考虑分批处理**")
                print("      - 使用pt-online-schema-change工具")
                print("      - 或者先创建新表，然后切换")
            else:
                print("📊 中等表优化策略:")
                print("   1. 增加超时时间配置")
                print("   2. 在应用低峰期执行")
                print("   3. 可以尝试在线创建索引")
            
            print(f"\n📋 立即可用的解决方案:")
            print("   1. **应用层优化**:")
            print("      - 使用现有索引优化查询")
            print("      - 避免全表JOIN操作")
            print("      - 分批处理大数据更新")
            print("   2. **查询优化**:")
            print("      - 继续使用BINARY比较避免排序规则冲突")
            print("      - 添加适当的WHERE条件限制结果集")
            print("      - 使用EXISTS代替JOIN when possible")
            
            print(f"\n⚠️  紧急问题预防:")
            print("   - 避免长时间运行的UPDATE JOIN操作")
            print("   - 监控slow query log")
            print("   - 实现查询超时机制")
            
    except Exception as e:
        print(f"分析失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_database_size()