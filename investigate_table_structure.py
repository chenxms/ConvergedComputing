#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调查数据库表结构，找到正确的表名
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text, inspect


def investigate_table_structure():
    """调查数据库表结构"""
    print("=" * 50)
    print("数据库表结构调查")
    print("=" * 50)
    print()
    
    db = next(get_db())
    
    try:
        # 获取所有表名
        inspector = inspect(db.bind)
        all_tables = inspector.get_table_names()
        
        print(f"发现 {len(all_tables)} 个表:")
        for i, table in enumerate(sorted(all_tables), 1):
            print(f"  {i:2d}. {table}")
        
        print()
        
        # 寻找可能相关的统计表
        print("寻找统计相关的表:")
        stats_tables = [t for t in all_tables if 'stat' in t.lower() or 'aggr' in t.lower()]
        if stats_tables:
            for table in stats_tables:
                print(f"  找到: {table}")
                # 获取表结构
                columns = inspector.get_columns(table)
                print(f"    列: {[col['name'] for col in columns]}")
        else:
            print("  没有找到明显的统计表")
        
        print()
        
        # 检查关键表的数据
        key_tables_to_check = ['student_score_detail', 'subject_question_config', 'batches']
        
        for table in key_tables_to_check:
            if table in all_tables:
                print(f"检查表 {table}:")
                try:
                    result = db.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.fetchone()[0]
                    print(f"  总记录数: {count}")
                    
                    # 检查是否有G7-2025数据
                    columns = inspector.get_columns(table)
                    column_names = [col['name'] for col in columns]
                    
                    if 'batch_code' in column_names:
                        result = db.execute(text(f"SELECT COUNT(*) FROM {table} WHERE batch_code = 'G7-2025'"))
                        g7_count = result.fetchone()[0]
                        print(f"  G7-2025记录数: {g7_count}")
                    elif 'name' in column_names and table == 'batches':
                        result = db.execute(text(f"SELECT COUNT(*) FROM {table} WHERE name = 'G7-2025'"))
                        g7_count = result.fetchone()[0]
                        print(f"  G7-2025记录数: {g7_count}")
                    
                    print(f"  列结构: {column_names}")
                    
                except Exception as e:
                    print(f"  错误: {e}")
                
                print()
        
        # 特别检查student_score_detail表的实际数据
        if 'student_score_detail' in all_tables:
            print("详细检查student_score_detail表:")
            try:
                # 检查是否有任何数据
                result = db.execute(text("SELECT COUNT(*) FROM student_score_detail"))
                total_count = result.fetchone()[0]
                print(f"  总记录数: {total_count}")
                
                if total_count > 0:
                    # 获取批次列表
                    result = db.execute(text("SELECT DISTINCT batch_code FROM student_score_detail"))
                    batches = [row[0] for row in result.fetchall() if row[0]]
                    print(f"  发现的批次: {batches}")
                    
                    # 检查G7-2025相关的批次（可能名称稍有不同）
                    result = db.execute(text("SELECT DISTINCT batch_code FROM student_score_detail WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'"))
                    g7_related = [row[0] for row in result.fetchall() if row[0]]
                    print(f"  G7相关批次: {g7_related}")
                    
                    if g7_related:
                        for batch in g7_related:
                            result = db.execute(text("SELECT COUNT(*) FROM student_score_detail WHERE batch_code = :batch"), 
                                              {'batch': batch})
                            count = result.fetchone()[0]
                            print(f"    {batch}: {count}条记录")
                            
                else:
                    print("  没有任何数据!")
                    
            except Exception as e:
                print(f"  查询错误: {e}")
        
        return True
        
    except Exception as e:
        print(f"调查失败: {e}")
        return False
    finally:
        db.close()


if __name__ == "__main__":
    investigate_table_structure()