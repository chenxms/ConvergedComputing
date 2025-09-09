#!/usr/bin/env python3
"""
数据库数据验证脚本
检查远程数据库是否包含必要的学生答题数据
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text, func
from app.database.connection import engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_database_tables():
    """检查必要的数据表是否存在"""
    print("🔍 检查数据库表结构...")
    
    required_tables = [
        'student_score_detail',      # 学生答题明细
        'subject_question_config',   # 题目配置
        'question_dimension_mapping', # 维度映射
        'grade_aggregation_main'     # 年级信息
    ]
    
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SHOW TABLES"))
            existing_tables = [row[0] for row in result.fetchall()]
            
            print(f"✓ 数据库连接成功，共发现 {len(existing_tables)} 个表")
            
            missing_tables = []
            for table in required_tables:
                if table in existing_tables:
                    print(f"  ✓ {table}")
                else:
                    print(f"  ✗ {table} - 缺失")
                    missing_tables.append(table)
            
            if missing_tables:
                print(f"\n❌ 缺失关键数据表: {missing_tables}")
                return False
            else:
                print("\n✅ 所有必要数据表都存在")
                return True
                
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return False

def check_data_availability():
    """检查关键数据表中是否有数据"""
    print("\n🔍 检查数据表内容...")
    
    data_checks = [
        ("student_score_detail", "学生答题数据"),
        ("subject_question_config", "题目配置数据"),
        ("question_dimension_mapping", "维度映射数据"),
        ("grade_aggregation_main", "年级信息数据")
    ]
    
    try:
        with engine.connect() as connection:
            all_have_data = True
            
            for table, description in data_checks:
                try:
                    result = connection.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
                    count = result.fetchone()[0]
                    
                    if count > 0:
                        print(f"  ✓ {description}: {count:,} 条记录")
                    else:
                        print(f"  ✗ {description}: 无数据")
                        all_have_data = False
                        
                except Exception as e:
                    print(f"  ✗ {description}: 查询失败 - {e}")
                    all_have_data = False
            
            return all_have_data
            
    except Exception as e:
        print(f"❌ 数据检查失败: {e}")
        return False

def check_sample_student_data():
    """检查学生数据样例"""
    print("\n🔍 检查学生数据结构...")
    
    try:
        with engine.connect() as connection:
            # 检查学生答题数据样例
            query = text("""
                SELECT student_id, subject_id, question_id, score, max_score 
                FROM student_score_detail 
                LIMIT 5
            """)
            
            result = connection.execute(query)
            rows = result.fetchall()
            
            if rows:
                print("✓ 学生答题数据样例:")
                print("  学生ID | 科目ID | 题目ID | 得分 | 满分")
                print("  " + "-" * 40)
                
                for row in rows:
                    print(f"  {row[0]:<8} | {row[1]:<6} | {row[2]:<6} | {row[3]:<4} | {row[4]:<4}")
                
                return True
            else:
                print("✗ 没有找到学生答题数据")
                return False
                
    except Exception as e:
        print(f"❌ 样例数据检查失败: {e}")
        return False

def check_subject_config():
    """检查科目配置"""
    print("\n🔍 检查科目配置...")
    
    try:
        with engine.connect() as connection:
            query = text("""
                SELECT subject_id, question_id, max_score, subject_type
                FROM subject_question_config 
                LIMIT 5
            """)
            
            result = connection.execute(query)
            rows = result.fetchall()
            
            if rows:
                print("✓ 科目配置数据样例:")
                print("  科目ID | 题目ID | 满分 | 科目类型")
                print("  " + "-" * 35)
                
                for row in rows:
                    print(f"  {row[0]:<7} | {row[1]:<6} | {row[2]:<4} | {row[3]}")
                
                return True
            else:
                print("✗ 没有找到科目配置数据")
                return False
                
    except Exception as e:
        print(f"❌ 科目配置检查失败: {e}")
        return False

def main():
    print("=" * 60)
    print("🏥 远程数据库数据验证")
    print("=" * 60)
    
    checks = [
        ("数据库表结构", check_database_tables),
        ("数据表内容", check_data_availability),
        ("学生数据样例", check_sample_student_data),
        ("科目配置", check_subject_config)
    ]
    
    all_passed = True
    
    for check_name, check_func in checks:
        print(f"\n【{check_name}】")
        if not check_func():
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 数据库验证通过！可以进行数据汇聚计算测试")
        print("\n📋 下一步:")
        print("   运行: python test_data_aggregation.py")
    else:
        print("❌ 数据库验证失败，需要补充必要数据")
        print("\n📋 需要确保:")
        print("   1. 所有必要表都存在且有数据")
        print("   2. student_score_detail 表有学生答题记录")
        print("   3. subject_question_config 表有题目配置")
        print("   4. 相关联表有对应的映射数据")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)