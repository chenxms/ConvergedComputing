#!/usr/bin/env python3
"""
远程数据库连接验证脚本
确认连接的具体数据库和可用的数据库列表
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 远程数据库连接信息
DATABASE_HOST = "117.72.14.166"
DATABASE_PORT = "23506"
DATABASE_USER = "root"
DATABASE_PASSWORD = "mysql_Lujing2022"

def test_connection_to_server():
    """测试到MySQL服务器的连接"""
    print("🔍 测试MySQL服务器连接...")
    
    # 不指定数据库名，连接到MySQL服务器
    server_url = f"mysql+pymysql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/?charset=utf8mb4"
    
    try:
        engine = create_engine(server_url, connect_args={"connect_timeout": 10})
        
        with engine.connect() as connection:
            # 检查连接和服务器版本
            result = connection.execute(text("SELECT VERSION()"))
            version = result.fetchone()[0]
            
            print(f"✅ MySQL服务器连接成功")
            print(f"   服务器版本: {version}")
            print(f"   连接地址: {DATABASE_HOST}:{DATABASE_PORT}")
            
            return True, engine
            
    except Exception as e:
        print(f"❌ MySQL服务器连接失败: {e}")
        return False, None

def list_all_databases(engine):
    """列出所有数据库"""
    print("\n🔍 查看所有可用数据库...")
    
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SHOW DATABASES"))
            databases = [row[0] for row in result.fetchall()]
            
            print(f"✅ 发现 {len(databases)} 个数据库:")
            for i, db in enumerate(databases, 1):
                print(f"   {i:2d}. {db}")
            
            return databases
            
    except Exception as e:
        print(f"❌ 获取数据库列表失败: {e}")
        return []

def check_database_tables(database_name):
    """检查指定数据库中的表"""
    print(f"\n🔍 检查数据库 '{database_name}' 中的表...")
    
    db_url = f"mysql+pymysql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{database_name}?charset=utf8mb4"
    
    try:
        engine = create_engine(db_url, connect_args={"connect_timeout": 10})
        
        with engine.connect() as connection:
            result = connection.execute(text("SHOW TABLES"))
            tables = [row[0] for row in result.fetchall()]
            
            print(f"✅ 数据库 '{database_name}' 中有 {len(tables)} 个表:")
            
            if tables:
                for i, table in enumerate(sorted(tables), 1):
                    print(f"   {i:2d}. {table}")
                    
                # 检查是否有学生相关的表
                student_related = [t for t in tables if any(keyword in t.lower() for keyword in ['student', 'score', 'answer', 'detail'])]
                if student_related:
                    print(f"\n📚 发现可能包含学生数据的表:")
                    for table in student_related:
                        print(f"   ⭐ {table}")
            else:
                print("   (数据库为空)")
            
            return tables
            
    except Exception as e:
        print(f"❌ 检查数据库 '{database_name}' 失败: {e}")
        return []

def check_specific_tables_in_database(database_name, table_names):
    """在指定数据库中检查特定表"""
    print(f"\n🔍 在数据库 '{database_name}' 中查找特定表...")
    
    db_url = f"mysql+pymysql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{database_name}?charset=utf8mb4"
    
    try:
        engine = create_engine(db_url, connect_args={"connect_timeout": 10})
        
        with engine.connect() as connection:
            found_tables = []
            
            for table_name in table_names:
                try:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    count = result.fetchone()[0]
                    found_tables.append((table_name, count))
                    print(f"   ✅ {table_name}: {count:,} 条记录")
                except Exception:
                    print(f"   ❌ {table_name}: 不存在或无法访问")
            
            return found_tables
            
    except Exception as e:
        print(f"❌ 检查失败: {e}")
        return []

def main():
    print("=" * 70)
    print("🏥 远程MySQL数据库连接验证")
    print("=" * 70)
    print(f"目标服务器: {DATABASE_HOST}:{DATABASE_PORT}")
    print(f"用户名: {DATABASE_USER}")
    
    # 1. 测试服务器连接
    success, engine = test_connection_to_server()
    if not success:
        print("❌ 无法连接到MySQL服务器，请检查网络和凭据")
        return False
    
    # 2. 列出所有数据库
    databases = list_all_databases(engine)
    if not databases:
        print("❌ 无法获取数据库列表")
        return False
    
    # 3. 检查每个数据库中的表
    target_tables = ['student_score_detail', 'subject_question_config', 'question_dimension_mapping', 'grade_aggregation_main']
    
    found_in_database = None
    
    for database in databases:
        if database.lower() in ['information_schema', 'performance_schema', 'mysql', 'sys']:
            continue  # 跳过系统数据库
            
        print(f"\n{'='*50}")
        print(f"🔍 检查数据库: {database}")
        print('='*50)
        
        tables = check_database_tables(database)
        
        if tables:
            # 检查是否包含目标表
            found_tables = check_specific_tables_in_database(database, target_tables)
            
            if found_tables:
                found_in_database = database
                print(f"\n🎉 在数据库 '{database}' 中找到了目标数据表！")
                break
    
    # 4. 总结和建议
    print(f"\n{'='*70}")
    print("📋 分析总结")
    print('='*70)
    
    if found_in_database:
        print(f"✅ 学生数据位于数据库: {found_in_database}")
        print(f"❗ 当前系统连接的数据库: appraisal_stats")
        print(f"💡 建议: 修改数据库配置指向 '{found_in_database}'")
        
        print(f"\n🔧 修改方法:")
        print(f"   方法1: 设置环境变量")
        print(f"     set DATABASE_NAME={found_in_database}")
        print(f"   方法2: 修改 connection.py 第23行")
        print(f"     DATABASE_NAME = \"{found_in_database}\"")
        
    else:
        print("❌ 未找到包含目标表的数据库")
        print("💡 可能的原因:")
        print("   1. 表名不同")
        print("   2. 数据在其他服务器")
        print("   3. 权限不足")
        
        # 显示发现的所有表，帮助分析
        print(f"\n📋 所有发现的非系统表:")
        for database in databases:
            if database.lower() not in ['information_schema', 'performance_schema', 'mysql', 'sys']:
                tables = check_database_tables(database)
                if tables:
                    print(f"   数据库 {database}: {', '.join(tables[:5])}")
                    if len(tables) > 5:
                        print(f"                     (还有 {len(tables)-5} 个表...)")
    
    return found_in_database is not None

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)