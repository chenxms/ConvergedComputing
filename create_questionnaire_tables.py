#!/usr/bin/env python3
"""
创建问卷相关数据表
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def create_questionnaire_tables():
    """创建问卷相关数据表"""
    print("=== 创建问卷数据表 ===")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 读取SQL文件
        sql_file_path = os.path.join(os.path.dirname(__file__), 'create_questionnaire_table.sql')
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 分割SQL语句（按分号分割）
        sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        print(f"准备执行 {len(sql_statements)} 条SQL语句...")
        
        for i, sql in enumerate(sql_statements, 1):
            if sql.strip():
                print(f"执行第 {i} 条SQL语句...")
                try:
                    session.execute(text(sql))
                    session.commit()
                    print(f"[SUCCESS] 第 {i} 条语句执行成功")
                except Exception as e:
                    print(f"[ERROR] 第 {i} 条语句执行失败: {e}")
                    session.rollback()
                    # 如果不是严重错误，继续执行下一条
                    if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                        print("  -> 表或数据已存在，跳过")
                        continue
                    else:
                        raise e
        
        print("\n=== 验证表创建结果 ===")
        
        # 验证主表是否创建成功
        verify_query = text("SHOW TABLES LIKE 'questionnaire%'")
        result = session.execute(verify_query)
        tables = [row[0] for row in result.fetchall()]
        
        print(f"创建的问卷相关表: {tables}")
        
        # 检查表结构
        for table in tables:
            print(f"\n表 {table} 的结构:")
            desc_query = text(f"DESCRIBE {table}")
            desc_result = session.execute(desc_query)
            for row in desc_result.fetchall():
                print(f"  {row[0]} - {row[1]} - {row[2] or 'NULL'}")
        
        # 检查量表选项映射数据
        print("\n=== 检查量表选项数据 ===")
        options_query = text("""
            SELECT instrument_type, scale_level, COUNT(*) as option_count
            FROM questionnaire_scale_options
            GROUP BY instrument_type, scale_level
            ORDER BY instrument_type, scale_level
        """)
        options_result = session.execute(options_query)
        
        print("量表类型及选项数量:")
        for row in options_result.fetchall():
            print(f"  {row[0]} ({row[1]}分位): {row[2]}个选项")
        
        print("\n[SUCCESS] 问卷数据表创建完成！")
        
    except Exception as e:
        print(f"[ERROR] 创建失败: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
        
    finally:
        session.close()

if __name__ == "__main__":
    create_questionnaire_tables()