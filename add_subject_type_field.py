#!/usr/bin/env python3
"""
添加subject_type字段到student_cleaned_scores表
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def add_subject_type_field():
    """添加subject_type字段到student_cleaned_scores表"""
    print("=== 检查和添加subject_type字段 ===")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 1. 检查当前表结构
        print("检查student_cleaned_scores表结构...")
        desc_query = text("DESCRIBE student_cleaned_scores")
        desc_result = session.execute(desc_query)
        
        columns = []
        has_subject_type = False
        
        print("当前字段:")
        for row in desc_result.fetchall():
            columns.append(row[0])
            print(f"  {row[0]} - {row[1]} - {row[2] or 'NULL'}")
            if row[0] == 'subject_type':
                has_subject_type = True
        
        # 2. 添加subject_type字段（如果不存在）
        if not has_subject_type:
            print("\nsubject_type字段不存在，正在添加...")
            alter_query = text("""
                ALTER TABLE student_cleaned_scores 
                ADD COLUMN subject_type VARCHAR(20) DEFAULT 'exam' COMMENT '科目类型(exam/questionnaire)'
            """)
            session.execute(alter_query)
            session.commit()
            print("[SUCCESS] subject_type字段添加成功")
            
            # 创建索引
            index_query = text("""
                CREATE INDEX idx_subject_type ON student_cleaned_scores(subject_type)
            """)
            try:
                session.execute(index_query)
                session.commit()
                print("[SUCCESS] subject_type索引创建成功")
            except Exception as e:
                if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                    print("[INFO] subject_type索引已存在")
                else:
                    print(f"[WARNING] 创建索引失败: {e}")
        else:
            print("\n[INFO] subject_type字段已存在，跳过添加")
        
        # 3. 验证字段添加结果
        print("\n验证添加结果...")
        desc_query2 = text("DESCRIBE student_cleaned_scores")
        desc_result2 = session.execute(desc_query2)
        
        updated_columns = []
        print("更新后的字段:")
        for row in desc_result2.fetchall():
            updated_columns.append(row[0])
            if row[0] == 'subject_type':
                print(f"  [NEW] {row[0]} - {row[1]} - {row[2] or 'NULL'} - {row[4] or ''}")
            else:
                print(f"  {row[0]} - {row[1]} - {row[2] or 'NULL'}")
        
        # 4. 检查现有数据中的subject_type分布
        print("\n检查现有数据中subject_type分布:")
        type_query = text("""
            SELECT 
                COALESCE(subject_type, 'NULL') as subject_type,
                COUNT(*) as count
            FROM student_cleaned_scores 
            GROUP BY subject_type
            ORDER BY count DESC
        """)
        type_result = session.execute(type_query)
        
        for row in type_result.fetchall():
            print(f"  {row[0]}: {row[1]:,} 条记录")
        
        print("\n[SUCCESS] subject_type字段配置完成！")
        
    except Exception as e:
        print(f"[ERROR] 操作失败: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
        
    finally:
        session.close()

if __name__ == "__main__":
    add_subject_type_field()