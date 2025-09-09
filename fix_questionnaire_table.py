#!/usr/bin/env python3
"""
修复问卷表结构
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def fix_questionnaire_table():
    """修复问卷表结构"""
    print("=== 修复问卷表结构 ===")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # 1. 修改question_id字段类型
        print("修改question_id字段类型从BIGINT到VARCHAR...")
        alter_query = text("""
            ALTER TABLE questionnaire_question_scores 
            MODIFY COLUMN question_id VARCHAR(50) NOT NULL COMMENT '题目ID'
        """)
        session.execute(alter_query)
        session.commit()
        print("[SUCCESS] question_id字段类型修改成功")
        
        # 2. 验证修改结果
        print("\n验证表结构...")
        desc_query = text("DESCRIBE questionnaire_question_scores")
        desc_result = session.execute(desc_query)
        
        print("修改后的字段结构:")
        for row in desc_result.fetchall():
            if row[0] == 'question_id':
                print(f"  [UPDATED] {row[0]} - {row[1]} - {row[2] if row[2] else 'NULL'}")
            else:
                print(f"  {row[0]} - {row[1]} - {row[2] if row[2] else 'NULL'}")
        
        print("\n[SUCCESS] 问卷表结构修复完成！")
        
    except Exception as e:
        print(f"[ERROR] 修复失败: {e}")
        import traceback
        traceback.print_exc()
        session.rollback()
        
    finally:
        session.close()

if __name__ == "__main__":
    fix_questionnaire_table()