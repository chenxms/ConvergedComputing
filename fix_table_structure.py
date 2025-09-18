#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
修复questionnaire_option_distribution表结构
"""

from app.database.connection import get_db_context
from sqlalchemy import text

def fix_table_structure():
    """修复题目选项分布表结构"""
    with get_db_context() as db:
        print("正在修复questionnaire_option_distribution表结构...")
        
        # 先备份现有数据
        backup_data = db.execute(text("""
            SELECT batch_code, subject_name, question_id, option_level, count
            FROM questionnaire_option_distribution
        """)).fetchall()
        
        print(f"备份了 {len(backup_data)} 条记录")
        
        # 删除现有表
        db.execute(text("DROP TABLE IF EXISTS questionnaire_option_distribution"))
        db.commit()
        
        # 重新创建表（符合v1.2规范）
        create_sql = """
        CREATE TABLE questionnaire_option_distribution (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            batch_code VARCHAR(50) NOT NULL,
            school_id VARCHAR(50) NOT NULL,
            subject_name VARCHAR(100) NOT NULL,
            question_id VARCHAR(100) NOT NULL,
            option_level BIGINT NOT NULL,
            option_label VARCHAR(100),
            count BIGINT NOT NULL DEFAULT 0,
            n_total BIGINT NOT NULL DEFAULT 0,
            pct DECIMAL(7,4) NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            UNIQUE KEY uk_questionnaire_option_distribution (
                batch_code, school_id, subject_name, question_id, option_level
            ),
            INDEX idx_batch_school_subject (batch_code, school_id, subject_name),
            INDEX idx_question_option (question_id, option_level)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        db.execute(text(create_sql))
        db.commit()
        
        print("✓ 表结构创建完成")
        
        # 恢复数据（添加必需的school_id字段，临时设为'UNKNOWN'）
        if backup_data:
            print("正在恢复数据...")
            for row in backup_data:
                batch_code, subject_name, question_id, option_level, count = row
                db.execute(text("""
                    INSERT INTO questionnaire_option_distribution 
                    (batch_code, school_id, subject_name, question_id, option_level, count, n_total, pct)
                    VALUES (:batch, 'UNKNOWN', :subject, :question, :level, :count, :count, 100.0)
                """), {
                    'batch': batch_code,
                    'subject': subject_name,
                    'question': question_id,
                    'level': option_level,
                    'count': count
                })
            db.commit()
            print(f"✓ 恢复了 {len(backup_data)} 条记录")
        
        # 验证表结构
        print("\n=== 修复后的表结构 ===")
        result = db.execute(text("DESCRIBE questionnaire_option_distribution")).fetchall()
        for row in result:
            print(f"{row[0]}: {row[1]} {row[2]}")
        
        print("\n✓ 表结构修复完成！")

if __name__ == "__main__":
    fix_table_structure()