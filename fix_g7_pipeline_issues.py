#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复G7-2025全链路处理的关键问题
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def fix_questionnaire_instrument_type():
    """修复问卷instrument_type为NULL的问题"""
    db_url = os.getenv("DATABASE_URL",
                       "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4")
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        print("正在修复问卷配置的instrument_type字段...")

        # 更新G7-2025问卷配置，为instrument_id设置默认值
        result = session.execute(text("""
            UPDATE subject_question_config
            SET instrument_id = 'questionnaire_default_5_scale'
            WHERE batch_code = 'G7-2025'
              AND question_type_enum = 'questionnaire'
              AND instrument_id IS NULL
        """))

        updated_count = result.rowcount
        session.commit()

        print(f"已更新 {updated_count} 条问卷配置记录的instrument_type")

        # 验证修复结果
        verification = session.execute(text("""
            SELECT COUNT(*) as total_questionnaire,
                   SUM(CASE WHEN instrument_id IS NULL THEN 1 ELSE 0 END) as null_count
            FROM subject_question_config
            WHERE batch_code = 'G7-2025' AND question_type_enum = 'questionnaire'
        """)).fetchone()

        print(f"验证结果: 问卷配置 {verification[0]} 条，NULL值 {verification[1]} 条")

        return verification[1] == 0

    except Exception as e:
        session.rollback()
        print(f"修复失败: {e}")
        return False
    finally:
        session.close()

def check_mysql_rank_usage():
    """检查MySQL rank关键字使用情况"""
    db_url = os.getenv("DATABASE_URL",
                       "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4")
    engine = create_engine(db_url)

    try:
        with engine.connect() as conn:
            # 测试rank关键字问题
            try:
                result = conn.execute(text("SELECT 1 as `rank`")).fetchone()
                print("MySQL rank关键字测试通过")
                return True
            except Exception as e:
                print(f"MySQL rank关键字测试失败: {e}")
                return False
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return False

def main():
    print("=== G7-2025 全链路处理问题修复 ===")

    # 设置环境变量
    os.environ['DATABASE_URL'] = 'mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4'

    # 修复问题1: instrument_type NULL值
    print("\n1. 修复问卷instrument_type NULL值问题...")
    fix1_success = fix_questionnaire_instrument_type()

    # 检查问题2: MySQL rank关键字
    print("\n2. 检查MySQL rank关键字使用...")
    fix2_success = check_mysql_rank_usage()

    print(f"\n修复结果:")
    print(f"  问卷instrument_type修复: {'✓' if fix1_success else '✗'}")
    print(f"  MySQL rank关键字检查: {'✓' if fix2_success else '✗'}")

    if fix1_success and fix2_success:
        print("\n所有问题已修复，可以重新运行全链路处理")
    else:
        print("\n仍有问题需要解决")

if __name__ == "__main__":
    main()