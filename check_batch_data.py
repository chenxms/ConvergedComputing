"""
检查批次数据是否存在
"""
import os
import sys
from sqlalchemy import create_engine, text

# 数据库连接
DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4")

def check_batch_data():
    """检查批次数据"""
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        # 1. 检查statistical_aggregations表
        print("=== 检查statistical_aggregations表 ===")
        result = conn.execute(text("""
            SELECT batch_code, aggregation_level, school_id,
                   calculation_status, statistics_data IS NOT NULL as has_data,
                   created_at, updated_at
            FROM statistical_aggregations
            WHERE batch_code = 'G4-2025'
            ORDER BY created_at DESC
            LIMIT 5
        """))

        rows = result.fetchall()
        if rows:
            for row in rows:
                print(f"批次: {row[0]}, 级别: {row[1]}, 学校: {row[2]}, 状态: {row[3]}, 有数据: {row[4]}")
        else:
            print("❌ 未找到G4-2025批次的聚合数据")

        # 2. 检查student_cleaned_scores表
        print("\n=== 检查student_cleaned_scores表 ===")
        result = conn.execute(text("""
            SELECT COUNT(DISTINCT student_id) as student_count,
                   COUNT(DISTINCT school_code) as school_count,
                   COUNT(DISTINCT subject_name) as subject_count
            FROM student_cleaned_scores
            WHERE batch_code = 'G4-2025'
        """))

        row = result.fetchone()
        if row and row[0] > 0:
            print(f"✅ 找到G4-2025批次数据:")
            print(f"  学生数: {row[0]}")
            print(f"  学校数: {row[1]}")
            print(f"  科目数: {row[2]}")
        else:
            print("❌ student_cleaned_scores表中未找到G4-2025批次数据")

        # 3. 检查school_master_data表
        print("\n=== 检查school_master_data表 ===")
        result = conn.execute(text("""
            SELECT COUNT(*) as school_count
            FROM school_master_data
            WHERE batch_code = 'G4-2025' AND status = 'ACTIVE'
        """))

        row = result.fetchone()
        if row:
            print(f"活跃学校数: {row[0]}")

if __name__ == "__main__":
    check_batch_data()