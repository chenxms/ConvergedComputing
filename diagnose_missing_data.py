#!/usr/bin/env python3
"""
诊断G7-2025批次缺失数据问题

功能:
1. 检查清洗数据表中各科目的数据情况
2. 对比原始数据和清洗数据的差异
3. 分析缺失数据的原因
4. 提供修复建议
"""

import sys
import os
from datetime import datetime
import mysql.connector
from mysql.connector import Error

def create_connection():
    """创建数据库连接"""
    try:
        config = {
            'host': os.getenv("DATABASE_HOST", "117.72.14.166"),
            'port': int(os.getenv("DATABASE_PORT", "23506")),
            'user': os.getenv("DATABASE_USER", "root"),
            'password': os.getenv("DATABASE_PASSWORD", "mysql_Lujing2022"),
            'database': os.getenv("DATABASE_NAME", "appraisal_test"),
            'charset': 'utf8mb4',
            'autocommit': False,
            'connection_timeout': 10
        }
        connection = mysql.connector.connect(**config)
        return connection
    except Error as e:
        print(f"错误: 数据库连接失败: {e}")
        return None

def check_raw_data(cursor, batch_code):
    """检查原始数据表的科目分布"""
    print(f"\n=== 检查批次 {batch_code} 的原始数据 ===")

    try:
        # 检查student_score_detail表
        cursor.execute("""
            SELECT subject_name, COUNT(*) as record_count, COUNT(DISTINCT student_id) as student_count
            FROM student_score_detail
            WHERE batch_code = %s
            GROUP BY subject_name
            ORDER BY record_count DESC
        """, (batch_code,))

        raw_data = cursor.fetchall()

        if not raw_data:
            print("错误: 没有找到原始数据记录")
            return {}

        print(f"原始数据统计 (共{len(raw_data)}个科目):")
        print(f"{'科目':<15} {'记录数':<10} {'学生数':<10}")
        print("-" * 40)

        raw_subjects = {}
        for subject, record_count, student_count in raw_data:
            print(f"{subject:<15} {record_count:<10} {student_count:<10}")
            raw_subjects[subject] = {
                'record_count': record_count,
                'student_count': student_count
            }

        return raw_subjects

    except Error as e:
        print(f"错误: 查询原始数据失败: {e}")
        return {}

def check_cleaned_data(cursor, batch_code):
    """检查清洗数据表的科目分布"""
    print(f"\n=== 检查批次 {batch_code} 的清洗数据 ===")

    try:
        # 检查student_cleaned_scores表
        cursor.execute("""
            SELECT subject_name, COUNT(*) as record_count, COUNT(DISTINCT student_id) as student_count
            FROM student_cleaned_scores
            WHERE batch_code = %s
            GROUP BY subject_name
            ORDER BY record_count DESC
        """, (batch_code,))

        cleaned_data = cursor.fetchall()

        if not cleaned_data:
            print("错误: 没有找到清洗数据记录")
            return {}

        print(f"清洗数据统计 (共{len(cleaned_data)}个科目):")
        print(f"{'科目':<15} {'记录数':<10} {'学生数':<10}")
        print("-" * 40)

        cleaned_subjects = {}
        for subject, record_count, student_count in cleaned_data:
            print(f"{subject:<15} {record_count:<10} {student_count:<10}")
            cleaned_subjects[subject] = {
                'record_count': record_count,
                'student_count': student_count
            }

        return cleaned_subjects

    except Error as e:
        print(f"错误: 查询清洗数据失败: {e}")
        return {}

def analyze_data_gap(raw_subjects, cleaned_subjects):
    """分析原始数据和清洗数据的差异"""
    print(f"\n=== 数据差异分析 ===")

    all_subjects = set(raw_subjects.keys()) | set(cleaned_subjects.keys())
    missing_subjects = []
    partial_subjects = []
    complete_subjects = []

    print(f"{'科目':<15} {'原始记录':<10} {'清洗记录':<10} {'学生差异':<10} {'状态':<10}")
    print("-" * 65)

    for subject in sorted(all_subjects):
        raw_count = raw_subjects.get(subject, {}).get('record_count', 0)
        cleaned_count = cleaned_subjects.get(subject, {}).get('record_count', 0)

        raw_students = raw_subjects.get(subject, {}).get('student_count', 0)
        cleaned_students = cleaned_subjects.get(subject, {}).get('student_count', 0)

        student_diff = raw_students - cleaned_students

        if cleaned_count == 0:
            status = "完全缺失"
            missing_subjects.append(subject)
        elif cleaned_count < raw_count * 0.9:  # 清洗后数据少于90%
            status = "部分缺失"
            partial_subjects.append(subject)
        else:
            status = "正常"
            complete_subjects.append(subject)

        print(f"{subject:<15} {raw_count:<10} {cleaned_count:<10} {student_diff:<10} {status:<10}")

    return missing_subjects, partial_subjects, complete_subjects

def check_aggregation_results(cursor, batch_code):
    """检查汇聚结果表"""
    print(f"\n=== 检查批次 {batch_code} 的汇聚结果 ===")

    try:
        # 检查statistical_aggregations表
        cursor.execute("""
            SELECT aggregation_level, COUNT(*) as count
            FROM statistical_aggregations
            WHERE batch_code = %s
            GROUP BY aggregation_level
        """, (batch_code,))

        agg_results = cursor.fetchall()

        if agg_results:
            print("汇聚结果统计:")
            for level, count in agg_results:
                print(f"  {level}: {count}条记录")
        else:
            print("没有找到汇聚结果")

        # 检查最新的汇聚时间
        cursor.execute("""
            SELECT MAX(created_at) as latest_time
            FROM statistical_aggregations
            WHERE batch_code = %s
        """, (batch_code,))

        result = cursor.fetchone()
        if result and result[0]:
            print(f"最新汇聚时间: {result[0]}")

    except Error as e:
        print(f"错误: 查询汇聚结果失败: {e}")

def provide_solutions(missing_subjects, partial_subjects):
    """提供解决方案"""
    print(f"\n=== 解决方案建议 ===")

    if missing_subjects:
        print(f"\n1. 完全缺失的科目 ({len(missing_subjects)}个):")
        for subject in missing_subjects:
            print(f"   - {subject}")
        print("\n建议操作:")
        print("   a) 重新运行数据清洗流程")
        print("   b) 检查subject_question_config表是否有这些科目的配置")
        print("   c) 验证原始数据的完整性")

    if partial_subjects:
        print(f"\n2. 部分缺失的科目 ({len(partial_subjects)}个):")
        for subject in partial_subjects:
            print(f"   - {subject}")
        print("\n建议操作:")
        print("   a) 检查清洗过程中的异常过滤规则")
        print("   b) 查看清洗日志中的异常记录")

    if not missing_subjects and not partial_subjects:
        print("✓ 所有科目的清洗数据都正常")

    print(f"\n修复命令:")
    print("python run_full_batch_pipeline.py G7-2025  # 重新运行完整流程")

def main():
    """主函数"""
    batch_code = "G7-2025"

    print("G7-2025批次数据完整性诊断")
    print("=" * 50)
    print(f"批次: {batch_code}")
    print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    connection = create_connection()
    if not connection:
        return

    try:
        cursor = connection.cursor()

        # 执行诊断步骤
        raw_subjects = check_raw_data(cursor, batch_code)
        cleaned_subjects = check_cleaned_data(cursor, batch_code)

        if raw_subjects and cleaned_subjects:
            missing_subjects, partial_subjects, complete_subjects = analyze_data_gap(raw_subjects, cleaned_subjects)
        else:
            missing_subjects, partial_subjects = [], []

        check_aggregation_results(cursor, batch_code)
        provide_solutions(missing_subjects, partial_subjects)

    except Error as e:
        print(f"错误: 执行诊断时出错: {e}")

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("\nOK: 数据库连接已关闭")

if __name__ == "__main__":
    main()