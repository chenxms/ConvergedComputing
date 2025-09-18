#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查statistical_aggregations表中的JSON数据结构
分析字段映射问题
"""

import os
import json
import mysql.connector
from mysql.connector import Error

def inspect_json_structure():
    """检查JSON数据结构"""

    # 数据库配置
    db_config = {
        'host': os.getenv('DATABASE_HOST', '117.72.14.166'),
        'port': int(os.getenv('DATABASE_PORT', '23506')),
        'user': os.getenv('DATABASE_USER', 'root'),
        'password': os.getenv('DATABASE_PASSWORD', 'mysql_Lujing2022'),
        'database': os.getenv('DATABASE_NAME', 'appraisal_test'),
        'charset': 'utf8mb4',
        'autocommit': True
    }

    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        print("检查G7-2025批次的JSON数据结构")
        print("=" * 80)

        # 获取一条区域级记录
        regional_query = """
        SELECT
            id,
            batch_code,
            aggregation_level,
            statistics_data,
            data_version
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025'
        AND aggregation_level = 'REGIONAL'
        LIMIT 1
        """

        cursor.execute(regional_query)
        regional_record = cursor.fetchone()

        if regional_record:
            print("区域级记录结构:")
            print("-" * 40)
            print(f"ID: {regional_record['id']}")
            print(f"批次代码: {regional_record['batch_code']}")
            print(f"聚合级别: {regional_record['aggregation_level']}")
            print(f"数据版本: {regional_record['data_version']}")

            # 解析JSON
            try:
                stats_data = json.loads(regional_record['statistics_data'])
                print("\nJSON结构分析:")
                print(f"顶级字段: {list(stats_data.keys())}")

                if 'subjects' in stats_data:
                    subjects = stats_data['subjects']
                    print(f"科目数量: {len(subjects)}")

                    # 分析第一个科目的结构
                    if subjects:
                        first_subject = subjects[0]
                        print(f"\n第一个科目结构:")
                        print(f"科目名称: {first_subject.get('subject_name', 'N/A')}")
                        print(f"主要字段: {list(first_subject.keys())}")

                        if 'metrics' in first_subject:
                            metrics = first_subject['metrics']
                            print(f"Metrics字段: {list(metrics.keys())}")
                            print("Metrics数值示例:")
                            for key, value in metrics.items():
                                if isinstance(value, (int, float)):
                                    print(f"  {key}: {value}")
                                elif value is None:
                                    print(f"  {key}: null")
                                else:
                                    print(f"  {key}: {type(value)} (非数值)")

                        # 检查增强字段
                        enhanced_fields = [
                            'percentiles', 'discrimination_index', 'grade_distribution',
                            'enhanced_fields'
                        ]
                        print("\n增强字段检查:")
                        for field in enhanced_fields:
                            exists = field in first_subject or (
                                'metrics' in first_subject and field in first_subject['metrics']
                            )
                            print(f"  {field}: {'✓' if exists else '✗'}")

                    # 显示所有科目的名称和基本信息
                    print("\n所有科目概览:")
                    for i, subject in enumerate(subjects):
                        name = subject.get('subject_name', f'科目{i+1}')
                        metrics = subject.get('metrics', {})
                        avg_score = metrics.get('avg', 'N/A')
                        student_count = metrics.get('student_count', 'N/A')
                        print(f"  {i+1}. {name}: 平均分={avg_score}, 学生数={student_count}")

            except json.JSONDecodeError as e:
                print(f"JSON解析失败: {e}")

        # 获取一条学校级记录
        print("\n" + "=" * 80)
        school_query = """
        SELECT
            id,
            batch_code,
            aggregation_level,
            school_id,
            statistics_data,
            data_version
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025'
        AND aggregation_level = 'SCHOOL'
        LIMIT 1
        """

        cursor.execute(school_query)
        school_record = cursor.fetchone()

        if school_record:
            print("学校级记录结构:")
            print("-" * 40)
            print(f"ID: {school_record['id']}")
            print(f"学校ID: {school_record['school_id']}")
            print(f"聚合级别: {school_record['aggregation_level']}")

            try:
                stats_data = json.loads(school_record['statistics_data'])
                print(f"\nJSON顶级字段: {list(stats_data.keys())}")

                if 'subjects' in stats_data:
                    subjects = stats_data['subjects']
                    print(f"科目数量: {len(subjects)}")

                    if subjects:
                        first_subject = subjects[0]
                        print(f"\n第一个科目字段: {list(first_subject.keys())}")

                        # 检查学校排名字段
                        school_ranking = first_subject.get('school_ranking')
                        print(f"学校排名字段: {school_ranking}")

                        if 'metrics' in first_subject:
                            metrics = first_subject['metrics']
                            print(f"Metrics字段: {list(metrics.keys())}")

            except json.JSONDecodeError as e:
                print(f"JSON解析失败: {e}")

        # 检查预聚合表的结构对比
        print("\n" + "=" * 80)
        print("预聚合表结构对比:")
        print("-" * 40)

        # 区域级对比
        core_metrics_query = """
        SELECT
            subject_name,
            avg_score,
            std_score,
            student_count,
            difficulty_coefficient
        FROM subject_core_metrics
        WHERE batch_code = 'G7-2025'
        LIMIT 3
        """

        cursor.execute(core_metrics_query)
        core_metrics = cursor.fetchall()

        print("subject_core_metrics 样本数据:")
        for record in core_metrics:
            print(f"  {record['subject_name']}: 平均分={record['avg_score']}, 学生数={record['student_count']}")

        # 学校级对比
        school_rankings_query = """
        SELECT
            school_code,
            subject_name,
            avg_score,
            `rank`,
            student_count
        FROM subject_school_rankings
        WHERE batch_code = 'G7-2025'
        LIMIT 5
        """

        cursor.execute(school_rankings_query)
        school_rankings = cursor.fetchall()

        print("\nsubject_school_rankings 样本数据:")
        for record in school_rankings:
            print(f"  学校{record['school_code']}-{record['subject_name']}: 平均分={record['avg_score']}, 排名={record['rank']}")

        cursor.close()
        connection.close()

        print(f"\n检查完成!")

    except Error as e:
        print(f"数据库错误: {e}")
    except Exception as e:
        print(f"其他错误: {e}")

if __name__ == "__main__":
    inspect_json_structure()