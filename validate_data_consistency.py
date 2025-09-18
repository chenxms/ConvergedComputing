#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据一致性校验脚本
验证预聚合表与汇聚结果的一致性
Author: PO测试方案执行
Date: 2025-09-18
"""

import json
import logging
import random
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Tuple, Any, Optional

import mysql.connector
import pandas as pd
from mysql.connector import Error

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'data_consistency_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataConsistencyValidator:
    """数据一致性校验器"""

    def __init__(self, db_config: dict):
        """初始化数据库连接"""
        self.db_config = db_config
        self.connection = None
        self.tolerance = 0.001  # 浮点数容差
        self.validation_report = {
            'start_time': datetime.now(),
            'regional_consistency': {},
            'school_consistency': {},
            'json_field_validation': {},
            'data_completeness': {},
            'random_sample_validation': {},
            'summary': {},
            'inconsistencies': []
        }

    def connect_database(self):
        """建立数据库连接"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            logger.info("数据库连接成功")
        except Error as e:
            logger.error(f"数据库连接失败: {e}")
            raise

    def close_connection(self):
        """关闭数据库连接"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("数据库连接已关闭")

    def execute_query(self, query: str, params=None) -> List[Tuple]:
        """执行SQL查询"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            cursor.close()
            return result
        except Error as e:
            logger.error(f"查询执行失败: {e}")
            logger.error(f"SQL: {query}")
            raise

    def execute_query_dict(self, query: str, params=None) -> List[Dict]:
        """执行SQL查询并返回字典格式结果"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            cursor.close()
            return result
        except Error as e:
            logger.error(f"查询执行失败: {e}")
            logger.error(f"SQL: {query}")
            raise

    def validate_regional_consistency(self):
        """验证区域级数据一致性"""
        logger.info("开始验证区域级数据一致性...")

        # 1. 获取预聚合表数据
        regional_metrics_query = """
        SELECT
            batch_code,
            subject_name,
            avg_score,
            std_score,
            student_count,
            difficulty_coefficient,
            max_score,
            created_at
        FROM subject_core_metrics
        WHERE batch_code = 'G7-2025'
        ORDER BY subject_name
        """

        regional_metrics = self.execute_query_dict(regional_metrics_query)
        logger.info(f"预聚合表区域级记录数: {len(regional_metrics)}")

        # 2. 获取汇聚结果数据（区域级）
        aggregation_query = """
        SELECT
            batch_code,
            aggregation_level,
            statistics_data,
            data_version,
            created_at
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025'
        AND aggregation_level = 'REGIONAL'
        """

        aggregation_data = self.execute_query_dict(aggregation_query)
        logger.info(f"汇聚结果区域级记录数: {len(aggregation_data)}")

        # 3. 解析JSON并进行对比
        inconsistencies = []

        for agg_record in aggregation_data:
            try:
                stats_data = json.loads(agg_record['statistics_data'])
                subjects = stats_data.get('subjects', [])

                for subject in subjects:
                    subject_name = subject.get('subject_name')
                    metrics = subject.get('metrics', {})

                    # 查找对应的预聚合记录
                    matching_metric = next(
                        (m for m in regional_metrics if m['subject_name'] == subject_name),
                        None
                    )

                    if not matching_metric:
                        inconsistencies.append({
                            'type': 'missing_regional_metric',
                            'subject': subject_name,
                            'description': '预聚合表中缺少对应记录'
                        })
                        continue

                    # 比较关键指标
                    comparisons = [
                        ('avg_score', metrics.get('avg'), matching_metric['avg_score']),
                        ('std_score', metrics.get('std'), matching_metric['std_score']),
                        ('student_count', metrics.get('student_count'), matching_metric['student_count']),
                        ('difficulty_coefficient', metrics.get('difficulty_coefficient'), matching_metric['difficulty_coefficient'])
                    ]

                    for field_name, json_value, table_value in comparisons:
                        if json_value is None or table_value is None:
                            inconsistencies.append({
                                'type': 'null_value',
                                'subject': subject_name,
                                'field': field_name,
                                'json_value': json_value,
                                'table_value': table_value
                            })
                            continue

                        # 对于计数字段，要求精确匹配
                        if field_name == 'student_count':
                            if int(json_value) != int(table_value):
                                inconsistencies.append({
                                    'type': 'value_mismatch',
                                    'subject': subject_name,
                                    'field': field_name,
                                    'json_value': json_value,
                                    'table_value': table_value,
                                    'difference': abs(int(json_value) - int(table_value))
                                })
                        else:
                            # 对于浮点数字段，使用容差比较
                            if abs(float(json_value) - float(table_value)) > self.tolerance:
                                inconsistencies.append({
                                    'type': 'value_mismatch',
                                    'subject': subject_name,
                                    'field': field_name,
                                    'json_value': json_value,
                                    'table_value': table_value,
                                    'difference': abs(float(json_value) - float(table_value))
                                })

            except json.JSONDecodeError as e:
                inconsistencies.append({
                    'type': 'json_parse_error',
                    'description': f'JSON解析失败: {e}',
                    'record_id': agg_record.get('id')
                })

        self.validation_report['regional_consistency'] = {
            'metrics_count': len(regional_metrics),
            'aggregation_count': len(aggregation_data),
            'inconsistencies_count': len(inconsistencies),
            'inconsistencies': inconsistencies
        }

        logger.info(f"区域级数据一致性验证完成，发现 {len(inconsistencies)} 个不一致项")

    def validate_school_consistency(self):
        """验证学校级数据一致性"""
        logger.info("开始验证学校级数据一致性...")

        # 1. 随机选择10所学校和3个科目
        school_query = """
        SELECT DISTINCT school_code
        FROM subject_school_rankings
        WHERE batch_code = 'G7-2025'
        ORDER BY RAND()
        LIMIT 10
        """

        subject_query = """
        SELECT DISTINCT subject_name
        FROM subject_school_rankings
        WHERE batch_code = 'G7-2025'
        ORDER BY RAND()
        LIMIT 3
        """

        sample_schools = [row[0] for row in self.execute_query(school_query)]
        sample_subjects = [row[0] for row in self.execute_query(subject_query)]

        logger.info(f"随机选择的学校: {sample_schools}")
        logger.info(f"随机选择的科目: {sample_subjects}")

        # 2. 获取这些学校和科目的预聚合数据
        placeholders = ','.join(['%s'] * len(sample_schools))
        subject_placeholders = ','.join(['%s'] * len(sample_subjects))

        school_rankings_query = f"""
        SELECT
            school_code,
            subject_name,
            avg_score,
            `rank`,
            student_count,
            std_score,
            difficulty_coefficient
        FROM subject_school_rankings
        WHERE batch_code = 'G7-2025'
        AND school_code IN ({placeholders})
        AND subject_name IN ({subject_placeholders})
        ORDER BY school_code, subject_name
        """

        school_rankings = self.execute_query_dict(
            school_rankings_query,
            sample_schools + sample_subjects
        )

        logger.info(f"预聚合表学校级记录数: {len(school_rankings)}")

        # 3. 获取对应的汇聚结果数据
        aggregation_query = f"""
        SELECT
            school_id,
            statistics_data,
            data_version
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025'
        AND aggregation_level = 'SCHOOL'
        AND school_id IN ({placeholders})
        """

        school_aggregations = self.execute_query_dict(aggregation_query, sample_schools)
        logger.info(f"汇聚结果学校级记录数: {len(school_aggregations)}")

        # 4. 进行详细对比
        inconsistencies = []

        for school_agg in school_aggregations:
            school_id = school_agg['school_id']

            try:
                stats_data = json.loads(school_agg['statistics_data'])
                subjects = stats_data.get('subjects', [])

                for subject in subjects:
                    subject_name = subject.get('subject_name')

                    # 只检查抽样的科目
                    if subject_name not in sample_subjects:
                        continue

                    # 查找对应的预聚合记录
                    matching_ranking = next(
                        (r for r in school_rankings
                         if r['school_code'] == school_id and r['subject_name'] == subject_name),
                        None
                    )

                    if not matching_ranking:
                        inconsistencies.append({
                            'type': 'missing_school_ranking',
                            'school_id': school_id,
                            'subject': subject_name,
                            'description': '预聚合表中缺少对应记录'
                        })
                        continue

                    # 比较关键字段
                    metrics = subject.get('metrics', {})
                    school_ranking = subject.get('school_ranking')

                    comparisons = [
                        ('avg_score', metrics.get('avg'), matching_ranking['avg_score']),
                        ('student_count', metrics.get('student_count'), matching_ranking['student_count']),
                        ('rank', school_ranking, matching_ranking['rank'])
                    ]

                    for field_name, json_value, table_value in comparisons:
                        if json_value is None or table_value is None:
                            inconsistencies.append({
                                'type': 'null_value',
                                'school_id': school_id,
                                'subject': subject_name,
                                'field': field_name,
                                'json_value': json_value,
                                'table_value': table_value
                            })
                            continue

                        # 精确比较或容差比较
                        if field_name in ['student_count', 'rank']:
                            if int(json_value) != int(table_value):
                                inconsistencies.append({
                                    'type': 'value_mismatch',
                                    'school_id': school_id,
                                    'subject': subject_name,
                                    'field': field_name,
                                    'json_value': json_value,
                                    'table_value': table_value
                                })
                        else:
                            if abs(float(json_value) - float(table_value)) > self.tolerance:
                                inconsistencies.append({
                                    'type': 'value_mismatch',
                                    'school_id': school_id,
                                    'subject': subject_name,
                                    'field': field_name,
                                    'json_value': json_value,
                                    'table_value': table_value,
                                    'difference': abs(float(json_value) - float(table_value))
                                })

            except json.JSONDecodeError as e:
                inconsistencies.append({
                    'type': 'json_parse_error',
                    'school_id': school_id,
                    'description': f'JSON解析失败: {e}'
                })

        self.validation_report['school_consistency'] = {
            'sample_schools': sample_schools,
            'sample_subjects': sample_subjects,
            'rankings_count': len(school_rankings),
            'aggregations_count': len(school_aggregations),
            'inconsistencies_count': len(inconsistencies),
            'inconsistencies': inconsistencies
        }

        logger.info(f"学校级数据一致性验证完成，发现 {len(inconsistencies)} 个不一致项")

    def validate_json_fields(self):
        """验证JSON字段解析和增强字段"""
        logger.info("开始验证JSON字段解析...")

        # 获取所有汇聚记录进行JSON字段验证
        json_validation_query = """
        SELECT
            id,
            batch_code,
            aggregation_level,
            school_id,
            statistics_data,
            data_version,
            created_at
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025'
        LIMIT 50
        """

        records = self.execute_query_dict(json_validation_query)
        logger.info(f"检查JSON字段的记录数: {len(records)}")

        json_issues = []
        schema_versions = {}

        for record in records:
            record_id = record['id']

            try:
                stats_data = json.loads(record['statistics_data'])

                # 检查data_version
                data_version = record['data_version']
                schema_versions[data_version] = schema_versions.get(data_version, 0) + 1

                if data_version != 'v1.2':
                    json_issues.append({
                        'type': 'incorrect_data_version',
                        'record_id': record_id,
                        'expected': 'v1.2',
                        'actual': data_version
                    })

                # 检查必要的顶级字段
                required_fields = ['batch_code', 'aggregation_level', 'subjects']
                for field in required_fields:
                    if field not in stats_data:
                        json_issues.append({
                            'type': 'missing_required_field',
                            'record_id': record_id,
                            'field': field
                        })

                # 检查subjects数组
                subjects = stats_data.get('subjects', [])
                if not isinstance(subjects, list):
                    json_issues.append({
                        'type': 'invalid_subjects_format',
                        'record_id': record_id,
                        'description': 'subjects应该是数组格式'
                    })
                    continue

                # 检查每个科目的字段结构
                for i, subject in enumerate(subjects):
                    subject_required_fields = ['subject_name', 'metrics']
                    for field in subject_required_fields:
                        if field not in subject:
                            json_issues.append({
                                'type': 'missing_subject_field',
                                'record_id': record_id,
                                'subject_index': i,
                                'field': field
                            })

                    # 检查metrics字段
                    metrics = subject.get('metrics', {})
                    if not isinstance(metrics, dict):
                        json_issues.append({
                            'type': 'invalid_metrics_format',
                            'record_id': record_id,
                            'subject_index': i,
                            'description': 'metrics应该是对象格式'
                        })
                        continue

                    # 检查增强字段存在性
                    enhanced_fields = [
                        'avg', 'std', 'student_count', 'difficulty_coefficient',
                        'percentiles', 'discrimination_index', 'grade_distribution'
                    ]

                    for field in enhanced_fields:
                        if field not in metrics:
                            json_issues.append({
                                'type': 'missing_enhanced_field',
                                'record_id': record_id,
                                'subject_index': i,
                                'subject_name': subject.get('subject_name'),
                                'field': field
                            })

            except json.JSONDecodeError as e:
                json_issues.append({
                    'type': 'json_parse_error',
                    'record_id': record_id,
                    'error': str(e)
                })
            except Exception as e:
                json_issues.append({
                    'type': 'unexpected_error',
                    'record_id': record_id,
                    'error': str(e)
                })

        self.validation_report['json_field_validation'] = {
            'total_records': len(records),
            'schema_versions': schema_versions,
            'json_issues_count': len(json_issues),
            'json_issues': json_issues
        }

        logger.info(f"JSON字段验证完成，发现 {len(json_issues)} 个问题")

    def validate_data_completeness(self):
        """验证数据完整性"""
        logger.info("开始验证数据完整性...")

        # 统计各表的记录数量
        counts_query = """
        SELECT
            'subject_core_metrics' as table_name,
            COUNT(*) as record_count
        FROM subject_core_metrics
        WHERE batch_code = 'G7-2025'

        UNION ALL

        SELECT
            'subject_school_rankings' as table_name,
            COUNT(*) as record_count
        FROM subject_school_rankings
        WHERE batch_code = 'G7-2025'

        UNION ALL

        SELECT
            'statistical_aggregations_regional' as table_name,
            COUNT(*) as record_count
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025' AND aggregation_level = 'REGIONAL'

        UNION ALL

        SELECT
            'statistical_aggregations_school' as table_name,
            COUNT(*) as record_count
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025' AND aggregation_level = 'SCHOOL'
        """

        count_results = self.execute_query_dict(counts_query)

        # 检查学校覆盖完整性
        school_coverage_query = """
        SELECT
            ssr.school_code,
            COUNT(DISTINCT ssr.subject_name) as subjects_in_rankings,
            CASE WHEN sa.school_id IS NOT NULL THEN 1 ELSE 0 END as has_aggregation
        FROM subject_school_rankings ssr
        LEFT JOIN statistical_aggregations sa ON ssr.school_code = sa.school_id
            AND sa.batch_code = 'G7-2025' AND sa.aggregation_level = 'SCHOOL'
        WHERE ssr.batch_code = 'G7-2025'
        GROUP BY ssr.school_code, sa.school_id
        HAVING subjects_in_rankings > 0
        ORDER BY subjects_in_rankings DESC
        """

        school_coverage = self.execute_query_dict(school_coverage_query)

        # 统计覆盖情况
        schools_with_aggregation = sum(1 for s in school_coverage if s['has_aggregation'])
        total_schools = len(school_coverage)
        coverage_rate = schools_with_aggregation / total_schools * 100 if total_schools > 0 else 0

        # 检查科目覆盖完整性
        subject_coverage_query = """
        SELECT
            scm.subject_name,
            scm.student_count as regional_student_count,
            COUNT(DISTINCT ssr.school_code) as schools_with_rankings
        FROM subject_core_metrics scm
        LEFT JOIN subject_school_rankings ssr ON scm.subject_name = ssr.subject_name
            AND ssr.batch_code = 'G7-2025'
        WHERE scm.batch_code = 'G7-2025'
        GROUP BY scm.subject_name, scm.student_count
        ORDER BY scm.subject_name
        """

        subject_coverage = self.execute_query_dict(subject_coverage_query)

        self.validation_report['data_completeness'] = {
            'table_counts': {result['table_name']: result['record_count'] for result in count_results},
            'school_coverage': {
                'total_schools': total_schools,
                'schools_with_aggregation': schools_with_aggregation,
                'coverage_rate': round(coverage_rate, 2)
            },
            'subject_coverage': subject_coverage
        }

        logger.info(f"数据完整性验证完成，学校覆盖率: {coverage_rate:.2f}%")

    def generate_summary_report(self):
        """生成汇总报告"""
        regional_issues = len(self.validation_report['regional_consistency'].get('inconsistencies', []))
        school_issues = len(self.validation_report['school_consistency'].get('inconsistencies', []))
        json_issues = len(self.validation_report['json_field_validation'].get('json_issues', []))

        total_issues = regional_issues + school_issues + json_issues

        # 计算通过率
        regional_metrics_count = self.validation_report['regional_consistency'].get('metrics_count', 0)
        school_rankings_count = self.validation_report['school_consistency'].get('rankings_count', 0)
        json_records_count = self.validation_report['json_field_validation'].get('total_records', 0)

        total_validations = regional_metrics_count + school_rankings_count + json_records_count
        pass_rate = ((total_validations - total_issues) / total_validations * 100) if total_validations > 0 else 0

        self.validation_report['summary'] = {
            'end_time': datetime.now(),
            'duration': str(datetime.now() - self.validation_report['start_time']),
            'total_issues': total_issues,
            'regional_issues': regional_issues,
            'school_issues': school_issues,
            'json_issues': json_issues,
            'total_validations': total_validations,
            'pass_rate': round(pass_rate, 2),
            'status': 'PASS' if total_issues == 0 else 'FAIL'
        }

        # 收集所有不一致项
        all_inconsistencies = []
        all_inconsistencies.extend(self.validation_report['regional_consistency'].get('inconsistencies', []))
        all_inconsistencies.extend(self.validation_report['school_consistency'].get('inconsistencies', []))
        all_inconsistencies.extend(self.validation_report['json_field_validation'].get('json_issues', []))

        self.validation_report['inconsistencies'] = all_inconsistencies

    def save_report(self, filename: str = None):
        """保存验证报告"""
        if filename is None:
            filename = f"data_consistency_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.validation_report, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"验证报告已保存到: {filename}")
        return filename

    def print_summary(self):
        """打印验证摘要"""
        summary = self.validation_report['summary']

        print("\n" + "="*80)
        print("数据一致性校验报告摘要")
        print("="*80)
        print(f"开始时间: {self.validation_report['start_time']}")
        print(f"结束时间: {summary['end_time']}")
        print(f"耗时: {summary['duration']}")
        print(f"总体状态: {summary['status']}")
        print(f"通过率: {summary['pass_rate']}%")
        print("-"*80)
        print(f"总验证项: {summary['total_validations']}")
        print(f"总问题数: {summary['total_issues']}")
        print(f"  - 区域级问题: {summary['regional_issues']}")
        print(f"  - 学校级问题: {summary['school_issues']}")
        print(f"  - JSON字段问题: {summary['json_issues']}")

        # 数据完整性信息
        completeness = self.validation_report['data_completeness']
        print("-"*80)
        print("数据完整性统计:")
        for table_name, count in completeness['table_counts'].items():
            print(f"  {table_name}: {count} 条记录")

        coverage = completeness['school_coverage']
        print(f"学校覆盖率: {coverage['schools_with_aggregation']}/{coverage['total_schools']} ({coverage['coverage_rate']}%)")

        if summary['total_issues'] > 0:
            print("-"*80)
            print("主要问题类型:")
            issue_types = {}
            for issue in self.validation_report['inconsistencies']:
                issue_type = issue.get('type', 'unknown')
                issue_types[issue_type] = issue_types.get(issue_type, 0) + 1

            for issue_type, count in sorted(issue_types.items()):
                print(f"  {issue_type}: {count} 个")

        print("="*80)


def main():
    """主函数"""
    # 数据库配置 - 使用环境变量或默认值
    import os

    db_config = {
        'host': os.getenv('DATABASE_HOST', '117.72.14.166'),
        'port': int(os.getenv('DATABASE_PORT', '23506')),
        'user': os.getenv('DATABASE_USER', 'root'),
        'password': os.getenv('DATABASE_PASSWORD', 'mysql_Lujing2022'),
        'database': os.getenv('DATABASE_NAME', 'appraisal_test'),
        'charset': 'utf8mb4',
        'autocommit': True
    }

    # 创建验证器
    validator = DataConsistencyValidator(db_config)

    try:
        # 连接数据库
        validator.connect_database()

        # 执行各项验证
        logger.info("开始执行数据一致性校验...")

        # 1. 区域级数据一致性验证
        validator.validate_regional_consistency()

        # 2. 学校级数据一致性验证（随机抽样）
        validator.validate_school_consistency()

        # 3. JSON字段解析验证
        validator.validate_json_fields()

        # 4. 数据完整性检查
        validator.validate_data_completeness()

        # 5. 生成汇总报告
        validator.generate_summary_report()

        # 6. 保存和显示结果
        report_file = validator.save_report()
        validator.print_summary()

        logger.info(f"数据一致性校验完成，详细报告: {report_file}")

    except Exception as e:
        logger.error(f"验证过程中发生错误: {e}")
        raise
    finally:
        validator.close_connection()


if __name__ == "__main__":
    main()