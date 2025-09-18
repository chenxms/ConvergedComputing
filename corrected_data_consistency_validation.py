#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修正版数据一致性校验脚本
根据实际的JSON数据结构进行验证
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
        logging.FileHandler(f'corrected_consistency_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CorrectedDataConsistencyValidator:
    """修正版数据一致性校验器"""

    def __init__(self, db_config: dict):
        """初始化数据库连接"""
        self.db_config = db_config
        self.connection = None
        self.tolerance = 0.01  # 增加容差到0.01，因为JSON中的数值可能有舍入
        self.validation_report = {
            'start_time': datetime.now(),
            'regional_consistency': {},
            'school_consistency': {},
            'json_field_validation': {},
            'data_completeness': {},
            'field_mapping_analysis': {},
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

    def analyze_field_mappings(self):
        """分析实际的字段映射关系"""
        logger.info("分析JSON字段映射关系...")

        # 获取区域级数据进行映射分析
        query = """
        SELECT statistics_data
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025'
        AND aggregation_level = 'REGIONAL'
        LIMIT 1
        """

        records = self.execute_query_dict(query)

        field_mappings = {
            'json_to_table': {},
            'available_json_fields': [],
            'missing_json_fields': []
        }

        if records:
            try:
                stats_data = json.loads(records[0]['statistics_data'])
                if 'subjects' in stats_data and stats_data['subjects']:
                    first_subject = stats_data['subjects'][0]
                    metrics = first_subject.get('metrics', {})

                    # 分析可用字段
                    field_mappings['available_json_fields'] = list(metrics.keys())

                    # 定义字段映射关系
                    field_mappings['json_to_table'] = {
                        'avg': 'avg_score',
                        'stddev': 'std_score',
                        'difficulty': 'difficulty_coefficient',
                        'max': 'max_score_achieved',
                        'min': 'min_score'
                    }

                    # 检查缺失的重要字段
                    expected_fields = ['student_count', 'score_rate']
                    field_mappings['missing_json_fields'] = [
                        field for field in expected_fields
                        if field not in metrics
                    ]

            except json.JSONDecodeError as e:
                logger.error(f"JSON解析失败: {e}")

        self.validation_report['field_mapping_analysis'] = field_mappings
        logger.info(f"字段映射分析完成: {field_mappings}")

    def validate_regional_consistency_corrected(self):
        """修正版区域级数据一致性验证"""
        logger.info("开始修正版区域级数据一致性验证...")

        # 获取预聚合表数据
        regional_metrics_query = """
        SELECT
            batch_code,
            subject_name,
            avg_score,
            std_score,
            student_count,
            difficulty_coefficient,
            max_score,
            min_score,
            max_score_achieved
        FROM subject_core_metrics
        WHERE batch_code = 'G7-2025'
        ORDER BY subject_name
        """

        regional_metrics = self.execute_query_dict(regional_metrics_query)
        logger.info(f"预聚合表区域级记录数: {len(regional_metrics)}")

        # 获取汇聚结果数据
        aggregation_query = """
        SELECT
            batch_code,
            aggregation_level,
            statistics_data,
            data_version
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025'
        AND aggregation_level = 'REGIONAL'
        """

        aggregation_data = self.execute_query_dict(aggregation_query)
        logger.info(f"汇聚结果区域级记录数: {len(aggregation_data)}")

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

                    # 使用修正的字段映射进行比较
                    comparisons = [
                        ('avg_score', metrics.get('avg'), matching_metric['avg_score']),
                        ('std_score', metrics.get('stddev'), matching_metric['std_score']),
                        ('difficulty_coefficient', metrics.get('difficulty'), matching_metric['difficulty_coefficient']),
                        ('max_score_achieved', metrics.get('max'), matching_metric['max_score_achieved']),
                        ('min_score', metrics.get('min'), matching_metric['min_score'])
                    ]

                    for field_name, json_value, table_value in comparisons:
                        if json_value is None and table_value is None:
                            continue

                        if json_value is None or table_value is None:
                            inconsistencies.append({
                                'type': 'null_value_mismatch',
                                'subject': subject_name,
                                'field': field_name,
                                'json_value': json_value,
                                'table_value': table_value
                            })
                            continue

                        # 浮点数容差比较
                        if abs(float(json_value) - float(table_value)) > self.tolerance:
                            inconsistencies.append({
                                'type': 'value_mismatch',
                                'subject': subject_name,
                                'field': field_name,
                                'json_value': json_value,
                                'table_value': table_value,
                                'difference': abs(float(json_value) - float(table_value))
                            })

                    # 检查缺失字段（student_count在JSON中不存在）
                    if 'student_count' not in metrics:
                        inconsistencies.append({
                            'type': 'missing_json_field',
                            'subject': subject_name,
                            'field': 'student_count',
                            'table_value': matching_metric['student_count'],
                            'description': 'JSON中缺少student_count字段'
                        })

            except json.JSONDecodeError as e:
                inconsistencies.append({
                    'type': 'json_parse_error',
                    'description': f'JSON解析失败: {e}'
                })

        self.validation_report['regional_consistency'] = {
            'metrics_count': len(regional_metrics),
            'aggregation_count': len(aggregation_data),
            'inconsistencies_count': len(inconsistencies),
            'inconsistencies': inconsistencies
        }

        logger.info(f"修正版区域级验证完成，发现 {len(inconsistencies)} 个问题")

    def validate_school_consistency_corrected(self):
        """修正版学校级数据一致性验证"""
        logger.info("开始修正版学校级数据一致性验证...")

        # 随机选择5所学校和3个科目（减少数量便于调试）
        school_query = """
        SELECT DISTINCT school_code
        FROM subject_school_rankings
        WHERE batch_code = 'G7-2025'
        ORDER BY RAND()
        LIMIT 5
        """

        subject_query = """
        SELECT DISTINCT subject_name
        FROM subject_school_rankings
        WHERE batch_code = 'G7-2025'
        ORDER BY RAND()
        LIMIT 3
        """

        sample_schools = [row['school_code'] for row in self.execute_query_dict(school_query)]
        sample_subjects = [row['subject_name'] for row in self.execute_query_dict(subject_query)]

        logger.info(f"随机选择的学校: {sample_schools}")
        logger.info(f"随机选择的科目: {sample_subjects}")

        # 获取预聚合数据
        placeholders = ','.join(['%s'] * len(sample_schools))
        subject_placeholders = ','.join(['%s'] * len(sample_subjects))

        school_rankings_query = f"""
        SELECT
            school_code,
            subject_name,
            avg_score,
            `rank`,
            student_count
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

        # 获取汇聚结果数据
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

                    # 字段比较
                    comparisons = [
                        ('avg_score', metrics.get('avg'), matching_ranking['avg_score']),
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

                        # 排名字段精确比较，分数字段容差比较
                        if field_name == 'rank':
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

        logger.info(f"修正版学校级验证完成，发现 {len(inconsistencies)} 个问题")

    def validate_data_versions(self):
        """验证数据版本一致性"""
        logger.info("开始验证数据版本...")

        query = """
        SELECT
            aggregation_level,
            data_version,
            COUNT(*) as count
        FROM statistical_aggregations
        WHERE batch_code = 'G7-2025'
        GROUP BY aggregation_level, data_version
        ORDER BY aggregation_level, data_version
        """

        version_stats = self.execute_query_dict(query)

        version_issues = []
        for stat in version_stats:
            if stat['data_version'] != 'v1.2':
                version_issues.append({
                    'type': 'incorrect_version',
                    'aggregation_level': stat['aggregation_level'],
                    'version': stat['data_version'],
                    'count': stat['count']
                })

        self.validation_report['data_versions'] = {
            'version_stats': version_stats,
            'version_issues': version_issues
        }

        logger.info(f"数据版本验证完成，发现 {len(version_issues)} 个版本问题")

    def generate_summary_report(self):
        """生成汇总报告"""
        regional_issues = len(self.validation_report['regional_consistency'].get('inconsistencies', []))
        school_issues = len(self.validation_report['school_consistency'].get('inconsistencies', []))
        version_issues = len(self.validation_report.get('data_versions', {}).get('version_issues', []))

        total_issues = regional_issues + school_issues + version_issues

        self.validation_report['summary'] = {
            'end_time': datetime.now(),
            'duration': str(datetime.now() - self.validation_report['start_time']),
            'total_issues': total_issues,
            'regional_issues': regional_issues,
            'school_issues': school_issues,
            'version_issues': version_issues,
            'status': 'PASS' if total_issues == 0 else 'ISSUES_FOUND',
            'field_mapping_status': 'ANALYZED'
        }

        # 收集所有问题
        all_issues = []
        all_issues.extend(self.validation_report['regional_consistency'].get('inconsistencies', []))
        all_issues.extend(self.validation_report['school_consistency'].get('inconsistencies', []))
        all_issues.extend(self.validation_report.get('data_versions', {}).get('version_issues', []))

        self.validation_report['inconsistencies'] = all_issues

    def save_report(self, filename: str = None):
        """保存验证报告"""
        if filename is None:
            filename = f"corrected_consistency_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.validation_report, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"修正版验证报告已保存到: {filename}")
        return filename

    def print_summary(self):
        """打印验证摘要"""
        summary = self.validation_report['summary']
        field_mapping = self.validation_report['field_mapping_analysis']

        print("\n" + "="*80)
        print("修正版数据一致性校验报告摘要")
        print("="*80)
        print(f"开始时间: {self.validation_report['start_time']}")
        print(f"结束时间: {summary['end_time']}")
        print(f"耗时: {summary['duration']}")
        print(f"总体状态: {summary['status']}")
        print("-"*80)
        print(f"总问题数: {summary['total_issues']}")
        print(f"  - 区域级问题: {summary['regional_issues']}")
        print(f"  - 学校级问题: {summary['school_issues']}")
        print(f"  - 版本问题: {summary['version_issues']}")

        print("-"*80)
        print("字段映射分析:")
        print(f"JSON可用字段: {field_mapping.get('available_json_fields', [])}")
        print(f"字段映射关系: {field_mapping.get('json_to_table', {})}")
        print(f"JSON缺失字段: {field_mapping.get('missing_json_fields', [])}")

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
    import os

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

    # 创建验证器
    validator = CorrectedDataConsistencyValidator(db_config)

    try:
        # 连接数据库
        validator.connect_database()

        logger.info("开始执行修正版数据一致性校验...")

        # 1. 分析字段映射关系
        validator.analyze_field_mappings()

        # 2. 区域级数据一致性验证（修正版）
        validator.validate_regional_consistency_corrected()

        # 3. 学校级数据一致性验证（修正版）
        validator.validate_school_consistency_corrected()

        # 4. 数据版本验证
        validator.validate_data_versions()

        # 5. 生成汇总报告
        validator.generate_summary_report()

        # 6. 保存和显示结果
        report_file = validator.save_report()
        validator.print_summary()

        logger.info(f"修正版数据一致性校验完成，详细报告: {report_file}")

    except Exception as e:
        logger.error(f"验证过程中发生错误: {e}")
        raise
    finally:
        validator.close_connection()


if __name__ == "__main__":
    main()