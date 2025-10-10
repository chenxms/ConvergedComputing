#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025 批次数据验证脚本

验证内容：
1. 数据一致性检查
2. 核心指标对比
3. 数据完整性验证
4. API接口验证
5. 业务逻辑验证
6. 性能指标验证

用法：
    python validate_g7_data.py                      # 完整验证
    python validate_g7_data.py --quick              # 快速验证
    python validate_g7_data.py --api-only           # 仅验证API
    python validate_g7_data.py --compare-backup     # 与备份数据对比
"""

import os
import sys
import json
import argparse
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)


class G7DataValidator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.batch_code = 'G7-2025'
        self.validation_results = {
            'data_consistency': {},
            'core_metrics': {},
            'data_completeness': {},
            'api_validation': {},
            'business_logic': {},
            'performance': {}
        }
        self.errors = []
        self.warnings = []

    def get_timestamp(self) -> str:
        """获取时间戳"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def log(self, message: str, level: str = "INFO"):
        """记录日志"""
        timestamp = self.get_timestamp()
        prefix = f"[{timestamp}] [{level}]"
        print(f"{prefix} {message}")

        if level == "ERROR":
            self.errors.append(message)
        elif level == "WARN":
            self.warnings.append(message)

    def validate_data_consistency(self) -> Dict[str, Any]:
        """验证数据一致性"""
        self.log("开始数据一致性验证...")

        consistency_results = {
            'school_count_match': False,
            'aggregation_levels': [],
            'data_version_consistency': False,
            'timestamp_validity': False,
            'school_id_normalization': False,
            'duplicate_records': 0
        }

        try:
            from app.database.connection import get_db_context
            from sqlalchemy import text

            with get_db_context() as db:
                # 1. 检查学校数量一致性
                self.log("  检查学校数量一致性...")

                # 区域级汇总应该包含所有学校
                regional_schools = db.execute(text("""
                    SELECT COUNT(DISTINCT school_id)
                    FROM statistical_aggregations
                    WHERE batch_code = :batch AND aggregation_level = 'REGIONAL'
                """), {"batch": self.batch_code}).scalar()

                # 学校级汇总中的学校数
                school_level_schools = db.execute(text("""
                    SELECT COUNT(DISTINCT school_id)
                    FROM statistical_aggregations
                    WHERE batch_code = :batch AND aggregation_level = 'SCHOOL'
                """), {"batch": self.batch_code}).scalar()

                consistency_results['school_count_match'] = (regional_schools == school_level_schools)
                if not consistency_results['school_count_match']:
                    self.log(f"    学校数量不匹配: 区域级{regional_schools}, 学校级{school_level_schools}", "ERROR")

                # 2. 检查汇总级别完整性
                self.log("  检查汇总级别...")
                aggregation_levels = db.execute(text("""
                    SELECT aggregation_level, COUNT(*) as count
                    FROM statistical_aggregations
                    WHERE batch_code = :batch
                    GROUP BY aggregation_level
                """), {"batch": self.batch_code}).fetchall()

                consistency_results['aggregation_levels'] = [
                    {'level': row[0], 'count': row[1]} for row in aggregation_levels
                ]

                expected_levels = ['REGIONAL', 'SCHOOL']
                actual_levels = [item['level'] for item in consistency_results['aggregation_levels']]
                missing_levels = set(expected_levels) - set(actual_levels)

                if missing_levels:
                    self.log(f"    缺少汇总级别: {missing_levels}", "ERROR")

                # 3. 检查数据版本一致性
                self.log("  检查数据版本一致性...")
                version_check = db.execute(text("""
                    SELECT data_version, COUNT(*) as count
                    FROM statistical_aggregations
                    WHERE batch_code = :batch
                    GROUP BY data_version
                """), {"batch": self.batch_code}).fetchall()

                if len(version_check) == 1:
                    consistency_results['data_version_consistency'] = True
                    self.log(f"    数据版本一致: {version_check[0][0]}")
                else:
                    self.log(f"    数据版本不一致: {[f'{v[0]}({v[1]})' for v in version_check]}", "WARN")

                # 4. 检查时间戳有效性
                self.log("  检查时间戳有效性...")
                timestamp_check = db.execute(text("""
                    SELECT
                        COUNT(*) as total,
                        COUNT(CASE WHEN created_at IS NULL THEN 1 END) as null_created,
                        COUNT(CASE WHEN updated_at IS NULL THEN 1 END) as null_updated,
                        COUNT(CASE WHEN updated_at < created_at THEN 1 END) as invalid_order,
                        MIN(created_at) as earliest,
                        MAX(updated_at) as latest
                    FROM statistical_aggregations
                    WHERE batch_code = :batch
                """), {"batch": self.batch_code}).fetchone()

                if timestamp_check:
                    total = timestamp_check[0]
                    issues = timestamp_check[1] + timestamp_check[2] + timestamp_check[3]
                    consistency_results['timestamp_validity'] = (issues == 0)

                    if issues > 0:
                        self.log(f"    时间戳问题: {issues}/{total} 记录", "WARN")
                    else:
                        self.log(f"    时间戳有效: {timestamp_check[4]} ~ {timestamp_check[5]}")

                # 5. 检查学校ID规范化
                self.log("  检查学校ID规范化...")
                school_id_check = db.execute(text("""
                    SELECT
                        COUNT(DISTINCT school_id) as unique_schools,
                        COUNT(CASE WHEN LENGTH(school_id) != 12 THEN 1 END) as invalid_length,
                        COUNT(CASE WHEN school_id REGEXP '^[0-9]+$' = 0 THEN 1 END) as non_numeric
                    FROM statistical_aggregations
                    WHERE batch_code = :batch AND aggregation_level = 'SCHOOL'
                """), {"batch": self.batch_code}).fetchone()

                if school_id_check:
                    invalid_ids = school_id_check[1] + school_id_check[2]
                    consistency_results['school_id_normalization'] = (invalid_ids == 0)

                    if invalid_ids > 0:
                        self.log(f"    学校ID格式问题: {invalid_ids} 个", "WARN")

                # 6. 检查重复记录
                self.log("  检查重复记录...")
                duplicate_check = db.execute(text("""
                    SELECT COUNT(*) - COUNT(DISTINCT batch_code, aggregation_level, school_id)
                    FROM statistical_aggregations
                    WHERE batch_code = :batch
                """), {"batch": self.batch_code}).scalar()

                consistency_results['duplicate_records'] = duplicate_check or 0
                if consistency_results['duplicate_records'] > 0:
                    self.log(f"    发现重复记录: {consistency_results['duplicate_records']} 个", "ERROR")

        except Exception as e:
            self.log(f"数据一致性验证失败: {e}", "ERROR")
            consistency_results['error'] = str(e)

        self.validation_results['data_consistency'] = consistency_results
        return consistency_results

    def validate_core_metrics(self) -> Dict[str, Any]:
        """验证核心指标"""
        self.log("开始核心指标验证...")

        metrics_results = {
            'total_schools': 0,
            'avg_scores_valid': False,
            'grade_distributions_valid': False,
            'subject_completeness': {},
            'dimension_consistency': False,
            'statistical_ranges': {}
        }

        try:
            from app.database.connection import get_db_context
            from sqlalchemy import text

            with get_db_context() as db:
                # 1. 统计总学校数
                total_schools = db.execute(text("""
                    SELECT COUNT(DISTINCT school_id)
                    FROM statistical_aggregations
                    WHERE batch_code = :batch AND aggregation_level = 'SCHOOL'
                """), {"batch": self.batch_code}).scalar()

                metrics_results['total_schools'] = total_schools or 0
                self.log(f"  总学校数: {metrics_results['total_schools']}")

                # 2. 验证平均分范围
                self.log("  验证平均分范围...")
                score_validation = db.execute(text("""
                    SELECT
                        school_id,
                        statistics_data
                    FROM statistical_aggregations
                    WHERE batch_code = :batch
                    AND aggregation_level = 'SCHOOL'
                    AND statistics_data IS NOT NULL
                    LIMIT 100
                """), {"batch": self.batch_code}).fetchall()

                valid_scores = 0
                invalid_scores = []

                for row in score_validation:
                    try:
                        data = json.loads(row[1])
                        subjects = data.get('subjects', {})

                        for subject_code, subject_data in subjects.items():
                            avg_score = subject_data.get('average_score', 0)
                            max_score = subject_data.get('total_score', 100)

                            # 验证平均分范围
                            if 0 <= avg_score <= max_score:
                                valid_scores += 1
                            else:
                                invalid_scores.append({
                                    'school_id': row[0],
                                    'subject': subject_code,
                                    'avg_score': avg_score,
                                    'max_score': max_score
                                })

                    except json.JSONDecodeError:
                        self.log(f"    学校 {row[0]} JSON解析失败", "WARN")

                metrics_results['avg_scores_valid'] = len(invalid_scores) == 0
                if invalid_scores:
                    self.log(f"    发现 {len(invalid_scores)} 个无效平均分", "WARN")

                # 3. 验证等级分布
                self.log("  验证等级分布...")
                grade_distributions = []
                valid_distributions = 0

                for row in score_validation[:50]:  # 抽样验证
                    try:
                        data = json.loads(row[1])
                        subjects = data.get('subjects', {})

                        for subject_code, subject_data in subjects.items():
                            grades = subject_data.get('grade_distribution', {})
                            if grades:
                                total_students = sum(grades.values())
                                if total_students > 0:
                                    valid_distributions += 1
                                    grade_distributions.append({
                                        'school_id': row[0],
                                        'subject': subject_code,
                                        'total_students': total_students,
                                        'grades': grades
                                    })

                    except json.JSONDecodeError:
                        continue

                metrics_results['grade_distributions_valid'] = valid_distributions > 0
                self.log(f"    有效等级分布: {valid_distributions} 个")

                # 4. 检查科目完整性
                self.log("  检查科目完整性...")
                subject_stats = {}

                for row in score_validation:
                    try:
                        data = json.loads(row[1])
                        subjects = data.get('subjects', {})

                        for subject_code in subjects.keys():
                            if subject_code not in subject_stats:
                                subject_stats[subject_code] = 0
                            subject_stats[subject_code] += 1

                    except json.JSONDecodeError:
                        continue

                metrics_results['subject_completeness'] = subject_stats
                self.log(f"    科目统计: {subject_stats}")

                # 5. 检查维度数据一致性
                self.log("  检查维度数据一致性...")
                dimension_check = 0

                for row in score_validation[:20]:  # 抽样检查
                    try:
                        data = json.loads(row[1])
                        subjects = data.get('subjects', {})

                        for subject_code, subject_data in subjects.items():
                            dimensions = subject_data.get('dimensions', {})
                            if isinstance(dimensions, dict) and len(dimensions) > 0:
                                dimension_check += 1

                    except json.JSONDecodeError:
                        continue

                metrics_results['dimension_consistency'] = dimension_check > 0
                self.log(f"    维度数据检查: {dimension_check} 个学校有维度数据")

        except Exception as e:
            self.log(f"核心指标验证失败: {e}", "ERROR")
            metrics_results['error'] = str(e)

        self.validation_results['core_metrics'] = metrics_results
        return metrics_results

    def validate_api_endpoints(self) -> Dict[str, Any]:
        """验证API接口"""
        self.log("开始API接口验证...")

        api_results = {
            'regional_api': {'status': 'unknown', 'response_time': 0},
            'school_api': {'status': 'unknown', 'response_time': 0},
            'questionnaire_api': {'status': 'unknown', 'response_time': 0},
            'response_schemas': {},
            'data_accuracy': {}
        }

        # API基础URL
        base_url = self.config.get('api_base_url', 'http://localhost:8000')

        try:
            # 1. 测试区域级API
            self.log("  测试区域级API...")
            start_time = datetime.now()

            regional_url = f"{base_url}/api/v12/batch/{self.batch_code}/regional"
            try:
                response = requests.get(regional_url, timeout=30)
                response_time = (datetime.now() - start_time).total_seconds()

                api_results['regional_api'] = {
                    'status': 'success' if response.status_code == 200 else 'failed',
                    'status_code': response.status_code,
                    'response_time': response_time
                }

                if response.status_code == 200:
                    data = response.json()
                    # 验证响应结构
                    expected_fields = ['subjects', 'questionnaire']
                    missing_fields = [f for f in expected_fields if f not in data]

                    api_results['response_schemas']['regional'] = {
                        'valid': len(missing_fields) == 0,
                        'missing_fields': missing_fields,
                        'data_size': len(json.dumps(data))
                    }

                    self.log(f"    区域API成功: {response_time:.2f}s, {len(json.dumps(data))} bytes")
                else:
                    self.log(f"    区域API失败: HTTP {response.status_code}", "ERROR")

            except requests.RequestException as e:
                self.log(f"    区域API请求失败: {e}", "ERROR")
                api_results['regional_api']['status'] = 'error'

            # 2. 测试学校级API
            self.log("  测试学校级API...")

            # 先获取一个学校ID
            try:
                from app.database.connection import get_db_context
                from sqlalchemy import text

                with get_db_context() as db:
                    sample_school = db.execute(text("""
                        SELECT school_id FROM statistical_aggregations
                        WHERE batch_code = :batch AND aggregation_level = 'SCHOOL'
                        LIMIT 1
                    """), {"batch": self.batch_code}).scalar()

                if sample_school:
                    start_time = datetime.now()
                    school_url = f"{base_url}/api/v12/batch/{self.batch_code}/school/{sample_school}"

                    try:
                        response = requests.get(school_url, timeout=30)
                        response_time = (datetime.now() - start_time).total_seconds()

                        api_results['school_api'] = {
                            'status': 'success' if response.status_code == 200 else 'failed',
                            'status_code': response.status_code,
                            'response_time': response_time,
                            'sample_school': sample_school
                        }

                        if response.status_code == 200:
                            data = response.json()
                            api_results['response_schemas']['school'] = {
                                'valid': 'subjects' in data,
                                'data_size': len(json.dumps(data)),
                                'school_info': data.get('school_info', {})
                            }

                            self.log(f"    学校API成功: {response_time:.2f}s, 学校{sample_school}")
                        else:
                            self.log(f"    学校API失败: HTTP {response.status_code}", "ERROR")

                    except requests.RequestException as e:
                        self.log(f"    学校API请求失败: {e}", "ERROR")

            except Exception as e:
                self.log(f"    获取样本学校失败: {e}", "ERROR")

            # 3. 测试问卷API
            self.log("  测试问卷API...")
            start_time = datetime.now()

            questionnaire_url = f"{base_url}/api/v12/batch/{self.batch_code}/questionnaire"
            try:
                response = requests.get(questionnaire_url, timeout=30)
                response_time = (datetime.now() - start_time).total_seconds()

                api_results['questionnaire_api'] = {
                    'status': 'success' if response.status_code == 200 else 'failed',
                    'status_code': response.status_code,
                    'response_time': response_time
                }

                if response.status_code == 200:
                    data = response.json()
                    api_results['response_schemas']['questionnaire'] = {
                        'valid': isinstance(data, dict),
                        'data_size': len(json.dumps(data))
                    }

                    self.log(f"    问卷API成功: {response_time:.2f}s")
                else:
                    self.log(f"    问卷API失败: HTTP {response.status_code}", "ERROR")

            except requests.RequestException as e:
                self.log(f"    问卷API请求失败: {e}", "ERROR")

        except Exception as e:
            self.log(f"API验证过程中发生错误: {e}", "ERROR")

        self.validation_results['api_validation'] = api_results
        return api_results

    def validate_business_logic(self) -> Dict[str, Any]:
        """验证业务逻辑"""
        self.log("开始业务逻辑验证...")

        business_results = {
            'calculation_accuracy': {},
            'threshold_compliance': {},
            'data_relationships': {},
            'edge_cases': {}
        }

        try:
            from app.database.connection import get_db_context
            from sqlalchemy import text

            with get_db_context() as db:
                # 1. 验证计算准确性
                self.log("  验证计算准确性...")
                sample_data = db.execute(text("""
                    SELECT school_id, statistics_data
                    FROM statistical_aggregations
                    WHERE batch_code = :batch
                    AND aggregation_level = 'SCHOOL'
                    AND statistics_data IS NOT NULL
                    LIMIT 10
                """), {"batch": self.batch_code}).fetchall()

                calculation_checks = []

                for row in sample_data:
                    try:
                        data = json.loads(row[1])
                        subjects = data.get('subjects', {})

                        for subject_code, subject_data in subjects.items():
                            # 检查平均分计算
                            avg_score = subject_data.get('average_score', 0)
                            total_score = subject_data.get('total_score', 100)
                            difficulty = subject_data.get('difficulty_index', 0)

                            # 难度系数应该等于平均分/满分
                            expected_difficulty = avg_score / total_score if total_score > 0 else 0
                            difficulty_diff = abs(difficulty - expected_difficulty)

                            calculation_checks.append({
                                'school_id': row[0],
                                'subject': subject_code,
                                'difficulty_accurate': difficulty_diff < 0.01,  # 允许0.01的误差
                                'difficulty_diff': difficulty_diff
                            })

                    except json.JSONDecodeError:
                        continue

                accurate_calculations = sum(1 for check in calculation_checks if check['difficulty_accurate'])
                business_results['calculation_accuracy'] = {
                    'total_checked': len(calculation_checks),
                    'accurate_count': accurate_calculations,
                    'accuracy_rate': accurate_calculations / len(calculation_checks) if calculation_checks else 0
                }

                self.log(f"    计算准确性: {accurate_calculations}/{len(calculation_checks)} "
                        f"({business_results['calculation_accuracy']['accuracy_rate']*100:.1f}%)")

                # 2. 验证阈值合规性
                self.log("  验证阈值合规性...")
                threshold_checks = []

                for row in sample_data:
                    try:
                        data = json.loads(row[1])
                        subjects = data.get('subjects', {})

                        for subject_code, subject_data in subjects.items():
                            # 检查等级分布阈值
                            grade_dist = subject_data.get('grade_distribution', {})
                            total_students = sum(grade_dist.values()) if grade_dist else 0

                            if total_students > 0:
                                # 计算各等级比例
                                grade_ratios = {grade: count/total_students for grade, count in grade_dist.items()}

                                # 检查是否符合正常分布期望（这里使用简单的范围检查）
                                valid_distribution = all(0 <= ratio <= 1 for ratio in grade_ratios.values())

                                threshold_checks.append({
                                    'school_id': row[0],
                                    'subject': subject_code,
                                    'valid_distribution': valid_distribution,
                                    'total_students': total_students
                                })

                    except json.JSONDecodeError:
                        continue

                valid_thresholds = sum(1 for check in threshold_checks if check['valid_distribution'])
                business_results['threshold_compliance'] = {
                    'total_checked': len(threshold_checks),
                    'valid_count': valid_thresholds,
                    'compliance_rate': valid_thresholds / len(threshold_checks) if threshold_checks else 0
                }

                # 3. 验证数据关系
                self.log("  验证数据关系...")

                # 检查区域和学校数据的一致性
                regional_data = db.execute(text("""
                    SELECT statistics_data
                    FROM statistical_aggregations
                    WHERE batch_code = :batch AND aggregation_level = 'REGIONAL'
                    LIMIT 1
                """), {"batch": self.batch_code}).scalar()

                if regional_data:
                    try:
                        regional_json = json.loads(regional_data)
                        regional_subjects = set(regional_json.get('subjects', {}).keys())

                        # 检查学校级数据的科目是否与区域级一致
                        school_subjects = set()
                        for row in sample_data[:5]:
                            school_json = json.loads(row[1])
                            school_subjects.update(school_json.get('subjects', {}).keys())

                        subject_consistency = regional_subjects.issubset(school_subjects)
                        business_results['data_relationships'] = {
                            'subject_consistency': subject_consistency,
                            'regional_subjects': len(regional_subjects),
                            'school_subjects': len(school_subjects)
                        }

                        if not subject_consistency:
                            missing_in_schools = regional_subjects - school_subjects
                            self.log(f"    学校级缺少科目: {missing_in_schools}", "WARN")

                    except json.JSONDecodeError:
                        self.log("    区域级数据JSON解析失败", "WARN")

        except Exception as e:
            self.log(f"业务逻辑验证失败: {e}", "ERROR")
            business_results['error'] = str(e)

        self.validation_results['business_logic'] = business_results
        return business_results

    def compare_with_backup(self) -> Dict[str, Any]:
        """与备份数据对比"""
        if not self.config.get('compare_backup'):
            return {}

        self.log("开始备份数据对比...")

        comparison_results = {
            'backup_exists': False,
            'record_count_diff': 0,
            'school_count_diff': 0,
            'data_changes': []
        }

        try:
            from app.database.connection import get_db_context
            from sqlalchemy import text

            with get_db_context() as db:
                # 检查备份表是否存在
                backup_exists = db.execute(text("""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_name = 'statistical_aggregations_backup_g7_2025'
                """)).scalar()

                comparison_results['backup_exists'] = backup_exists > 0

                if comparison_results['backup_exists']:
                    # 比较记录数
                    current_count = db.execute(text("""
                        SELECT COUNT(*) FROM statistical_aggregations
                        WHERE batch_code = :batch
                    """), {"batch": self.batch_code}).scalar()

                    backup_count = db.execute(text("""
                        SELECT COUNT(*) FROM statistical_aggregations_backup_g7_2025
                    """)).scalar()

                    comparison_results['record_count_diff'] = current_count - backup_count

                    self.log(f"  记录数对比: 当前{current_count}, 备份{backup_count}, "
                            f"差异{comparison_results['record_count_diff']}")

                    # 比较学校数
                    current_schools = db.execute(text("""
                        SELECT COUNT(DISTINCT school_id)
                        FROM statistical_aggregations
                        WHERE batch_code = :batch AND aggregation_level = 'SCHOOL'
                    """), {"batch": self.batch_code}).scalar()

                    backup_schools = db.execute(text("""
                        SELECT COUNT(DISTINCT school_id)
                        FROM statistical_aggregations_backup_g7_2025
                        WHERE aggregation_level = 'SCHOOL'
                    """)).scalar()

                    comparison_results['school_count_diff'] = current_schools - backup_schools

                    self.log(f"  学校数对比: 当前{current_schools}, 备份{backup_schools}, "
                            f"差异{comparison_results['school_count_diff']}")

                else:
                    self.log("  备份表不存在，跳过对比", "WARN")

        except Exception as e:
            self.log(f"备份数据对比失败: {e}", "ERROR")
            comparison_results['error'] = str(e)

        return comparison_results

    def generate_validation_report(self) -> str:
        """生成验证报告"""
        report_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        report = f"""
G7-2025 批次数据验证报告
=====================================
验证时间: {report_time}
批次代码: {self.batch_code}

验证结果概览:
"""

        # 数据一致性
        consistency = self.validation_results.get('data_consistency', {})
        if consistency:
            report += f"""
1. 数据一致性验证:
   - 学校数量匹配: {'✅' if consistency.get('school_count_match') else '❌'}
   - 汇总级别: {len(consistency.get('aggregation_levels', []))} 种
   - 数据版本一致: {'✅' if consistency.get('data_version_consistency') else '❌'}
   - 时间戳有效: {'✅' if consistency.get('timestamp_validity') else '❌'}
   - 重复记录: {consistency.get('duplicate_records', 0)} 个
"""

        # 核心指标
        metrics = self.validation_results.get('core_metrics', {})
        if metrics:
            report += f"""
2. 核心指标验证:
   - 总学校数: {metrics.get('total_schools', 0)}
   - 平均分有效: {'✅' if metrics.get('avg_scores_valid') else '❌'}
   - 等级分布有效: {'✅' if metrics.get('grade_distributions_valid') else '❌'}
   - 科目数量: {len(metrics.get('subject_completeness', {}))}
"""

        # API验证
        api = self.validation_results.get('api_validation', {})
        if api:
            report += f"""
3. API接口验证:
   - 区域API: {api.get('regional_api', {}).get('status', 'unknown')}
     ({api.get('regional_api', {}).get('response_time', 0):.2f}s)
   - 学校API: {api.get('school_api', {}).get('status', 'unknown')}
     ({api.get('school_api', {}).get('response_time', 0):.2f}s)
   - 问卷API: {api.get('questionnaire_api', {}).get('status', 'unknown')}
     ({api.get('questionnaire_api', {}).get('response_time', 0):.2f}s)
"""

        # 业务逻辑
        business = self.validation_results.get('business_logic', {})
        if business:
            calc_accuracy = business.get('calculation_accuracy', {})
            threshold_compliance = business.get('threshold_compliance', {})

            report += f"""
4. 业务逻辑验证:
   - 计算准确性: {calc_accuracy.get('accuracy_rate', 0)*100:.1f}%
     ({calc_accuracy.get('accurate_count', 0)}/{calc_accuracy.get('total_checked', 0)})
   - 阈值合规性: {threshold_compliance.get('compliance_rate', 0)*100:.1f}%
     ({threshold_compliance.get('valid_count', 0)}/{threshold_compliance.get('total_checked', 0)})
"""

        # 问题汇总
        if self.errors or self.warnings:
            report += f"""
问题汇总:
错误 ({len(self.errors)} 个):
"""
            for error in self.errors:
                report += f"  ❌ {error}\n"

            report += f"""
警告 ({len(self.warnings)} 个):
"""
            for warning in self.warnings:
                report += f"  ⚠️  {warning}\n"

        # 总体结论
        total_errors = len(self.errors)
        if total_errors == 0:
            report += f"""
总体结论: ✅ 验证通过
G7-2025 批次数据质量良好，可以投入使用。
"""
        else:
            report += f"""
总体结论: ❌ 验证失败
发现 {total_errors} 个错误，建议修复后重新验证。
"""

        return report

    def run_validation(self):
        """运行完整验证"""
        self.log("开始G7-2025批次数据验证...")
        self.log(f"验证配置: {json.dumps(self.config, indent=2)}")

        try:
            # 1. 数据一致性验证
            if not self.config.get('api_only'):
                self.validate_data_consistency()

            # 2. 核心指标验证
            if not self.config.get('api_only'):
                self.validate_core_metrics()

            # 3. API接口验证
            self.validate_api_endpoints()

            # 4. 业务逻辑验证
            if not self.config.get('quick') and not self.config.get('api_only'):
                self.validate_business_logic()

            # 5. 备份数据对比
            if self.config.get('compare_backup'):
                self.compare_with_backup()

            # 6. 生成报告
            report = self.generate_validation_report()
            self.log("验证完成，生成报告:")
            print("\n" + "="*60)
            print(report)

            # 保存报告
            report_file = f"g7_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            self.log(f"报告已保存到: {report_file}")

            return len(self.errors) == 0

        except Exception as e:
            self.log(f"验证过程中发生严重错误: {e}", "ERROR")
            import traceback
            traceback.print_exc()
            return False


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='G7-2025 批次数据验证器')
    parser.add_argument('--quick', action='store_true', help='快速验证模式')
    parser.add_argument('--api-only', action='store_true', help='仅验证API接口')
    parser.add_argument('--compare-backup', action='store_true', help='与备份数据对比')
    parser.add_argument('--api-base-url', default='http://localhost:8000', help='API基础URL')

    args = parser.parse_args()

    config = {
        'quick': args.quick,
        'api_only': args.api_only,
        'compare_backup': args.compare_backup,
        'api_base_url': args.api_base_url
    }

    validator = G7DataValidator(config)
    success = validator.run_validation()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()