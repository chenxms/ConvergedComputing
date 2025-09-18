#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据质量监控工具 - 完整的数据质量检查和预警系统
基于G4数据问题的经验教训设计，防止类似问题再次发生
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from sqlalchemy import text
from sqlalchemy.orm import Session

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database.connection import get_db
from app.utils.data_validation import DataValidator

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_quality_monitor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class QualityCheckResult:
    """质量检查结果数据类"""
    check_type: str
    status: str  # PASS, WARN, FAIL
    message: str
    details: Dict[str, Any]
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    recommendations: List[str]
    affected_count: int = 0


@dataclass
class BatchQualityReport:
    """批次质量报告数据类"""
    batch_code: str
    check_timestamp: datetime
    overall_status: str  # PASS, WARN, FAIL
    total_checks: int
    passed_checks: int
    warned_checks: int
    failed_checks: int
    critical_issues: int
    results: List[QualityCheckResult]
    recommendations: List[str]
    summary: Dict[str, Any]


class DataQualityMonitor:
    """数据质量监控器 - 多层次质量检查"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.validator = DataValidator(db_session)
        self.quality_thresholds = {
            'max_school_count_deviation': 0.05,  # 5%偏差阈值
            'max_null_school_names': 0,  # 零容忍NULL学校名称
            'min_student_per_school': 10,  # 每校最少学生数
            'max_orphaned_schools': 0,  # 零容忍孤立学校
            'min_data_completeness': 0.95  # 95%数据完整性要求
        }
    
    def run_comprehensive_check(self, batch_code: str) -> BatchQualityReport:
        """运行全面的数据质量检查"""
        logger.info(f"开始对批次 {batch_code} 进行全面质量检查")
        
        check_results = []
        start_time = datetime.now()
        
        # 1. 基础数据存在性检查
        check_results.append(self._check_basic_data_existence(batch_code))
        
        # 2. 学校数据一致性检查（最关键）
        check_results.append(self._check_school_data_consistency(batch_code))
        
        # 3. 学校名称完整性检查（防止NULL问题）
        check_results.append(self._check_school_name_completeness(batch_code))
        
        # 4. 数据源统一性检查
        check_results.append(self._check_data_source_consistency(batch_code))
        
        # 5. 学生数据分布检查
        check_results.append(self._check_student_data_distribution(batch_code))
        
        # 6. 历史数据对比检查
        check_results.append(self._check_historical_data_comparison(batch_code))
        
        # 7. 数据完整性检查
        check_results.append(self._check_data_completeness(batch_code))
        
        # 8. 性能影响预估
        check_results.append(self._check_performance_impact(batch_code))
        
        # 生成报告
        report = self._generate_quality_report(batch_code, check_results, start_time)
        
        # 保存报告
        self._save_quality_report(report)
        
        # 如果有关键问题，发出警告
        if report.critical_issues > 0:
            self._send_critical_alert(report)
        
        logger.info(f"批次 {batch_code} 质量检查完成 - 状态: {report.overall_status}")
        return report
    
    def _check_basic_data_existence(self, batch_code: str) -> QualityCheckResult:
        """检查基础数据是否存在"""
        try:
            # 检查主要数据表是否有数据
            checks = {}
            
            # school_master_data
            query = text("SELECT COUNT(*) FROM school_master_data WHERE batch_code = :batch_code AND status = 'ACTIVE'")
            checks['school_master_data'] = self.db_session.execute(query, {'batch_code': batch_code}).scalar() or 0
            
            # student_cleaned_scores
            query = text("SELECT COUNT(*) FROM student_cleaned_scores WHERE batch_code = :batch_code")
            checks['student_cleaned_scores'] = self.db_session.execute(query, {'batch_code': batch_code}).scalar() or 0
            
            # student_score_detail
            query = text("SELECT COUNT(*) FROM student_score_detail WHERE batch_code = :batch_code")
            checks['student_score_detail'] = self.db_session.execute(query, {'batch_code': batch_code}).scalar() or 0
            
            missing_tables = [table for table, count in checks.items() if count == 0]
            
            if missing_tables:
                return QualityCheckResult(
                    check_type="BASIC_DATA_EXISTENCE",
                    status="FAIL",
                    message=f"缺少必要的数据表数据: {', '.join(missing_tables)}",
                    details=checks,
                    severity="CRITICAL",
                    recommendations=[
                        "检查数据导入是否完成",
                        "验证批次代码是否正确",
                        "重新运行数据清洗流程"
                    ],
                    affected_count=len(missing_tables)
                )
            
            return QualityCheckResult(
                check_type="BASIC_DATA_EXISTENCE",
                status="PASS",
                message="所有必要的数据表都有数据",
                details=checks,
                severity="LOW",
                recommendations=[],
                affected_count=0
            )
            
        except Exception as e:
            logger.error(f"基础数据存在性检查失败: {e}")
            return QualityCheckResult(
                check_type="BASIC_DATA_EXISTENCE",
                status="FAIL",
                message=f"检查过程出错: {str(e)}",
                details={},
                severity="CRITICAL",
                recommendations=["检查数据库连接", "重新运行检查"],
                affected_count=1
            )
    
    def _check_school_data_consistency(self, batch_code: str) -> QualityCheckResult:
        """检查学校数据一致性（最关键的检查）"""
        try:
            validation_result = self.validator.validate_batch_school_consistency(batch_code)
            
            if not validation_result['is_valid']:
                # 关键错误 - 立即阻断
                error_details = {
                    'validation_result': validation_result,
                    'orphaned_schools': validation_result['statistics'].get('orphaned_schools', 0),
                    'master_schools': validation_result['statistics'].get('master_schools', 0)
                }
                
                recommendations = [
                    "立即停止处理此批次数据！",
                    "修复school_master_data中缺失的学校记录",
                    "使用DataValidator.enforce_master_data_constraint()清理不匹配数据",
                    "重新运行数据清洗流程",
                    "验证所有学校都在school_master_data中"
                ]
                
                return QualityCheckResult(
                    check_type="SCHOOL_DATA_CONSISTENCY",
                    status="FAIL",
                    message=f"发现 {validation_result['statistics'].get('orphaned_schools', 0)} 所孤立学校",
                    details=error_details,
                    severity="CRITICAL",
                    recommendations=recommendations,
                    affected_count=validation_result['statistics'].get('orphaned_schools', 0)
                )
            
            # 检查警告项
            warnings = validation_result.get('warnings', [])
            if warnings:
                return QualityCheckResult(
                    check_type="SCHOOL_DATA_CONSISTENCY",
                    status="WARN",
                    message=f"学校数据有 {len(warnings)} 个警告项",
                    details=validation_result,
                    severity="MEDIUM",
                    recommendations=[
                        "检查学校名称标准化",
                        "验证school_id格式规范",
                        "统一数据源命名"
                    ],
                    affected_count=len(warnings)
                )
            
            return QualityCheckResult(
                check_type="SCHOOL_DATA_CONSISTENCY",
                status="PASS",
                message="学校数据一致性检查通过",
                details=validation_result,
                severity="LOW",
                recommendations=[],
                affected_count=0
            )
            
        except Exception as e:
            logger.error(f"学校数据一致性检查失败: {e}")
            return QualityCheckResult(
                check_type="SCHOOL_DATA_CONSISTENCY",
                status="FAIL",
                message=f"检查过程出错: {str(e)}",
                details={},
                severity="CRITICAL",
                recommendations=["检查数据库连接", "重新运行检查"],
                affected_count=1
            )
    
    def _check_school_name_completeness(self, batch_code: str) -> QualityCheckResult:
        """检查学校名称完整性（防止NULL问题）"""
        try:
            # 检查NULL和空字符串的学校名称
            null_name_queries = {
                'student_cleaned_scores': text("""
                    SELECT COUNT(*) FROM student_cleaned_scores 
                    WHERE batch_code = :batch_code 
                    AND (school_name IS NULL OR school_name = '' OR TRIM(school_name) = '')
                """),
                'school_master_data': text("""
                    SELECT COUNT(*) FROM school_master_data 
                    WHERE batch_code = :batch_code 
                    AND (standard_school_name IS NULL OR standard_school_name = '' OR TRIM(standard_school_name) = '')
                """),
                'statistical_aggregations': text("""
                    SELECT COUNT(*) FROM statistical_aggregations 
                    WHERE batch_code = :batch_code 
                    AND (school_name IS NULL OR school_name = '' OR TRIM(school_name) = '')
                """)
            }
            
            null_counts = {}
            total_null_count = 0
            
            for table, query in null_name_queries.items():
                try:
                    count = self.db_session.execute(query, {'batch_code': batch_code}).scalar() or 0
                    null_counts[table] = count
                    total_null_count += count
                except Exception as e:
                    logger.warning(f"检查表 {table} 时出错: {e}")
                    null_counts[table] = 0
            
            if total_null_count > 0:
                return QualityCheckResult(
                    check_type="SCHOOL_NAME_COMPLETENESS",
                    status="FAIL",
                    message=f"发现 {total_null_count} 条NULL或空的学校名称记录",
                    details={
                        'null_counts_by_table': null_counts,
                        'total_null_count': total_null_count
                    },
                    severity="CRITICAL",
                    recommendations=[
                        "立即停止处理！NULL学校名称会导致前端显示错误",
                        "修复所有NULL或空的学校名称",
                        "确保school_master_data中所有学校都有标准化名称",
                        "重新运行数据清洗和汇聚流程",
                        "添加数据库约束防止NULL值"
                    ],
                    affected_count=total_null_count
                )
            
            return QualityCheckResult(
                check_type="SCHOOL_NAME_COMPLETENESS",
                status="PASS",
                message="所有学校名称都完整",
                details=null_counts,
                severity="LOW",
                recommendations=[],
                affected_count=0
            )
            
        except Exception as e:
            logger.error(f"学校名称完整性检查失败: {e}")
            return QualityCheckResult(
                check_type="SCHOOL_NAME_COMPLETENESS",
                status="FAIL",
                message=f"检查过程出错: {str(e)}",
                details={},
                severity="CRITICAL",
                recommendations=["检查数据库连接", "重新运行检查"],
                affected_count=1
            )
    
    def _check_data_source_consistency(self, batch_code: str) -> QualityCheckResult:
        """检查数据源统一性"""
        try:
            # 检查不同表中的数据源一致性
            query = text("""
                SELECT 
                    'school_master_data' as table_name,
                    data_source,
                    COUNT(*) as count
                FROM school_master_data 
                WHERE batch_code = :batch_code AND status = 'ACTIVE'
                GROUP BY data_source
                
                UNION ALL
                
                SELECT 
                    'student_cleaned_scores' as table_name,
                    'CLEANED_DATA' as data_source,
                    COUNT(*) as count
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            data_sources = {}
            
            for row in result:
                table = row[0]
                source = row[1] or 'UNKNOWN'
                count = row[2]
                if table not in data_sources:
                    data_sources[table] = {}
                data_sources[table][source] = count
            
            # 检查是否有多个数据源
            issues = []
            for table, sources in data_sources.items():
                if len(sources) > 1:
                    issues.append(f"{table}表有多个数据源: {list(sources.keys())}")
            
            if issues:
                return QualityCheckResult(
                    check_type="DATA_SOURCE_CONSISTENCY",
                    status="WARN",
                    message=f"发现 {len(issues)} 个数据源不一致问题",
                    details=data_sources,
                    severity="MEDIUM",
                    recommendations=[
                        "统一数据源标识",
                        "检查数据导入流程",
                        "验证数据来源的准确性"
                    ],
                    affected_count=len(issues)
                )
            
            return QualityCheckResult(
                check_type="DATA_SOURCE_CONSISTENCY",
                status="PASS",
                message="数据源一致性检查通过",
                details=data_sources,
                severity="LOW",
                recommendations=[],
                affected_count=0
            )
            
        except Exception as e:
            logger.error(f"数据源一致性检查失败: {e}")
            return QualityCheckResult(
                check_type="DATA_SOURCE_CONSISTENCY",
                status="FAIL",
                message=f"检查过程出错: {str(e)}",
                details={},
                severity="MEDIUM",
                recommendations=["检查数据库连接", "重新运行检查"],
                affected_count=1
            )
    
    def _check_student_data_distribution(self, batch_code: str) -> QualityCheckResult:
        """检查学生数据分布"""
        try:
            query = text("""
                SELECT 
                    scs.school_code,
                    scs.school_name,
                    COUNT(*) as student_count,
                    COUNT(DISTINCT scs.student_id) as unique_students
                FROM student_cleaned_scores scs
                WHERE scs.batch_code = :batch_code
                GROUP BY scs.school_code, scs.school_name
                ORDER BY student_count DESC
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            schools_data = []
            
            for row in result:
                schools_data.append({
                    'school_code': row[0],
                    'school_name': row[1],
                    'student_count': row[2],
                    'unique_students': row[3]
                })
            
            # 分析分布
            if not schools_data:
                return QualityCheckResult(
                    check_type="STUDENT_DATA_DISTRIBUTION",
                    status="FAIL",
                    message="没有找到学生数据",
                    details={},
                    severity="CRITICAL",
                    recommendations=["检查数据导入", "验证批次代码"],
                    affected_count=0
                )
            
            # 检查异常分布
            student_counts = [s['student_count'] for s in schools_data]
            avg_students = sum(student_counts) / len(student_counts)
            min_students = min(student_counts)
            max_students = max(student_counts)
            
            # 检查学校学生数过少的情况
            low_student_schools = [s for s in schools_data if s['student_count'] < self.quality_thresholds['min_student_per_school']]
            
            issues = []
            if low_student_schools:
                issues.append(f"{len(low_student_schools)}所学校学生数少于{self.quality_thresholds['min_student_per_school']}人")
            
            # 检查数据重复
            duplicate_data_schools = [s for s in schools_data if s['student_count'] != s['unique_students']]
            if duplicate_data_schools:
                issues.append(f"{len(duplicate_data_schools)}所学校存在重复学生数据")
            
            distribution_stats = {
                'total_schools': len(schools_data),
                'total_students': sum(student_counts),
                'avg_students_per_school': round(avg_students, 2),
                'min_students': min_students,
                'max_students': max_students,
                'low_student_schools': low_student_schools[:5],  # 只显示前5个
                'duplicate_data_schools': duplicate_data_schools[:5]
            }
            
            if issues:
                severity = "HIGH" if len(low_student_schools) > 5 else "MEDIUM"
                return QualityCheckResult(
                    check_type="STUDENT_DATA_DISTRIBUTION",
                    status="WARN",
                    message=f"学生数据分布有 {len(issues)} 个问题",
                    details=distribution_stats,
                    severity=severity,
                    recommendations=[
                        "检查学生数过少的学校是否正常",
                        "清理重复的学生数据",
                        "验证学校规模是否合理"
                    ],
                    affected_count=len(low_student_schools) + len(duplicate_data_schools)
                )
            
            return QualityCheckResult(
                check_type="STUDENT_DATA_DISTRIBUTION",
                status="PASS",
                message="学生数据分布正常",
                details=distribution_stats,
                severity="LOW",
                recommendations=[],
                affected_count=0
            )
            
        except Exception as e:
            logger.error(f"学生数据分布检查失败: {e}")
            return QualityCheckResult(
                check_type="STUDENT_DATA_DISTRIBUTION",
                status="FAIL",
                message=f"检查过程出错: {str(e)}",
                details={},
                severity="MEDIUM",
                recommendations=["检查数据库连接", "重新运行检查"],
                affected_count=1
            )
    
    def _check_historical_data_comparison(self, batch_code: str) -> QualityCheckResult:
        """与历史数据对比检查"""
        try:
            # 获取历史批次的学校数量进行对比
            query = text("""
                SELECT 
                    batch_code,
                    COUNT(DISTINCT school_id) as school_count
                FROM school_master_data 
                WHERE status = 'ACTIVE'
                GROUP BY batch_code 
                ORDER BY batch_code DESC 
                LIMIT 5
            """)
            
            result = self.db_session.execute(query)
            historical_data = {}
            current_school_count = 0
            
            for row in result:
                batch = row[0]
                count = row[1]
                historical_data[batch] = count
                if batch == batch_code:
                    current_school_count = count
            
            if len(historical_data) < 2:
                return QualityCheckResult(
                    check_type="HISTORICAL_DATA_COMPARISON",
                    status="WARN",
                    message="没有足够的历史数据进行对比",
                    details=historical_data,
                    severity="LOW",
                    recommendations=["积累更多批次数据用于对比"],
                    affected_count=0
                )
            
            # 计算与最近批次的偏差
            other_batches = [count for batch, count in historical_data.items() if batch != batch_code]
            if other_batches:
                avg_historical_count = sum(other_batches) / len(other_batches)
                deviation = abs(current_school_count - avg_historical_count) / avg_historical_count
                
                comparison_details = {
                    'current_batch': batch_code,
                    'current_school_count': current_school_count,
                    'historical_average': round(avg_historical_count, 2),
                    'deviation_percentage': round(deviation * 100, 2),
                    'threshold_percentage': self.quality_thresholds['max_school_count_deviation'] * 100,
                    'historical_data': historical_data
                }
                
                if deviation > self.quality_thresholds['max_school_count_deviation']:
                    return QualityCheckResult(
                        check_type="HISTORICAL_DATA_COMPARISON",
                        status="WARN",
                        message=f"学校数量偏差{deviation*100:.1f}%，超过{self.quality_thresholds['max_school_count_deviation']*100}%阈值",
                        details=comparison_details,
                        severity="MEDIUM",
                        recommendations=[
                            "检查是否有学校数据缺失",
                            "验证批次范围是否正确",
                            "确认是否有新增或减少的学校"
                        ],
                        affected_count=1
                    )
                
                return QualityCheckResult(
                    check_type="HISTORICAL_DATA_COMPARISON",
                    status="PASS",
                    message=f"学校数量偏差{deviation*100:.1f}%，在正常范围内",
                    details=comparison_details,
                    severity="LOW",
                    recommendations=[],
                    affected_count=0
                )
            
            return QualityCheckResult(
                check_type="HISTORICAL_DATA_COMPARISON",
                status="PASS",
                message="首批数据，无历史对比",
                details=historical_data,
                severity="LOW",
                recommendations=[],
                affected_count=0
            )
            
        except Exception as e:
            logger.error(f"历史数据对比检查失败: {e}")
            return QualityCheckResult(
                check_type="HISTORICAL_DATA_COMPARISON",
                status="FAIL",
                message=f"检查过程出错: {str(e)}",
                details={},
                severity="LOW",
                recommendations=["检查数据库连接", "重新运行检查"],
                affected_count=1
            )
    
    def _check_data_completeness(self, batch_code: str) -> QualityCheckResult:
        """检查数据完整性"""
        try:
            # 检查各个阶段的数据完整性
            completeness_checks = {}
            
            # 原始数据完整性
            query = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(student_id) as non_null_student_ids,
                    COUNT(school_code) as non_null_school_codes,
                    COUNT(school_name) as non_null_school_names,
                    COUNT(subject_code) as non_null_subject_codes,
                    COUNT(score) as non_null_scores
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code}).fetchone()
            if result:
                total = result[0]
                completeness_checks['student_cleaned_scores'] = {
                    'total_records': total,
                    'completeness_rates': {
                        'student_id': round((result[1] / total * 100) if total > 0 else 0, 2),
                        'school_code': round((result[2] / total * 100) if total > 0 else 0, 2),
                        'school_name': round((result[3] / total * 100) if total > 0 else 0, 2),
                        'subject_code': round((result[4] / total * 100) if total > 0 else 0, 2),
                        'score': round((result[5] / total * 100) if total > 0 else 0, 2)
                    }
                }
            
            # 汇聚数据完整性
            query = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(school_name) as non_null_school_names,
                    COUNT(statistics_json) as non_null_statistics
                FROM statistical_aggregations 
                WHERE batch_code = :batch_code
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code}).fetchone()
            if result:
                total = result[0]
                completeness_checks['statistical_aggregations'] = {
                    'total_records': total,
                    'completeness_rates': {
                        'school_name': round((result[1] / total * 100) if total > 0 else 0, 2),
                        'statistics_json': round((result[2] / total * 100) if total > 0 else 0, 2)
                    }
                }
            
            # 检查完整性问题
            issues = []
            min_completeness = self.quality_thresholds['min_data_completeness'] * 100
            
            for table, data in completeness_checks.items():
                for field, rate in data['completeness_rates'].items():
                    if rate < min_completeness:
                        issues.append(f"{table}.{field}: {rate}%完整性低于{min_completeness}%阈值")
            
            if issues:
                return QualityCheckResult(
                    check_type="DATA_COMPLETENESS",
                    status="FAIL" if len(issues) > 3 else "WARN",
                    message=f"数据完整性有 {len(issues)} 个问题",
                    details=completeness_checks,
                    severity="HIGH" if len(issues) > 3 else "MEDIUM",
                    recommendations=[
                        "检查数据导入过程",
                        "修复缺失的必要字段",
                        "重新运行数据处理流程",
                        "加强数据验证规则"
                    ],
                    affected_count=len(issues)
                )
            
            return QualityCheckResult(
                check_type="DATA_COMPLETENESS",
                status="PASS",
                message="数据完整性检查通过",
                details=completeness_checks,
                severity="LOW",
                recommendations=[],
                affected_count=0
            )
            
        except Exception as e:
            logger.error(f"数据完整性检查失败: {e}")
            return QualityCheckResult(
                check_type="DATA_COMPLETENESS",
                status="FAIL",
                message=f"检查过程出错: {str(e)}",
                details={},
                severity="MEDIUM",
                recommendations=["检查数据库连接", "重新运行检查"],
                affected_count=1
            )
    
    def _check_performance_impact(self, batch_code: str) -> QualityCheckResult:
        """检查性能影响预估"""
        try:
            # 统计数据量
            data_volume_query = text("""
                SELECT 
                    'student_cleaned_scores' as table_name,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT school_code) as school_count,
                    COUNT(DISTINCT student_id) as student_count
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
                
                UNION ALL
                
                SELECT 
                    'student_score_detail' as table_name,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT school_id) as school_count,
                    COUNT(DISTINCT student_id) as student_count
                FROM student_score_detail 
                WHERE batch_code = :batch_code
            """)
            
            result = self.db_session.execute(data_volume_query, {'batch_code': batch_code})
            data_volume = {}
            
            for row in result:
                table = row[0]
                data_volume[table] = {
                    'records': row[1],
                    'schools': row[2],
                    'students': row[3]
                }
            
            # 预估处理时间（基于经验值）
            total_records = sum(data['records'] for data in data_volume.values())
            estimated_minutes = max(1, total_records // 3000)  # 每3000条记录约1分钟
            
            performance_assessment = {
                'data_volume': data_volume,
                'total_records': total_records,
                'estimated_processing_time_minutes': estimated_minutes,
                'performance_level': 'LOW' if total_records < 100000 else 'MEDIUM' if total_records < 500000 else 'HIGH'
            }
            
            if total_records > 1000000:  # 超过100万条记录
                return QualityCheckResult(
                    check_type="PERFORMANCE_IMPACT",
                    status="WARN",
                    message=f"数据量较大({total_records:,}条)，预估处理时间{estimated_minutes}分钟",
                    details=performance_assessment,
                    severity="MEDIUM",
                    recommendations=[
                        "考虑分批处理以减少内存压力",
                        "监控处理过程中的资源使用",
                        "准备充足的处理时间",
                        "考虑在低峰时段运行"
                    ],
                    affected_count=1
                )
            
            return QualityCheckResult(
                check_type="PERFORMANCE_IMPACT",
                status="PASS",
                message=f"数据量适中({total_records:,}条)，预估处理时间{estimated_minutes}分钟",
                details=performance_assessment,
                severity="LOW",
                recommendations=[],
                affected_count=0
            )
            
        except Exception as e:
            logger.error(f"性能影响检查失败: {e}")
            return QualityCheckResult(
                check_type="PERFORMANCE_IMPACT",
                status="FAIL",
                message=f"检查过程出错: {str(e)}",
                details={},
                severity="LOW",
                recommendations=["检查数据库连接", "重新运行检查"],
                affected_count=1
            )
    
    def _generate_quality_report(self, batch_code: str, check_results: List[QualityCheckResult], start_time: datetime) -> BatchQualityReport:
        """生成质量报告"""
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        # 统计结果
        total_checks = len(check_results)
        passed_checks = sum(1 for r in check_results if r.status == "PASS")
        warned_checks = sum(1 for r in check_results if r.status == "WARN")
        failed_checks = sum(1 for r in check_results if r.status == "FAIL")
        critical_issues = sum(1 for r in check_results if r.severity == "CRITICAL")
        
        # 确定总体状态
        if critical_issues > 0 or failed_checks > 0:
            overall_status = "FAIL"
        elif warned_checks > 0:
            overall_status = "WARN"
        else:
            overall_status = "PASS"
        
        # 收集所有建议
        all_recommendations = []
        for result in check_results:
            all_recommendations.extend(result.recommendations)
        
        # 去重并排序建议
        unique_recommendations = list(dict.fromkeys(all_recommendations))
        
        # 生成摘要
        summary = {
            'processing_time_seconds': round(processing_time, 2),
            'total_affected_count': sum(r.affected_count for r in check_results),
            'critical_checks': [r.check_type for r in check_results if r.severity == "CRITICAL" and r.status != "PASS"],
            'failed_checks': [r.check_type for r in check_results if r.status == "FAIL"],
            'check_completion_rate': round((passed_checks + warned_checks) / total_checks * 100, 1) if total_checks > 0 else 0
        }
        
        return BatchQualityReport(
            batch_code=batch_code,
            check_timestamp=end_time,
            overall_status=overall_status,
            total_checks=total_checks,
            passed_checks=passed_checks,
            warned_checks=warned_checks,
            failed_checks=failed_checks,
            critical_issues=critical_issues,
            results=check_results,
            recommendations=unique_recommendations,
            summary=summary
        )
    
    def _save_quality_report(self, report: BatchQualityReport) -> str:
        """保存质量报告到文件"""
        timestamp = report.check_timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"data_quality_report_{report.batch_code}_{timestamp}.json"
        
        # 创建报告目录
        os.makedirs("quality_reports", exist_ok=True)
        filepath = os.path.join("quality_reports", filename)
        
        # 序列化报告
        report_dict = asdict(report)
        
        # 处理datetime对象
        report_dict['check_timestamp'] = report.check_timestamp.isoformat()
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report_dict, f, ensure_ascii=False, indent=2)
        
        logger.info(f"质量报告已保存到: {filepath}")
        return filepath
    
    def _send_critical_alert(self, report: BatchQualityReport):
        """发送关键问题警报"""
        logger.critical(f"批次 {report.batch_code} 发现 {report.critical_issues} 个关键问题!")
        logger.critical("建议立即停止处理此批次数据！")
        
        # 这里可以扩展为发送邮件、短信等通知方式
        critical_issues = [r for r in report.results if r.severity == "CRITICAL" and r.status != "PASS"]
        for issue in critical_issues:
            logger.critical(f"关键问题: {issue.check_type} - {issue.message}")


def main():
    """主函数 - 命令行接口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="数据质量监控工具")
    parser.add_argument("batch_code", help="批次代码，如 G4-2025")
    parser.add_argument("--save-report", action="store_true", help="保存详细报告到文件")
    parser.add_argument("--verbose", action="store_true", help="显示详细信息")
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    try:
        with next(get_db()) as db:
            monitor = DataQualityMonitor(db)
            report = monitor.run_comprehensive_check(args.batch_code)
            
            # 打印报告摘要
            print("="*80)
            print(f"数据质量监控报告 - 批次: {report.batch_code}")
            print("="*80)
            print(f"检查时间: {report.check_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"总体状态: {report.overall_status}")
            print(f"处理时间: {report.summary['processing_time_seconds']}秒")
            print()
            
            print(f"检查项目: {report.total_checks}个")
            print(f"  ✓ 通过: {report.passed_checks}个")
            print(f"  ⚠ 警告: {report.warned_checks}个") 
            print(f"  ✗ 失败: {report.failed_checks}个")
            print(f"  🔴 关键问题: {report.critical_issues}个")
            print()
            
            if report.critical_issues > 0 or report.failed_checks > 0:
                print("❌ 发现严重问题，建议立即处理:")
                for result in report.results:
                    if result.status == "FAIL" or result.severity == "CRITICAL":
                        print(f"  • {result.check_type}: {result.message}")
                print()
            
            if report.recommendations:
                print("💡 建议措施:")
                for i, rec in enumerate(report.recommendations[:10], 1):  # 只显示前10条
                    print(f"  {i}. {rec}")
                if len(report.recommendations) > 10:
                    print(f"  ... 还有 {len(report.recommendations) - 10} 条建议")
                print()
            
            # 显示详细结果
            if args.verbose:
                print("详细检查结果:")
                print("-" * 80)
                for result in report.results:
                    status_icon = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}[result.status]
                    severity_icon = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🟠", "CRITICAL": "🔴"}[result.severity]
                    print(f"{status_icon} {severity_icon} {result.check_type}")
                    print(f"    消息: {result.message}")
                    if result.affected_count > 0:
                        print(f"    影响数量: {result.affected_count}")
                    print()
            
            # 保存报告
            if args.save_report:
                filepath = monitor._save_quality_report(report)
                print(f"详细报告已保存到: {filepath}")
            
            # 设置退出代码
            if report.critical_issues > 0:
                sys.exit(2)  # 关键问题
            elif report.failed_checks > 0:
                sys.exit(1)  # 一般失败
            else:
                sys.exit(0)  # 成功
                
    except Exception as e:
        logger.error(f"监控过程失败: {e}")
        sys.exit(3)


if __name__ == "__main__":
    main()