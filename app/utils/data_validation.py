#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据验证工具 - 确保数据质量和一致性
专门针对school_master_data与其他表的一致性验证
"""

from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)


class DataValidator:
    """增强版数据验证器 - 确保school_master_data作为唯一数据源，提供预警和监控功能"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        # 预警阈值配置
        self.alert_thresholds = {
            'max_orphaned_schools': 0,  # 零容忍孤立学校
            'max_null_school_names': 0,  # 零容忍NULL学校名称
            'max_school_count_deviation_percent': 5,  # 学校数量偏差阈值5%
            'min_student_per_school': 10,  # 每校最少学生数
            'max_duplicate_schools': 2,  # 最多允许2个重复学校
        }
    
    def validate_batch_school_consistency(self, batch_code: str) -> Dict[str, Any]:
        """验证批次中学校数据的一致性"""
        logger.info(f"开始验证批次 {batch_code} 的学校数据一致性")
        
        validation_result = {
            'batch_code': batch_code,
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'statistics': {},
            'recommendations': []
        }
        
        try:
            # 1. 检查school_master_data中的学校数量
            master_count = self._get_master_school_count(batch_code)
            validation_result['statistics']['master_schools'] = master_count
            
            # 2. 检查student_cleaned_scores中不在master表中的学校
            orphaned_schools = self._find_orphaned_schools(batch_code)
            validation_result['statistics']['orphaned_schools'] = len(orphaned_schools)
            
            if orphaned_schools:
                validation_result['is_valid'] = False
                validation_result['errors'].append({
                    'type': 'ORPHANED_SCHOOLS',
                    'message': f'发现 {len(orphaned_schools)} 所学校在student_cleaned_scores中但不在school_master_data中',
                    'schools': orphaned_schools[:10]  # 只显示前10个
                })
            
            # 3. 检查学校名称不一致的情况
            name_inconsistencies = self._find_school_name_inconsistencies(batch_code)
            validation_result['statistics']['name_inconsistencies'] = len(name_inconsistencies)
            
            if name_inconsistencies:
                validation_result['warnings'].append({
                    'type': 'NAME_INCONSISTENCIES',
                    'message': f'发现 {len(name_inconsistencies)} 所学校名称不一致',
                    'examples': name_inconsistencies[:5]
                })
            
            # 4. 检查school_id格式
            invalid_school_ids = self._validate_school_id_format(batch_code)
            validation_result['statistics']['invalid_school_ids'] = len(invalid_school_ids)
            
            if invalid_school_ids:
                validation_result['warnings'].append({
                    'type': 'INVALID_SCHOOL_IDS',
                    'message': f'发现 {len(invalid_school_ids)} 个不符合格式的school_id',
                    'examples': invalid_school_ids[:5]
                })
            
            # 5. 生成修复建议
            if not validation_result['is_valid']:
                validation_result['recommendations'].extend([
                    '立即停止处理此批次数据',
                    '修复school_master_data中缺失的学校记录',
                    '重新运行数据清洗流程',
                    '验证所有学校名称使用标准化名称'
                ])
            
            logger.info(f"批次 {batch_code} 验证完成 - 有效: {validation_result['is_valid']}")
            return validation_result
            
        except Exception as e:
            logger.error(f"数据验证失败: {e}")
            validation_result['is_valid'] = False
            validation_result['errors'].append({
                'type': 'VALIDATION_ERROR',
                'message': f'验证过程出错: {str(e)}'
            })
            return validation_result
    
    def _get_master_school_count(self, batch_code: str) -> int:
        """获取school_master_data中的学校数量"""
        query = text("""
            SELECT COUNT(DISTINCT school_id) as count
            FROM school_master_data 
            WHERE batch_code = :batch_code 
                AND status = 'ACTIVE'
        """)
        result = self.db_session.execute(query, {'batch_code': batch_code})
        return result.scalar() or 0
    
    def _find_orphaned_schools(self, batch_code: str) -> List[Dict[str, Any]]:
        """查找在student_cleaned_scores中但不在school_master_data中的学校"""
        query = text("""
            SELECT DISTINCT 
                scs.school_code,
                scs.school_name,
                COUNT(*) as student_count
            FROM student_cleaned_scores scs
            LEFT JOIN school_master_data smd 
                ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
                AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
                AND smd.status = 'ACTIVE'
            WHERE scs.batch_code = :batch_code
                AND smd.school_id IS NULL
            GROUP BY scs.school_code, scs.school_name
            ORDER BY student_count DESC
        """)
        result = self.db_session.execute(query, {'batch_code': batch_code})
        return [{'school_code': r[0], 'school_name': r[1], 'student_count': r[2]} for r in result.fetchall()]
    
    def _find_school_name_inconsistencies(self, batch_code: str) -> List[Dict[str, Any]]:
        """查找学校名称不一致的情况"""
        query = text("""
            SELECT DISTINCT
                scs.school_code,
                scs.school_name as cleaned_name,
                smd.standard_school_name as master_name
            FROM student_cleaned_scores scs
            INNER JOIN school_master_data smd 
                ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
                AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
                AND smd.status = 'ACTIVE'
            WHERE scs.batch_code = :batch_code
                AND scs.school_name COLLATE utf8mb4_unicode_ci != smd.standard_school_name COLLATE utf8mb4_unicode_ci
            ORDER BY scs.school_code
        """)
        result = self.db_session.execute(query, {'batch_code': batch_code})
        return [{'school_code': r[0], 'cleaned_name': r[1], 'master_name': r[2]} for r in result.fetchall()]
    
    def _validate_school_id_format(self, batch_code: str) -> List[str]:
        """验证school_id格式（应该是5044-5099范围内的4位数字）"""
        query = text("""
            SELECT DISTINCT school_id
            FROM school_master_data 
            WHERE batch_code = :batch_code
                AND status = 'ACTIVE'
                AND (
                    LENGTH(school_id) != 4 
                    OR school_id NOT REGEXP '^[0-9]{4}$'
                    OR CAST(school_id AS UNSIGNED) < 5044 
                    OR CAST(school_id AS UNSIGNED) > 5099
                )
            ORDER BY school_id
        """)
        result = self.db_session.execute(query, {'batch_code': batch_code})
        return [r[0] for r in result.fetchall()]
    
    def enforce_master_data_constraint(self, batch_code: str) -> Dict[str, Any]:
        """强制执行school_master_data约束 - 删除不匹配的数据"""
        logger.warning(f"强制执行school_master_data约束 - 这将删除不匹配的数据!")
        
        result = {
            'batch_code': batch_code,
            'deleted_records': 0,
            'affected_tables': []
        }
        
        try:
            # 删除student_cleaned_scores中不在master表中的记录
            delete_query = text("""
                DELETE scs FROM student_cleaned_scores scs
                LEFT JOIN school_master_data smd 
                    ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
                    AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
                    AND smd.status = 'ACTIVE'
                WHERE scs.batch_code = :batch_code
                    AND smd.school_id IS NULL
            """)
            delete_result = self.db_session.execute(delete_query, {'batch_code': batch_code})
            deleted_count = delete_result.rowcount
            
            result['deleted_records'] = deleted_count
            result['affected_tables'] = ['student_cleaned_scores']
            
            if deleted_count > 0:
                logger.warning(f"已从student_cleaned_scores删除 {deleted_count} 条不匹配的记录")
                self.db_session.commit()
            
            return result
            
        except Exception as e:
            logger.error(f"强制约束执行失败: {e}")
            self.db_session.rollback()
            result['error'] = str(e)
            return result
    
    def run_pre_processing_check(self, batch_code: str) -> Dict[str, Any]:
        """处理前的必要检查 - 阻止有问题的批次继续处理"""
        logger.info(f"对批次 {batch_code} 运行处理前检查")
        
        check_result = {
            'batch_code': batch_code,
            'can_proceed': True,
            'blocking_issues': [],
            'warnings': [],
            'checks_performed': [],
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # 1. 关键检查：NULL学校名称
            null_name_result = self._check_null_school_names(batch_code)
            check_result['checks_performed'].append('null_school_names')
            if null_name_result['count'] > self.alert_thresholds['max_null_school_names']:
                check_result['can_proceed'] = False
                check_result['blocking_issues'].append({
                    'type': 'CRITICAL_NULL_SCHOOL_NAMES',
                    'count': null_name_result['count'],
                    'message': f"发现 {null_name_result['count']} 个NULL学校名称，必须修复后才能处理",
                    'details': null_name_result
                })
            
            # 2. 关键检查：孤立学校
            orphaned_result = self._check_orphaned_schools_quick(batch_code)
            check_result['checks_performed'].append('orphaned_schools')
            if orphaned_result['count'] > self.alert_thresholds['max_orphaned_schools']:
                check_result['can_proceed'] = False
                check_result['blocking_issues'].append({
                    'type': 'CRITICAL_ORPHANED_SCHOOLS',
                    'count': orphaned_result['count'],
                    'message': f"发现 {orphaned_result['count']} 个孤立学校，必须修复后才能处理",
                    'details': orphaned_result
                })
            
            # 3. 警告检查：学校数量偏差
            deviation_result = self._check_school_count_deviation(batch_code)
            check_result['checks_performed'].append('school_count_deviation')
            if deviation_result['deviation_percent'] > self.alert_thresholds['max_school_count_deviation_percent']:
                check_result['warnings'].append({
                    'type': 'SCHOOL_COUNT_DEVIATION',
                    'deviation_percent': deviation_result['deviation_percent'],
                    'message': f"学校数量偏差 {deviation_result['deviation_percent']:.1f}%，超过阈值",
                    'details': deviation_result
                })
            
            # 4. 警告检查：学生数据分布
            distribution_result = self._check_student_distribution_quick(batch_code)
            check_result['checks_performed'].append('student_distribution')
            if distribution_result['low_student_schools'] > 0:
                check_result['warnings'].append({
                    'type': 'LOW_STUDENT_SCHOOLS',
                    'count': distribution_result['low_student_schools'],
                    'message': f"发现 {distribution_result['low_student_schools']} 所学校学生数过少",
                    'details': distribution_result
                })
            
            logger.info(f"批次 {batch_code} 处理前检查完成 - 可处理: {check_result['can_proceed']}")
            return check_result
            
        except Exception as e:
            logger.error(f"处理前检查失败: {e}")
            check_result['can_proceed'] = False
            check_result['blocking_issues'].append({
                'type': 'CHECK_ERROR',
                'message': f'检查过程出错: {str(e)}',
                'details': {'error': str(e)}
            })
            return check_result
    
    def run_post_processing_check(self, batch_code: str) -> Dict[str, Any]:
        """处理后的验证检查 - 确保结果质量"""
        logger.info(f"对批次 {batch_code} 运行处理后检查")
        
        check_result = {
            'batch_code': batch_code,
            'processing_success': True,
            'quality_issues': [],
            'warnings': [],
            'checks_performed': [],
            'statistics': {},
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # 1. 检查汇聚结果完整性
            aggregation_result = self._check_aggregation_completeness(batch_code)
            check_result['checks_performed'].append('aggregation_completeness')
            check_result['statistics']['aggregation'] = aggregation_result
            
            if aggregation_result['missing_schools'] > 0:
                check_result['quality_issues'].append({
                    'type': 'MISSING_SCHOOL_AGGREGATIONS',
                    'count': aggregation_result['missing_schools'],
                    'message': f"有 {aggregation_result['missing_schools']} 所学校缺少汇聚结果",
                    'details': aggregation_result
                })
            
            # 2. 检查JSON数据格式
            json_result = self._check_statistics_json_format(batch_code)
            check_result['checks_performed'].append('json_format')
            check_result['statistics']['json_format'] = json_result
            
            if json_result['invalid_json_count'] > 0:
                check_result['quality_issues'].append({
                    'type': 'INVALID_JSON_FORMAT',
                    'count': json_result['invalid_json_count'],
                    'message': f"有 {json_result['invalid_json_count']} 条记录JSON格式无效",
                    'details': json_result
                })
            
            # 3. 检查学校名称一致性（处理后不应该有NULL）
            post_null_result = self._check_null_school_names_post_processing(batch_code)
            check_result['checks_performed'].append('post_null_names')
            
            if post_null_result['count'] > 0:
                check_result['processing_success'] = False
                check_result['quality_issues'].append({
                    'type': 'POST_PROCESSING_NULL_NAMES',
                    'count': post_null_result['count'],
                    'message': f"处理后仍有 {post_null_result['count']} 个NULL学校名称！",
                    'details': post_null_result,
                    'severity': 'CRITICAL'
                })
            
            # 确定处理成功状态
            critical_issues = [i for i in check_result['quality_issues'] if i.get('severity') == 'CRITICAL']
            if critical_issues:
                check_result['processing_success'] = False
            
            logger.info(f"批次 {batch_code} 处理后检查完成 - 处理成功: {check_result['processing_success']}")
            return check_result
            
        except Exception as e:
            logger.error(f"处理后检查失败: {e}")
            check_result['processing_success'] = False
            check_result['quality_issues'].append({
                'type': 'CHECK_ERROR',
                'message': f'检查过程出错: {str(e)}',
                'details': {'error': str(e)},
                'severity': 'HIGH'
            })
            return check_result
    
    def monitor_batch_progress(self, batch_code: str) -> Dict[str, Any]:
        """监控批次处理进度"""
        logger.info(f"监控批次 {batch_code} 处理进度")
        
        progress_result = {
            'batch_code': batch_code,
            'timestamp': datetime.now().isoformat(),
            'processing_stages': {},
            'overall_progress': 0.0,
            'estimated_completion': None,
            'issues_detected': []
        }
        
        try:
            # 检查各个处理阶段
            stages = [
                ('master_data', '主数据准备'),
                ('cleaned_scores', '数据清洗'),
                ('aggregation', '统计汇聚'),
                ('json_serialization', 'JSON序列化')
            ]
            
            total_progress = 0
            for stage_key, stage_name in stages:
                stage_progress = self._check_stage_progress(batch_code, stage_key)
                progress_result['processing_stages'][stage_key] = {
                    'name': stage_name,
                    'progress_percent': stage_progress['progress_percent'],
                    'records_processed': stage_progress['records_processed'],
                    'total_records': stage_progress['total_records'],
                    'status': stage_progress['status']
                }
                total_progress += stage_progress['progress_percent']
            
            progress_result['overall_progress'] = total_progress / len(stages)
            
            # 检测进度中的异常
            for stage_key, stage_info in progress_result['processing_stages'].items():
                if stage_info['status'] == 'STALLED':
                    progress_result['issues_detected'].append({
                        'type': 'PROCESSING_STALLED',
                        'stage': stage_key,
                        'message': f"阶段 {stage_info['name']} 处理停滞"
                    })
                elif stage_info['status'] == 'ERROR':
                    progress_result['issues_detected'].append({
                        'type': 'PROCESSING_ERROR',
                        'stage': stage_key,
                        'message': f"阶段 {stage_info['name']} 处理错误"
                    })
            
            return progress_result
            
        except Exception as e:
            logger.error(f"监控批次进度失败: {e}")
            progress_result['issues_detected'].append({
                'type': 'MONITOR_ERROR',
                'message': f'监控过程出错: {str(e)}'
            })
            return progress_result
    
    def generate_quality_trend_report(self, batch_codes: List[str]) -> Dict[str, Any]:
        """生成数据质量趋势报告"""
        logger.info(f"生成质量趋势报告，涉及批次: {batch_codes}")
        
        trend_report = {
            'report_timestamp': datetime.now().isoformat(),
            'batches_analyzed': batch_codes,
            'trends': {},
            'recommendations': []
        }
        
        try:
            # 分析各批次的质量指标
            batch_metrics = []
            for batch_code in batch_codes:
                metrics = self._get_batch_quality_metrics(batch_code)
                batch_metrics.append(metrics)
            
            # 计算趋势
            if len(batch_metrics) >= 2:
                trend_report['trends'] = {
                    'school_count_trend': self._calculate_trend([m['school_count'] for m in batch_metrics]),
                    'data_quality_trend': self._calculate_trend([m['data_quality_score'] for m in batch_metrics]),
                    'processing_time_trend': self._calculate_trend([m['processing_time'] for m in batch_metrics])
                }
            
            # 生成建议
            trend_report['recommendations'] = self._generate_trend_recommendations(batch_metrics, trend_report['trends'])
            
            return trend_report
            
        except Exception as e:
            logger.error(f"生成质量趋势报告失败: {e}")
            trend_report['error'] = str(e)
            return trend_report


def validate_g4_batch_data(db_session: Session) -> Dict[str, Any]:
    """专门验证G4批次的数据问题"""
    validator = DataValidator(db_session)
    return validator.validate_batch_school_consistency('G4-2025')


if __name__ == "__main__":
    # 可以作为独立脚本运行
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from app.database.connection import get_db
    
    with next(get_db()) as db:
        result = validate_g4_batch_data(db)
        print("="*60)
        print("G4批次数据验证结果")
        print("="*60)
        print(f"批次代码: {result['batch_code']}")
        print(f"验证结果: {'通过' if result['is_valid'] else '失败'}")
        print(f"统计信息: {result['statistics']}")
        
        if result['errors']:
            print("\n错误:")
            for error in result['errors']:
                print(f"  - {error['type']}: {error['message']}")
        
        if result['warnings']:
            print("\n警告:")
            for warning in result['warnings']:
                print(f"  - {warning['type']}: {warning['message']}")
        
        if result['recommendations']:
            print("\n建议:")
            for rec in result['recommendations']:
                print(f"  - {rec}")