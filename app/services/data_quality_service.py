#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据质量服务 - 集成数据验证、监控和预警功能
提供统一的数据质量检查接口，集成到现有处理流程中
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from sqlalchemy.orm import Session

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.data_governance_config import DataGovernanceConfigManager, Severity, ActionType
from app.utils.data_validation import DataValidator

logger = logging.getLogger(__name__)


@dataclass
class QualityCheckResult:
    """质量检查结果"""
    rule_id: str
    rule_name: str
    status: str  # PASS, WARN, FAIL, ERROR
    severity: str
    action: str
    check_value: Optional[float]
    threshold_value: Optional[float]
    message: str
    details: Dict[str, Any]
    recommendations: List[str]
    execution_time_ms: float


@dataclass
class StageQualityReport:
    """阶段质量报告"""
    stage_id: str
    stage_name: str
    batch_code: str
    timestamp: datetime
    overall_status: str  # PASS, WARN, FAIL, BLOCKED
    can_proceed: bool
    check_results: List[QualityCheckResult]
    blocking_issues: List[QualityCheckResult]
    execution_time_ms: float
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'stage_id': self.stage_id,
            'stage_name': self.stage_name,
            'batch_code': self.batch_code,
            'timestamp': self.timestamp.isoformat(),
            'overall_status': self.overall_status,
            'can_proceed': self.can_proceed,
            'check_results': [asdict(result) for result in self.check_results],
            'blocking_issues': [asdict(result) for result in self.blocking_issues],
            'execution_time_ms': self.execution_time_ms,
            'summary': {
                'total_checks': len(self.check_results),
                'passed_checks': len([r for r in self.check_results if r.status == 'PASS']),
                'warned_checks': len([r for r in self.check_results if r.status == 'WARN']),
                'failed_checks': len([r for r in self.check_results if r.status == 'FAIL']),
                'error_checks': len([r for r in self.check_results if r.status == 'ERROR']),
                'blocking_issues_count': len(self.blocking_issues)
            }
        }


class DataQualityService:
    """数据质量服务"""
    
    def __init__(self, db_session: Session, config_manager: Optional[DataGovernanceConfigManager] = None):
        self.db_session = db_session
        self.config_manager = config_manager or DataGovernanceConfigManager()
        self.validator = DataValidator(db_session)
        
        # 性能跟踪
        self.performance_metrics = {
            'total_checks_run': 0,
            'total_execution_time_ms': 0.0,
            'checks_by_rule': {},
            'checks_by_severity': {}
        }
    
    def check_stage_quality(self, batch_code: str, stage_id: str) -> StageQualityReport:
        """检查指定阶段的数据质量"""
        start_time = datetime.now()
        logger.info(f"开始检查批次 {batch_code} 阶段 {stage_id} 的数据质量")
        
        if stage_id not in self.config_manager.config.processing_stages:
            raise ValueError(f"未知的处理阶段: {stage_id}")
        
        stage = self.config_manager.config.processing_stages[stage_id]
        check_results = []
        blocking_issues = []
        
        # 执行该阶段的所有质量规则检查
        rules = self.config_manager.get_rules_for_stage(stage_id)
        blocking_rule_ids = [rule.rule_id for rule in self.config_manager.get_blocking_rules_for_stage(stage_id)]
        
        for rule in rules:
            try:
                result = self._execute_quality_rule(batch_code, rule)
                check_results.append(result)
                
                # 收集性能指标
                self.performance_metrics['total_checks_run'] += 1
                self.performance_metrics['total_execution_time_ms'] += result.execution_time_ms
                self.performance_metrics['checks_by_rule'][rule.rule_id] = self.performance_metrics['checks_by_rule'].get(rule.rule_id, 0) + 1
                self.performance_metrics['checks_by_severity'][rule.severity.value] = self.performance_metrics['checks_by_severity'].get(rule.severity.value, 0) + 1
                
                # 判断是否为阻断问题
                if rule.rule_id in blocking_rule_ids and result.status in ['FAIL', 'ERROR']:
                    blocking_issues.append(result)
                    
            except Exception as e:
                logger.error(f"执行规则 {rule.rule_id} 时出错: {e}")
                error_result = QualityCheckResult(
                    rule_id=rule.rule_id,
                    rule_name=rule.name,
                    status="ERROR",
                    severity=rule.severity.value,
                    action=rule.action.value,
                    check_value=None,
                    threshold_value=rule.threshold_value,
                    message=f"规则执行出错: {str(e)}",
                    details={'error': str(e)},
                    recommendations=["检查数据库连接", "重新运行检查", "联系技术支持"],
                    execution_time_ms=0.0
                )
                check_results.append(error_result)
                
                if rule.rule_id in blocking_rule_ids:
                    blocking_issues.append(error_result)
        
        # 确定总体状态
        overall_status = self._determine_overall_status(check_results, blocking_issues)
        can_proceed = len(blocking_issues) == 0
        
        end_time = datetime.now()
        execution_time_ms = (end_time - start_time).total_seconds() * 1000
        
        report = StageQualityReport(
            stage_id=stage_id,
            stage_name=stage.name,
            batch_code=batch_code,
            timestamp=end_time,
            overall_status=overall_status,
            can_proceed=can_proceed,
            check_results=check_results,
            blocking_issues=blocking_issues,
            execution_time_ms=execution_time_ms
        )
        
        logger.info(f"批次 {batch_code} 阶段 {stage_id} 质量检查完成 - 状态: {overall_status}, 可继续: {can_proceed}")
        
        # 如果有阻断问题，发送警报
        if blocking_issues:
            self._send_quality_alert(report)
        
        return report
    
    def check_batch_processing_pipeline(self, batch_code: str) -> Dict[str, Any]:
        """检查整个批次处理管道的质量"""
        logger.info(f"开始检查批次 {batch_code} 的完整处理管道质量")
        
        pipeline_report = {
            'batch_code': batch_code,
            'pipeline_check_timestamp': datetime.now().isoformat(),
            'overall_pipeline_status': 'UNKNOWN',
            'stage_reports': {},
            'pipeline_blocked': False,
            'blocking_stages': [],
            'total_execution_time_ms': 0.0,
            'recommendations': []
        }
        
        try:
            # 按正确顺序检查各个阶段
            processing_order = self.config_manager.get_processing_order()
            total_start_time = datetime.now()
            
            for stage_id in processing_order:
                try:
                    stage_report = self.check_stage_quality(batch_code, stage_id)
                    pipeline_report['stage_reports'][stage_id] = stage_report.to_dict()
                    
                    # 如果阶段被阻断，停止后续检查
                    if not stage_report.can_proceed:
                        pipeline_report['pipeline_blocked'] = True
                        pipeline_report['blocking_stages'].append(stage_id)
                        logger.warning(f"阶段 {stage_id} 被阻断，停止后续检查")
                        break
                        
                except Exception as e:
                    logger.error(f"检查阶段 {stage_id} 时出错: {e}")
                    pipeline_report['stage_reports'][stage_id] = {
                        'stage_id': stage_id,
                        'error': str(e),
                        'status': 'ERROR'
                    }
                    pipeline_report['pipeline_blocked'] = True
                    pipeline_report['blocking_stages'].append(stage_id)
                    break
            
            # 计算总体状态
            if pipeline_report['pipeline_blocked']:
                pipeline_report['overall_pipeline_status'] = 'BLOCKED'
            else:
                stage_statuses = [report.get('overall_status') for report in pipeline_report['stage_reports'].values() if isinstance(report, dict) and 'overall_status' in report]
                if 'FAIL' in stage_statuses:
                    pipeline_report['overall_pipeline_status'] = 'FAIL'
                elif 'WARN' in stage_statuses:
                    pipeline_report['overall_pipeline_status'] = 'WARN'
                else:
                    pipeline_report['overall_pipeline_status'] = 'PASS'
            
            # 计算总执行时间
            total_end_time = datetime.now()
            pipeline_report['total_execution_time_ms'] = (total_end_time - total_start_time).total_seconds() * 1000
            
            # 生成管道级建议
            pipeline_report['recommendations'] = self._generate_pipeline_recommendations(pipeline_report)
            
            logger.info(f"批次 {batch_code} 管道质量检查完成 - 状态: {pipeline_report['overall_pipeline_status']}")
            
        except Exception as e:
            logger.error(f"管道质量检查失败: {e}")
            pipeline_report['overall_pipeline_status'] = 'ERROR'
            pipeline_report['error'] = str(e)
        
        return pipeline_report
    
    def _execute_quality_rule(self, batch_code: str, rule) -> QualityCheckResult:
        """执行单个质量规则检查"""
        start_time = datetime.now()
        
        try:
            # 根据规则类型执行不同的检查逻辑
            if rule.rule_id == "NULL_SCHOOL_NAMES":
                result = self.validator._check_null_school_names(batch_code)
                check_value = result['count']
                
            elif rule.rule_id == "ORPHANED_SCHOOLS":
                result = self.validator._check_orphaned_schools_quick(batch_code)
                check_value = result['count']
                
            elif rule.rule_id == "SCHOOL_COUNT_DEVIATION":
                result = self.validator._check_school_count_deviation(batch_code)
                check_value = result['deviation_percent']
                
            elif rule.rule_id == "LOW_STUDENT_SCHOOLS":
                result = self.validator._check_student_distribution_quick(batch_code)
                check_value = result['low_student_schools']
                
            elif rule.rule_id == "AGGREGATION_COMPLETENESS":
                result = self.validator._check_aggregation_completeness(batch_code)
                check_value = 100 - result['completeness_rate']  # 转换为缺失率
                
            elif rule.rule_id == "POST_PROCESSING_NULL_NAMES":
                result = self.validator._check_null_school_names_post_processing(batch_code)
                check_value = result['count']
                
            elif rule.check_sql:
                # 使用SQL查询执行检查
                from sqlalchemy import text
                query = text(rule.check_sql)
                query_result = self.db_session.execute(query, {'batch_code': batch_code, 'threshold': rule.threshold_value})
                
                # 假设查询返回单个数值
                row = query_result.fetchone()
                if row:
                    check_value = float(row[0]) if row[0] is not None else 0.0
                else:
                    check_value = 0.0
                
                result = {'check_value': check_value}
                
            else:
                # 默认处理
                check_value = 0.0
                result = {'message': '规则未实现具体检查逻辑'}
            
            # 判断检查结果
            status = self._evaluate_rule_result(check_value, rule)
            
            end_time = datetime.now()
            execution_time_ms = (end_time - start_time).total_seconds() * 1000
            
            # 生成消息
            if status == "PASS":
                message = f"检查通过 - {rule.description}"
            elif status == "WARN":
                message = f"警告 - {rule.description}，检查值: {check_value}"
            elif status == "FAIL":
                message = f"失败 - {rule.description}，检查值: {check_value}，阈值: {rule.threshold_value}"
            else:
                message = f"错误 - {rule.description}"
            
            return QualityCheckResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                status=status,
                severity=rule.severity.value,
                action=rule.action.value,
                check_value=check_value,
                threshold_value=rule.threshold_value,
                message=message,
                details=result,
                recommendations=rule.recommendations,
                execution_time_ms=execution_time_ms
            )
            
        except Exception as e:
            end_time = datetime.now()
            execution_time_ms = (end_time - start_time).total_seconds() * 1000
            
            logger.error(f"执行规则 {rule.rule_id} 时出错: {e}")
            return QualityCheckResult(
                rule_id=rule.rule_id,
                rule_name=rule.name,
                status="ERROR",
                severity=rule.severity.value,
                action=rule.action.value,
                check_value=None,
                threshold_value=rule.threshold_value,
                message=f"规则执行出错: {str(e)}",
                details={'error': str(e)},
                recommendations=["检查数据库连接", "重新运行检查"],
                execution_time_ms=execution_time_ms
            )
    
    def _evaluate_rule_result(self, check_value: float, rule) -> str:
        """评估规则检查结果"""
        if check_value is None:
            return "ERROR"
        
        threshold = rule.threshold_value
        operator = rule.threshold_operator
        
        # 根据操作符比较值
        if operator == "gt" and check_value > threshold:
            return "FAIL"
        elif operator == "gte" and check_value >= threshold:
            return "FAIL"
        elif operator == "lt" and check_value < threshold:
            return "FAIL"
        elif operator == "lte" and check_value <= threshold:
            return "FAIL"
        elif operator == "eq" and check_value == threshold:
            return "FAIL"
        elif operator == "ne" and check_value != threshold:
            return "FAIL"
        else:
            # 根据严重程度决定是否为警告
            if rule.severity in [Severity.LOW, Severity.MEDIUM] and rule.action == ActionType.WARN:
                # 对于低和中等严重程度的规则，可能设置警告区间
                warning_threshold = threshold * 0.8 if operator in ["gt", "gte"] else threshold * 1.2
                if (operator in ["gt", "gte"] and check_value > warning_threshold) or \
                   (operator in ["lt", "lte"] and check_value < warning_threshold):
                    return "WARN"
            
            return "PASS"
    
    def _determine_overall_status(self, check_results: List[QualityCheckResult], blocking_issues: List[QualityCheckResult]) -> str:
        """确定总体状态"""
        if blocking_issues:
            return "BLOCKED"
        
        statuses = [result.status for result in check_results]
        
        if "ERROR" in statuses:
            return "ERROR"
        elif "FAIL" in statuses:
            return "FAIL"
        elif "WARN" in statuses:
            return "WARN"
        else:
            return "PASS"
    
    def _generate_pipeline_recommendations(self, pipeline_report: Dict[str, Any]) -> List[str]:
        """生成管道级建议"""
        recommendations = []
        
        if pipeline_report['pipeline_blocked']:
            recommendations.append("发现阻断性问题，必须修复后才能继续处理")
            for stage_id in pipeline_report['blocking_stages']:
                stage_report = pipeline_report['stage_reports'].get(stage_id, {})
                if isinstance(stage_report, dict) and 'blocking_issues' in stage_report:
                    for issue in stage_report['blocking_issues']:
                        recommendations.extend(issue.get('recommendations', []))
        
        # 分析阶段间的关联问题
        stage_reports = pipeline_report['stage_reports']
        if 'DATA_PREPARATION' in stage_reports and 'DATA_CLEANING' in stage_reports:
            prep_status = stage_reports['DATA_PREPARATION'].get('overall_status')
            clean_status = stage_reports['DATA_CLEANING'].get('overall_status')
            
            if prep_status in ['WARN', 'FAIL'] and clean_status in ['WARN', 'FAIL']:
                recommendations.append("数据准备和清洗阶段都有问题，建议从源头检查数据质量")
        
        # 性能建议
        total_time = pipeline_report.get('total_execution_time_ms', 0)
        if total_time > 300000:  # 超过5分钟
            recommendations.append("质量检查耗时较长，考虑优化检查规则或并行处理")
        
        return recommendations if recommendations else ["质量检查通过，可以继续处理"]
    
    def _send_quality_alert(self, report: StageQualityReport):
        """发送质量警报"""
        alert_settings = self.config_manager.config.alert_settings
        
        if not alert_settings.get('email_enabled', False) and not alert_settings.get('sms_enabled', False):
            # 如果没有配置外部通知，至少记录日志
            logger.critical(f"数据质量警报 - 批次: {report.batch_code}, 阶段: {report.stage_name}")
            for issue in report.blocking_issues:
                logger.critical(f"阻断问题: {issue.rule_name} - {issue.message}")
        
        # 这里可以扩展为发送邮件、短信等
        # 例如：
        # if alert_settings.get('email_enabled', False):
        #     self._send_email_alert(report)
        # if alert_settings.get('sms_enabled', False):
        #     self._send_sms_alert(report)
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取性能指标"""
        avg_execution_time = (
            self.performance_metrics['total_execution_time_ms'] / 
            self.performance_metrics['total_checks_run']
        ) if self.performance_metrics['total_checks_run'] > 0 else 0
        
        return {
            'total_checks_run': self.performance_metrics['total_checks_run'],
            'total_execution_time_ms': self.performance_metrics['total_execution_time_ms'],
            'average_execution_time_ms': round(avg_execution_time, 2),
            'checks_by_rule': self.performance_metrics['checks_by_rule'],
            'checks_by_severity': self.performance_metrics['checks_by_severity']
        }
    
    def save_quality_report(self, report: StageQualityReport, output_dir: str = "quality_reports") -> str:
        """保存质量报告到文件"""
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = report.timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"quality_report_{report.batch_code}_{report.stage_id}_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
        
        logger.info(f"质量报告已保存到: {filepath}")
        return filepath


# 便利函数
def create_quality_service(db_session: Session) -> DataQualityService:
    """创建数据质量服务实例"""
    return DataQualityService(db_session)


def check_batch_ready_for_processing(db_session: Session, batch_code: str) -> Dict[str, Any]:
    """检查批次是否准备好进行处理"""
    service = create_quality_service(db_session)
    return service.check_stage_quality(batch_code, "DATA_PREPARATION").to_dict()


def check_batch_ready_for_cleaning(db_session: Session, batch_code: str) -> Dict[str, Any]:
    """检查批次是否准备好进行清洗"""
    service = create_quality_service(db_session)
    return service.check_stage_quality(batch_code, "DATA_CLEANING").to_dict()


def check_batch_aggregation_quality(db_session: Session, batch_code: str) -> Dict[str, Any]:
    """检查批次汇聚质量"""
    service = create_quality_service(db_session)
    return service.check_stage_quality(batch_code, "DATA_AGGREGATION").to_dict()


def check_batch_final_quality(db_session: Session, batch_code: str) -> Dict[str, Any]:
    """检查批次最终输出质量"""
    service = create_quality_service(db_session)
    return service.check_stage_quality(batch_code, "RESULT_SERIALIZATION").to_dict()


def run_complete_batch_quality_check(db_session: Session, batch_code: str) -> Dict[str, Any]:
    """运行完整的批次质量检查"""
    service = create_quality_service(db_session)
    return service.check_batch_processing_pipeline(batch_code)


if __name__ == "__main__":
    # 用于测试
    import sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    
    from app.database.connection import get_db
    
    batch_code = sys.argv[1] if len(sys.argv) > 1 else "G4-2025"
    stage_id = sys.argv[2] if len(sys.argv) > 2 else "DATA_PREPARATION"
    
    with next(get_db()) as db:
        service = create_quality_service(db)
        
        if stage_id == "ALL":
            result = service.check_batch_processing_pipeline(batch_code)
            print("完整管道质量检查结果:")
            print(f"批次: {result['batch_code']}")
            print(f"管道状态: {result['overall_pipeline_status']}")
            print(f"管道阻断: {result['pipeline_blocked']}")
            
            if result['blocking_stages']:
                print(f"阻断阶段: {result['blocking_stages']}")
            
            print(f"执行时间: {result['total_execution_time_ms']:.2f}ms")
            
            if result['recommendations']:
                print("建议:")
                for rec in result['recommendations']:
                    print(f"  - {rec}")
        else:
            result = service.check_stage_quality(batch_code, stage_id)
            print(f"阶段 {stage_id} 质量检查结果:")
            print(f"批次: {result.batch_code}")
            print(f"状态: {result.overall_status}")
            print(f"可继续: {result.can_proceed}")
            print(f"检查数量: {len(result.check_results)}")
            print(f"阻断问题: {len(result.blocking_issues)}")
            
            if result.blocking_issues:
                print("阻断问题:")
                for issue in result.blocking_issues:
                    print(f"  - {issue.rule_name}: {issue.message}")