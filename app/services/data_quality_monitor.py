"""
数据质量监控服务

提供持续的数据质量监控功能，用于：
- 监控学校信息一致性
- 检测数据汇聚异常
- 生成质量报告
- 提供告警机制
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database.connection import get_db
from app.utils.school_validation import validate_school_data_before_aggregation, SchoolValidationReport
from app.services.subjects_builder_enhanced import school_data_validator

logger = logging.getLogger(__name__)


class QualityLevel(Enum):
    """质量等级"""
    EXCELLENT = "excellent"  # 优秀 (>95%)
    GOOD = "good"           # 良好 (85-95%)  
    FAIR = "fair"           # 一般 (70-85%)
    POOR = "poor"           # 较差 (<70%)


class AlertLevel(Enum):
    """告警级别"""
    CRITICAL = "critical"   # 严重 - 阻止汇聚
    WARNING = "warning"     # 警告 - 需要关注
    INFO = "info"          # 信息 - 仅记录


@dataclass
class QualityMetric:
    """质量指标"""
    name: str
    value: float
    max_value: float
    percentage: float
    level: QualityLevel
    description: str
    details: Optional[Dict[str, Any]] = None


@dataclass
class QualityAlert:
    """质量告警"""
    level: AlertLevel
    title: str
    message: str
    batch_code: str
    timestamp: datetime
    details: Optional[Dict[str, Any]] = None
    resolved: bool = False


class DataQualityMonitor:
    """数据质量监控器"""
    
    def __init__(self):
        self.alerts: List[QualityAlert] = []
    
    def monitor_batch_quality(self, batch_code: str, db_session: Session = None) -> Dict[str, Any]:
        """监控批次数据质量"""
        if not db_session:
            db_session = next(get_db())
            should_close = True
        else:
            should_close = False
            
        try:
            logger.info(f"Starting quality monitoring for batch {batch_code}")
            
            # 1. 基础数据质量检查
            basic_metrics = self._check_basic_data_quality(batch_code, db_session)
            
            # 2. 学校信息一致性检查
            school_consistency = self._check_school_consistency(batch_code, db_session)
            
            # 3. 汇聚数据质量检查
            aggregation_quality = self._check_aggregation_quality(batch_code, db_session)
            
            # 4. 数据完整性检查
            completeness = self._check_data_completeness(batch_code, db_session)
            
            # 5. 计算总体质量分数
            overall_quality = self._calculate_overall_quality([
                basic_metrics, school_consistency, aggregation_quality, completeness
            ])
            
            # 6. 生成告警
            self._generate_alerts(batch_code, {
                "basic_metrics": basic_metrics,
                "school_consistency": school_consistency,
                "aggregation_quality": aggregation_quality,
                "completeness": completeness
            })
            
            report = {
                "batch_code": batch_code,
                "timestamp": datetime.now().isoformat(),
                "overall_quality": asdict(overall_quality),
                "metrics": {
                    "basic_data": asdict(basic_metrics),
                    "school_consistency": asdict(school_consistency),
                    "aggregation_quality": asdict(aggregation_quality),
                    "data_completeness": asdict(completeness)
                },
                "alerts": [asdict(alert) for alert in self.alerts if alert.batch_code == batch_code and not alert.resolved],
                "recommendations": self._generate_recommendations(basic_metrics, school_consistency, aggregation_quality, completeness)
            }
            
            logger.info(f"Quality monitoring completed for batch {batch_code}, overall quality: {overall_quality.level.value}")
            return report
            
        except Exception as e:
            logger.error(f"Error monitoring batch quality for {batch_code}: {str(e)}")
            raise
        finally:
            if should_close:
                db_session.close()
    
    def _check_basic_data_quality(self, batch_code: str, db_session: Session) -> QualityMetric:
        """检查基础数据质量"""
        try:
            # 统计基础数据指标
            stats = db_session.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT student_id) as unique_students,
                    COUNT(DISTINCT school_code) as unique_schools,
                    COUNT(DISTINCT subject_name) as unique_subjects,
                    COUNT(CASE WHEN total_score IS NOT NULL THEN 1 END) as valid_scores,
                    COUNT(CASE WHEN school_name IS NOT NULL AND school_name != '' THEN 1 END) as valid_school_names
                FROM student_cleaned_scores
                WHERE batch_code = :batch_code
            """), {"batch_code": batch_code}).fetchone()
            
            if not stats or stats.total_records == 0:
                return QualityMetric(
                    name="basic_data_quality",
                    value=0, max_value=100, percentage=0.0,
                    level=QualityLevel.POOR,
                    description="No cleaned data found",
                    details={"error": "no_data"}
                )
            
            # 计算数据质量分数
            score_completeness = (stats.valid_scores / stats.total_records * 100) if stats.total_records > 0 else 0
            name_completeness = (stats.valid_school_names / stats.total_records * 100) if stats.total_records > 0 else 0
            
            # 综合质量分数
            quality_score = (score_completeness + name_completeness) / 2
            
            # 确定质量等级
            if quality_score >= 95:
                level = QualityLevel.EXCELLENT
            elif quality_score >= 85:
                level = QualityLevel.GOOD
            elif quality_score >= 70:
                level = QualityLevel.FAIR
            else:
                level = QualityLevel.POOR
            
            return QualityMetric(
                name="basic_data_quality",
                value=quality_score, max_value=100, percentage=quality_score,
                level=level,
                description=f"Basic data quality: {quality_score:.1f}%",
                details={
                    "total_records": stats.total_records,
                    "unique_students": stats.unique_students,
                    "unique_schools": stats.unique_schools,
                    "unique_subjects": stats.unique_subjects,
                    "score_completeness": score_completeness,
                    "name_completeness": name_completeness
                }
            )
            
        except Exception as e:
            logger.error(f"Error checking basic data quality: {str(e)}")
            return QualityMetric(
                name="basic_data_quality",
                value=0, max_value=100, percentage=0.0,
                level=QualityLevel.POOR,
                description=f"Error checking basic data quality: {str(e)}"
            )
    
    def _check_school_consistency(self, batch_code: str, db_session: Session) -> QualityMetric:
        """检查学校信息一致性"""
        try:
            # 使用现有的学校验证功能
            validation_report = validate_school_data_before_aggregation(db_session, batch_code)
            
            if validation_report.has_errors():
                level = QualityLevel.POOR
                score = 30.0
            elif validation_report.has_warnings():
                level = QualityLevel.FAIR
                score = 70.0
            else:
                level = QualityLevel.EXCELLENT
                score = 95.0
            
            return QualityMetric(
                name="school_consistency",
                value=score, max_value=100, percentage=score,
                level=level,
                description=f"School consistency: {validation_report.summary}",
                details=validation_report.get_report()
            )
            
        except Exception as e:
            logger.error(f"Error checking school consistency: {str(e)}")
            return QualityMetric(
                name="school_consistency",
                value=0, max_value=100, percentage=0.0,
                level=QualityLevel.POOR,
                description=f"Error checking school consistency: {str(e)}"
            )
    
    def _check_aggregation_quality(self, batch_code: str, db_session: Session) -> QualityMetric:
        """检查汇聚数据质量"""
        try:
            # 检查汇聚数据统计
            agg_stats = db_session.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN aggregation_level = 'SCHOOL' THEN 1 END) as school_records,
                    COUNT(CASE WHEN aggregation_level = 'REGIONAL' THEN 1 END) as regional_records,
                    COUNT(CASE WHEN school_name IS NOT NULL AND aggregation_level = 'SCHOOL' THEN 1 END) as school_with_names,
                    COUNT(CASE WHEN calculation_status = 'COMPLETED' THEN 1 END) as completed_records
                FROM statistical_aggregations
                WHERE batch_code = :batch_code
            """), {"batch_code": batch_code}).fetchone()
            
            if not agg_stats or agg_stats.total_records == 0:
                return QualityMetric(
                    name="aggregation_quality",
                    value=0, max_value=100, percentage=0.0,
                    level=QualityLevel.POOR,
                    description="No aggregation data found"
                )
            
            # 计算汇聚质量分数
            completion_rate = (agg_stats.completed_records / agg_stats.total_records * 100) if agg_stats.total_records > 0 else 0
            name_rate = (agg_stats.school_with_names / agg_stats.school_records * 100) if agg_stats.school_records > 0 else 100
            
            # 检查是否有合理数量的区域记录（应该只有1条）
            regional_penalty = 0
            if agg_stats.regional_records > 1:
                regional_penalty = 20  # 有重复区域记录扣分
            elif agg_stats.regional_records == 0:
                regional_penalty = 10  # 没有区域记录扣分
            
            quality_score = (completion_rate + name_rate) / 2 - regional_penalty
            quality_score = max(0, min(100, quality_score))  # 限制在0-100范围
            
            if quality_score >= 95:
                level = QualityLevel.EXCELLENT
            elif quality_score >= 85:
                level = QualityLevel.GOOD
            elif quality_score >= 70:
                level = QualityLevel.FAIR
            else:
                level = QualityLevel.POOR
            
            return QualityMetric(
                name="aggregation_quality",
                value=quality_score, max_value=100, percentage=quality_score,
                level=level,
                description=f"Aggregation quality: {quality_score:.1f}%",
                details={
                    "total_records": agg_stats.total_records,
                    "school_records": agg_stats.school_records,
                    "regional_records": agg_stats.regional_records,
                    "completion_rate": completion_rate,
                    "name_rate": name_rate,
                    "regional_penalty": regional_penalty
                }
            )
            
        except Exception as e:
            logger.error(f"Error checking aggregation quality: {str(e)}")
            return QualityMetric(
                name="aggregation_quality",
                value=0, max_value=100, percentage=0.0,
                level=QualityLevel.POOR,
                description=f"Error checking aggregation quality: {str(e)}"
            )
    
    def _check_data_completeness(self, batch_code: str, db_session: Session) -> QualityMetric:
        """检查数据完整性"""
        try:
            # 检查主数据vs清洗数据vs汇聚数据的完整性
            completeness_stats = db_session.execute(text("""
                SELECT 
                    (SELECT COUNT(*) FROM school_master_data WHERE batch_code = :batch_code AND status = 'ACTIVE') as master_schools,
                    (SELECT COUNT(DISTINCT school_code) FROM student_cleaned_scores WHERE batch_code = :batch_code) as cleaned_schools,
                    (SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = :batch_code AND aggregation_level = 'SCHOOL') as agg_schools
            """), {"batch_code": batch_code}).fetchone()
            
            master_schools = completeness_stats.master_schools or 0
            cleaned_schools = completeness_stats.cleaned_schools or 0  
            agg_schools = completeness_stats.agg_schools or 0
            
            if master_schools == 0:
                return QualityMetric(
                    name="data_completeness",
                    value=0, max_value=100, percentage=0.0,
                    level=QualityLevel.POOR,
                    description="No master data found"
                )
            
            # 计算完整性分数
            cleaning_completeness = (cleaned_schools / master_schools * 100) if master_schools > 0 else 0
            aggregation_completeness = (agg_schools / master_schools * 100) if master_schools > 0 else 0
            
            # 综合完整性分数
            completeness_score = (cleaning_completeness + aggregation_completeness) / 2
            
            if completeness_score >= 95:
                level = QualityLevel.EXCELLENT
            elif completeness_score >= 85:
                level = QualityLevel.GOOD
            elif completeness_score >= 70:
                level = QualityLevel.FAIR
            else:
                level = QualityLevel.POOR
            
            return QualityMetric(
                name="data_completeness",
                value=completeness_score, max_value=100, percentage=completeness_score,
                level=level,
                description=f"Data completeness: {completeness_score:.1f}%",
                details={
                    "master_schools": master_schools,
                    "cleaned_schools": cleaned_schools,
                    "agg_schools": agg_schools,
                    "cleaning_completeness": cleaning_completeness,
                    "aggregation_completeness": aggregation_completeness
                }
            )
            
        except Exception as e:
            logger.error(f"Error checking data completeness: {str(e)}")
            return QualityMetric(
                name="data_completeness",
                value=0, max_value=100, percentage=0.0,
                level=QualityLevel.POOR,
                description=f"Error checking data completeness: {str(e)}"
            )
    
    def _calculate_overall_quality(self, metrics: List[QualityMetric]) -> QualityMetric:
        """计算总体质量分数"""
        if not metrics:
            return QualityMetric(
                name="overall_quality",
                value=0, max_value=100, percentage=0.0,
                level=QualityLevel.POOR,
                description="No metrics available"
            )
        
        # 加权平均（可以根据需要调整权重）
        weights = {
            "basic_data_quality": 0.3,
            "school_consistency": 0.3,
            "aggregation_quality": 0.25,
            "data_completeness": 0.15
        }
        
        weighted_score = 0.0
        total_weight = 0.0
        
        for metric in metrics:
            weight = weights.get(metric.name, 0.25)  # 默认权重
            weighted_score += metric.value * weight
            total_weight += weight
        
        if total_weight > 0:
            overall_score = weighted_score / total_weight
        else:
            overall_score = 0.0
        
        # 确定总体质量等级
        if overall_score >= 95:
            level = QualityLevel.EXCELLENT
        elif overall_score >= 85:
            level = QualityLevel.GOOD
        elif overall_score >= 70:
            level = QualityLevel.FAIR
        else:
            level = QualityLevel.POOR
        
        return QualityMetric(
            name="overall_quality",
            value=overall_score, max_value=100, percentage=overall_score,
            level=level,
            description=f"Overall quality: {overall_score:.1f}%",
            details={metric.name: metric.value for metric in metrics}
        )
    
    def _generate_alerts(self, batch_code: str, metrics: Dict[str, QualityMetric]):
        """生成告警"""
        timestamp = datetime.now()
        
        for name, metric in metrics.items():
            if metric.level == QualityLevel.POOR:
                alert_level = AlertLevel.CRITICAL if metric.value < 50 else AlertLevel.WARNING
                
                alert = QualityAlert(
                    level=alert_level,
                    title=f"数据质量问题: {name}",
                    message=f"批次 {batch_code} 的 {metric.description} 质量分数为 {metric.value:.1f}%，需要关注",
                    batch_code=batch_code,
                    timestamp=timestamp,
                    details=metric.details
                )
                self.alerts.append(alert)
            
            elif metric.level == QualityLevel.FAIR:
                alert = QualityAlert(
                    level=AlertLevel.INFO,
                    title=f"数据质量提醒: {name}",
                    message=f"批次 {batch_code} 的 {metric.description} 质量分数为 {metric.value:.1f}%，建议优化",
                    batch_code=batch_code,
                    timestamp=timestamp,
                    details=metric.details
                )
                self.alerts.append(alert)
    
    def _generate_recommendations(self, *metrics: QualityMetric) -> List[str]:
        """生成改进建议"""
        recommendations = []
        
        for metric in metrics:
            if metric.level == QualityLevel.POOR:
                if metric.name == "basic_data_quality":
                    recommendations.append("检查数据清洗流程，确保分数和学校名称字段完整")
                elif metric.name == "school_consistency":
                    recommendations.append("核实学校主数据，解决学校信息不匹配问题")
                elif metric.name == "aggregation_quality":
                    recommendations.append("检查汇聚算法，修复学校名称缺失和重复区域记录问题")
                elif metric.name == "data_completeness":
                    recommendations.append("确保所有主数据中的学校都参与清洗和汇聚流程")
            
            elif metric.level == QualityLevel.FAIR:
                recommendations.append(f"关注 {metric.name} 的质量提升，当前分数: {metric.value:.1f}%")
        
        if not recommendations:
            recommendations.append("数据质量良好，继续保持")
        
        return recommendations
    
    def get_alerts(self, batch_code: str = None, level: AlertLevel = None) -> List[QualityAlert]:
        """获取告警列表"""
        alerts = self.alerts
        
        if batch_code:
            alerts = [alert for alert in alerts if alert.batch_code == batch_code]
        
        if level:
            alerts = [alert for alert in alerts if alert.level == level]
        
        return alerts
    
    def resolve_alert(self, alert_index: int):
        """解决告警"""
        if 0 <= alert_index < len(self.alerts):
            self.alerts[alert_index].resolved = True
    
    def save_quality_report(self, report: Dict[str, Any], filename: str = None):
        """保存质量报告到文件"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"quality_report_{report['batch_code']}_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"Quality report saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save quality report: {str(e)}")


# 全局监控器实例
quality_monitor = DataQualityMonitor()


def monitor_batch_quality_simple(batch_code: str) -> Dict[str, Any]:
    """简便函数：监控批次质量"""
    return quality_monitor.monitor_batch_quality(batch_code)


def get_quality_summary(batch_code: str) -> str:
    """简便函数：获取质量摘要"""
    try:
        report = quality_monitor.monitor_batch_quality(batch_code)
        overall = report["overall_quality"]
        level_cn = {
            "excellent": "优秀",
            "good": "良好", 
            "fair": "一般",
            "poor": "较差"
        }
        
        return f"批次 {batch_code} 数据质量: {level_cn.get(overall['level'], '未知')} ({overall['percentage']:.1f}%)"
        
    except Exception as e:
        return f"批次 {batch_code} 质量检查失败: {str(e)}"