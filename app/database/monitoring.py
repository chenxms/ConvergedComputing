# 数据库和缓存性能监控工具
import time
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from collections import defaultdict
import statistics

logger = logging.getLogger(__name__)


@dataclass
class QueryMetrics:
    """查询指标数据类"""
    operation: str
    duration: float
    timestamp: datetime
    query_info: Optional[Dict[str, Any]] = None
    cache_hit: bool = False


@dataclass  
class PerformanceSnapshot:
    """性能快照数据类"""
    timestamp: datetime
    total_queries: int
    avg_query_time: float
    cache_hit_rate: float
    slow_queries_count: int
    error_rate: float
    memory_usage_mb: float


class RepositoryMonitor:
    """Repository性能监控器"""
    
    def __init__(self, slow_query_threshold: float = 1.0):
        self.slow_query_threshold = slow_query_threshold
        self.query_metrics: List[QueryMetrics] = []
        self.error_count = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.snapshots: List[PerformanceSnapshot] = []
        
        # 操作类型统计
        self.operation_stats = defaultdict(list)
        
    def record_query(
        self, 
        operation: str, 
        duration: float, 
        query_info: Optional[Dict[str, Any]] = None,
        cache_hit: bool = False
    ) -> None:
        """记录查询性能"""
        metric = QueryMetrics(
            operation=operation,
            duration=duration,
            timestamp=datetime.now(),
            query_info=query_info,
            cache_hit=cache_hit
        )
        
        self.query_metrics.append(metric)
        self.operation_stats[operation].append(duration)
        
        if cache_hit:
            self.cache_hits += 1
        else:
            self.cache_misses += 1
        
        # 记录到日志
        level = logging.INFO
        if duration > self.slow_query_threshold:
            level = logging.WARNING
            
        logger.log(level, 
            f"Query {operation} completed in {duration:.3f}s "
            f"(cache_hit: {cache_hit})"
        )
        
        # 保持最近1000条记录
        if len(self.query_metrics) > 1000:
            self.query_metrics = self.query_metrics[-1000:]
    
    def record_error(self, operation: str, error: Exception) -> None:
        """记录错误"""
        self.error_count += 1
        logger.error(f"Error in {operation}: {str(error)}")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计"""
        if not self.query_metrics:
            return self._empty_stats()
        
        durations = [m.duration for m in self.query_metrics]
        total_requests = self.cache_hits + self.cache_misses
        cache_hit_rate = (self.cache_hits / total_requests) if total_requests > 0 else 0.0
        
        slow_queries = [m for m in self.query_metrics if m.duration > self.slow_query_threshold]
        
        return {
            "total_queries": len(self.query_metrics),
            "average_query_time": statistics.mean(durations),
            "median_query_time": statistics.median(durations),
            "min_query_time": min(durations),
            "max_query_time": max(durations),
            "cache_hit_rate": cache_hit_rate,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "slow_queries_count": len(slow_queries),
            "error_count": self.error_count,
            "error_rate": self.error_count / len(self.query_metrics),
            "operation_breakdown": self._get_operation_breakdown(),
            "recent_slow_queries": self._get_recent_slow_queries(10)
        }
    
    def _empty_stats(self) -> Dict[str, Any]:
        """空统计数据"""
        return {
            "total_queries": 0,
            "average_query_time": 0.0,
            "median_query_time": 0.0,
            "min_query_time": 0.0,
            "max_query_time": 0.0,
            "cache_hit_rate": 0.0,
            "cache_hits": 0,
            "cache_misses": 0,
            "slow_queries_count": 0,
            "error_count": 0,
            "error_rate": 0.0,
            "operation_breakdown": {},
            "recent_slow_queries": []
        }
    
    def _get_operation_breakdown(self) -> Dict[str, Dict[str, float]]:
        """获取操作类型统计分析"""
        breakdown = {}
        
        for operation, durations in self.operation_stats.items():
            if durations:
                breakdown[operation] = {
                    "count": len(durations),
                    "avg_duration": statistics.mean(durations),
                    "min_duration": min(durations),
                    "max_duration": max(durations),
                    "total_duration": sum(durations)
                }
        
        return breakdown
    
    def _get_recent_slow_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取最近的慢查询"""
        slow_queries = [
            m for m in self.query_metrics 
            if m.duration > self.slow_query_threshold
        ]
        
        # 按时间倒序排列，取最近的
        slow_queries.sort(key=lambda x: x.timestamp, reverse=True)
        
        return [
            {
                "operation": q.operation,
                "duration": q.duration,
                "timestamp": q.timestamp.isoformat(),
                "query_info": q.query_info
            }
            for q in slow_queries[:limit]
        ]
    
    def take_snapshot(self) -> PerformanceSnapshot:
        """创建性能快照"""
        stats = self.get_performance_stats()
        
        snapshot = PerformanceSnapshot(
            timestamp=datetime.now(),
            total_queries=stats["total_queries"],
            avg_query_time=stats["average_query_time"],
            cache_hit_rate=stats["cache_hit_rate"],
            slow_queries_count=stats["slow_queries_count"],
            error_rate=stats["error_rate"],
            memory_usage_mb=0.0  # TODO: 实际内存使用统计
        )
        
        self.snapshots.append(snapshot)
        
        # 保持最近100个快照
        if len(self.snapshots) > 100:
            self.snapshots = self.snapshots[-100:]
        
        return snapshot
    
    def get_performance_trend(self, hours: int = 24) -> Dict[str, Any]:
        """获取性能趋势（最近N小时）"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_snapshots = [
            s for s in self.snapshots 
            if s.timestamp >= cutoff_time
        ]
        
        if not recent_snapshots:
            return {"error": "No recent snapshots available"}
        
        return {
            "period_hours": hours,
            "snapshots_count": len(recent_snapshots),
            "avg_query_time_trend": [s.avg_query_time for s in recent_snapshots],
            "cache_hit_rate_trend": [s.cache_hit_rate for s in recent_snapshots],
            "slow_queries_trend": [s.slow_queries_count for s in recent_snapshots],
            "timestamps": [s.timestamp.isoformat() for s in recent_snapshots]
        }
    
    def reset(self) -> None:
        """重置所有统计数据"""
        self.query_metrics.clear()
        self.operation_stats.clear()
        self.error_count = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.snapshots.clear()
        
        logger.info("Repository monitor stats reset")
    
    def get_alerts(self) -> List[Dict[str, Any]]:
        """获取性能告警"""
        alerts = []
        stats = self.get_performance_stats()
        
        # 检查平均查询时间
        if stats["average_query_time"] > self.slow_query_threshold:
            alerts.append({
                "type": "slow_queries",
                "severity": "warning",
                "message": f"Average query time ({stats['average_query_time']:.3f}s) exceeds threshold ({self.slow_query_threshold}s)",
                "value": stats["average_query_time"],
                "threshold": self.slow_query_threshold
            })
        
        # 检查缓存命中率
        if stats["cache_hit_rate"] < 0.8:  # 低于80%
            alerts.append({
                "type": "low_cache_hit_rate",
                "severity": "warning",
                "message": f"Cache hit rate ({stats['cache_hit_rate']:.2%}) is below recommended threshold (80%)",
                "value": stats["cache_hit_rate"],
                "threshold": 0.8
            })
        
        # 检查错误率
        if stats["error_rate"] > 0.05:  # 高于5%
            alerts.append({
                "type": "high_error_rate",
                "severity": "critical",
                "message": f"Error rate ({stats['error_rate']:.2%}) is above acceptable threshold (5%)",
                "value": stats["error_rate"],
                "threshold": 0.05
            })
        
        return alerts


class DatabaseConnectionMonitor:
    """数据库连接监控器"""
    
    def __init__(self):
        self.connection_stats = {
            "active_connections": 0,
            "total_connections_created": 0,
            "connection_errors": 0,
            "avg_connection_time": 0.0,
            "connection_times": []
        }
    
    def record_connection_created(self, connection_time: float) -> None:
        """记录连接创建"""
        self.connection_stats["total_connections_created"] += 1
        self.connection_stats["active_connections"] += 1
        self.connection_stats["connection_times"].append(connection_time)
        
        # 保持最近100个连接时间记录
        if len(self.connection_stats["connection_times"]) > 100:
            self.connection_stats["connection_times"] = self.connection_stats["connection_times"][-100:]
        
        # 更新平均连接时间
        self.connection_stats["avg_connection_time"] = statistics.mean(
            self.connection_stats["connection_times"]
        )
    
    def record_connection_closed(self) -> None:
        """记录连接关闭"""
        if self.connection_stats["active_connections"] > 0:
            self.connection_stats["active_connections"] -= 1
    
    def record_connection_error(self) -> None:
        """记录连接错误"""
        self.connection_stats["connection_errors"] += 1
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """获取连接统计"""
        return self.connection_stats.copy()


class PerformanceAlert:
    """性能告警系统"""
    
    def __init__(self):
        self.alert_rules = {
            "slow_query_threshold": 1.0,
            "cache_hit_rate_threshold": 0.8,
            "error_rate_threshold": 0.05,
            "memory_usage_threshold": 0.9,
            "connection_count_threshold": 50
        }
        self.alert_history: List[Dict[str, Any]] = []
    
    def check_performance_alerts(
        self, 
        monitor: RepositoryMonitor,
        connection_monitor: DatabaseConnectionMonitor
    ) -> List[Dict[str, Any]]:
        """检查性能告警"""
        alerts = []
        
        # 获取统计数据
        perf_stats = monitor.get_performance_stats()
        conn_stats = connection_monitor.get_connection_stats()
        
        # 检查各项指标
        alerts.extend(self._check_query_performance(perf_stats))
        alerts.extend(self._check_cache_performance(perf_stats))
        alerts.extend(self._check_error_rates(perf_stats))
        alerts.extend(self._check_connection_health(conn_stats))
        
        # 记录告警历史
        for alert in alerts:
            alert["timestamp"] = datetime.now().isoformat()
            self.alert_history.append(alert)
        
        # 保持最近100条告警历史
        if len(self.alert_history) > 100:
            self.alert_history = self.alert_history[-100:]
        
        return alerts
    
    def _check_query_performance(self, stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """检查查询性能"""
        alerts = []
        
        if stats["average_query_time"] > self.alert_rules["slow_query_threshold"]:
            alerts.append({
                "type": "slow_queries",
                "severity": "warning",
                "message": f"Average query time too high: {stats['average_query_time']:.3f}s",
                "current_value": stats["average_query_time"],
                "threshold": self.alert_rules["slow_query_threshold"]
            })
        
        return alerts
    
    def _check_cache_performance(self, stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """检查缓存性能"""
        alerts = []
        
        if stats["cache_hit_rate"] < self.alert_rules["cache_hit_rate_threshold"]:
            alerts.append({
                "type": "low_cache_hit_rate",
                "severity": "warning",
                "message": f"Cache hit rate too low: {stats['cache_hit_rate']:.2%}",
                "current_value": stats["cache_hit_rate"],
                "threshold": self.alert_rules["cache_hit_rate_threshold"]
            })
        
        return alerts
    
    def _check_error_rates(self, stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """检查错误率"""
        alerts = []
        
        if stats["error_rate"] > self.alert_rules["error_rate_threshold"]:
            alerts.append({
                "type": "high_error_rate",
                "severity": "critical",
                "message": f"Error rate too high: {stats['error_rate']:.2%}",
                "current_value": stats["error_rate"],
                "threshold": self.alert_rules["error_rate_threshold"]
            })
        
        return alerts
    
    def _check_connection_health(self, stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """检查连接健康"""
        alerts = []
        
        if stats["active_connections"] > self.alert_rules["connection_count_threshold"]:
            alerts.append({
                "type": "high_connection_count",
                "severity": "warning",
                "message": f"Too many active connections: {stats['active_connections']}",
                "current_value": stats["active_connections"],
                "threshold": self.alert_rules["connection_count_threshold"]
            })
        
        return alerts
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """获取告警摘要"""
        if not self.alert_history:
            return {"total_alerts": 0}
        
        recent_alerts = [
            a for a in self.alert_history 
            if datetime.fromisoformat(a["timestamp"]) > datetime.now() - timedelta(hours=24)
        ]
        
        severity_counts = defaultdict(int)
        type_counts = defaultdict(int)
        
        for alert in recent_alerts:
            severity_counts[alert["severity"]] += 1
            type_counts[alert["type"]] += 1
        
        return {
            "total_alerts": len(self.alert_history),
            "recent_alerts_24h": len(recent_alerts),
            "severity_breakdown": dict(severity_counts),
            "type_breakdown": dict(type_counts),
            "latest_alert": self.alert_history[-1] if self.alert_history else None
        }