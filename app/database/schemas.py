# 数据库层数据模型和结果类
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class BatchOperationResult:
    """批量操作结果"""
    total_processed: int
    total_created: int
    total_updated: int
    errors: List[str]
    success_rate: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_processed": self.total_processed,
            "total_created": self.total_created,
            "total_updated": self.total_updated,
            "errors": self.errors,
            "success_rate": self.success_rate
        }


@dataclass
class BatchResult:
    """单批次操作结果"""
    processed_count: int
    created_count: int
    updated_count: int


@dataclass
class DeletionResult:
    """删除操作结果"""
    deleted_count: int
    deleted_ids: List[int]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "deleted_count": self.deleted_count,
            "deleted_ids": self.deleted_ids
        }


@dataclass
class TimelineItem:
    """时间线项目"""
    aggregation_level: str
    calculation_status: str
    count: int
    first_created: str
    last_updated: str
    avg_duration: float


@dataclass
class BatchTimelineResult:
    """批次时间线结果"""
    batch_code: str
    timeline: List[TimelineItem]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_code": self.batch_code,
            "timeline": [item.__dict__ for item in self.timeline]
        }


@dataclass
class PerformanceCriteria:
    """性能筛选条件"""
    min_avg_score: Optional[float] = None
    excellent_percentage_threshold: Optional[float] = None
    min_difficulty_coefficient: Optional[float] = None
    max_difficulty_coefficient: Optional[float] = None
    min_discrimination_index: Optional[float] = None
    max_discrimination_index: Optional[float] = None


@dataclass
class QueryCriteria:
    """查询条件"""
    batch_codes: Optional[List[str]] = None
    school_ids: Optional[List[str]] = None
    aggregation_level: Optional[str] = None
    calculation_status: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    min_students: Optional[int] = None
    max_students: Optional[int] = None
    min_duration: Optional[float] = None
    max_duration: Optional[float] = None
    school_name_pattern: Optional[str] = None
    data_version: Optional[str] = None
    json_filters: Optional[List[Dict[str, Any]]] = None
    order_by: str = "created_at"
    order_direction: str = "desc"
    offset: int = 0
    limit: int = 100
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典，过滤空值"""
        result = {}
        for key, value in self.__dict__.items():
            if value is not None:
                if isinstance(value, datetime):
                    result[key] = value.isoformat()
                else:
                    result[key] = value
        return result


@dataclass
class CacheStats:
    """缓存统计信息"""
    redis_memory_used: str
    redis_connected_clients: int
    cache_key_counts: Dict[str, int]
    total_keys: int
    hit_rate: Optional[float] = None
    miss_rate: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "redis_memory_used": self.redis_memory_used,
            "redis_connected_clients": self.redis_connected_clients,
            "cache_key_counts": self.cache_key_counts,
            "total_keys": self.total_keys,
            "hit_rate": self.hit_rate,
            "miss_rate": self.miss_rate
        }


@dataclass
class RepositoryStats:
    """Repository性能统计"""
    total_queries: int
    average_query_time: float
    cache_hit_rate: float
    total_batch_operations: int
    error_rate: float
    slow_queries: List[Dict[str, Any]]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_queries": self.total_queries,
            "average_query_time": self.average_query_time,
            "cache_hit_rate": self.cache_hit_rate,
            "total_batch_operations": self.total_batch_operations,
            "error_rate": self.error_rate,
            "slow_queries": self.slow_queries
        }


class QueryPerformanceTracker:
    """查询性能跟踪器"""
    
    def __init__(self):
        self.query_times = []
        self.slow_query_threshold = 1.0  # 1秒
        self.slow_queries = []
        
    def record_query(self, operation: str, duration: float, query_info: Dict[str, Any] = None):
        """记录查询性能"""
        self.query_times.append(duration)
        
        if duration > self.slow_query_threshold:
            self.slow_queries.append({
                "operation": operation,
                "duration": duration,
                "query_info": query_info or {},
                "timestamp": datetime.now().isoformat()
            })
    
    def get_stats(self) -> Dict[str, Any]:
        """获取性能统计"""
        if not self.query_times:
            return {
                "total_queries": 0,
                "average_time": 0.0,
                "min_time": 0.0,
                "max_time": 0.0,
                "slow_queries_count": 0
            }
        
        return {
            "total_queries": len(self.query_times),
            "average_time": sum(self.query_times) / len(self.query_times),
            "min_time": min(self.query_times),
            "max_time": max(self.query_times),
            "slow_queries_count": len(self.slow_queries),
            "slow_queries": self.slow_queries[-10:]  # 最近10个慢查询
        }
    
    def reset(self):
        """重置统计"""
        self.query_times.clear()
        self.slow_queries.clear()


# 异常类
class QueryBuilderError(Exception):
    """查询构建器异常"""
    pass


class CacheOperationError(Exception):
    """缓存操作异常"""
    pass


class BatchOperationError(Exception):
    """批量操作异常"""
    pass