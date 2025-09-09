# Repository工厂和依赖注入
from typing import Optional
from sqlalchemy.orm import Session

from .repositories import (
    StatisticalAggregationRepository,
    StatisticalMetadataRepository,
    StatisticalHistoryRepository,
    BatchRepository,
    TaskRepository
)
from .cached_repositories import CachedStatisticalAggregationRepository
from .cache import StatisticalDataCache
from .monitoring import RepositoryMonitor, PerformanceAlert
from .connection import get_cache_manager


class RepositoryFactory:
    """Repository工厂类"""
    
    def __init__(self, db_session: Session, cache_enabled: bool = True):
        self.db_session = db_session
        self.cache_enabled = cache_enabled
        self.cache_manager = get_cache_manager() if cache_enabled else None
        self.monitor = RepositoryMonitor()
        self.alert_system = PerformanceAlert()
    
    def create_statistical_aggregation_repository(
        self, 
        use_cache: bool = True
    ) -> StatisticalAggregationRepository:
        """创建统计汇聚Repository"""
        if use_cache and self.cache_enabled and self.cache_manager:
            return CachedStatisticalAggregationRepository(self.db_session, self.cache_manager)
        else:
            return StatisticalAggregationRepository(self.db_session)
    
    def create_statistical_metadata_repository(self) -> StatisticalMetadataRepository:
        """创建统计元数据Repository"""
        return StatisticalMetadataRepository(self.db_session)
    
    def create_statistical_history_repository(self) -> StatisticalHistoryRepository:
        """创建统计历史Repository"""
        return StatisticalHistoryRepository(self.db_session)
    
    def create_batch_repository(self) -> BatchRepository:
        """创建批次Repository"""
        return BatchRepository(self.db_session)
    
    def create_task_repository(self) -> TaskRepository:
        """创建任务Repository"""
        return TaskRepository(self.db_session)
    
    def get_monitor(self) -> RepositoryMonitor:
        """获取性能监控器"""
        return self.monitor
    
    def get_alert_system(self) -> PerformanceAlert:
        """获取告警系统"""
        return self.alert_system
    
    def get_cache_manager(self) -> Optional[StatisticalDataCache]:
        """获取缓存管理器"""
        return self.cache_manager


class RepositoryManager:
    """Repository管理器 - 单例模式"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.initialized = False
        return cls._instance
    
    def __init__(self):
        if not self.initialized:
            self.factories = {}
            self.initialized = True
    
    def get_factory(self, db_session: Session, cache_enabled: bool = True) -> RepositoryFactory:
        """获取Repository工厂（会话级别）"""
        session_id = id(db_session)
        
        if session_id not in self.factories:
            self.factories[session_id] = RepositoryFactory(db_session, cache_enabled)
        
        return self.factories[session_id]
    
    def cleanup_session(self, db_session: Session):
        """清理会话相关的工厂"""
        session_id = id(db_session)
        if session_id in self.factories:
            del self.factories[session_id]


# 便利函数
def create_repository_factory(db_session: Session, cache_enabled: bool = True) -> RepositoryFactory:
    """创建Repository工厂的便利函数"""
    return RepositoryFactory(db_session, cache_enabled)


def get_repository_manager() -> RepositoryManager:
    """获取Repository管理器单例"""
    return RepositoryManager()