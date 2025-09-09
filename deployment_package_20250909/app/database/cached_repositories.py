# 带缓存的Repository实现
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
import logging
import time
from datetime import datetime

from .repositories import StatisticalAggregationRepository
from .cache import StatisticalDataCache
from .query_builder import QueryResult
from .schemas import BatchOperationResult
from .models import StatisticalAggregation, AggregationLevel, CalculationStatus

logger = logging.getLogger(__name__)


class CachedStatisticalAggregationRepository(StatisticalAggregationRepository):
    """带缓存的统计汇聚Repository"""
    
    def __init__(self, db_session: Session, cache: Optional[StatisticalDataCache] = None):
        super().__init__(db_session)
        self.cache = cache
        self.cache_enabled = cache is not None
        self.cache_hits = 0
        self.cache_misses = 0
    
    async def get_regional_statistics(self, batch_code: str) -> Optional[StatisticalAggregation]:
        """获取区域级统计数据(带缓存)"""
        if not self.cache_enabled:
            return super().get_regional_statistics(batch_code)
        
        start_time = time.time()
        
        try:
            # 先检查缓存
            cached_result = await self.cache.get_regional_statistics(batch_code)
            if cached_result:
                self.cache_hits += 1
                result = self._build_aggregation_from_cache(cached_result)
                duration = time.time() - start_time
                self.performance_tracker.record_query("get_regional_statistics_cache_hit", duration)
                return result
            
            # 缓存未命中，查询数据库
            self.cache_misses += 1
            result = super().get_regional_statistics(batch_code)
            
            if result:
                # 将结果存入缓存
                cache_data = self._serialize_aggregation_for_cache(result)
                await self.cache.set_regional_statistics(batch_code, cache_data)
            
            duration = time.time() - start_time
            self.performance_tracker.record_query("get_regional_statistics_cache_miss", duration)
            
            return result
            
        except Exception as e:
            logger.error(f"Cache operation failed for regional statistics: {str(e)}")
            # 缓存失败时降级到直接数据库查询
            return super().get_regional_statistics(batch_code)
    
    async def get_school_statistics(self, batch_code: str, school_id: str) -> Optional[StatisticalAggregation]:
        """获取学校级统计数据(带缓存)"""
        if not self.cache_enabled:
            return super().get_school_statistics(batch_code, school_id)
        
        start_time = time.time()
        
        try:
            # 先检查缓存
            cached_result = await self.cache.get_school_statistics(batch_code, school_id)
            if cached_result:
                self.cache_hits += 1
                result = self._build_aggregation_from_cache(cached_result)
                duration = time.time() - start_time
                self.performance_tracker.record_query("get_school_statistics_cache_hit", duration)
                return result
            
            # 缓存未命中，查询数据库
            self.cache_misses += 1
            result = super().get_school_statistics(batch_code, school_id)
            
            if result:
                # 存入缓存
                cache_data = self._serialize_aggregation_for_cache(result)
                await self.cache.set_school_statistics(batch_code, school_id, cache_data)
            
            duration = time.time() - start_time
            self.performance_tracker.record_query("get_school_statistics_cache_miss", duration)
            
            return result
            
        except Exception as e:
            logger.error(f"Cache operation failed for school statistics: {str(e)}")
            # 缓存失败时降级到直接数据库查询
            return super().get_school_statistics(batch_code, school_id)
    
    async def get_batch_statistics_summary_cached(self, batch_code: str) -> Dict[str, Any]:
        """获取批次统计数据摘要(带缓存)"""
        if not self.cache_enabled:
            return super().get_batch_statistics_summary(batch_code)
        
        start_time = time.time()
        
        try:
            # 先检查缓存
            cached_result = await self.cache.get_batch_summary_cache(batch_code)
            if cached_result:
                self.cache_hits += 1
                duration = time.time() - start_time
                self.performance_tracker.record_query("get_batch_summary_cache_hit", duration)
                return cached_result
            
            # 缓存未命中，查询数据库
            self.cache_misses += 1
            result = super().get_batch_statistics_summary(batch_code)
            
            if result:
                # 存入缓存
                await self.cache.set_batch_summary_cache(batch_code, result)
            
            duration = time.time() - start_time
            self.performance_tracker.record_query("get_batch_summary_cache_miss", duration)
            
            return result
            
        except Exception as e:
            logger.error(f"Cache operation failed for batch summary: {str(e)}")
            # 缓存失败时降级到直接数据库查询
            return super().get_batch_statistics_summary(batch_code)
    
    async def get_statistics_by_criteria_cached(self, criteria: Dict[str, Any]) -> QueryResult:
        """根据复合条件查询统计数据(带查询缓存)"""
        if not self.cache_enabled:
            return super().get_statistics_by_criteria(criteria)
        
        start_time = time.time()
        
        try:
            # 生成查询哈希
            query_hash = self.cache.generate_query_hash(criteria)
            
            # 检查查询缓存
            cached_result = await self.cache.get_query_result_cache(query_hash)
            if cached_result:
                self.cache_hits += 1
                duration = time.time() - start_time
                self.performance_tracker.record_query("get_statistics_by_criteria_cache_hit", duration)
                return cached_result
            
            # 执行查询
            self.cache_misses += 1
            result = super().get_statistics_by_criteria(criteria)
            
            # 缓存查询结果
            if result.data:
                await self.cache.set_query_result_cache(query_hash, result)
            
            duration = time.time() - start_time
            self.performance_tracker.record_query("get_statistics_by_criteria_cache_miss", duration)
            
            return result
            
        except Exception as e:
            logger.error(f"Cache operation failed for criteria query: {str(e)}")
            # 缓存失败时降级到直接数据库查询
            return super().get_statistics_by_criteria(criteria)
    
    async def upsert_statistics_with_cache_invalidation(self, aggregation_data: Dict[str, Any]) -> StatisticalAggregation:
        """插入或更新统计数据(自动清理缓存)"""
        result = super().upsert_statistics(aggregation_data)
        
        # 清理相关缓存
        if self.cache_enabled:
            try:
                await self.cache.smart_invalidate_on_update(aggregation_data)
            except Exception as e:
                logger.error(f"Cache invalidation failed: {str(e)}")
        
        return result
    
    async def batch_upsert_statistics_with_cache_invalidation(
        self, 
        statistics_list: List[Dict[str, Any]],
        batch_size: int = 100
    ) -> BatchOperationResult:
        """批量插入或更新统计数据(自动清理缓存)"""
        result = super().batch_upsert_statistics(statistics_list, batch_size)
        
        # 清理相关缓存
        if self.cache_enabled and result.success_rate > 0:
            try:
                # 收集所有涉及的批次代码
                batch_codes = set()
                for item in statistics_list:
                    if 'batch_code' in item:
                        batch_codes.add(item['batch_code'])
                
                # 清理所有相关批次的缓存
                for batch_code in batch_codes:
                    await self.cache.invalidate_batch_cache(batch_code)
                    
            except Exception as e:
                logger.error(f"Batch cache invalidation failed: {str(e)}")
        
        return result
    
    def _serialize_aggregation_for_cache(self, aggregation: StatisticalAggregation) -> Dict[str, Any]:
        """将StatisticalAggregation对象序列化为缓存数据"""
        return {
            'id': aggregation.id,
            'batch_code': aggregation.batch_code,
            'aggregation_level': aggregation.aggregation_level.value,
            'school_id': aggregation.school_id,
            'school_name': aggregation.school_name,
            'statistics_data': aggregation.statistics_data,
            'data_version': aggregation.data_version,
            'calculation_status': aggregation.calculation_status.value,
            'total_students': aggregation.total_students,
            'total_schools': aggregation.total_schools,
            'calculation_duration': float(aggregation.calculation_duration) if aggregation.calculation_duration else None,
            'created_at': aggregation.created_at.isoformat(),
            'updated_at': aggregation.updated_at.isoformat()
        }
    
    def _build_aggregation_from_cache(self, cache_data: Dict[str, Any]) -> StatisticalAggregation:
        """从缓存数据重建StatisticalAggregation对象"""
        # 注意: 这里创建的是一个数据传输对象，不是完整的ORM对象
        # 如需要完整ORM功能，应该查询数据库
        aggregation = StatisticalAggregation()
        aggregation.id = cache_data['id']
        aggregation.batch_code = cache_data['batch_code']
        aggregation.aggregation_level = AggregationLevel(cache_data['aggregation_level'])
        aggregation.school_id = cache_data['school_id']
        aggregation.school_name = cache_data['school_name']
        aggregation.statistics_data = cache_data['statistics_data']
        aggregation.data_version = cache_data['data_version']
        aggregation.calculation_status = CalculationStatus(cache_data['calculation_status'])
        aggregation.total_students = cache_data['total_students']
        aggregation.total_schools = cache_data['total_schools']
        aggregation.calculation_duration = cache_data['calculation_duration']
        aggregation.created_at = datetime.fromisoformat(cache_data['created_at'])
        aggregation.updated_at = datetime.fromisoformat(cache_data['updated_at'])
        
        return aggregation
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        stats = super().get_performance_stats()
        
        if self.cache_enabled:
            total_requests = self.cache_hits + self.cache_misses
            cache_hit_rate = (self.cache_hits / total_requests) if total_requests > 0 else 0.0
            
            stats.update({
                'cache_enabled': True,
                'cache_hits': self.cache_hits,
                'cache_misses': self.cache_misses,
                'cache_hit_rate': cache_hit_rate
            })
        else:
            stats.update({
                'cache_enabled': False,
                'cache_hits': 0,
                'cache_misses': 0,
                'cache_hit_rate': 0.0
            })
        
        return stats
    
    def reset_cache_stats(self) -> None:
        """重置缓存统计"""
        self.cache_hits = 0
        self.cache_misses = 0
        super().reset_performance_stats()
    
    async def warm_up_batch_cache(self, batch_code: str) -> Dict[str, int]:
        """预热指定批次的缓存"""
        if not self.cache_enabled:
            return {"regional": 0, "schools": 0}
        
        warmed = {"regional": 0, "schools": 0}
        
        try:
            # 预热区域级数据
            regional_data = super().get_regional_statistics(batch_code)
            if regional_data:
                cache_data = self._serialize_aggregation_for_cache(regional_data)
                await self.cache.set_regional_statistics(batch_code, cache_data)
                warmed["regional"] = 1
            
            # 预热学校级数据
            school_data_list = super().get_all_school_statistics(batch_code)
            for school_data in school_data_list:
                cache_data = self._serialize_aggregation_for_cache(school_data)
                await self.cache.set_school_statistics(batch_code, school_data.school_id, cache_data)
                warmed["schools"] += 1
            
            logger.info(f"Cache warmed up for batch {batch_code}: {warmed}")
            
        except Exception as e:
            logger.error(f"Cache warm-up failed for batch {batch_code}: {str(e)}")
        
        return warmed


class CacheHealthChecker:
    """缓存健康检查器"""
    
    def __init__(self, cache: StatisticalDataCache):
        self.cache = cache
    
    async def check_cache_health(self) -> Dict[str, Any]:
        """检查缓存健康状态"""
        health_status = {
            "status": "unknown",
            "redis_connected": False,
            "response_time_ms": 0,
            "memory_usage": "unknown",
            "key_count": 0,
            "errors": []
        }
        
        try:
            # 检查Redis连接
            start_time = time.time()
            redis_info = await self.cache._get_redis_info()
            response_time = (time.time() - start_time) * 1000
            
            health_status.update({
                "status": "healthy",
                "redis_connected": True,
                "response_time_ms": response_time,
                "memory_usage": redis_info.get("used_memory_human", "unknown"),
            })
            
            # 获取缓存统计
            cache_stats = await self.cache.get_cache_stats()
            health_status["key_count"] = cache_stats.get("total_keys", 0)
            
        except Exception as e:
            health_status.update({
                "status": "unhealthy",
                "errors": [str(e)]
            })
            logger.error(f"Cache health check failed: {str(e)}")
        
        return health_status
    
    async def cleanup_expired_cache(self) -> Dict[str, int]:
        """清理过期缓存（手动触发）"""
        cleanup_stats = {
            "cleaned_keys": 0,
            "errors": 0
        }
        
        try:
            # Redis自动处理过期，这里主要用于统计和日志
            logger.info("Cache cleanup initiated (Redis handles expiration automatically)")
            
        except Exception as e:
            cleanup_stats["errors"] += 1
            logger.error(f"Cache cleanup failed: {str(e)}")
        
        return cleanup_stats