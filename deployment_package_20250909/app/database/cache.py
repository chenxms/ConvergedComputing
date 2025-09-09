# Redis缓存层实现
import redis
import json
import pickle
import hashlib
import logging
from typing import Optional, Any, Dict, List, Union
from datetime import timedelta, datetime
from contextlib import contextmanager

from .enums import AggregationLevel, CalculationStatus

logger = logging.getLogger(__name__)


class CacheError(Exception):
    """缓存相关异常"""
    pass


class StatisticalDataCache:
    """统计数据缓存管理器"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.default_ttl = 3600  # 1小时默认过期时间
        self.prefix = "stats_cache:"
        self.ttl_config = {
            "regional_statistics": 3600,      # 区域统计缓存1小时
            "school_statistics": 1800,        # 学校统计缓存30分钟  
            "query_results": 600,             # 查询结果缓存10分钟
            "batch_summary": 900,             # 批次摘要缓存15分钟
            "metadata": 7200                  # 元数据缓存2小时
        }
    
    def _make_key(self, key_components: List[str]) -> str:
        """生成缓存键"""
        key_string = ":".join(str(c) for c in key_components)
        # 对长键名进行哈希以避免Redis键名长度限制
        if len(key_string) > 200:
            key_hash = hashlib.md5(key_string.encode()).hexdigest()
            return f"{self.prefix}hash:{key_hash}"
        return f"{self.prefix}{key_string}"
    
    async def get_regional_statistics(self, batch_code: str) -> Optional[Dict[str, Any]]:
        """获取区域统计数据缓存"""
        try:
            key = self._make_key(["regional", batch_code])
            cached_data = await self._get_cached_data(key)
            
            if cached_data:
                logger.debug(f"Cache hit for regional statistics: {batch_code}")
                return json.loads(cached_data)
            
            logger.debug(f"Cache miss for regional statistics: {batch_code}")
            return None
        except Exception as e:
            logger.error(f"Error getting regional statistics cache: {str(e)}")
            return None
    
    async def set_regional_statistics(
        self, 
        batch_code: str, 
        statistics_data: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> bool:
        """设置区域统计数据缓存"""
        try:
            key = self._make_key(["regional", batch_code])
            expire_time = ttl or self.ttl_config["regional_statistics"]
            
            success = await self._set_cached_data(
                key, 
                json.dumps(statistics_data, ensure_ascii=False),
                expire_time
            )
            
            if success:
                logger.debug(f"Cached regional statistics for batch: {batch_code}")
            
            return success
        except Exception as e:
            logger.error(f"Error setting regional statistics cache: {str(e)}")
            return False
    
    async def get_school_statistics(self, batch_code: str, school_id: str) -> Optional[Dict[str, Any]]:
        """获取学校统计数据缓存"""
        try:
            key = self._make_key(["school", batch_code, school_id])
            cached_data = await self._get_cached_data(key)
            
            if cached_data:
                logger.debug(f"Cache hit for school statistics: {batch_code}/{school_id}")
                return json.loads(cached_data)
            
            logger.debug(f"Cache miss for school statistics: {batch_code}/{school_id}")
            return None
        except Exception as e:
            logger.error(f"Error getting school statistics cache: {str(e)}")
            return None
    
    async def set_school_statistics(
        self, 
        batch_code: str, 
        school_id: str,
        statistics_data: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> bool:
        """设置学校统计数据缓存"""
        try:
            key = self._make_key(["school", batch_code, school_id])
            expire_time = ttl or self.ttl_config["school_statistics"]
            
            success = await self._set_cached_data(
                key, 
                json.dumps(statistics_data, ensure_ascii=False),
                expire_time
            )
            
            if success:
                logger.debug(f"Cached school statistics for {batch_code}/{school_id}")
            
            return success
        except Exception as e:
            logger.error(f"Error setting school statistics cache: {str(e)}")
            return False
    
    async def get_batch_summary_cache(self, batch_code: str) -> Optional[Dict[str, Any]]:
        """获取批次摘要缓存"""
        try:
            key = self._make_key(["summary", batch_code])
            cached_data = await self._get_cached_data(key)
            
            if cached_data:
                logger.debug(f"Cache hit for batch summary: {batch_code}")
                return json.loads(cached_data)
            
            return None
        except Exception as e:
            logger.error(f"Error getting batch summary cache: {str(e)}")
            return None
    
    async def set_batch_summary_cache(
        self,
        batch_code: str,
        summary_data: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> bool:
        """设置批次摘要缓存"""
        try:
            key = self._make_key(["summary", batch_code])
            expire_time = ttl or self.ttl_config["batch_summary"]
            
            success = await self._set_cached_data(
                key,
                json.dumps(summary_data, ensure_ascii=False),
                expire_time
            )
            
            if success:
                logger.debug(f"Cached batch summary for: {batch_code}")
            
            return success
        except Exception as e:
            logger.error(f"Error setting batch summary cache: {str(e)}")
            return False
    
    async def invalidate_batch_cache(self, batch_code: str) -> int:
        """清除批次相关所有缓存"""
        try:
            pattern = f"{self.prefix}*{batch_code}*"
            keys = await self._scan_keys(pattern)
            
            if keys:
                deleted_count = await self._delete_keys(keys)
                logger.info(f"Invalidated {deleted_count} cache entries for batch: {batch_code}")
                return deleted_count
            
            return 0
        except Exception as e:
            logger.error(f"Error invalidating batch cache: {str(e)}")
            return 0
    
    async def get_query_result_cache(self, query_hash: str) -> Optional[Any]:
        """获取查询结果缓存"""
        try:
            key = self._make_key(["query", query_hash])
            cached_data = await self._get_cached_data(key, use_pickle=True)
            
            if cached_data:
                logger.debug(f"Cache hit for query: {query_hash[:8]}...")
                return pickle.loads(cached_data)
            
            return None
        except Exception as e:
            logger.error(f"Error getting query result cache: {str(e)}")
            return None
    
    async def set_query_result_cache(
        self, 
        query_hash: str, 
        result: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """设置查询结果缓存"""
        try:
            key = self._make_key(["query", query_hash])
            expire_time = ttl or self.ttl_config["query_results"]
            
            success = await self._set_cached_data(
                key,
                pickle.dumps(result),
                expire_time
            )
            
            if success:
                logger.debug(f"Cached query result: {query_hash[:8]}...")
            
            return success
        except Exception as e:
            logger.error(f"Error setting query result cache: {str(e)}")
            return False
    
    def generate_query_hash(self, query_params: Dict[str, Any]) -> str:
        """生成查询参数哈希"""
        # 将查询参数排序后生成哈希
        sorted_params = json.dumps(query_params, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.md5(sorted_params.encode()).hexdigest()
    
    async def smart_invalidate_on_update(self, aggregation_data: Dict[str, Any]) -> None:
        """基于更新数据智能清理相关缓存"""
        batch_code = aggregation_data['batch_code']
        
        # 清理批次相关缓存
        await self.invalidate_batch_cache(batch_code)
        
        # 如果是区域级数据更新，清理所有相关学校的缓存
        if aggregation_data.get('aggregation_level') == AggregationLevel.REGIONAL:
            await self._invalidate_batch_school_caches(batch_code)
        
        # 清理相关查询缓存
        await self._invalidate_related_query_caches(batch_code)
    
    async def warm_up_cache(self, batch_codes: List[str]) -> Dict[str, int]:
        """预热缓存 - 返回预热统计"""
        warmed_up = {
            "regional": 0,
            "schools": 0,
            "summaries": 0
        }
        
        for batch_code in batch_codes:
            try:
                # 这里可以调用Repository方法预加载热门数据
                # 实际实现需要Repository的引用
                logger.info(f"Cache warm-up requested for batch: {batch_code}")
                # TODO: 实现具体的预热逻辑
            except Exception as e:
                logger.error(f"Error warming up cache for {batch_code}: {str(e)}")
        
        return warmed_up
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        try:
            info = await self._get_redis_info()
            
            # 获取统计数据键的数量
            patterns = {
                "regional": f"{self.prefix}regional:*",
                "school": f"{self.prefix}school:*", 
                "query": f"{self.prefix}query:*",
                "summary": f"{self.prefix}summary:*"
            }
            
            key_counts = {}
            for pattern_name, pattern in patterns.items():
                keys = await self._scan_keys(pattern)
                key_counts[pattern_name] = len(keys)
            
            return {
                "redis_memory_used": info.get("used_memory_human", "unknown"),
                "redis_connected_clients": info.get("connected_clients", 0),
                "cache_key_counts": key_counts,
                "total_keys": sum(key_counts.values())
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {str(e)}")
            return {"error": str(e)}
    
    # 私有辅助方法
    async def _get_cached_data(self, key: str, use_pickle: bool = False) -> Optional[bytes]:
        """获取缓存数据"""
        return self.redis.get(key)
    
    async def _set_cached_data(self, key: str, data: Union[str, bytes], ttl: int) -> bool:
        """设置缓存数据"""
        return self.redis.setex(key, ttl, data)
    
    async def _delete_keys(self, keys: List[str]) -> int:
        """删除多个缓存键"""
        if keys:
            return self.redis.delete(*keys)
        return 0
    
    async def _scan_keys(self, pattern: str) -> List[str]:
        """扫描匹配模式的键"""
        return self.redis.keys(pattern)
    
    async def _get_redis_info(self) -> Dict[str, Any]:
        """获取Redis信息"""
        return self.redis.info()
    
    async def _invalidate_batch_school_caches(self, batch_code: str) -> None:
        """清理批次下所有学校缓存"""
        pattern = f"{self.prefix}school:{batch_code}:*"
        keys = await self._scan_keys(pattern)
        if keys:
            deleted = await self._delete_keys(keys)
            logger.debug(f"Invalidated {deleted} school cache entries for batch: {batch_code}")
    
    async def _invalidate_related_query_caches(self, batch_code: str) -> None:
        """清理相关查询缓存"""
        pattern = f"{self.prefix}query:*"
        keys = await self._scan_keys(pattern)
        
        # 这里可以实现更智能的查询缓存失效策略
        # 当前简单删除所有查询缓存
        if keys:
            deleted = await self._delete_keys(keys)
            logger.debug(f"Invalidated {deleted} query cache entries")
    
    @contextmanager
    def cache_fallback(self, operation_name: str):
        """缓存降级上下文管理器"""
        try:
            yield self
        except Exception as e:
            logger.warning(f"Cache operation '{operation_name}' failed, falling back: {str(e)}")
            yield None


def create_redis_client() -> redis.Redis:
    """创建Redis客户端"""
    import os
    
    redis_config = {
        "host": os.getenv("REDIS_HOST", "127.0.0.1"),
        "port": int(os.getenv("REDIS_PORT", 6379)),
        "db": int(os.getenv("REDIS_DB", 0)),
        "password": os.getenv("REDIS_PASSWORD"),
        "decode_responses": True,
        "max_connections": 50,
        "socket_timeout": 30,
        "socket_connect_timeout": 30,
        "retry_on_timeout": True
    }
    
    # 移除空密码
    if redis_config["password"] is None:
        del redis_config["password"]
    
    try:
        client = redis.Redis(**redis_config)
        # 测试连接
        client.ping()
        logger.info("Redis client connected successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {str(e)}")
        raise CacheError(f"Redis connection failed: {str(e)}")


def create_cache_manager() -> Optional[StatisticalDataCache]:
    """创建缓存管理器"""
    try:
        redis_client = create_redis_client()
        return StatisticalDataCache(redis_client)
    except Exception as e:
        logger.warning(f"Failed to create cache manager: {str(e)}")
        return None