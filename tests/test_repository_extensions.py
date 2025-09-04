# 测试扩展的Repository功能
import pytest
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
from unittest.mock import Mock, patch

from app.database.repositories import StatisticalAggregationRepository
from app.database.cached_repositories import CachedStatisticalAggregationRepository
from app.database.query_builder import StatisticalQueryBuilder, QueryResult
from app.database.schemas import BatchOperationResult, QueryCriteria
from app.database.cache import StatisticalDataCache
from app.database.monitoring import RepositoryMonitor, PerformanceAlert
from app.database.factory import RepositoryFactory
from app.database.models import StatisticalAggregation, AggregationLevel, CalculationStatus


class TestComplexQueries:
    """测试复杂查询功能"""
    
    @pytest.fixture
    def mock_db_session(self):
        """模拟数据库会话"""
        return Mock()
    
    @pytest.fixture  
    def repository(self, mock_db_session):
        """创建Repository实例"""
        return StatisticalAggregationRepository(mock_db_session)
    
    def test_get_statistics_by_date_range(self, repository, mock_db_session):
        """测试按时间范围查询"""
        # 准备测试数据
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()
        batch_codes = ["BATCH_2025_001", "BATCH_2025_002"]
        
        # 模拟查询结果
        mock_query = Mock()
        mock_db_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.all.return_value = []
        
        # 执行测试
        result = repository.get_statistics_by_date_range(
            start_date=start_date,
            end_date=end_date,
            batch_codes=batch_codes,
            aggregation_level=AggregationLevel.SCHOOL,
            limit=100
        )
        
        # 验证结果
        assert isinstance(result, list)
        mock_db_session.query.assert_called_once()
    
    def test_get_batch_statistics_timeline(self, repository, mock_db_session):
        """测试批次时间线查询"""
        batch_code = "BATCH_2025_001"
        
        # 模拟聚合查询结果
        mock_timeline_item = Mock()
        mock_timeline_item.aggregation_level.value = "SCHOOL"
        mock_timeline_item.calculation_status.value = "COMPLETED"
        mock_timeline_item.count = 10
        mock_timeline_item.first_created = datetime.now()
        mock_timeline_item.last_updated = datetime.now()
        mock_timeline_item.avg_duration = 150.5
        
        mock_query = Mock()
        mock_db_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.group_by.return_value = mock_query
        mock_query.all.return_value = [mock_timeline_item]
        
        # 执行测试
        result = repository.get_batch_statistics_timeline(batch_code)
        
        # 验证结果
        assert isinstance(result, dict)
        assert result["batch_code"] == batch_code
        assert "timeline" in result
        assert len(result["timeline"]) == 1
        
        timeline_item = result["timeline"][0]
        assert timeline_item["aggregation_level"] == "SCHOOL"
        assert timeline_item["calculation_status"] == "COMPLETED"
        assert timeline_item["count"] == 10
        assert timeline_item["avg_duration"] == 150.5
    
    def test_get_statistics_by_performance_criteria(self, repository, mock_db_session):
        """测试性能条件查询"""
        criteria = {
            "min_avg_score": 80.0,
            "excellent_percentage_threshold": 0.3,
            "min_difficulty_coefficient": 0.5
        }
        
        mock_query = Mock()
        mock_db_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.all.return_value = []
        
        # 执行测试
        result = repository.get_statistics_by_performance_criteria(criteria)
        
        # 验证结果
        assert isinstance(result, list)
        # 验证过滤条件被正确应用
        assert mock_query.filter.call_count >= len(criteria)


class TestQueryBuilder:
    """测试查询构建器功能"""
    
    @pytest.fixture
    def mock_base_query(self):
        """模拟基础查询"""
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.offset.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.count.return_value = 50
        mock_query.all.return_value = []
        mock_query.first.return_value = None
        return mock_query
    
    def test_query_builder_chain(self, mock_base_query):
        """测试查询构建器链式调用"""
        builder = StatisticalQueryBuilder(mock_base_query)
        
        # 链式构建查询
        result_builder = (builder
                         .filter_by_batch_codes(["BATCH_2025_001"])
                         .filter_by_aggregation_level(AggregationLevel.SCHOOL)
                         .filter_by_student_count_range(50, 500)
                         .order_by_created_at("desc"))
        
        # 验证返回的是同一个构建器实例
        assert result_builder is builder
        
        # 验证条件和排序被正确添加
        assert len(builder.conditions) == 3  # batch_codes, level, student_count
        assert len(builder.order_clauses) == 1  # created_at desc
    
    def test_query_builder_json_criteria(self, mock_base_query):
        """测试JSON字段查询条件"""
        builder = StatisticalQueryBuilder(mock_base_query)
        
        # 添加JSON查询条件
        builder.filter_by_json_criteria("$.academic_subjects.数学.avg_score", ">=", 80.0)
        builder.filter_by_json_criteria("$.academic_subjects.数学.grade_distribution.excellent.percentage", ">", 0.3)
        
        # 构建查询
        query = builder.build()
        
        assert query is mock_base_query
        assert len(builder.conditions) == 2
    
    def test_query_builder_pagination(self, mock_base_query):
        """测试查询构建器分页"""
        builder = StatisticalQueryBuilder(mock_base_query)
        
        # 测试分页
        paginated_query = builder.paginate(20, 10)
        
        # 验证分页调用
        mock_base_query.offset.assert_called_with(20)
        mock_base_query.limit.assert_called_with(10)
        
        # 测试计数
        count = builder.count()
        assert count == 50


class TestBatchOperations:
    """测试批量操作功能"""
    
    @pytest.fixture
    def mock_db_session(self):
        """模拟数据库会话"""
        session = Mock()
        session.query.return_value.filter.return_value.first.return_value = None
        session.commit.return_value = None
        session.rollback.return_value = None
        return session
    
    @pytest.fixture
    def repository(self, mock_db_session):
        """创建Repository实例"""
        return StatisticalAggregationRepository(mock_db_session)
    
    def test_batch_upsert_statistics_success(self, repository, mock_db_session):
        """测试成功的批量插入/更新操作"""
        # 准备测试数据
        test_data = [
            {
                "batch_code": "BATCH_2025_001",
                "aggregation_level": AggregationLevel.SCHOOL,
                "school_id": "SCHOOL_001",
                "statistics_data": {"test": "data1"},
                "total_students": 100
            },
            {
                "batch_code": "BATCH_2025_001", 
                "aggregation_level": AggregationLevel.SCHOOL,
                "school_id": "SCHOOL_002",
                "statistics_data": {"test": "data2"},
                "total_students": 150
            }
        ]
        
        # 执行批量操作
        result = repository.batch_upsert_statistics(test_data, batch_size=10)
        
        # 验证结果
        assert isinstance(result, BatchOperationResult)
        assert result.total_processed == 2
        assert result.success_rate > 0
        assert len(result.errors) == 0
    
    def test_batch_upsert_statistics_with_errors(self, repository, mock_db_session):
        """测试带错误的批量操作"""
        # 模拟部分失败的场景
        def side_effect(*args, **kwargs):
            # 模拟第二次调用失败
            if side_effect.call_count == 2:
                raise Exception("Database error")
            side_effect.call_count += 1
            return None
        
        side_effect.call_count = 0
        mock_db_session.commit.side_effect = side_effect
        
        # 准备测试数据（足够触发多个批次）
        test_data = [
            {
                "batch_code": f"BATCH_2025_{i:03d}",
                "aggregation_level": AggregationLevel.SCHOOL,
                "school_id": f"SCHOOL_{i:03d}",
                "statistics_data": {"test": f"data{i}"},
                "total_students": 100 + i
            }
            for i in range(150)  # 150条记录，批次大小50，会分3个批次
        ]
        
        # 执行批量操作
        result = repository.batch_upsert_statistics(test_data, batch_size=50)
        
        # 验证结果包含错误信息
        assert isinstance(result, BatchOperationResult)
        assert len(result.errors) > 0
        assert result.success_rate < 1.0


class TestCachedRepository:
    """测试带缓存的Repository功能"""
    
    @pytest.fixture
    def mock_cache(self):
        """模拟缓存管理器"""
        cache = Mock(spec=StatisticalDataCache)
        cache.get_regional_statistics.return_value = None
        cache.set_regional_statistics.return_value = True
        cache.get_school_statistics.return_value = None
        cache.set_school_statistics.return_value = True
        cache.invalidate_batch_cache.return_value = 5
        return cache
    
    @pytest.fixture
    def mock_db_session(self):
        """模拟数据库会话"""
        return Mock()
    
    @pytest.fixture
    async def cached_repository(self, mock_db_session, mock_cache):
        """创建带缓存的Repository实例"""
        return CachedStatisticalAggregationRepository(mock_db_session, mock_cache)
    
    @pytest.mark.asyncio
    async def test_cache_miss_then_hit(self, cached_repository, mock_cache, mock_db_session):
        """测试缓存未命中然后命中的场景"""
        batch_code = "BATCH_2025_001"
        
        # 模拟数据库查询结果
        mock_aggregation = Mock(spec=StatisticalAggregation)
        mock_aggregation.id = 1
        mock_aggregation.batch_code = batch_code
        mock_aggregation.aggregation_level = AggregationLevel.REGIONAL
        mock_aggregation.statistics_data = {"test": "data"}
        mock_aggregation.calculation_status = CalculationStatus.COMPLETED
        mock_aggregation.total_students = 1000
        mock_aggregation.total_schools = 10
        mock_aggregation.calculation_duration = 120.5
        mock_aggregation.created_at = datetime.now()
        mock_aggregation.updated_at = datetime.now()
        mock_aggregation.school_id = None
        mock_aggregation.school_name = None
        mock_aggregation.data_version = "1.0"
        
        mock_query = Mock()
        mock_db_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = mock_aggregation
        
        # 第一次调用 - 缓存未命中
        result1 = await cached_repository.get_regional_statistics(batch_code)
        
        # 验证调用了数据库查询
        mock_db_session.query.assert_called_once()
        # 验证调用了缓存设置
        mock_cache.set_regional_statistics.assert_called_once()
        assert result1 is mock_aggregation
        
        # 模拟缓存命中
        cache_data = {
            'id': 1,
            'batch_code': batch_code,
            'aggregation_level': 'REGIONAL',
            'statistics_data': {"test": "data"},
            'calculation_status': 'COMPLETED',
            'total_students': 1000,
            'total_schools': 10,
            'calculation_duration': 120.5,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'school_id': None,
            'school_name': None,
            'data_version': '1.0'
        }
        mock_cache.get_regional_statistics.return_value = cache_data
        
        # 第二次调用 - 缓存命中
        result2 = await cached_repository.get_regional_statistics(batch_code)
        
        # 验证缓存命中，没有额外的数据库查询
        assert mock_db_session.query.call_count == 1  # 仍然只有一次调用
        assert result2.batch_code == batch_code
        assert cached_repository.cache_hits == 1
        assert cached_repository.cache_misses == 1


class TestPerformanceMonitoring:
    """测试性能监控功能"""
    
    def test_repository_monitor_basic_stats(self):
        """测试基本性能统计"""
        monitor = RepositoryMonitor(slow_query_threshold=1.0)
        
        # 记录一些查询
        monitor.record_query("test_query_1", 0.5, cache_hit=False)
        monitor.record_query("test_query_2", 1.5, cache_hit=False)  # 慢查询
        monitor.record_query("test_query_3", 0.2, cache_hit=True)
        
        # 获取统计信息
        stats = monitor.get_performance_stats()
        
        assert stats["total_queries"] == 3
        assert stats["cache_hits"] == 1
        assert stats["cache_misses"] == 2
        assert stats["cache_hit_rate"] == 1/3
        assert stats["slow_queries_count"] == 1
        assert 0.5 < stats["average_query_time"] < 1.0
    
    def test_performance_alerts(self):
        """测试性能告警"""
        monitor = RepositoryMonitor(slow_query_threshold=0.5)
        
        # 记录大量慢查询
        for i in range(10):
            monitor.record_query(f"slow_query_{i}", 1.0, cache_hit=False)
        
        alerts = monitor.get_alerts()
        
        # 应该有慢查询告警
        assert len(alerts) > 0
        slow_query_alert = next((a for a in alerts if a["type"] == "slow_queries"), None)
        assert slow_query_alert is not None
        assert slow_query_alert["severity"] == "warning"


class TestRepositoryFactory:
    """测试Repository工厂"""
    
    @pytest.fixture
    def mock_db_session(self):
        """模拟数据库会话"""
        return Mock()
    
    def test_factory_creates_repositories(self, mock_db_session):
        """测试工厂创建各种Repository"""
        factory = RepositoryFactory(mock_db_session, cache_enabled=False)
        
        # 测试创建各种Repository
        agg_repo = factory.create_statistical_aggregation_repository(use_cache=False)
        assert isinstance(agg_repo, StatisticalAggregationRepository)
        assert not isinstance(agg_repo, CachedStatisticalAggregationRepository)
        
        metadata_repo = factory.create_statistical_metadata_repository()
        assert metadata_repo is not None
        
        history_repo = factory.create_statistical_history_repository()
        assert history_repo is not None
        
        batch_repo = factory.create_batch_repository()
        assert batch_repo is not None
        
        task_repo = factory.create_task_repository()
        assert task_repo is not None
    
    @patch('app.database.factory.get_cache_manager')
    def test_factory_creates_cached_repository(self, mock_get_cache, mock_db_session):
        """测试工厂创建带缓存的Repository"""
        # 模拟缓存管理器
        mock_cache = Mock(spec=StatisticalDataCache)
        mock_get_cache.return_value = mock_cache
        
        factory = RepositoryFactory(mock_db_session, cache_enabled=True)
        
        # 测试创建带缓存的Repository
        cached_repo = factory.create_statistical_aggregation_repository(use_cache=True)
        assert isinstance(cached_repo, CachedStatisticalAggregationRepository)
        assert cached_repo.cache_enabled is True


# 性能基准测试
class TestPerformanceBenchmark:
    """性能基准测试"""
    
    def test_query_performance_target(self):
        """验证查询性能目标"""
        monitor = RepositoryMonitor()
        
        # 模拟快速查询
        start_time = time.time()
        time.sleep(0.1)  # 模拟100ms查询
        duration = time.time() - start_time
        
        monitor.record_query("fast_query", duration)
        
        stats = monitor.get_performance_stats()
        
        # 验证查询时间在合理范围内
        assert stats["average_query_time"] < 0.5  # 小于500ms目标
    
    def test_cache_performance_simulation(self):
        """模拟缓存性能测试"""
        monitor = RepositoryMonitor()
        
        # 模拟缓存命中和未命中的性能差异
        # 缓存未命中（慢）
        monitor.record_query("cache_miss", 0.5, cache_hit=False)
        
        # 缓存命中（快）
        monitor.record_query("cache_hit", 0.05, cache_hit=True)
        
        stats = monitor.get_performance_stats()
        
        # 验证缓存提升了性能
        assert stats["cache_hits"] == 1
        assert stats["cache_misses"] == 1
        assert stats["cache_hit_rate"] == 0.5


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])