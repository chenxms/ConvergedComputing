import pytest
import asyncio
from datetime import datetime
from typing import Dict, Any
from unittest.mock import MagicMock, patch

from app.database.repositories import (
    StatisticalAggregationsRepository, 
    StatisticalMetadataRepository, 
    StatisticalHistoryRepository,
    RepositoryError,
    DataIntegrityError
)
from app.database.models import (
    StatisticalAggregation, StatisticalMetadata, StatisticalHistory,
    AggregationLevel, MetadataType, ChangeType, CalculationStatus
)


class TestStatisticalAggregationsRepository:
    """测试统计汇聚数据Repository"""
    
    def setup_method(self):
        """设置测试"""
        self.mock_db = MagicMock()
        self.repo = StatisticalAggregationsRepository(self.mock_db)
    
    def test_get_regional_statistics(self):
        """测试获取区域级统计数据"""
        # 准备测试数据
        batch_code = "BATCH_2025_001"
        mock_aggregation = StatisticalAggregation(
            id=1,
            batch_code=batch_code,
            aggregation_level=AggregationLevel.REGIONAL,
            statistics_data={"test": "data"},
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        self.mock_db.query().filter().first.return_value = mock_aggregation
        
        # 执行测试
        result = self.repo.get_regional_statistics(batch_code)
        
        # 验证结果
        assert result is not None
        assert result.batch_code == batch_code
        assert result.aggregation_level == AggregationLevel.REGIONAL
        
    def test_get_school_statistics(self):
        """测试获取学校级统计数据"""
        batch_code = "BATCH_2025_001"
        school_id = "SCHOOL_001"
        
        mock_aggregation = StatisticalAggregation(
            id=2,
            batch_code=batch_code,
            aggregation_level=AggregationLevel.SCHOOL,
            school_id=school_id,
            statistics_data={"test": "data"},
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        self.mock_db.query().filter().first.return_value = mock_aggregation
        
        result = self.repo.get_school_statistics(batch_code, school_id)
        
        assert result is not None
        assert result.school_id == school_id
        assert result.aggregation_level == AggregationLevel.SCHOOL
    
    def test_get_batch_statistics_summary(self):
        """测试获取批次统计数据摘要"""
        batch_code = "BATCH_2025_001"
        
        # Mock区域级数据
        mock_regional = StatisticalAggregation(
            id=1,
            batch_code=batch_code,
            aggregation_level=AggregationLevel.REGIONAL,
            calculation_status=CalculationStatus.COMPLETED,
            statistics_data={"test": "data"},
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # Mock学校统计查询结果
        mock_school_stats = MagicMock()
        mock_school_stats.total_schools = 50
        mock_school_stats.total_students = 15000
        mock_school_stats.avg_duration = 125.5
        
        # 设置mock返回值
        self.repo.get_regional_statistics = MagicMock(return_value=mock_regional)
        self.mock_db.query().filter().first.return_value = mock_school_stats
        
        result = self.repo.get_batch_statistics_summary(batch_code)
        
        assert result['batch_code'] == batch_code
        assert result['has_regional_data'] is True
        assert result['regional_status'] == 'completed'
        assert result['total_schools'] == 50
        assert result['total_students'] == 15000
        assert result['avg_calculation_duration'] == 125.5
    
    def test_upsert_statistics_new_record(self):
        """测试插入新统计记录"""
        aggregation_data = {
            'batch_code': 'BATCH_2025_001',
            'aggregation_level': AggregationLevel.REGIONAL,
            'statistics_data': {'test': 'data'},
            'total_students': 15000
        }
        
        # Mock不存在现有记录
        self.mock_db.query().filter().first.return_value = None
        
        # Mock新记录
        mock_new_record = StatisticalAggregation(**aggregation_data)
        mock_new_record.id = 1
        mock_new_record.created_at = datetime.now()
        mock_new_record.updated_at = datetime.now()
        
        self.mock_db.refresh = MagicMock()
        
        with patch.object(StatisticalAggregation, '__init__', return_value=None) as mock_init:
            result = self.repo.upsert_statistics(aggregation_data)
            
            # 验证调用了add方法
            self.mock_db.add.assert_called_once()
            self.mock_db.commit.assert_called_once()
    
    def test_upsert_statistics_update_existing(self):
        """测试更新现有统计记录"""
        aggregation_data = {
            'batch_code': 'BATCH_2025_001',
            'aggregation_level': AggregationLevel.REGIONAL,
            'statistics_data': {'updated': 'data'},
            'total_students': 16000
        }
        
        # Mock现有记录
        mock_existing = StatisticalAggregation(
            id=1,
            batch_code='BATCH_2025_001',
            aggregation_level=AggregationLevel.REGIONAL,
            statistics_data={'old': 'data'},
            total_students=15000,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        self.mock_db.query().filter().first.return_value = mock_existing
        self.repo._record_history_change = MagicMock()
        
        result = self.repo.upsert_statistics(aggregation_data)
        
        # 验证历史记录被调用
        self.repo._record_history_change.assert_called_once()
        # 验证提交被调用
        self.mock_db.commit.assert_called_once()
    
    def test_update_calculation_status(self):
        """测试更新计算状态"""
        aggregation_id = 1
        new_status = CalculationStatus.COMPLETED
        duration = 125.5
        
        mock_aggregation = StatisticalAggregation(
            id=aggregation_id,
            batch_code='BATCH_2025_001',
            calculation_status=CalculationStatus.PROCESSING,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        self.mock_db.query().filter().first.return_value = mock_aggregation
        
        result = self.repo.update_calculation_status(aggregation_id, new_status, duration)
        
        assert result is True
        assert mock_aggregation.calculation_status == new_status
        assert mock_aggregation.calculation_duration == duration
        self.mock_db.commit.assert_called_once()
    
    def test_database_error_handling(self):
        """测试数据库错误处理"""
        self.mock_db.query.side_effect = Exception("Database connection failed")
        
        with pytest.raises(RepositoryError):
            self.repo.get_regional_statistics("BATCH_2025_001")


class TestStatisticalMetadataRepository:
    """测试统计元数据Repository"""
    
    def setup_method(self):
        """设置测试"""
        self.mock_db = MagicMock()
        self.repo = StatisticalMetadataRepository(self.mock_db)
    
    def test_get_metadata_by_key(self):
        """测试根据键获取元数据"""
        mock_metadata = StatisticalMetadata(
            id=1,
            metadata_type=MetadataType.GRADE_CONFIG,
            metadata_key="grade_thresholds_primary",
            metadata_value={"excellent": 0.85, "good": 0.70},
            is_active=True,
            version="1.0",
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        self.mock_db.query().filter().first.return_value = mock_metadata
        
        result = self.repo.get_metadata_by_key(
            MetadataType.GRADE_CONFIG, 
            "grade_thresholds_primary"
        )
        
        assert result is not None
        assert result.metadata_key == "grade_thresholds_primary"
        assert result.is_active is True
    
    def test_get_grade_config_primary(self):
        """测试获取小学年级配置"""
        mock_metadata = StatisticalMetadata(
            metadata_value={"excellent": 0.85, "good": 0.70, "pass": 0.60}
        )
        
        self.repo.get_metadata_by_key = MagicMock(return_value=mock_metadata)
        
        result = self.repo.get_grade_config("1th_grade")
        
        assert result is not None
        assert result["excellent"] == 0.85
        # 验证调用了正确的配置键
        self.repo.get_metadata_by_key.assert_called_with(
            MetadataType.GRADE_CONFIG, 
            "grade_thresholds_primary"
        )
    
    def test_get_grade_config_middle(self):
        """测试获取初中年级配置"""
        mock_metadata = StatisticalMetadata(
            metadata_value={"A": 0.85, "B": 0.70, "C": 0.60}
        )
        
        self.repo.get_metadata_by_key = MagicMock(return_value=mock_metadata)
        
        result = self.repo.get_grade_config("7th_grade")
        
        # 验证调用了初中配置
        self.repo.get_metadata_by_key.assert_called_with(
            MetadataType.GRADE_CONFIG, 
            "grade_thresholds_middle"
        )
    
    def test_create_metadata(self):
        """测试创建元数据"""
        metadata_data = {
            'metadata_type': MetadataType.CALCULATION_RULE,
            'metadata_key': 'percentile_algorithm',
            'metadata_value': {'formula': 'floor(student_count × percentile)'},
            'description': '百分位数计算规则'
        }
        
        mock_metadata = StatisticalMetadata(**metadata_data)
        mock_metadata.id = 1
        mock_metadata.created_at = datetime.now()
        mock_metadata.updated_at = datetime.now()
        
        self.mock_db.refresh = MagicMock()
        
        with patch.object(StatisticalMetadata, '__init__', return_value=None):
            result = self.repo.create_metadata(metadata_data)
            
            self.mock_db.add.assert_called_once()
            self.mock_db.commit.assert_called_once()


class TestStatisticalHistoryRepository:
    """测试统计历史记录Repository"""
    
    def setup_method(self):
        """设置测试"""
        self.mock_db = MagicMock()
        self.repo = StatisticalHistoryRepository(self.mock_db)
    
    def test_get_change_history(self):
        """测试获取变更历史"""
        aggregation_id = 1
        
        mock_history_records = [
            StatisticalHistory(
                id=1,
                aggregation_id=aggregation_id,
                change_type=ChangeType.UPDATED,
                batch_code="BATCH_2025_001",
                created_at=datetime.now()
            ),
            StatisticalHistory(
                id=2,
                aggregation_id=aggregation_id,
                change_type=ChangeType.CREATED,
                batch_code="BATCH_2025_001",
                created_at=datetime.now()
            )
        ]
        
        self.mock_db.query().filter().order_by().limit().all.return_value = mock_history_records
        
        result = self.repo.get_change_history(aggregation_id)
        
        assert len(result) == 2
        assert all(record.aggregation_id == aggregation_id for record in result)
    
    def test_get_batch_change_history(self):
        """测试获取批次变更历史"""
        batch_code = "BATCH_2025_001"
        
        mock_history_records = [
            StatisticalHistory(
                id=1,
                aggregation_id=1,
                change_type=ChangeType.UPDATED,
                batch_code=batch_code,
                created_at=datetime.now()
            )
        ]
        
        self.mock_db.query().filter().order_by().limit().all.return_value = mock_history_records
        
        result = self.repo.get_batch_change_history(batch_code)
        
        assert len(result) == 1
        assert result[0].batch_code == batch_code
    
    def test_create_history_record(self):
        """测试创建历史记录"""
        history_data = {
            'aggregation_id': 1,
            'change_type': ChangeType.UPDATED,
            'change_reason': 'Test update',
            'batch_code': 'BATCH_2025_001'
        }
        
        mock_history = StatisticalHistory(**history_data)
        mock_history.id = 1
        mock_history.created_at = datetime.now()
        
        self.mock_db.refresh = MagicMock()
        
        with patch.object(StatisticalHistory, '__init__', return_value=None):
            result = self.repo.create_history_record(history_data)
            
            self.mock_db.add.assert_called_once()
            self.mock_db.commit.assert_called_once()


# 集成测试示例（需要真实数据库连接）
class TestRepositoryIntegration:
    """Repository集成测试"""
    
    @pytest.mark.integration
    def test_end_to_end_statistics_workflow(self):
        """测试端到端统计数据工作流"""
        # 这个测试需要真实的数据库连接
        # 在实际环境中，你需要设置测试数据库
        pass
    
    @pytest.mark.integration  
    def test_metadata_configuration_loading(self):
        """测试元数据配置加载"""
        # 测试从数据库加载各种配置
        pass
    
    @pytest.mark.integration
    def test_history_tracking_accuracy(self):
        """测试历史记录追踪准确性"""
        # 测试历史记录是否准确记录变更
        pass


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])