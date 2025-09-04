# 数据仓库层
from typing import List, Optional, Dict, Any, Union
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, asc, func, select
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from datetime import datetime
import logging

from .models import (
    Batch, Task, StatisticalAggregation, StatisticalMetadata, StatisticalHistory,
    AggregationLevel, MetadataType, ChangeType, CalculationStatus
)

logger = logging.getLogger(__name__)


class RepositoryError(Exception):
    """Repository层异常基类"""
    pass


class DataIntegrityError(RepositoryError):
    """数据完整性异常"""
    pass


class BaseRepository:
    """基础仓库类"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def _handle_db_error(self, error: Exception, operation: str) -> None:
        """统一处理数据库异常"""
        logger.error(f"Database error in {operation}: {str(error)}")
        self.db.rollback()
        
        if isinstance(error, IntegrityError):
            raise DataIntegrityError(f"数据完整性错误: {str(error)}")
        elif isinstance(error, SQLAlchemyError):
            raise RepositoryError(f"数据库操作失败: {str(error)}")
        else:
            raise RepositoryError(f"未知数据库错误: {str(error)}")


class BatchRepository(BaseRepository):
    """批次数据仓库"""
    
    def create_batch(self, batch_data: Dict[str, Any]) -> Batch:
        """创建批次"""
        try:
            batch = Batch(**batch_data)
            self.db.add(batch)
            self.db.commit()
            self.db.refresh(batch)
            return batch
        except Exception as e:
            self._handle_db_error(e, "create_batch")
    
    def get_batch(self, batch_id: int) -> Optional[Batch]:
        """获取批次"""
        try:
            return self.db.query(Batch).filter(Batch.id == batch_id).first()
        except Exception as e:
            self._handle_db_error(e, "get_batch")
    
    def delete_batch(self, batch_id: int) -> bool:
        """删除批次"""
        try:
            batch = self.get_batch(batch_id)
            if batch:
                self.db.delete(batch)
                self.db.commit()
                return True
            return False
        except Exception as e:
            self._handle_db_error(e, "delete_batch")


class TaskRepository(BaseRepository):
    """任务数据仓库"""
    
    def create_task(self, task_data: Dict[str, Any]) -> Task:
        """创建任务"""
        try:
            task = Task(**task_data)
            self.db.add(task)
            self.db.commit()
            self.db.refresh(task)
            return task
        except Exception as e:
            self._handle_db_error(e, "create_task")
    
    def get_task(self, task_id: int) -> Optional[Task]:
        """获取任务"""
        try:
            return self.db.query(Task).filter(Task.id == task_id).first()
        except Exception as e:
            self._handle_db_error(e, "get_task")
    
    def update_task_status(self, task_id: int, status: str) -> bool:
        """更新任务状态"""
        try:
            task = self.get_task(task_id)
            if task:
                task.status = status
                if status == "completed":
                    task.completed_at = datetime.now()
                self.db.commit()
                return True
            return False
        except Exception as e:
            self._handle_db_error(e, "update_task_status")


class StatisticalAggregationsRepository(BaseRepository):
    """统计汇聚数据Repository"""
    
    def get_regional_statistics(self, batch_code: str) -> Optional[StatisticalAggregation]:
        """获取区域级统计数据"""
        try:
            return self.db.query(StatisticalAggregation).filter(
                and_(
                    StatisticalAggregation.batch_code == batch_code,
                    StatisticalAggregation.aggregation_level == AggregationLevel.REGIONAL
                )
            ).first()
        except Exception as e:
            self._handle_db_error(e, "get_regional_statistics")
    
    def get_school_statistics(self, batch_code: str, school_id: str) -> Optional[StatisticalAggregation]:
        """获取学校级统计数据"""
        try:
            return self.db.query(StatisticalAggregation).filter(
                and_(
                    StatisticalAggregation.batch_code == batch_code,
                    StatisticalAggregation.aggregation_level == AggregationLevel.SCHOOL,
                    StatisticalAggregation.school_id == school_id
                )
            ).first()
        except Exception as e:
            self._handle_db_error(e, "get_school_statistics")
    
    def get_all_school_statistics(self, batch_code: str) -> List[StatisticalAggregation]:
        """获取批次所有学校统计数据"""
        try:
            return self.db.query(StatisticalAggregation).filter(
                and_(
                    StatisticalAggregation.batch_code == batch_code,
                    StatisticalAggregation.aggregation_level == AggregationLevel.SCHOOL
                )
            ).order_by(asc(StatisticalAggregation.school_name)).all()
        except Exception as e:
            self._handle_db_error(e, "get_all_school_statistics")
    
    def get_by_calculation_status(self, status: CalculationStatus, limit: int = 100) -> List[StatisticalAggregation]:
        """根据计算状态获取统计数据"""
        try:
            return self.db.query(StatisticalAggregation).filter(
                StatisticalAggregation.calculation_status == status
            ).order_by(desc(StatisticalAggregation.created_at)).limit(limit).all()
        except Exception as e:
            self._handle_db_error(e, "get_by_calculation_status")
    
    def get_batch_statistics_summary(self, batch_code: str) -> Dict[str, Any]:
        """获取批次统计数据摘要"""
        try:
            # 查询区域级数据
            regional = self.get_regional_statistics(batch_code)
            
            # 查询学校级数据统计
            school_stats = self.db.query(
                func.count(StatisticalAggregation.id).label('total_schools'),
                func.sum(StatisticalAggregation.total_students).label('total_students'),
                func.avg(StatisticalAggregation.calculation_duration).label('avg_duration')
            ).filter(
                and_(
                    StatisticalAggregation.batch_code == batch_code,
                    StatisticalAggregation.aggregation_level == AggregationLevel.SCHOOL,
                    StatisticalAggregation.calculation_status == CalculationStatus.COMPLETED
                )
            ).first()
            
            return {
                'batch_code': batch_code,
                'has_regional_data': regional is not None,
                'regional_status': regional.calculation_status.value if regional else None,
                'total_schools': school_stats.total_schools or 0,
                'total_students': school_stats.total_students or 0,
                'avg_calculation_duration': float(school_stats.avg_duration) if school_stats.avg_duration else 0.0
            }
        except Exception as e:
            self._handle_db_error(e, "get_batch_statistics_summary")
    
    def upsert_statistics(self, aggregation_data: Dict[str, Any]) -> StatisticalAggregation:
        """插入或更新统计数据"""
        try:
            # 查找现有记录
            existing = self.db.query(StatisticalAggregation).filter(
                and_(
                    StatisticalAggregation.batch_code == aggregation_data['batch_code'],
                    StatisticalAggregation.aggregation_level == aggregation_data['aggregation_level'],
                    StatisticalAggregation.school_id == aggregation_data.get('school_id')
                )
            ).first()
            
            if existing:
                # 记录历史变更
                self._record_history_change(existing, aggregation_data)
                # 更新现有记录
                for key, value in aggregation_data.items():
                    setattr(existing, key, value)
                existing.updated_at = datetime.now()
                record = existing
            else:
                # 创建新记录
                aggregation_data['created_at'] = datetime.now()
                aggregation_data['updated_at'] = datetime.now()
                record = StatisticalAggregation(**aggregation_data)
                self.db.add(record)
            
            self.db.commit()
            self.db.refresh(record)
            return record
        except Exception as e:
            self._handle_db_error(e, "upsert_statistics")
    
    def update_calculation_status(self, aggregation_id: int, status: CalculationStatus, 
                                 duration: Optional[float] = None) -> bool:
        """更新计算状态"""
        try:
            aggregation = self.db.query(StatisticalAggregation).filter(
                StatisticalAggregation.id == aggregation_id
            ).first()
            
            if aggregation:
                aggregation.calculation_status = status
                if duration is not None:
                    aggregation.calculation_duration = duration
                aggregation.updated_at = datetime.now()
                self.db.commit()
                return True
            return False
        except Exception as e:
            self._handle_db_error(e, "update_calculation_status")
    
    def delete_batch_statistics(self, batch_code: str) -> int:
        """删除批次的所有统计数据"""
        try:
            deleted_count = self.db.query(StatisticalAggregation).filter(
                StatisticalAggregation.batch_code == batch_code
            ).delete()
            self.db.commit()
            return deleted_count
        except Exception as e:
            self._handle_db_error(e, "delete_batch_statistics")
    
    def _record_history_change(self, existing: StatisticalAggregation, new_data: Dict[str, Any]) -> None:
        """记录历史变更"""
        try:
            # 创建历史记录
            history_data = {
                'aggregation_id': existing.id,
                'change_type': ChangeType.UPDATED,
                'previous_data': {
                    'statistics_data': existing.statistics_data,
                    'calculation_status': existing.calculation_status.value,
                    'total_students': existing.total_students,
                    'calculation_duration': float(existing.calculation_duration) if existing.calculation_duration else None
                },
                'current_data': {
                    'statistics_data': new_data.get('statistics_data'),
                    'calculation_status': new_data.get('calculation_status', existing.calculation_status).value,
                    'total_students': new_data.get('total_students', existing.total_students),
                    'calculation_duration': new_data.get('calculation_duration')
                },
                'change_summary': {
                    'updated_fields': list(new_data.keys()),
                    'update_time': datetime.now().isoformat()
                },
                'change_reason': new_data.get('change_reason', 'Data update'),
                'triggered_by': new_data.get('triggered_by', 'system'),
                'batch_code': existing.batch_code,
                'created_at': datetime.now()
            }
            
            history_record = StatisticalHistory(**history_data)
            self.db.add(history_record)
        except Exception as e:
            logger.error(f"Failed to record history change: {str(e)}")
            # 历史记录失败不应阻止主要操作


class StatisticalMetadataRepository(BaseRepository):
    """统计元数据Repository"""
    
    def get_metadata_by_key(self, metadata_type: MetadataType, 
                           metadata_key: str, version: str = '1.0') -> Optional[StatisticalMetadata]:
        """根据键获取元数据"""
        try:
            return self.db.query(StatisticalMetadata).filter(
                and_(
                    StatisticalMetadata.metadata_type == metadata_type,
                    StatisticalMetadata.metadata_key == metadata_key,
                    StatisticalMetadata.version == version,
                    StatisticalMetadata.is_active == True
                )
            ).first()
        except Exception as e:
            self._handle_db_error(e, "get_metadata_by_key")
    
    def get_grade_config(self, grade_level: str) -> Optional[Dict[str, Any]]:
        """获取年级配置"""
        try:
            # 根据年级范围确定配置键
            if grade_level in ['1th_grade', '2th_grade', '3th_grade', '4th_grade', '5th_grade', '6th_grade']:
                config_key = "grade_thresholds_primary"
            elif grade_level in ['7th_grade', '8th_grade', '9th_grade']:
                config_key = "grade_thresholds_middle"
            else:
                config_key = "grade_thresholds_default"
            
            metadata = self.get_metadata_by_key(MetadataType.GRADE_CONFIG, config_key)
            return metadata.metadata_value if metadata else None
        except Exception as e:
            self._handle_db_error(e, "get_grade_config")
    
    def get_calculation_rule(self, rule_name: str) -> Optional[Dict[str, Any]]:
        """获取计算规则"""
        try:
            metadata = self.get_metadata_by_key(MetadataType.CALCULATION_RULE, rule_name)
            return metadata.metadata_value if metadata else None
        except Exception as e:
            self._handle_db_error(e, "get_calculation_rule")
    
    def get_dimension_config(self, dimension_name: str, grade_level: str = None) -> Optional[Dict[str, Any]]:
        """获取维度配置"""
        try:
            query = self.db.query(StatisticalMetadata).filter(
                and_(
                    StatisticalMetadata.metadata_type == MetadataType.DIMENSION_CONFIG,
                    StatisticalMetadata.metadata_key == dimension_name,
                    StatisticalMetadata.is_active == True
                )
            )
            
            if grade_level:
                query = query.filter(StatisticalMetadata.grade_level == grade_level)
            
            metadata = query.first()
            return metadata.metadata_value if metadata else None
        except Exception as e:
            self._handle_db_error(e, "get_dimension_config")
    
    def list_metadata_by_type(self, metadata_type: MetadataType, 
                             is_active: bool = True) -> List[StatisticalMetadata]:
        """根据类型列出元数据"""
        try:
            query = self.db.query(StatisticalMetadata).filter(
                StatisticalMetadata.metadata_type == metadata_type
            )
            
            if is_active is not None:
                query = query.filter(StatisticalMetadata.is_active == is_active)
            
            return query.order_by(asc(StatisticalMetadata.metadata_key)).all()
        except Exception as e:
            self._handle_db_error(e, "list_metadata_by_type")
    
    def create_metadata(self, metadata_data: Dict[str, Any]) -> StatisticalMetadata:
        """创建元数据"""
        try:
            metadata_data['created_at'] = datetime.now()
            metadata_data['updated_at'] = datetime.now()
            metadata = StatisticalMetadata(**metadata_data)
            self.db.add(metadata)
            self.db.commit()
            self.db.refresh(metadata)
            return metadata
        except Exception as e:
            self._handle_db_error(e, "create_metadata")
    
    def update_metadata(self, metadata_id: int, update_data: Dict[str, Any]) -> Optional[StatisticalMetadata]:
        """更新元数据"""
        try:
            metadata = self.db.query(StatisticalMetadata).filter(
                StatisticalMetadata.id == metadata_id
            ).first()
            
            if metadata:
                for key, value in update_data.items():
                    setattr(metadata, key, value)
                metadata.updated_at = datetime.now()
                self.db.commit()
                self.db.refresh(metadata)
                return metadata
            return None
        except Exception as e:
            self._handle_db_error(e, "update_metadata")
    
    def deactivate_metadata(self, metadata_id: int) -> bool:
        """停用元数据"""
        try:
            metadata = self.db.query(StatisticalMetadata).filter(
                StatisticalMetadata.id == metadata_id
            ).first()
            
            if metadata:
                metadata.is_active = False
                metadata.updated_at = datetime.now()
                self.db.commit()
                return True
            return False
        except Exception as e:
            self._handle_db_error(e, "deactivate_metadata")


class StatisticalHistoryRepository(BaseRepository):
    """统计历史记录Repository"""
    
    def get_change_history(self, aggregation_id: int, limit: int = 50) -> List[StatisticalHistory]:
        """获取指定统计数据的变更历史"""
        try:
            return self.db.query(StatisticalHistory).filter(
                StatisticalHistory.aggregation_id == aggregation_id
            ).order_by(desc(StatisticalHistory.created_at)).limit(limit).all()
        except Exception as e:
            self._handle_db_error(e, "get_change_history")
    
    def get_batch_change_history(self, batch_code: str, limit: int = 100) -> List[StatisticalHistory]:
        """获取批次的变更历史"""
        try:
            return self.db.query(StatisticalHistory).filter(
                StatisticalHistory.batch_code == batch_code
            ).order_by(desc(StatisticalHistory.created_at)).limit(limit).all()
        except Exception as e:
            self._handle_db_error(e, "get_batch_change_history")
    
    def get_changes_by_type(self, change_type: ChangeType, 
                           start_date: datetime = None, 
                           end_date: datetime = None,
                           limit: int = 100) -> List[StatisticalHistory]:
        """根据变更类型和时间范围获取历史记录"""
        try:
            query = self.db.query(StatisticalHistory).filter(
                StatisticalHistory.change_type == change_type
            )
            
            if start_date:
                query = query.filter(StatisticalHistory.created_at >= start_date)
            if end_date:
                query = query.filter(StatisticalHistory.created_at <= end_date)
            
            return query.order_by(desc(StatisticalHistory.created_at)).limit(limit).all()
        except Exception as e:
            self._handle_db_error(e, "get_changes_by_type")
    
    def create_history_record(self, history_data: Dict[str, Any]) -> StatisticalHistory:
        """创建历史记录"""
        try:
            history_data['created_at'] = datetime.now()
            history_record = StatisticalHistory(**history_data)
            self.db.add(history_record)
            self.db.commit()
            self.db.refresh(history_record)
            return history_record
        except Exception as e:
            self._handle_db_error(e, "create_history_record")
    
    def get_statistics_with_history(self, batch_code: str, 
                                   aggregation_level: AggregationLevel,
                                   school_id: str = None) -> Dict[str, Any]:
        """获取统计数据及其完整历史记录"""
        try:
            # 获取统计数据
            query = self.db.query(StatisticalAggregation).filter(
                and_(
                    StatisticalAggregation.batch_code == batch_code,
                    StatisticalAggregation.aggregation_level == aggregation_level
                )
            )
            
            if school_id:
                query = query.filter(StatisticalAggregation.school_id == school_id)
            
            aggregation = query.first()
            
            if not aggregation:
                return None
            
            # 获取历史记录
            history = self.get_change_history(aggregation.id)
            
            return {
                'aggregation': aggregation,
                'history': history,
                'total_changes': len(history)
            }
        except Exception as e:
            self._handle_db_error(e, "get_statistics_with_history")
    
    def cleanup_old_history(self, days_to_keep: int = 90) -> int:
        """清理旧的历史记录"""
        try:
            cutoff_date = datetime.now() - datetime.timedelta(days=days_to_keep)
            deleted_count = self.db.query(StatisticalHistory).filter(
                StatisticalHistory.created_at < cutoff_date
            ).delete()
            self.db.commit()
            return deleted_count
        except Exception as e:
            self._handle_db_error(e, "cleanup_old_history")