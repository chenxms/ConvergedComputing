# 数据仓库层
from typing import List, Optional, Dict, Any, Union, Callable
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, asc, func, select
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from datetime import datetime, timedelta
import logging
import time

from .models import (
    Batch, Task, StatisticalAggregation, StatisticalMetadata, StatisticalHistory,
    AggregationLevel, MetadataType, ChangeType, CalculationStatus
)
from .query_builder import (
    StatisticalQueryBuilder, QueryResult, build_complex_query_from_dict,
    create_statistical_query_builder
)
from .schemas import (
    BatchOperationResult, BatchResult, DeletionResult, QueryCriteria,
    PerformanceCriteria, QueryPerformanceTracker
)
from .cache import StatisticalDataCache

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
    
    def create(self, task_data: Dict[str, Any]) -> Task:
        """创建任务"""
        try:
            task = Task(**task_data)
            self.db.add(task)
            self.db.commit()
            self.db.refresh(task)
            return task
        except Exception as e:
            self._handle_db_error(e, "create")
    
    def get_by_id(self, task_id: str) -> Optional[Task]:
        """获取任务"""
        try:
            return self.db.query(Task).filter(Task.id == task_id).first()
        except Exception as e:
            self._handle_db_error(e, "get_by_id")
    
    def update(self, task_id: str, update_data: Dict[str, Any]) -> bool:
        """更新任务"""
        try:
            task = self.get_by_id(task_id)
            if task:
                for key, value in update_data.items():
                    if hasattr(task, key):
                        setattr(task, key, value)
                self.db.commit()
                return True
            return False
        except Exception as e:
            self._handle_db_error(e, "update")
    
    def delete(self, task_id: str) -> bool:
        """删除任务"""
        try:
            task = self.get_by_id(task_id)
            if task:
                self.db.delete(task)
                self.db.commit()
                return True
            return False
        except Exception as e:
            self._handle_db_error(e, "delete")
    
    def get_paginated(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0,
        order_by: str = "started_at",
        order_direction: str = "desc"
    ) -> List[Task]:
        """分页获取任务列表"""
        try:
            query = self.db.query(Task)
            
            # 应用筛选条件
            if filters:
                for key, value in filters.items():
                    if hasattr(Task, key):
                        query = query.filter(getattr(Task, key) == value)
            
            # 排序
            if hasattr(Task, order_by):
                order_attr = getattr(Task, order_by)
                if order_direction.lower() == "desc":
                    query = query.order_by(desc(order_attr))
                else:
                    query = query.order_by(asc(order_attr))
            
            # 分页
            return query.offset(offset).limit(limit).all()
        except Exception as e:
            self._handle_db_error(e, "get_paginated")
    
    # Legacy methods for backward compatibility
    def create_task(self, task_data: Dict[str, Any]) -> Task:
        """创建任务（兼容性方法）"""
        return self.create(task_data)
    
    def get_task(self, task_id: Union[int, str]) -> Optional[Task]:
        """获取任务（兼容性方法）"""
        return self.get_by_id(str(task_id))
    
    def update_task_status(self, task_id: str, status: str) -> bool:
        """更新任务状态（兼容性方法）"""
        update_data = {"status": status}
        if status == "completed":
            update_data["completed_at"] = datetime.now()
        return self.update(task_id, update_data)


class StatisticalAggregationRepository(BaseRepository):
    """统计汇聚数据Repository"""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
        self.performance_tracker = QueryPerformanceTracker()
    
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
    
    # =================================
    # 基础CRUD方法
    # =================================
    
    def create(self, aggregation_data: Dict[str, Any]) -> StatisticalAggregation:
        """创建统计汇聚记录"""
        try:
            aggregation_data['created_at'] = datetime.now()
            aggregation_data['updated_at'] = datetime.now()
            record = StatisticalAggregation(**aggregation_data)
            self.db.add(record)
            self.db.commit()
            self.db.refresh(record)
            return record
        except Exception as e:
            self._handle_db_error(e, "create")
    
    def get_by_id(self, aggregation_id: int) -> Optional[StatisticalAggregation]:
        """根据ID获取统计汇聚记录"""
        try:
            return self.db.query(StatisticalAggregation).filter(
                StatisticalAggregation.id == aggregation_id
            ).first()
        except Exception as e:
            self._handle_db_error(e, "get_by_id")
    
    def get_by_filters(self, filters: Dict[str, Any]) -> Optional[StatisticalAggregation]:
        """根据筛选条件获取统计汇聚记录"""
        try:
            query = self.db.query(StatisticalAggregation)
            for key, value in filters.items():
                if hasattr(StatisticalAggregation, key):
                    query = query.filter(getattr(StatisticalAggregation, key) == value)
            return query.first()
        except Exception as e:
            self._handle_db_error(e, "get_by_filters")
    
    def update(self, aggregation_id: int, update_data: Dict[str, Any]) -> Optional[StatisticalAggregation]:
        """更新统计汇聚记录"""
        try:
            record = self.get_by_id(aggregation_id)
            if not record:
                return None
            
            for key, value in update_data.items():
                if hasattr(record, key):
                    setattr(record, key, value)
            
            record.updated_at = datetime.now()
            self.db.commit()
            self.db.refresh(record)
            return record
        except Exception as e:
            self._handle_db_error(e, "update")
    
    def delete(self, aggregation_id: int) -> bool:
        """删除统计汇聚记录"""
        try:
            record = self.get_by_id(aggregation_id)
            if not record:
                return False
            
            self.db.delete(record)
            self.db.commit()
            return True
        except Exception as e:
            self._handle_db_error(e, "delete")
    
    def get_paginated(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0,
        order_by: str = "created_at",
        order_direction: str = "desc"
    ) -> List[StatisticalAggregation]:
        """分页获取统计汇聚记录"""
        try:
            query = self.db.query(StatisticalAggregation)
            
            # 应用筛选条件
            if filters:
                for key, value in filters.items():
                    if hasattr(StatisticalAggregation, key):
                        query = query.filter(getattr(StatisticalAggregation, key) == value)
            
            # 排序
            if hasattr(StatisticalAggregation, order_by):
                order_attr = getattr(StatisticalAggregation, order_by)
                if order_direction.lower() == "desc":
                    query = query.order_by(desc(order_attr))
                else:
                    query = query.order_by(asc(order_attr))
            
            # 分页
            return query.offset(offset).limit(limit).all()
        except Exception as e:
            self._handle_db_error(e, "get_paginated")
    
    # =================================
    # 复杂查询方法扩展
    # =================================
    
    def get_statistics_by_date_range(
        self, 
        start_date: datetime, 
        end_date: datetime,
        batch_codes: Optional[List[str]] = None,
        aggregation_level: Optional[AggregationLevel] = None,
        calculation_status: Optional[CalculationStatus] = None,
        limit: int = 1000
    ) -> List[StatisticalAggregation]:
        """根据时间范围和条件获取统计数据"""
        start_time = time.time()
        try:
            query = self.db.query(StatisticalAggregation).filter(
                and_(
                    StatisticalAggregation.created_at >= start_date,
                    StatisticalAggregation.created_at <= end_date
                )
            )
            
            if batch_codes:
                query = query.filter(StatisticalAggregation.batch_code.in_(batch_codes))
            
            if aggregation_level:
                query = query.filter(StatisticalAggregation.aggregation_level == aggregation_level)
                
            if calculation_status:
                query = query.filter(StatisticalAggregation.calculation_status == calculation_status)
            
            result = query.order_by(desc(StatisticalAggregation.created_at)).limit(limit).all()
            
            duration = time.time() - start_time
            self.performance_tracker.record_query("get_statistics_by_date_range", duration)
            
            return result
        except Exception as e:
            self._handle_db_error(e, "get_statistics_by_date_range")
    
    def get_batch_statistics_timeline(self, batch_code: str) -> Dict[str, Any]:
        """获取批次统计数据时间线"""
        start_time = time.time()
        try:
            timeline_data = self.db.query(
                StatisticalAggregation.aggregation_level,
                StatisticalAggregation.calculation_status,
                func.count(StatisticalAggregation.id).label('count'),
                func.min(StatisticalAggregation.created_at).label('first_created'),
                func.max(StatisticalAggregation.updated_at).label('last_updated'),
                func.avg(StatisticalAggregation.calculation_duration).label('avg_duration')
            ).filter(
                StatisticalAggregation.batch_code == batch_code
            ).group_by(
                StatisticalAggregation.aggregation_level,
                StatisticalAggregation.calculation_status
            ).all()
            
            result = {
                'batch_code': batch_code,
                'timeline': [
                    {
                        'aggregation_level': item.aggregation_level.value,
                        'calculation_status': item.calculation_status.value,
                        'count': item.count,
                        'first_created': item.first_created.isoformat(),
                        'last_updated': item.last_updated.isoformat(),
                        'avg_duration': float(item.avg_duration) if item.avg_duration else 0.0
                    }
                    for item in timeline_data
                ]
            }
            
            duration = time.time() - start_time
            self.performance_tracker.record_query("get_batch_statistics_timeline", duration)
            
            return result
        except Exception as e:
            self._handle_db_error(e, "get_batch_statistics_timeline")
    
    def get_statistics_by_criteria(self, criteria: Dict[str, Any]) -> QueryResult:
        """根据复合条件查询统计数据"""
        start_time = time.time()
        try:
            base_query = self.db.query(StatisticalAggregation)
            builder = build_complex_query_from_dict(base_query, criteria)
            
            # 获取总数
            total_count = builder.count()
            
            # 获取分页结果
            offset = criteria.get('offset', 0)
            limit = criteria.get('limit', 100)
            results = builder.paginate(offset, limit).all()
            
            query_result = QueryResult(
                data=results,
                total_count=total_count,
                offset=offset,
                limit=limit,
                has_more=offset + limit < total_count
            )
            
            duration = time.time() - start_time
            self.performance_tracker.record_query(
                "get_statistics_by_criteria", 
                duration,
                {"criteria_keys": list(criteria.keys()), "total_count": total_count}
            )
            
            return query_result
        except Exception as e:
            self._handle_db_error(e, "get_statistics_by_criteria")
    
    def get_statistics_by_performance_criteria(
        self,
        performance_criteria: Dict[str, Any]
    ) -> List[StatisticalAggregation]:
        """根据教育统计性能指标查询"""
        start_time = time.time()
        try:
            query = self.db.query(StatisticalAggregation)
            
            # JSON路径查询示例
            if 'min_avg_score' in performance_criteria:
                # 查询学校平均分大于指定值的记录
                query = query.filter(
                    func.json_extract(
                        StatisticalAggregation.statistics_data, 
                        '$.academic_subjects.数学.school_stats.avg_score'
                    ) >= performance_criteria['min_avg_score']
                )
            
            if 'excellent_percentage_threshold' in performance_criteria:
                # 查询优秀率大于阈值的记录
                query = query.filter(
                    func.json_extract(
                        StatisticalAggregation.statistics_data,
                        '$.academic_subjects.数学.grade_distribution.excellent.percentage'
                    ) >= performance_criteria['excellent_percentage_threshold']
                )
            
            if 'min_difficulty_coefficient' in performance_criteria:
                query = query.filter(
                    func.json_extract(
                        StatisticalAggregation.statistics_data,
                        '$.academic_subjects.数学.statistical_indicators.difficulty_coefficient'
                    ) >= performance_criteria['min_difficulty_coefficient']
                )
            
            result = query.all()
            
            duration = time.time() - start_time
            self.performance_tracker.record_query("get_statistics_by_performance_criteria", duration)
            
            return result
        except Exception as e:
            self._handle_db_error(e, "get_statistics_by_performance_criteria")
    
    def create_query_builder(self) -> StatisticalQueryBuilder:
        """创建查询构建器"""
        base_query = self.db.query(StatisticalAggregation)
        return StatisticalQueryBuilder(base_query)
    
    def get_statistics_with_builder(
        self, 
        builder_func: Callable[[StatisticalQueryBuilder], StatisticalQueryBuilder],
        offset: int = 0,
        limit: int = 100
    ) -> QueryResult:
        """使用查询构建器获取统计数据"""
        start_time = time.time()
        try:
            builder = self.create_query_builder()
            # 应用用户定义的查询逻辑
            builder = builder_func(builder)
            
            # 获取总数
            total_count = builder.count()
            
            # 获取分页结果
            results = builder.paginate(offset, limit).all()
            
            query_result = QueryResult(
                data=results,
                total_count=total_count,
                offset=offset,
                limit=limit,
                has_more=offset + limit < total_count
            )
            
            duration = time.time() - start_time
            self.performance_tracker.record_query(
                "get_statistics_with_builder", 
                duration,
                builder.get_query_info()
            )
            
            return query_result
        except Exception as e:
            self._handle_db_error(e, "get_statistics_with_builder")
    
    # =================================
    # 批量操作接口
    # =================================
    
    def batch_upsert_statistics(
        self, 
        statistics_list: List[Dict[str, Any]],
        batch_size: int = 100
    ) -> BatchOperationResult:
        """批量插入或更新统计数据"""
        start_time = time.time()
        total_processed = 0
        total_created = 0
        total_updated = 0
        errors = []
        
        try:
            # 分批处理，避免内存溢出
            for i in range(0, len(statistics_list), batch_size):
                batch = statistics_list[i:i + batch_size]
                
                try:
                    result = self._process_statistics_batch(batch)
                    total_processed += result.processed_count
                    total_created += result.created_count
                    total_updated += result.updated_count
                except Exception as e:
                    error_msg = f"Batch {i//batch_size + 1}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(f"Batch operation failed for items {i}-{i+len(batch)}: {str(e)}")
            
            success_rate = total_processed / len(statistics_list) if statistics_list else 0.0
            result = BatchOperationResult(
                total_processed=total_processed,
                total_created=total_created,
                total_updated=total_updated,
                errors=errors,
                success_rate=success_rate
            )
            
            duration = time.time() - start_time
            self.performance_tracker.record_query(
                "batch_upsert_statistics", 
                duration,
                {"total_records": len(statistics_list), "batch_size": batch_size}
            )
            
            return result
            
        except Exception as e:
            self._handle_db_error(e, "batch_upsert_statistics")
    
    def _process_statistics_batch(self, batch: List[Dict[str, Any]]) -> BatchResult:
        """处理单个批次的数据"""
        created_count = 0
        updated_count = 0
        
        try:
            # 批量查询现有记录
            batch_keys = [
                (item['batch_code'], item['aggregation_level'], item.get('school_id'))
                for item in batch
            ]
            
            existing_records = {}
            for batch_code, level, school_id in batch_keys:
                key = f"{batch_code}_{level.value if hasattr(level, 'value') else level}_{school_id or 'regional'}"
                record = self.db.query(StatisticalAggregation).filter(
                    and_(
                        StatisticalAggregation.batch_code == batch_code,
                        StatisticalAggregation.aggregation_level == level,
                        StatisticalAggregation.school_id == school_id
                    )
                ).first()
                if record:
                    existing_records[key] = record
            
            # 处理每条记录
            for item in batch:
                level_value = item['aggregation_level'].value if hasattr(item['aggregation_level'], 'value') else item['aggregation_level']
                key = f"{item['batch_code']}_{level_value}_{item.get('school_id') or 'regional'}"
                
                if key in existing_records:
                    # 更新现有记录
                    existing = existing_records[key]
                    self._record_history_change(existing, item)
                    
                    for field, value in item.items():
                        setattr(existing, field, value)
                    existing.updated_at = datetime.now()
                    updated_count += 1
                else:
                    # 创建新记录
                    item['created_at'] = datetime.now()
                    item['updated_at'] = datetime.now()
                    record = StatisticalAggregation(**item)
                    self.db.add(record)
                    created_count += 1
            
            self.db.commit()
            return BatchResult(
                processed_count=len(batch),
                created_count=created_count,
                updated_count=updated_count
            )
            
        except Exception as e:
            self.db.rollback()
            raise RepositoryError(f"Batch processing failed: {str(e)}")
    
    def batch_delete_statistics(
        self,
        deletion_criteria: Dict[str, Any]
    ) -> DeletionResult:
        """批量删除统计数据"""
        start_time = time.time()
        try:
            # 构建删除查询
            query = self.db.query(StatisticalAggregation)
            
            if 'batch_codes' in deletion_criteria:
                query = query.filter(StatisticalAggregation.batch_code.in_(deletion_criteria['batch_codes']))
            
            if 'older_than' in deletion_criteria:
                query = query.filter(StatisticalAggregation.created_at < deletion_criteria['older_than'])
            
            if 'calculation_status' in deletion_criteria:
                query = query.filter(StatisticalAggregation.calculation_status == deletion_criteria['calculation_status'])
            
            # 获取即将删除的记录数量和ID
            records_to_delete = query.all()
            deletion_count = len(records_to_delete)
            deleted_ids = [record.id for record in records_to_delete]
            
            if deletion_count > 0:
                # 记录删除历史
                for record in records_to_delete:
                    self._record_deletion_history(record)
                
                # 执行删除
                query.delete(synchronize_session=False)
                self.db.commit()
            
            result = DeletionResult(
                deleted_count=deletion_count,
                deleted_ids=deleted_ids
            )
            
            duration = time.time() - start_time
            self.performance_tracker.record_query(
                "batch_delete_statistics", 
                duration,
                {"deletion_count": deletion_count}
            )
            
            return result
            
        except Exception as e:
            self.db.rollback()
            self._handle_db_error(e, "batch_delete_statistics")
    
    def _record_deletion_history(self, record: StatisticalAggregation) -> None:
        """记录删除历史"""
        try:
            history_data = {
                'aggregation_id': record.id,
                'change_type': ChangeType.DELETED,
                'previous_data': {
                    'statistics_data': record.statistics_data,
                    'calculation_status': record.calculation_status.value,
                    'total_students': record.total_students,
                    'calculation_duration': float(record.calculation_duration) if record.calculation_duration else None
                },
                'current_data': None,
                'change_summary': {
                    'deleted_at': datetime.now().isoformat(),
                    'reason': 'Batch deletion operation'
                },
                'change_reason': 'Batch deletion',
                'triggered_by': 'system',
                'batch_code': record.batch_code,
                'created_at': datetime.now()
            }
            
            history_record = StatisticalHistory(**history_data)
            self.db.add(history_record)
        except Exception as e:
            logger.error(f"Failed to record deletion history: {str(e)}")
    
    # =================================
    # 性能监控方法
    # =================================
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """获取Repository性能统计"""
        return self.performance_tracker.get_stats()
    
    def reset_performance_stats(self) -> None:
        """重置性能统计"""
        self.performance_tracker.reset()


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