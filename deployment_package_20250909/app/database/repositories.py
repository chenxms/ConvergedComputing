# 数据仓库层
from typing import List, Optional, Dict, Any, Union, Callable
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, asc, func, select, text
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
            # 手动创建Task对象，避免字典展开导致的SQLAlchemy错误
            task = Task()
            task.id = task_data.get('id')
            task.batch_id = task_data.get('batch_id')
            task.status = task_data.get('status')
            task.progress = task_data.get('progress', 0.0)
            task.started_at = task_data.get('started_at')
            task.completed_at = task_data.get('completed_at')
            task.error_message = task_data.get('error_message')
            
            self.db.add(task)
            self.db.commit()
            self.db.refresh(task)
            return task
        except Exception as e:
            import traceback
            logger.error(f"Task creation error: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
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
    
    def get_by_batch_code_and_level(self, batch_code: str, aggregation_level: AggregationLevel) -> Optional[StatisticalAggregation]:
        """
        根据批次代码和聚合级别获取统计数据
        
        Args:
            batch_code: 批次代码
            aggregation_level: 聚合级别
            
        Returns:
            统计汇聚记录或None
        """
        try:
            return self.db.query(StatisticalAggregation).filter(
                and_(
                    StatisticalAggregation.batch_code == batch_code,
                    StatisticalAggregation.aggregation_level == aggregation_level
                )
            ).first()
        except Exception as e:
            self._handle_db_error(e, "get_by_batch_code_and_level")
    
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
            # 查找现有记录 - 需要包含school_name以区分相同school_id的不同学校
            existing = self.db.query(StatisticalAggregation).filter(
                and_(
                    StatisticalAggregation.batch_code == aggregation_data['batch_code'],
                    StatisticalAggregation.aggregation_level == aggregation_data['aggregation_level'],
                    StatisticalAggregation.school_id == aggregation_data.get('school_id'),
                    StatisticalAggregation.school_name == aggregation_data.get('school_name')
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
    
    def get_by_batch_school(self, batch_code: str, school_id: str) -> Optional[StatisticalAggregation]:
        """
        根据批次代码和学校ID获取学校级统计数据
        
        Args:
            batch_code: 批次代码
            school_id: 学校ID
            
        Returns:
            学校级统计汇聚记录或None
        """
        try:
            return self.db.query(StatisticalAggregation).filter(
                and_(
                    StatisticalAggregation.batch_code == batch_code,
                    StatisticalAggregation.aggregation_level == AggregationLevel.SCHOOL,
                    StatisticalAggregation.school_id == school_id
                )
            ).first()
        except Exception as e:
            self._handle_db_error(e, "get_by_batch_school")
    
    def get_schools_by_batch_code(self, batch_code: str) -> List[StatisticalAggregation]:
        """
        根据批次代码获取所有学校级统计数据
        
        Args:
            batch_code: 批次代码
            
        Returns:
            学校级统计汇聚记录列表
        """
        try:
            return self.db.query(StatisticalAggregation).filter(
                and_(
                    StatisticalAggregation.batch_code == batch_code,
                    StatisticalAggregation.aggregation_level == AggregationLevel.SCHOOL
                )
            ).order_by(asc(StatisticalAggregation.school_name)).all()
        except Exception as e:
            self._handle_db_error(e, "get_schools_by_batch_code")
    
    def create_or_update(self, **kwargs) -> StatisticalAggregation:
        """
        创建或更新统计汇聚记录
        
        Args:
            **kwargs: 统计数据字段
            
        Returns:
            统计汇聚记录
        """
        return self.upsert_statistics(kwargs)
    
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


class DataAdapterRepository(BaseRepository):
    """数据适配器Repository - 统一清洗数据与汇聚计算的接口"""
    
    def __init__(self, db_session: Session):
        super().__init__(db_session)
        self.json_parser = DimensionJSONParser()
    
    def check_data_readiness(self, batch_code: str) -> Dict[str, Any]:
        """检查批次数据清洗状态和可用性"""
        try:
            # 检查清洗数据表是否存在记录
            cleaned_count_query = """
            SELECT COUNT(*) as count, COUNT(DISTINCT student_id) as students
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
            """
            cleaned_result = self.db.execute(text(cleaned_count_query), {'batch_code': batch_code}).fetchone()
            
            # 检查原始数据数量作为对比
            original_count_query = """
            SELECT COUNT(DISTINCT student_id) as students
            FROM student_score_detail 
            WHERE batch_code = :batch_code
            """
            original_result = self.db.execute(text(original_count_query), {'batch_code': batch_code}).fetchone()
            
            # 检查问卷数据状态
            questionnaire_count_query = """
            SELECT COUNT(*) as count, COUNT(DISTINCT student_id) as students
            FROM questionnaire_question_scores 
            WHERE batch_code = :batch_code
            """
            questionnaire_result = self.db.execute(text(questionnaire_count_query), {'batch_code': batch_code}).fetchone()
            
            cleaned_students = cleaned_result.students if cleaned_result else 0
            original_students = original_result.students if original_result else 0
            questionnaire_students = questionnaire_result.students if questionnaire_result else 0
            
            # 计算清洗完成度
            completeness_ratio = (cleaned_students / original_students) if original_students > 0 else 0.0
            
            # 确定数据状态
            has_cleaned = cleaned_students > 0
            has_original = original_students > 0
            has_questionnaire = questionnaire_students > 0
            
            # 确定总体状态
            if has_cleaned and completeness_ratio >= 0.95:
                overall_status = 'READY'
            elif has_cleaned and completeness_ratio >= 0.80:
                overall_status = 'READY_WITH_WARNINGS'
            elif has_original:
                overall_status = 'ORIGINAL_DATA_ONLY'
            else:
                overall_status = 'NO_DATA'
            
            # 确定主要数据源
            if has_cleaned:
                primary_source = 'cleaned_data'
            elif has_original:
                primary_source = 'original_data'
            else:
                primary_source = 'none'
            
            return {
                'batch_code': batch_code,
                'overall_status': overall_status,
                'is_ready': completeness_ratio >= 0.95,
                'student_count': max(cleaned_students, original_students),
                'school_count': 0,  # 需要额外查询获取
                'subject_count': 0,  # 需要额外查询获取
                'cleaned_records': cleaned_result.count if cleaned_result else 0,
                'cleaned_students': cleaned_students,
                'original_students': original_students,
                'questionnaire_records': questionnaire_result.count if questionnaire_result else 0,
                'questionnaire_students': questionnaire_students,
                'completeness_ratio': completeness_ratio,
                'data_sources': {
                    'has_cleaned_data': has_cleaned,
                    'has_questionnaire_data': has_questionnaire,
                    'has_original_data': has_original,
                    'primary_source': primary_source
                }
            }
        except Exception as e:
            self._handle_db_error(e, "check_data_readiness")
    
    def get_student_scores(self, batch_code: str, subject_type: str = None, school_id: str = None) -> List[Dict[str, Any]]:
        """获取学生分数数据 - 自动选择最优数据源"""
        try:
            # 检查数据准备状态
            readiness = self.check_data_readiness(batch_code)
            
            if readiness['data_sources']['has_cleaned_data']:
                return self._get_cleaned_student_scores(batch_code, subject_type, school_id)
            elif readiness['data_sources']['has_original_data']:
                logger.warning(f"Batch {batch_code} using legacy data source - cleaned data not available")
                return self._get_legacy_student_scores(batch_code, subject_type, school_id)
            else:
                raise RepositoryError(f"No data available for batch {batch_code}")
        except Exception as e:
            self._handle_db_error(e, "get_student_scores")
    
    def _get_cleaned_student_scores(self, batch_code: str, subject_type: str = None, school_id: str = None) -> List[Dict[str, Any]]:
        """从清洗数据表获取学生分数"""
        try:
            base_query = """
            SELECT 
                student_id,
                student_name,
                subject_id,
                subject_name,
                subject_type,
                total_score as score,
                max_score,
                dimension_scores,
                dimension_max_scores,
                school_id,
                school_name,
                class_name,
                question_count,
                is_valid
            FROM student_cleaned_scores
            WHERE batch_code = :batch_code
            """
            params = {"batch_code": batch_code}
            
            # 添加科目类型过滤
            if subject_type:
                base_query += " AND subject_type = :subject_type"
                params["subject_type"] = subject_type
            
            # 添加学校过滤
            if school_id:
                base_query += " AND school_id = :school_id"
                params["school_id"] = school_id
            
            base_query += " ORDER BY school_id, student_id, subject_name"
            
            results = self.db.execute(text(base_query), params).fetchall()
            
            # 转换为标准格式
            student_scores = []
            for row in results:
                score_data = {
                    'student_id': row.student_id,
                    'student_name': row.student_name,
                    'subject_id': row.subject_id,
                    'subject_name': row.subject_name,
                    'subject_type': row.subject_type,
                    'score': float(row.score) if row.score else 0.0,
                    'total_score': float(row.score) if row.score else 0.0,  # 保持兼容性
                    'max_score': float(row.max_score) if row.max_score else 0.0,
                    'school_id': row.school_id,
                    'school_name': row.school_name,
                    'class_name': row.class_name,
                    'question_count': row.question_count or 0,
                    'is_valid': bool(row.is_valid) if row.is_valid is not None else True,
                    'data_source': 'cleaned'
                }
                
                # 解析维度分数JSON
                if row.dimension_scores and row.dimension_max_scores:
                    dimension_data = self.json_parser.parse_dimension_scores(
                        row.dimension_scores, 
                        row.dimension_max_scores
                    )
                    score_data['dimensions'] = dimension_data
                
                student_scores.append(score_data)
            
            return student_scores
        except Exception as e:
            raise RepositoryError(f"Failed to get cleaned student scores: {str(e)}")
    
    def _get_legacy_student_scores(self, batch_code: str, subject_type: str = None, school_id: str = None) -> List[Dict[str, Any]]:
        """从原始数据表获取学生分数（兼容性方法）"""
        try:
            base_query = """
            SELECT 
                ssd.student_id,
                ssd.subject_id as subject_name,
                ssd.score as total_score,
                sqc.max_score,
                ssd.school_id,
                ssd.grade,
                COUNT(*) OVER (PARTITION BY ssd.student_id, ssd.subject_id) as student_count
            FROM student_score_detail ssd
            LEFT JOIN subject_question_config sqc ON ssd.subject_id = sqc.subject_name
            WHERE ssd.batch_code = :batch_code
            """
            params = {"batch_code": batch_code}
            
            if school_id:
                base_query += " AND ssd.school_id = :school_id"
                params["school_id"] = school_id
            
            base_query += " GROUP BY ssd.student_id, ssd.subject_id, ssd.score, sqc.max_score, ssd.school_id, ssd.grade"
            base_query += " ORDER BY ssd.school_id, ssd.student_id, ssd.subject_id"
            
            results = self.db.execute(text(base_query), params).fetchall()
            
            # 转换为标准格式
            student_scores = []
            for row in results:
                score_data = {
                    'student_id': row.student_id,
                    'subject_name': row.subject_name,
                    'subject_type': 'exam',  # 默认考试类型
                    'total_score': float(row.total_score) if row.total_score else 0.0,
                    'max_score': float(row.max_score) if row.max_score else 0.0,
                    'school_id': row.school_id,
                    'school_name': None,  # 原始数据可能不包含学校名称
                    'grade': row.grade,
                    'student_count': row.student_count or 1,
                    'data_source': 'legacy',
                    'dimensions': {}  # 原始数据需要单独处理维度
                }
                student_scores.append(score_data)
            
            return student_scores
        except Exception as e:
            raise RepositoryError(f"Failed to get legacy student scores: {str(e)}")
    
    def get_questionnaire_details(self, batch_code: str, subject_name: str = None) -> List[Dict[str, Any]]:
        """获取问卷明细数据"""
        try:
            base_query = """
            SELECT 
                student_id,
                subject_name,
                question_id,
                original_score,
                max_score,
                scale_level,
                instrument_type,
                school_id,
                school_name,
                grade
            FROM questionnaire_question_scores
            WHERE batch_code = :batch_code
            """
            params = {"batch_code": batch_code}
            
            if subject_name:
                base_query += " AND subject_name = :subject_name"
                params["subject_name"] = subject_name
            
            base_query += " ORDER BY school_id, student_id, question_id"
            
            results = self.db.execute(text(base_query), params).fetchall()
            
            questionnaire_details = []
            for row in results:
                detail_data = {
                    'student_id': row.student_id,
                    'subject_name': row.subject_name,
                    'question_id': row.question_id,
                    'original_score': float(row.original_score) if row.original_score else 0.0,
                    'max_score': float(row.max_score) if row.max_score else 0.0,
                    'scale_level': row.scale_level,
                    'instrument_type': row.instrument_type,
                    'school_id': row.school_id,
                    'school_name': row.school_name,
                    'grade': row.grade
                }
                questionnaire_details.append(detail_data)
            
            return questionnaire_details
        except Exception as e:
            self._handle_db_error(e, "get_questionnaire_details")
    
    def get_questionnaire_distribution(self, batch_code: str, subject_name: str = None) -> List[Dict[str, Any]]:
        """获取问卷选项分布统计"""
        try:
            base_query = """
            SELECT 
                subject_name,
                question_id,
                option_level,
                student_count,
                percentage,
                scale_level
            FROM questionnaire_option_distribution
            WHERE batch_code = :batch_code
            """
            params = {"batch_code": batch_code}
            
            if subject_name:
                base_query += " AND subject_name = :subject_name"
                params["subject_name"] = subject_name
            
            base_query += " ORDER BY subject_name, question_id, option_level"
            
            results = self.db.execute(text(base_query), params).fetchall()
            
            distribution_data = []
            for row in results:
                dist_data = {
                    'subject_name': row.subject_name,
                    'question_id': row.question_id,
                    'option_level': row.option_level,
                    'student_count': row.student_count,
                    'percentage': float(row.percentage) if row.percentage else 0.0,
                    'scale_level': row.scale_level
                }
                distribution_data.append(dist_data)
            
            return distribution_data
        except Exception as e:
            self._handle_db_error(e, "get_questionnaire_distribution")
    
    def get_subject_configurations(self, batch_code: str) -> List[Dict[str, Any]]:
        """获取科目配置信息"""
        try:
            # 根据实际表结构查询科目配置
            query = """
            SELECT 
                subject_name,
                subject,
                question_type_enum,
                COUNT(*) as question_count,
                SUM(max_score) as total_max_score,
                MAX(max_score) as single_question_max_score
            FROM subject_question_config
            WHERE batch_code = :batch_code
            GROUP BY subject_name, subject, question_type_enum
            ORDER BY subject_name
            """
            
            results = self.db.execute(text(query), {'batch_code': batch_code}).fetchall()
            
            configurations = []
            for row in results:
                # 确定科目类型
                if row.question_type_enum == 'questionnaire':
                    subject_type = 'questionnaire'
                elif row.question_type_enum == 'interaction':
                    subject_type = 'interaction'
                else:
                    subject_type = 'exam'  # 默认为考试类型
                
                config_data = {
                    'subject_name': row.subject_name,
                    'subject_type': subject_type,
                    'max_score': float(row.total_max_score) if row.total_max_score else 0.0,
                    'question_count': row.question_count,
                    'question_type_enum': row.question_type_enum,
                    'subject_code': row.subject  # 额外返回科目代码
                }
                configurations.append(config_data)
            
            return configurations
        except Exception as e:
            self._handle_db_error(e, "get_subject_configurations")
    
    def get_dimension_configurations(self, batch_code: str) -> List[Dict[str, Any]]:
        """获取维度配置信息"""
        try:
            # 这里返回空列表，因为维度信息已经在JSON中
            return []
        except Exception as e:
            self._handle_db_error(e, "get_dimension_configurations")
    
    def get_dimension_statistics(self, batch_code: str, subject_name: str, dimension_name: str) -> List[Dict[str, Any]]:
        """获取维度统计数据"""
        try:
            # 这里返回空列表，因为维度统计数据已经在JSON中
            return []
        except Exception as e:
            self._handle_db_error(e, "get_dimension_statistics")
    
    def _normalize_subject_type(self, subject_type: str, question_type_enum: str) -> str:
        """统一科目类型判断逻辑"""
        if question_type_enum and question_type_enum.lower() == 'questionnaire':
            return 'questionnaire'
        elif subject_type:
            return subject_type.lower()
        else:
            return 'exam'  # 默认考试类型
    
    def get_batch_summary(self, batch_code: str) -> Dict[str, Any]:
        """获取批次数据摘要"""
        try:
            readiness = self.check_data_readiness(batch_code)
            
            # 获取科目配置
            subject_configs = self.get_subject_configurations(batch_code)
            
            # 按科目类型分组统计
            exam_subjects = [s for s in subject_configs if s['subject_type'] == 'exam']
            questionnaire_subjects = [s for s in subject_configs if s['subject_type'] == 'questionnaire']
            
            summary = {
                'batch_code': batch_code,
                'readiness': readiness,
                'subjects': {
                    'total': len(subject_configs),
                    'exam': len(exam_subjects),
                    'questionnaire': len(questionnaire_subjects),
                    'exam_subjects': [s['subject_name'] for s in exam_subjects],
                    'questionnaire_subjects': [s['subject_name'] for s in questionnaire_subjects]
                },
                'data_source': 'cleaned' if readiness['is_ready'] else 'legacy'
            }
            
            return summary
        except Exception as e:
            self._handle_db_error(e, "get_batch_summary")


class DimensionJSONParser:
    """JSON格式维度数据解析器"""
    
    def parse_dimension_scores(self, dimension_scores_json: str, dimension_max_scores_json: str) -> Dict[str, Any]:
        """解析JSON格式的维度分数数据"""
        try:
            import json
            
            scores = json.loads(dimension_scores_json) if isinstance(dimension_scores_json, str) else dimension_scores_json
            max_scores = json.loads(dimension_max_scores_json) if isinstance(dimension_max_scores_json, str) else dimension_max_scores_json
            
            dimensions = {}
            
            # 确保scores和max_scores都是字典
            if not isinstance(scores, dict) or not isinstance(max_scores, dict):
                return dimensions
            
            for dimension_code, score in scores.items():
                max_score = max_scores.get(dimension_code, 0)
                
                # 安全地转换分数值
                try:
                    if isinstance(score, dict):
                        # 如果是字典，尝试获取score或total字段
                        score_value = score.get('score', score.get('total', 0))
                    else:
                        score_value = score
                    score_float = float(score_value) if score_value is not None else 0.0
                except (TypeError, ValueError):
                    score_float = 0.0
                
                try:
                    if isinstance(max_score, dict):
                        # 如果是字典，尝试获取max_score或total字段
                        max_score_value = max_score.get('max_score', max_score.get('total', 0))
                    else:
                        max_score_value = max_score
                    max_score_float = float(max_score_value) if max_score_value is not None else 0.0
                except (TypeError, ValueError):
                    max_score_float = 0.0
                
                dimensions[dimension_code] = {
                    'score': score_float,
                    'max_score': max_score_float,
                    'score_rate': (score_float / max_score_float) if max_score_float > 0 else 0.0
                }
            
            return dimensions
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"Failed to parse dimension JSON data: {str(e)}")
            return {}
    
    def format_dimensions_for_calculation(self, dimensions: Dict[str, Any]) -> Dict[str, float]:
        """将维度数据格式化为计算引擎期望的格式"""
        try:
            formatted_dimensions = {}
            
            for dimension_code, dimension_data in dimensions.items():
                if isinstance(dimension_data, dict) and 'score' in dimension_data:
                    formatted_dimensions[dimension_code] = dimension_data['score']
                elif isinstance(dimension_data, (int, float)):
                    formatted_dimensions[dimension_code] = float(dimension_data)
            
            return formatted_dimensions
        except Exception as e:
            logger.error(f"Failed to format dimensions for calculation: {str(e)}")
            return {}