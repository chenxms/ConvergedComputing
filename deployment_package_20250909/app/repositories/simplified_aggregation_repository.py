"""
简化汇聚数据仓库
用于保存和管理汇聚结果数据
"""

import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from ..database.models import (
    StatisticalAggregation, AggregationLevel, CalculationStatus,
    StatisticalHistory, ChangeType
)
from ..database.repositories import BaseRepository, RepositoryError, DataIntegrityError
from ..schemas.simplified_aggregation_schema import (
    RegionalAggregationData, SchoolAggregationData
)

logger = logging.getLogger(__name__)


class SimplifiedAggregationRepository(BaseRepository):
    """简化汇聚数据仓库"""
    
    def __init__(self, db_session: Session):
        """
        初始化仓库
        
        Args:
            db_session: 数据库会话
        """
        super().__init__(db_session)
        logger.info("初始化简化汇聚数据仓库")
    
    def save_aggregation_data(
        self,
        batch_code: str,
        aggregation_level: AggregationLevel,
        data: Union[RegionalAggregationData, SchoolAggregationData],
        school_id: Optional[str] = None,
        school_name: Optional[str] = None,
        calculation_duration: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        保存汇聚结果
        
        Args:
            batch_code: 批次代码
            aggregation_level: 汇聚级别
            data: 汇聚数据
            school_id: 学校ID（学校级时必填）
            school_name: 学校名称
            calculation_duration: 计算耗时（秒）
            
        Returns:
            保存结果
        """
        logger.info(f"保存汇聚数据: {batch_code}, {aggregation_level.value}")
        start_time = time.time()
        
        try:
            # 1. 转换数据为字典格式
            if hasattr(data, 'dict'):
                statistics_data = data.dict()
            else:
                statistics_data = dict(data) if isinstance(data, dict) else data
            
            # 2. 检查是否已存在记录
            existing_record = self._get_existing_record(
                batch_code, aggregation_level, school_id, school_name
            )
            
            # 3. 计算元数据
            total_students = getattr(data, 'total_students', 0)
            total_schools = getattr(data, 'total_schools', 0) if aggregation_level == AggregationLevel.REGIONAL else 0
            
            if existing_record:
                # 更新现有记录
                result = self._update_aggregation_record(
                    existing_record, statistics_data, total_students, 
                    total_schools, calculation_duration
                )
            else:
                # 创建新记录
                result = self._create_aggregation_record(
                    batch_code, aggregation_level, statistics_data,
                    school_id, school_name, total_students, total_schools,
                    calculation_duration
                )
            
            duration = time.time() - start_time
            logger.info(f"保存汇聚数据完成，耗时: {duration:.2f}秒")
            
            return {
                'success': True,
                'record_id': result['record_id'],
                'action': result['action'],
                'duration': duration
            }
            
        except Exception as e:
            self._handle_db_error(e, "save_aggregation_data")
            return {
                'success': False,
                'error': str(e),
                'duration': time.time() - start_time
            }
    
    def get_aggregation_data(
        self,
        batch_code: str,
        aggregation_level: AggregationLevel,
        school_id: Optional[str] = None,
        school_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        读取汇聚结果
        
        Args:
            batch_code: 批次代码
            aggregation_level: 汇聚级别
            school_id: 学校ID
            school_name: 学校名称
            
        Returns:
            汇聚数据或None
        """
        try:
            record = self._get_existing_record(
                batch_code, aggregation_level, school_id, school_name
            )
            
            if not record:
                logger.info(f"未找到汇聚数据: {batch_code}, {aggregation_level.value}")
                return None
            
            return {
                'id': record.id,
                'batch_code': record.batch_code,
                'aggregation_level': record.aggregation_level.value,
                'school_id': record.school_id,
                'school_name': record.school_name,
                'statistics_data': record.statistics_data,
                'data_version': record.data_version,
                'calculation_status': record.calculation_status.value,
                'total_students': record.total_students,
                'total_schools': record.total_schools,
                'calculation_duration': float(record.calculation_duration) if record.calculation_duration else None,
                'created_at': record.created_at.isoformat(),
                'updated_at': record.updated_at.isoformat()
            }
            
        except Exception as e:
            logger.error(f"读取汇聚数据失败: {str(e)}")
            return None
    
    def update_aggregation_status(
        self,
        batch_code: str,
        aggregation_level: AggregationLevel,
        status: CalculationStatus,
        school_id: Optional[str] = None,
        school_name: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> bool:
        """
        更新汇聚状态
        
        Args:
            batch_code: 批次代码
            aggregation_level: 汇聚级别
            status: 计算状态
            school_id: 学校ID
            school_name: 学校名称
            error_message: 错误信息
            
        Returns:
            更新是否成功
        """
        try:
            record = self._get_existing_record(
                batch_code, aggregation_level, school_id, school_name
            )
            
            if not record:
                logger.warning(f"未找到待更新的记录: {batch_code}, {aggregation_level.value}")
                return False
            
            # 保存历史记录
            if record.calculation_status != status:
                self._create_history_record(
                    record, ChangeType.STATUS_CHANGE,
                    change_reason=f"状态从 {record.calculation_status.value} 变更为 {status.value}"
                )
            
            # 更新状态
            record.calculation_status = status
            record.updated_at = datetime.now()
            
            # 如果有错误信息，可以存储在统计数据中
            if error_message and status in [CalculationStatus.FAILED, CalculationStatus.ERROR]:
                if not record.statistics_data:
                    record.statistics_data = {}
                record.statistics_data['error_message'] = error_message
                record.statistics_data['error_time'] = datetime.now().isoformat()
            
            self.db.commit()
            
            logger.info(f"更新汇聚状态成功: {batch_code}, {status.value}")
            return True
            
        except Exception as e:
            self._handle_db_error(e, "update_aggregation_status")
            return False
    
    def get_batch_aggregation_status(self, batch_code: str) -> Dict[str, Any]:
        """
        获取批次汇聚状态
        
        Args:
            batch_code: 批次代码
            
        Returns:
            批次汇聚状态信息
        """
        try:
            query = text("""
                SELECT 
                    aggregation_level,
                    calculation_status,
                    COUNT(*) as count,
                    AVG(total_students) as avg_students,
                    SUM(total_schools) as total_schools,
                    AVG(calculation_duration) as avg_duration,
                    MIN(created_at) as first_created,
                    MAX(updated_at) as last_updated
                FROM statistical_aggregations
                WHERE batch_code = :batch_code
                GROUP BY aggregation_level, calculation_status
                ORDER BY aggregation_level, calculation_status
            """)
            
            result = self.db.execute(query, {'batch_code': batch_code})
            rows = result.fetchall()
            
            status_info = {
                'batch_code': batch_code,
                'aggregation_levels': {},
                'total_records': 0,
                'summary': {
                    'completed_count': 0,
                    'failed_count': 0,
                    'pending_count': 0
                }
            }
            
            for row in rows:
                level = row.aggregation_level
                status = row.calculation_status
                count = row.count
                
                if level not in status_info['aggregation_levels']:
                    status_info['aggregation_levels'][level] = {}
                
                status_info['aggregation_levels'][level][status] = {
                    'count': count,
                    'avg_students': int(row.avg_students) if row.avg_students else 0,
                    'total_schools': int(row.total_schools) if row.total_schools else 0,
                    'avg_duration': round(float(row.avg_duration), 2) if row.avg_duration else 0,
                    'first_created': row.first_created.isoformat() if row.first_created else None,
                    'last_updated': row.last_updated.isoformat() if row.last_updated else None
                }
                
                status_info['total_records'] += count
                
                # 更新汇总统计
                if status == CalculationStatus.COMPLETED:
                    status_info['summary']['completed_count'] += count
                elif status in [CalculationStatus.FAILED, CalculationStatus.ERROR]:
                    status_info['summary']['failed_count'] += count
                else:
                    status_info['summary']['pending_count'] += count
            
            return status_info
            
        except Exception as e:
            logger.error(f"获取批次汇聚状态失败: {str(e)}")
            return {
                'batch_code': batch_code,
                'error': str(e)
            }
    
    def delete_batch_aggregations(self, batch_code: str) -> Dict[str, Any]:
        """
        删除批次的所有汇聚数据
        
        Args:
            batch_code: 批次代码
            
        Returns:
            删除结果
        """
        try:
            # 查询将要删除的记录数量
            count_query = text("""
                SELECT COUNT(*) as total_count
                FROM statistical_aggregations 
                WHERE batch_code = :batch_code
            """)
            count_result = self.db.execute(count_query, {'batch_code': batch_code})
            total_count = count_result.scalar()
            
            if total_count == 0:
                return {
                    'success': True,
                    'deleted_count': 0,
                    'message': f'批次 {batch_code} 没有汇聚数据'
                }
            
            # 删除记录（历史记录会通过外键级联删除）
            delete_query = text("""
                DELETE FROM statistical_aggregations 
                WHERE batch_code = :batch_code
            """)
            self.db.execute(delete_query, {'batch_code': batch_code})
            self.db.commit()
            
            logger.info(f"删除批次 {batch_code} 的汇聚数据，共删除 {total_count} 条记录")
            
            return {
                'success': True,
                'deleted_count': total_count,
                'message': f'成功删除批次 {batch_code} 的 {total_count} 条汇聚数据'
            }
            
        except Exception as e:
            self._handle_db_error(e, "delete_batch_aggregations")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_recent_aggregations(
        self,
        limit: int = 50,
        aggregation_level: Optional[AggregationLevel] = None,
        status: Optional[CalculationStatus] = None
    ) -> List[Dict[str, Any]]:
        """
        获取最近的汇聚记录
        
        Args:
            limit: 限制数量
            aggregation_level: 汇聚级别过滤
            status: 状态过滤
            
        Returns:
            汇聚记录列表
        """
        try:
            query_conditions = []
            query_params = {}
            
            if aggregation_level:
                query_conditions.append("aggregation_level = :aggregation_level")
                query_params['aggregation_level'] = aggregation_level
            
            if status:
                query_conditions.append("calculation_status = :status")
                query_params['status'] = status
            
            where_clause = ""
            if query_conditions:
                where_clause = "WHERE " + " AND ".join(query_conditions)
            
            query = text(f"""
                SELECT 
                    id, batch_code, aggregation_level, school_id, school_name,
                    calculation_status, total_students, total_schools,
                    calculation_duration, data_version,
                    created_at, updated_at
                FROM statistical_aggregations
                {where_clause}
                ORDER BY updated_at DESC
                LIMIT :limit
            """)
            
            query_params['limit'] = limit
            result = self.db.execute(query, query_params)
            
            records = []
            for row in result.fetchall():
                records.append({
                    'id': row.id,
                    'batch_code': row.batch_code,
                    'aggregation_level': row.aggregation_level,
                    'school_id': row.school_id,
                    'school_name': row.school_name,
                    'calculation_status': row.calculation_status,
                    'total_students': row.total_students,
                    'total_schools': row.total_schools,
                    'calculation_duration': float(row.calculation_duration) if row.calculation_duration else None,
                    'data_version': row.data_version,
                    'created_at': row.created_at.isoformat(),
                    'updated_at': row.updated_at.isoformat()
                })
            
            return records
            
        except Exception as e:
            logger.error(f"获取最近汇聚记录失败: {str(e)}")
            return []
    
    def _get_existing_record(
        self,
        batch_code: str,
        aggregation_level: AggregationLevel,
        school_id: Optional[str] = None,
        school_name: Optional[str] = None
    ) -> Optional[StatisticalAggregation]:
        """获取已存在的记录"""
        try:
            query = self.db.query(StatisticalAggregation).filter(
                and_(
                    StatisticalAggregation.batch_code == batch_code,
                    StatisticalAggregation.aggregation_level == aggregation_level
                )
            )
            
            if aggregation_level == AggregationLevel.SCHOOL:
                if school_id:
                    query = query.filter(StatisticalAggregation.school_id == school_id)
                if school_name:
                    query = query.filter(StatisticalAggregation.school_name == school_name)
            else:
                query = query.filter(
                    and_(
                        StatisticalAggregation.school_id.is_(None),
                        StatisticalAggregation.school_name.is_(None)
                    )
                )
            
            return query.first()
            
        except Exception as e:
            logger.error(f"查询现有记录失败: {str(e)}")
            return None
    
    def _create_aggregation_record(
        self,
        batch_code: str,
        aggregation_level: AggregationLevel,
        statistics_data: Dict[str, Any],
        school_id: Optional[str],
        school_name: Optional[str],
        total_students: int,
        total_schools: int,
        calculation_duration: Optional[float]
    ) -> Dict[str, Any]:
        """创建新的汇聚记录"""
        try:
            record = StatisticalAggregation(
                batch_code=batch_code,
                aggregation_level=aggregation_level,
                school_id=school_id,
                school_name=school_name,
                statistics_data=statistics_data,
                data_version="2.0",
                calculation_status=CalculationStatus.COMPLETED,
                total_students=total_students,
                total_schools=total_schools,
                calculation_duration=calculation_duration
            )
            
            self.db.add(record)
            self.db.commit()
            self.db.refresh(record)
            
            # 创建历史记录
            self._create_history_record(
                record, ChangeType.CREATED,
                change_reason="初始创建汇聚数据"
            )
            
            logger.info(f"创建汇聚记录成功，ID: {record.id}")
            return {
                'record_id': record.id,
                'action': 'created'
            }
            
        except Exception as e:
            self.db.rollback()
            raise e
    
    def _update_aggregation_record(
        self,
        record: StatisticalAggregation,
        statistics_data: Dict[str, Any],
        total_students: int,
        total_schools: int,
        calculation_duration: Optional[float]
    ) -> Dict[str, Any]:
        """更新现有记录"""
        try:
            # 保存更新前的数据快照
            previous_data = {
                'statistics_data': record.statistics_data,
                'total_students': record.total_students,
                'total_schools': record.total_schools,
                'calculation_duration': float(record.calculation_duration) if record.calculation_duration else None
            }
            
            # 更新数据
            record.statistics_data = statistics_data
            record.total_students = total_students
            record.total_schools = total_schools
            record.calculation_duration = calculation_duration
            record.calculation_status = CalculationStatus.COMPLETED
            record.updated_at = datetime.now()
            
            self.db.commit()
            
            # 创建历史记录
            self._create_history_record(
                record, ChangeType.UPDATED,
                previous_data=previous_data,
                change_reason="数据重新计算更新"
            )
            
            logger.info(f"更新汇聚记录成功，ID: {record.id}")
            return {
                'record_id': record.id,
                'action': 'updated'
            }
            
        except Exception as e:
            self.db.rollback()
            raise e
    
    def _create_history_record(
        self,
        aggregation: StatisticalAggregation,
        change_type: ChangeType,
        previous_data: Optional[Dict[str, Any]] = None,
        change_reason: Optional[str] = None
    ) -> None:
        """创建历史记录"""
        try:
            current_data = {
                'total_students': aggregation.total_students,
                'total_schools': aggregation.total_schools,
                'calculation_status': aggregation.calculation_status.value,
                'data_version': aggregation.data_version
            }
            
            history = StatisticalHistory(
                aggregation_id=aggregation.id,
                change_type=change_type,
                previous_data=previous_data,
                current_data=current_data,
                change_reason=change_reason,
                batch_code=aggregation.batch_code,
                triggered_by="system"
            )
            
            self.db.add(history)
            # 注意：不在这里commit，由调用方统一commit
            
        except Exception as e:
            logger.error(f"创建历史记录失败: {str(e)}")
            # 历史记录失败不应该影响主流程，记录错误但不抛出异常