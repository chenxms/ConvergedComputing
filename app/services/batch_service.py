# 批次管理服务
from typing import List, Optional, Dict, Any, Union
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc
from datetime import datetime
import logging

from ..database.models import StatisticalAggregation, StatisticalHistory
from ..database.enums import AggregationLevel, CalculationStatus, ChangeType
from ..database.repositories import StatisticalAggregationRepository
from ..schemas.request_schemas import (
    StatisticalAggregationCreateRequest,
    StatisticalAggregationUpdateRequest
)
from ..schemas.response_schemas import StatisticalAggregationResponse

logger = logging.getLogger(__name__)


class BatchService:
    """批次管理服务类"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self.aggregation_repo = StatisticalAggregationRepository(db_session)
    
    async def create_batch(self, request: StatisticalAggregationCreateRequest) -> StatisticalAggregation:
        """创建批次"""
        try:
            # 验证批次是否已存在
            existing = await self._get_existing_batch(
                request.batch_code,
                request.aggregation_level,
                request.school_id
            )
            
            if existing:
                raise ValueError(f"批次 {request.batch_code} 在该级别和学校已存在")
            
            # 创建批次数据
            batch_data = {
                "batch_code": request.batch_code,
                "aggregation_level": request.aggregation_level,
                "school_id": request.school_id,
                "school_name": request.school_name,
                "statistics_data": request.statistics_data,
                "data_version": request.data_version,
                "calculation_status": CalculationStatus.PENDING,
                "total_students": request.total_students,
                "total_schools": request.total_schools
            }
            
            # 使用仓库创建批次
            batch = self.aggregation_repo.create(batch_data)
            
            # 记录历史
            await self._record_history(
                aggregation_id=batch.id,
                change_type=ChangeType.CREATED,
                current_data=batch_data,
                change_reason=request.change_reason or "批次创建",
                triggered_by=request.triggered_by or "system",
                batch_code=request.batch_code
            )
            
            logger.info(f"Created batch {request.batch_code} with ID {batch.id}")
            return batch
            
        except Exception as e:
            logger.error(f"Error creating batch {request.batch_code}: {str(e)}")
            raise
    
    async def get_batch(
        self, 
        batch_code: str,
        aggregation_level: Optional[AggregationLevel] = None,
        school_id: Optional[str] = None
    ) -> Optional[StatisticalAggregation]:
        """获取批次信息"""
        try:
            filters = {"batch_code": batch_code}
            if aggregation_level:
                filters["aggregation_level"] = aggregation_level
            if school_id:
                filters["school_id"] = school_id
                
            return self.aggregation_repo.get_by_filters(filters)
            
        except Exception as e:
            logger.error(f"Error getting batch {batch_code}: {str(e)}")
            raise
    
    async def list_batches(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[StatisticalAggregation]:
        """查询批次列表"""
        try:
            return self.aggregation_repo.get_paginated(
                filters=filters or {},
                limit=limit,
                offset=offset,
                order_by="created_at",
                order_direction="desc"
            )
            
        except Exception as e:
            logger.error(f"Error listing batches: {str(e)}")
            raise
    
    async def update_batch(
        self,
        batch_code: str,
        update_data: StatisticalAggregationUpdateRequest,
        aggregation_level: Optional[AggregationLevel] = None,
        school_id: Optional[str] = None
    ) -> Optional[StatisticalAggregation]:
        """更新批次信息"""
        try:
            # 获取现有批次
            batch = await self.get_batch(batch_code, aggregation_level, school_id)
            if not batch:
                return None
            
            # 记录变更前数据
            previous_data = {
                "statistics_data": batch.statistics_data,
                "calculation_status": batch.calculation_status.value if batch.calculation_status else None,
                "total_students": batch.total_students,
                "total_schools": batch.total_schools,
                "calculation_duration": float(batch.calculation_duration) if batch.calculation_duration else None
            }
            
            # 准备更新数据
            update_dict = {}
            if update_data.statistics_data is not None:
                update_dict["statistics_data"] = update_data.statistics_data
            if update_data.calculation_status is not None:
                update_dict["calculation_status"] = update_data.calculation_status
            if update_data.calculation_duration is not None:
                update_dict["calculation_duration"] = update_data.calculation_duration
            if update_data.total_students is not None:
                update_dict["total_students"] = update_data.total_students
            if update_data.total_schools is not None:
                update_dict["total_schools"] = update_data.total_schools
            
            if not update_dict:
                logger.warning(f"No data to update for batch {batch_code}")
                return batch
            
            # 执行更新
            updated_batch = self.aggregation_repo.update(batch.id, update_dict)
            
            # 记录历史
            await self._record_history(
                aggregation_id=batch.id,
                change_type=ChangeType.UPDATED,
                previous_data=previous_data,
                current_data=update_dict,
                change_reason=update_data.change_reason or "批次更新",
                triggered_by=update_data.triggered_by or "system",
                batch_code=batch_code
            )
            
            logger.info(f"Updated batch {batch_code} with ID {batch.id}")
            return updated_batch
            
        except Exception as e:
            logger.error(f"Error updating batch {batch_code}: {str(e)}")
            raise
    
    async def delete_batch(
        self,
        batch_code: str,
        force: bool = False,
        aggregation_level: Optional[AggregationLevel] = None,
        school_id: Optional[str] = None
    ) -> bool:
        """删除批次"""
        try:
            # 获取批次
            batch = await self.get_batch(batch_code, aggregation_level, school_id)
            if not batch:
                return False
            
            # 检查是否可以删除（非强制删除时检查状态）
            if not force and batch.calculation_status == CalculationStatus.PROCESSING:
                raise ValueError("正在计算中的批次不能删除，请使用force参数强制删除")
            
            # 记录删除历史
            await self._record_history(
                aggregation_id=batch.id,
                change_type=ChangeType.DELETED,
                previous_data={
                    "batch_code": batch.batch_code,
                    "aggregation_level": batch.aggregation_level.value,
                    "calculation_status": batch.calculation_status.value,
                    "total_students": batch.total_students,
                    "total_schools": batch.total_schools
                },
                change_reason=f"批次删除 (force={force})",
                triggered_by="system",
                batch_code=batch_code
            )
            
            # 删除批次（会级联删除相关历史记录）
            deleted = self.aggregation_repo.delete(batch.id)
            
            if deleted:
                logger.info(f"Deleted batch {batch_code} with ID {batch.id} (force={force})")
            
            return deleted
            
        except Exception as e:
            logger.error(f"Error deleting batch {batch_code}: {str(e)}")
            raise
    
    async def get_batch_summary(self, batch_code: str) -> Dict[str, Any]:
        """获取批次统计摘要"""
        try:
            # 获取区域级数据
            regional_batch = await self.get_batch(batch_code, AggregationLevel.REGIONAL)
            
            # 获取学校级数据统计
            school_filters = {
                "batch_code": batch_code,
                "aggregation_level": AggregationLevel.SCHOOL
            }
            school_batches = await self.list_batches(filters=school_filters, limit=1000)
            
            return {
                "batch_code": batch_code,
                "has_regional_data": regional_batch is not None,
                "regional_status": regional_batch.calculation_status.value if regional_batch else None,
                "total_schools": len(school_batches),
                "total_students": sum(b.total_students for b in school_batches),
                "completed_schools": len([b for b in school_batches if b.calculation_status == CalculationStatus.COMPLETED]),
                "processing_schools": len([b for b in school_batches if b.calculation_status == CalculationStatus.PROCESSING]),
                "failed_schools": len([b for b in school_batches if b.calculation_status == CalculationStatus.FAILED]),
                "avg_calculation_duration": sum(float(b.calculation_duration or 0) for b in school_batches) / len(school_batches) if school_batches else 0
            }
            
        except Exception as e:
            logger.error(f"Error getting batch summary {batch_code}: {str(e)}")
            raise
    
    async def _get_existing_batch(
        self,
        batch_code: str,
        aggregation_level: AggregationLevel,
        school_id: Optional[str]
    ) -> Optional[StatisticalAggregation]:
        """检查批次是否已存在"""
        filters = {
            "batch_code": batch_code,
            "aggregation_level": aggregation_level
        }
        if school_id:
            filters["school_id"] = school_id
        
        return self.aggregation_repo.get_by_filters(filters)
    
    async def _record_history(
        self,
        aggregation_id: int,
        change_type: ChangeType,
        batch_code: str,
        change_reason: str,
        triggered_by: str,
        previous_data: Optional[Dict[str, Any]] = None,
        current_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """记录变更历史"""
        try:
            history_data = {
                "aggregation_id": aggregation_id,
                "change_type": change_type,
                "previous_data": previous_data,
                "current_data": current_data,
                "change_summary": self._generate_change_summary(previous_data, current_data),
                "change_reason": change_reason,
                "triggered_by": triggered_by,
                "batch_code": batch_code
            }
            
            history = StatisticalHistory(**history_data)
            self.db.add(history)
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Error recording history for aggregation {aggregation_id}: {str(e)}")
            # 历史记录失败不应该影响主要操作
    
    def _generate_change_summary(self, previous_data: Optional[Dict], current_data: Optional[Dict]) -> Dict[str, Any]:
        """生成变更摘要"""
        summary = {
            "change_time": datetime.now().isoformat(),
            "updated_fields": []
        }
        
        if previous_data and current_data:
            for key in current_data.keys():
                if key in previous_data and previous_data[key] != current_data[key]:
                    summary["updated_fields"].append(key)
        
        return summary