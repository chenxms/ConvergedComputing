from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid
import logging

from ..schemas.request_schemas import (
    BatchCreateRequest, 
    StatisticalAggregationCreateRequest,
    StatisticalAggregationUpdateRequest
)
from ..schemas.response_schemas import (
    BatchResponse, 
    TaskResponse, 
    StatisticalAggregationResponse,
    OperationResultResponse,
    ErrorResponse,
    PaginatedStatisticsResponse
)
from ..services.batch_service import BatchService
from ..services.task_manager import TaskManager
from ..database.connection import get_db
from ..database.connection import SessionLocal
from ..database.enums import CalculationStatus, AggregationLevel
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
router = APIRouter()


def get_db_session():
    """获取数据库会话"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_batch_service(db: Session = Depends(get_db_session)) -> BatchService:
    """获取批次服务实例"""
    return BatchService(db)


def get_task_manager(db: Session = Depends(get_db_session)) -> TaskManager:
    """获取任务管理器实例"""
    return TaskManager(db)


# 批次管理接口
@router.post("/batches", response_model=OperationResultResponse)
async def create_batch(
    request: StatisticalAggregationCreateRequest,
    batch_service: BatchService = Depends(get_batch_service)
):
    """创建新的统计批次"""
    try:
        result = await batch_service.create_batch(request)
        logger.info(f"Created batch: {request.batch_code}")
        
        return OperationResultResponse(
            success=True,
            message=f"批次 {request.batch_code} 创建成功",
            data={
                "batch_id": result.id,
                "batch_code": result.batch_code,
                "aggregation_level": result.aggregation_level.value,
                "created_at": result.created_at.isoformat()
            }
        )
    except ValueError as e:
        logger.error(f"Validation error creating batch: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating batch: {str(e)}")
        raise HTTPException(status_code=500, detail="批次创建失败")


@router.get("/batches", response_model=List[StatisticalAggregationResponse])
async def list_batches(
    batch_code: Optional[str] = Query(None, description="批次代码筛选"),
    aggregation_level: Optional[AggregationLevel] = Query(None, description="汇聚级别筛选"),
    calculation_status: Optional[CalculationStatus] = Query(None, description="计算状态筛选"),
    school_id: Optional[str] = Query(None, description="学校ID筛选"),
    limit: int = Query(50, ge=1, le=1000, description="返回记录数限制"),
    offset: int = Query(0, ge=0, description="偏移量"),
    batch_service: BatchService = Depends(get_batch_service)
):
    """查询批次列表"""
    try:
        filters = {}
        if batch_code:
            filters['batch_code'] = batch_code
        if aggregation_level:
            filters['aggregation_level'] = aggregation_level
        if calculation_status:
            filters['calculation_status'] = calculation_status
        if school_id:
            filters['school_id'] = school_id
            
        batches = await batch_service.list_batches(
            filters=filters, 
            limit=limit, 
            offset=offset
        )
        
        logger.info(f"Retrieved {len(batches)} batches with filters: {filters}")
        return batches
    except Exception as e:
        logger.error(f"Error listing batches: {str(e)}")
        raise HTTPException(status_code=500, detail="批次查询失败")


@router.get("/batches/{batch_code}", response_model=StatisticalAggregationResponse)
async def get_batch(
    batch_code: str,
    aggregation_level: Optional[AggregationLevel] = Query(None, description="汇聚级别"),
    school_id: Optional[str] = Query(None, description="学校ID"),
    batch_service: BatchService = Depends(get_batch_service)
):
    """查询指定批次信息"""
    try:
        batch = await batch_service.get_batch(
            batch_code=batch_code,
            aggregation_level=aggregation_level,
            school_id=school_id
        )
        
        if not batch:
            raise HTTPException(status_code=404, detail="批次不存在")
            
        logger.info(f"Retrieved batch: {batch_code}")
        return batch
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting batch {batch_code}: {str(e)}")
        raise HTTPException(status_code=500, detail="批次查询失败")


@router.put("/batches/{batch_code}", response_model=OperationResultResponse)
async def update_batch(
    batch_code: str,
    request: StatisticalAggregationUpdateRequest,
    aggregation_level: Optional[AggregationLevel] = Query(None, description="汇聚级别"),
    school_id: Optional[str] = Query(None, description="学校ID"),
    batch_service: BatchService = Depends(get_batch_service)
):
    """更新批次信息"""
    try:
        updated_batch = await batch_service.update_batch(
            batch_code=batch_code,
            update_data=request,
            aggregation_level=aggregation_level,
            school_id=school_id
        )
        
        if not updated_batch:
            raise HTTPException(status_code=404, detail="批次不存在")
            
        logger.info(f"Updated batch: {batch_code}")
        return OperationResultResponse(
            success=True,
            message=f"批次 {batch_code} 更新成功",
            data={
                "batch_id": updated_batch.id,
                "batch_code": updated_batch.batch_code,
                "updated_at": updated_batch.updated_at.isoformat()
            }
        )
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error updating batch: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating batch {batch_code}: {str(e)}")
        raise HTTPException(status_code=500, detail="批次更新失败")


@router.delete("/batches/{batch_code}", response_model=OperationResultResponse)
async def delete_batch(
    batch_code: str,
    force: bool = Query(False, description="是否强制删除"),
    aggregation_level: Optional[AggregationLevel] = Query(None, description="汇聚级别"),
    school_id: Optional[str] = Query(None, description="学校ID"),
    batch_service: BatchService = Depends(get_batch_service)
):
    """删除批次"""
    try:
        deleted = await batch_service.delete_batch(
            batch_code=batch_code,
            force=force,
            aggregation_level=aggregation_level,
            school_id=school_id
        )
        
        if not deleted:
            raise HTTPException(status_code=404, detail="批次不存在")
            
        logger.info(f"Deleted batch: {batch_code} (force={force})")
        return OperationResultResponse(
            success=True,
            message=f"批次 {batch_code} 删除成功",
            data={
                "batch_code": batch_code,
                "deleted_at": datetime.now().isoformat()
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting batch {batch_code}: {str(e)}")
        raise HTTPException(status_code=500, detail="批次删除失败")


# 任务管理接口
@router.post("/tasks/{batch_code}/start", response_model=TaskResponse)
async def start_calculation_task(
    batch_code: str,
    background_tasks: BackgroundTasks,
    aggregation_level: Optional[AggregationLevel] = Query(None, description="汇聚级别"),
    school_id: Optional[str] = Query(None, description="学校ID"),
    priority: int = Query(1, ge=1, le=10, description="任务优先级(1-10)"),
    task_manager: TaskManager = Depends(get_task_manager)
):
    """手动启动统计任务"""
    try:
        task = await task_manager.start_calculation_task(
            batch_code=batch_code,
            aggregation_level=aggregation_level,
            school_id=school_id,
            priority=priority,
            background_tasks=background_tasks
        )
        
        logger.info(f"Started calculation task for batch: {batch_code}, task_id: {task.id}")
        return task
    except ValueError as e:
        logger.error(f"Validation error starting task: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error starting calculation task: {str(e)}")
        raise HTTPException(status_code=500, detail="任务启动失败")


@router.post("/tasks/{task_id}/cancel", response_model=OperationResultResponse)
async def cancel_task(
    task_id: str,
    task_manager: TaskManager = Depends(get_task_manager)
):
    """取消执行中的任务"""
    try:
        cancelled = await task_manager.cancel_task(task_id)
        
        if not cancelled:
            raise HTTPException(status_code=404, detail="任务不存在或无法取消")
            
        logger.info(f"Cancelled task: {task_id}")
        return OperationResultResponse(
            success=True,
            message=f"任务 {task_id} 取消成功",
            data={
                "task_id": task_id,
                "cancelled_at": datetime.now().isoformat()
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling task {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="任务取消失败")


@router.get("/tasks/{task_id}/status", response_model=TaskResponse)
async def get_task_status(
    task_id: str,
    task_manager: TaskManager = Depends(get_task_manager)
):
    """查询任务状态"""
    try:
        task = await task_manager.get_task_status(task_id)
        
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")
            
        return task
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting task status {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="任务状态查询失败")


@router.get("/tasks/{task_id}/progress", response_model=Dict[str, Any])
async def get_task_progress(
    task_id: str,
    task_manager: TaskManager = Depends(get_task_manager)
):
    """查询任务进度"""
    try:
        progress = await task_manager.get_task_progress(task_id)
        
        if not progress:
            raise HTTPException(status_code=404, detail="任务不存在")
            
        return progress
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting task progress {task_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="任务进度查询失败")


# 批量操作接口
@router.post("/tasks/batch-start", response_model=List[TaskResponse])
async def batch_start_tasks(
    batch_codes: List[str],
    background_tasks: BackgroundTasks,
    priority: int = Query(1, ge=1, le=10, description="任务优先级(1-10)"),
    task_manager: TaskManager = Depends(get_task_manager)
):
    """批量启动任务"""
    try:
        if len(batch_codes) > 50:
            raise HTTPException(status_code=400, detail="一次最多启动50个批次任务")
            
        tasks = await task_manager.batch_start_tasks(
            batch_codes=batch_codes,
            priority=priority,
            background_tasks=background_tasks
        )
        
        logger.info(f"Batch started {len(tasks)} tasks for batches: {batch_codes}")
        return tasks
    except ValueError as e:
        logger.error(f"Validation error batch starting tasks: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error batch starting tasks: {str(e)}")
        raise HTTPException(status_code=500, detail="批量任务启动失败")


@router.post("/tasks/batch-cancel", response_model=OperationResultResponse)
async def batch_cancel_tasks(
    task_ids: List[str],
    task_manager: TaskManager = Depends(get_task_manager)
):
    """批量取消任务"""
    try:
        if len(task_ids) > 100:
            raise HTTPException(status_code=400, detail="一次最多取消100个任务")
            
        cancelled_count = await task_manager.batch_cancel_tasks(task_ids)
        
        logger.info(f"Batch cancelled {cancelled_count} out of {len(task_ids)} tasks")
        return OperationResultResponse(
            success=True,
            message=f"成功取消 {cancelled_count} 个任务",
            data={
                "requested_count": len(task_ids),
                "cancelled_count": cancelled_count,
                "cancelled_at": datetime.now().isoformat()
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error batch cancelling tasks: {str(e)}")
        raise HTTPException(status_code=500, detail="批量任务取消失败")


@router.post("/tasks/batch-delete", response_model=OperationResultResponse)
async def batch_delete_tasks(
    task_ids: List[str],
    force: bool = Query(False, description="是否强制删除"),
    task_manager: TaskManager = Depends(get_task_manager)
):
    """批量删除任务"""
    try:
        if len(task_ids) > 100:
            raise HTTPException(status_code=400, detail="一次最多删除100个任务")
            
        deleted_count = await task_manager.batch_delete_tasks(task_ids, force)
        
        logger.info(f"Batch deleted {deleted_count} out of {len(task_ids)} tasks (force={force})")
        return OperationResultResponse(
            success=True,
            message=f"成功删除 {deleted_count} 个任务",
            data={
                "requested_count": len(task_ids),
                "deleted_count": deleted_count,
                "deleted_at": datetime.now().isoformat()
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error batch deleting tasks: {str(e)}")
        raise HTTPException(status_code=500, detail="批量任务删除失败")


# 系统状态接口
@router.get("/system/status", response_model=Dict[str, Any])
async def get_system_status(
    task_manager: TaskManager = Depends(get_task_manager)
):
    """获取系统状态"""
    try:
        status = await task_manager.get_system_status()
        return status
    except Exception as e:
        logger.error(f"Error getting system status: {str(e)}")
        raise HTTPException(status_code=500, detail="系统状态查询失败")


@router.get("/tasks", response_model=List[TaskResponse])
async def list_tasks(
    status: Optional[str] = Query(None, description="任务状态筛选"),
    batch_code: Optional[str] = Query(None, description="批次代码筛选"),
    limit: int = Query(50, ge=1, le=1000, description="返回记录数限制"),
    offset: int = Query(0, ge=0, description="偏移量"),
    task_manager: TaskManager = Depends(get_task_manager)
):
    """查询任务列表"""
    try:
        filters = {}
        if status:
            filters['status'] = status
        if batch_code:
            filters['batch_code'] = batch_code
            
        tasks = await task_manager.list_tasks(
            filters=filters,
            limit=limit,
            offset=offset
        )
        
        logger.info(f"Retrieved {len(tasks)} tasks with filters: {filters}")
        return tasks
    except Exception as e:
        logger.error(f"Error listing tasks: {str(e)}")
        raise HTTPException(status_code=500, detail="任务查询失败")