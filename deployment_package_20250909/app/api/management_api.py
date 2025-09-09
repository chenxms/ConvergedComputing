from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from datetime import datetime
import logging

from ..schemas.request_schemas import (
    StatisticalAggregationCreateRequest,
    StatisticalAggregationUpdateRequest
)
from ..schemas.response_schemas import (
    StatisticalAggregationResponse,
    OperationResultResponse,
    TaskResponse
)
from ..services.batch_service import BatchService
from ..services.task_manager import TaskManager
from ..database.connection import SessionLocal, get_db
from ..database.enums import AggregationLevel, CalculationStatus

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
def create_batch(
    request: StatisticalAggregationCreateRequest,
    batch_service: BatchService = Depends(get_batch_service)
):
    """创建新的统计批次 - 修复：移除async关键字和await"""
    try:
        result = batch_service.create_batch(request)
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
        raise HTTPException(status_code=500, detail=f"批次创建失败: {str(e)}")


@router.get("/batches", response_model=List[StatisticalAggregationResponse])
def list_batches(
    batch_code: Optional[str] = Query(None, description="批次代码筛选"),
    aggregation_level: Optional[AggregationLevel] = Query(None, description="汇聚级别筛选"),
    calculation_status: Optional[CalculationStatus] = Query(None, description="计算状态筛选"),
    school_id: Optional[str] = Query(None, description="学校ID筛选"),
    limit: int = Query(50, ge=1, le=1000, description="返回记录数限制"),
    offset: int = Query(0, ge=0, description="偏移量"),
    batch_service: BatchService = Depends(get_batch_service)
):
    """查询批次列表 - 修复：移除async关键字和await"""
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
            
        batches = batch_service.list_batches(
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
def get_batch(
    batch_code: str,
    aggregation_level: Optional[AggregationLevel] = Query(None, description="汇聚级别"),
    school_id: Optional[str] = Query(None, description="学校ID"),
    batch_service: BatchService = Depends(get_batch_service)
):
    """查询指定批次信息 - 修复：移除async关键字和await"""
    try:
        batch = batch_service.get_batch(
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
def update_batch(
    batch_code: str,
    request: StatisticalAggregationUpdateRequest,
    aggregation_level: Optional[AggregationLevel] = Query(None, description="汇聚级别"),
    school_id: Optional[str] = Query(None, description="学校ID"),
    batch_service: BatchService = Depends(get_batch_service)
):
    """更新批次信息 - 修复：移除async关键字和await"""
    try:
        updated_batch = batch_service.update_batch(
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
def delete_batch(
    batch_code: str,
    force: bool = Query(False, description="是否强制删除"),
    aggregation_level: Optional[AggregationLevel] = Query(None, description="汇聚级别"),
    school_id: Optional[str] = Query(None, description="学校ID"),
    batch_service: BatchService = Depends(get_batch_service)
):
    """删除批次 - 修复：移除async关键字和await"""
    try:
        deleted = batch_service.delete_batch(
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


@router.get("/tasks/{task_id}/progress")
async def get_task_progress(
    task_id: str,
    task_manager: TaskManager = Depends(get_task_manager)
):
    """查询任务进度详情（阶段与整体进度）。"""
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


@router.get("/batches/{batch_code}/summary", response_model=Dict[str, Any])
def get_batch_summary(
    batch_code: str,
    batch_service: BatchService = Depends(get_batch_service)
):
    """获取批次统计摘要 - 修复：移除async关键字和await"""
    try:
        summary = batch_service.get_batch_summary(batch_code)
        return summary
    except Exception as e:
        logger.error(f"Error getting batch summary {batch_code}: {str(e)}")
        raise HTTPException(status_code=500, detail="批次摘要查询失败")


# 数据清洗任务接口
@router.post("/batches/{batch_code}/clean", response_model=TaskResponse)
async def start_cleaning_task(
    batch_code: str,
    background_tasks: BackgroundTasks,
    task_manager: TaskManager = Depends(get_task_manager)
):
    """触发批次数据清洗任务（exam/interaction/questionnaire），返回任务ID用于前端轮询。"""
    try:
        task = await task_manager.start_cleaning_task(
            batch_code=batch_code,
            background_tasks=background_tasks
        )
        logger.info(f"Started cleaning task for batch: {batch_code}, task_id: {task.id}")
        return task
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error starting cleaning task: {str(e)}")
        raise HTTPException(status_code=500, detail="清洗任务启动失败")
