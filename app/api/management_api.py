from fastapi import APIRouter, HTTPException
from typing import Dict, Any

from app.schemas.request_schemas import BatchCreateRequest, BatchDeleteRequest
from app.schemas.response_schemas import BatchResponse, TaskResponse
from app.services.batch_service import BatchService
from app.services.task_service import TaskService

router = APIRouter()

@router.post("/batches", response_model=BatchResponse)
async def create_batch(request: BatchCreateRequest):
    """创建新的统计批次"""
    try:
        batch_service = BatchService()
        batch = await batch_service.create_batch(request)
        return batch
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/batches/{batch_id}")
async def delete_batch(batch_id: int):
    """删除统计批次及相关数据"""
    try:
        batch_service = BatchService()
        await batch_service.delete_batch(batch_id)
        return {"message": "批次删除成功"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/batches/{batch_id}/calculate")
async def trigger_calculation(batch_id: int):
    """手动触发批次统计计算"""
    try:
        task_service = TaskService()
        task = await task_service.create_calculation_task(batch_id)
        return task
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/tasks/{task_id}")
async def get_task_status(task_id: int):
    """获取任务状态"""
    try:
        task_service = TaskService()
        task = await task_service.get_task_status(task_id)
        return task
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))