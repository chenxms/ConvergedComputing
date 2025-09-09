# 任务管理服务（兼容性封装）
from typing import Optional
from sqlalchemy.orm import Session
from .task_manager import TaskManager
from ..schemas.response_schemas import TaskResponse
import logging

logger = logging.getLogger(__name__)


class TaskService:
    """任务管理服务类（兼容性封装）"""
    
    def __init__(self, db_session: Session):
        self.task_manager = TaskManager(db_session)
    
    async def create_calculation_task(self, batch_id: int) -> TaskResponse:
        """创建计算任务（兼容性方法）"""
        logger.warning("TaskService.create_calculation_task is deprecated, use TaskManager directly")
        # 这个方法保留是为了向后兼容，实际应该使用TaskManager
        # 由于这里只有batch_id，需要先获取批次信息
        raise NotImplementedError("Use TaskManager.start_calculation_task with batch_code instead")
    
    async def get_task_status(self, task_id: str) -> Optional[TaskResponse]:
        """获取任务状态（兼容性方法）"""
        return await self.task_manager.get_task_status(task_id)