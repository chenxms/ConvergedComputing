# 任务管理器服务
from typing import List, Optional, Dict, Any, Callable
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from fastapi import BackgroundTasks
import uuid
import logging
import asyncio
import threading
from enum import Enum

from ..database.models import StatisticalAggregation, Task
from ..database.enums import CalculationStatus, AggregationLevel
from ..database.repositories import StatisticalAggregationRepository, TaskRepository
from ..schemas.response_schemas import TaskResponse
from ..services.calculation_service import CalculationService
from data_cleaning_service import DataCleaningService

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    """任务状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskPriority(int, Enum):
    """任务优先级枚举"""
    LOW = 1
    NORMAL = 5
    HIGH = 10


class TaskManager:
    """任务管理器服务类"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self.aggregation_repo = StatisticalAggregationRepository(db_session)
        self.task_repo = TaskRepository(db_session)
        self.calculation_service = CalculationService(db_session)
        
        # 任务缓存和状态跟踪
        self._running_tasks: Dict[str, Dict[str, Any]] = {}
        self._task_progress: Dict[str, Dict[str, Any]] = {}
        self._cancelled_tasks: set = set()
        
        # 系统状态
        self._system_stats = {
            "total_tasks": 0,
            "running_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
            "cancelled_tasks": 0,
            "system_start_time": datetime.now()
        }
    
    async def start_calculation_task(
        self,
        batch_code: str,
        aggregation_level: Optional[AggregationLevel] = None,
        school_id: Optional[str] = None,
        priority: int = TaskPriority.NORMAL,
        background_tasks: BackgroundTasks = None
    ) -> TaskResponse:
        """启动统计计算任务"""
        try:
            # 验证批次是否存在
            batch = self.aggregation_repo.get_by_filters({
                "batch_code": batch_code,
                **({"aggregation_level": aggregation_level} if aggregation_level else {}),
                **({"school_id": school_id} if school_id else {})
            })
            
            if not batch:
                raise ValueError(f"批次 {batch_code} 不存在")
            
            # 检查是否已经在运行
            if batch.calculation_status == CalculationStatus.PROCESSING:
                existing_task = self._find_running_task_by_batch(batch_code, school_id)
                if existing_task:
                    return self._convert_to_task_response(existing_task)
            
            # 创建新任务 - 使用时间戳生成数字ID
            import time
            task_id = int(time.time() * 1000000) + batch.id  # 微秒时间戳 + batch_id确保唯一性
            task_data = {
                "id": task_id,
                "batch_id": batch.id,
                "batch_code": batch_code,
                "school_id": school_id,
                "aggregation_level": aggregation_level.value if aggregation_level else batch.aggregation_level.value,
                "status": TaskStatus.PENDING,
                "priority": priority,
                "progress": 0.0,
                "started_at": datetime.now(),
                "completed_at": None,
                "error_message": None,
                "stage_details": [
                    {"stage": "data_loading", "status": "pending", "progress": 0.0, "description": "数据加载和验证"},
                    {"stage": "statistical_calculation", "status": "pending", "progress": 0.0, "description": "统计计算和数据生成"},
                    {"stage": "result_aggregation", "status": "pending", "progress": 0.0, "description": "结果汇聚和存储"}
                ]
            }
            
            # 保存到数据库
            task_create_data = {
                "id": task_data["id"],
                "batch_id": task_data["batch_id"],
                "status": task_data["status"].value,  # 转换枚举为字符串值
                "progress": task_data["progress"],
                "started_at": task_data["started_at"]
            }
            
            self.task_repo.create(task_create_data)
            
            # 更新批次状态
            self.aggregation_repo.update(batch.id, {
                "calculation_status": CalculationStatus.PROCESSING
            })
            
            # 缓存任务信息
            self._running_tasks[task_id] = task_data
            self._task_progress[task_id] = {
                "overall_progress": 0.0,
                "stage_details": task_data["stage_details"],
                "last_updated": datetime.now()
            }
            
            # 启动后台任务
            if background_tasks:
                background_tasks.add_task(self._execute_calculation_task, task_id)
            else:
                # 如果没有BackgroundTasks，启动独立线程
                threading.Thread(
                    target=self._execute_calculation_task_sync,
                    args=(task_id,),
                    daemon=True
                ).start()
            
            # 更新系统统计
            self._system_stats["total_tasks"] += 1
            self._system_stats["running_tasks"] += 1
            
            logger.info(f"Started calculation task {task_id} for batch {batch_code}")
            return self._convert_to_task_response(task_data)
            
        except Exception as e:
            logger.error(f"Error starting calculation task: {str(e)}")
            raise

    async def start_cleaning_task(
        self,
        batch_code: str,
        background_tasks: BackgroundTasks = None
    ) -> TaskResponse:
        """启动数据清洗任务，返回任务信息（task_id用于前端轮询）。"""
        try:
            import time
            task_id = int(time.time() * 1000000)
            task_data = {
                "id": task_id,
                "batch_id": 0,
                "batch_code": batch_code,
                "status": TaskStatus.PENDING,
                "priority": TaskPriority.NORMAL,
                "progress": 0.0,
                "started_at": datetime.now(),
                "completed_at": None,
                "error_message": None,
                "stage_details": [
                    {"stage": "precheck", "status": "pending", "progress": 0.0, "description": "预检查与准备"},
                    {"stage": "cleaning", "status": "pending", "progress": 0.0, "description": "执行数据清洗"},
                    {"stage": "verification", "status": "pending", "progress": 0.0, "description": "结果校验"}
                ]
            }

            # 入库
            self.task_repo.create({
                "id": task_id,
                "batch_id": task_data["batch_id"],
                "status": task_data["status"].value,
                "progress": task_data["progress"],
                "started_at": task_data["started_at"]
            })

            # 缓存
            self._running_tasks[task_id] = task_data
            self._task_progress[task_id] = {
                "overall_progress": 0.0,
                "stage_details": [
                    {"stage": "precheck", "status": "pending", "progress": 0.0},
                    {"stage": "cleaning", "status": "pending", "progress": 0.0},
                    {"stage": "verification", "status": "pending", "progress": 0.0}
                ],
                "last_updated": datetime.now()
            }

            # 启动后台执行
            if background_tasks:
                background_tasks.add_task(self._execute_cleaning_task_sync, task_id, batch_code)
            else:
                threading.Thread(target=self._execute_cleaning_task_sync, args=(task_id, batch_code), daemon=True).start()

            # 统计
            self._system_stats["total_tasks"] += 1
            self._system_stats["running_tasks"] += 1

            return self._convert_to_task_response(task_data)

        except Exception as e:
            logger.error(f"Error starting cleaning task: {str(e)}")
            raise

    def _execute_cleaning_task_sync(self, task_id: str, batch_code: str) -> None:
        asyncio.run(self._execute_cleaning_task(task_id, batch_code))

    async def _execute_cleaning_task(self, task_id: str, batch_code: str) -> None:
        try:
            task_info = self._running_tasks.get(task_id)
            if not task_info:
                return

            # 运行中
            task_info["status"] = TaskStatus.RUNNING
            self._update_task_progress(task_id, 5.0, "precheck", "processing")

            # 执行清洗
            svc = DataCleaningService(self.db)
            await self._simulate_progress(task_id, 5, 15, "precheck")
            self._update_task_progress(task_id, 20.0, "cleaning", "processing")
            await svc.clean_batch_scores(batch_code)
            await self._simulate_progress(task_id, 20, 85, "cleaning")

            # 简要校验
            from sqlalchemy import text
            row = self.db.execute(text("SELECT COUNT(*) FROM student_cleaned_scores WHERE batch_code=:b"), {"b": batch_code}).fetchone()
            _ = int(row[0]) if row and row[0] is not None else 0
            self._update_task_progress(task_id, 95.0, "verification", "processing")

            await self._complete_task_successfully(task_id)

        except Exception as e:
            await self._complete_task_with_error(task_id, str(e))
    
    async def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        try:
            if task_id not in self._running_tasks:
                # 检查数据库中的任务
                db_task = self.task_repo.get_by_id(task_id)
                if not db_task:
                    return False
                
                # 如果任务已经完成或失败，不能取消
                if db_task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                    return False
            
            # 标记为取消
            self._cancelled_tasks.add(task_id)
            
            # 更新内存中的任务状态
            if task_id in self._running_tasks:
                self._running_tasks[task_id]["status"] = TaskStatus.CANCELLED
                self._running_tasks[task_id]["completed_at"] = datetime.now()
                
                # 更新批次状态
                batch_id = self._running_tasks[task_id]["batch_id"]
                self.aggregation_repo.update(batch_id, {
                    "calculation_status": CalculationStatus.FAILED
                })
            
            # 更新数据库中的任务
            self.task_repo.update(task_id, {
                "status": TaskStatus.CANCELLED,
                "completed_at": datetime.now()
            })
            
            # 更新系统统计
            self._system_stats["cancelled_tasks"] += 1
            if self._system_stats["running_tasks"] > 0:
                self._system_stats["running_tasks"] -= 1
            
            logger.info(f"Cancelled task {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error cancelling task {task_id}: {str(e)}")
            return False
    
    async def get_task_status(self, task_id: str) -> Optional[TaskResponse]:
        """获取任务状态"""
        try:
            # 先从内存缓存查找
            if task_id in self._running_tasks:
                return self._convert_to_task_response(self._running_tasks[task_id])
            
            # 从数据库查找
            db_task = self.task_repo.get_by_id(task_id)
            if db_task:
                return TaskResponse(
                    id=db_task.id,
                    batch_id=db_task.batch_id,
                    status=db_task.status,
                    progress=db_task.progress or 0.0,
                    started_at=db_task.started_at,
                    completed_at=db_task.completed_at,
                    error_message=db_task.error_message
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting task status {task_id}: {str(e)}")
            return None
    
    async def get_task_progress(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务进度详情"""
        try:
            if task_id in self._task_progress:
                progress_info = self._task_progress[task_id].copy()
                progress_info["task_id"] = task_id
                return progress_info
            
            # 从数据库获取基本进度信息
            db_task = self.task_repo.get_by_id(task_id)
            if db_task:
                return {
                    "task_id": task_id,
                    "overall_progress": db_task.progress or 0.0,
                    "stage_details": [
                        {"stage": "data_loading", "status": "completed" if db_task.progress > 0 else "pending", "progress": min(db_task.progress * 3, 100.0)},
                        {"stage": "statistical_calculation", "status": "completed" if db_task.progress > 33 else ("processing" if db_task.progress > 0 else "pending"), "progress": max(0, min((db_task.progress - 33) * 3, 100.0))},
                        {"stage": "result_aggregation", "status": "completed" if db_task.progress > 66 else ("processing" if db_task.progress > 33 else "pending"), "progress": max(0, (db_task.progress - 66) * 3)}
                    ],
                    "last_updated": db_task.updated_at or db_task.started_at
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting task progress {task_id}: {str(e)}")
            return None
    
    async def batch_start_tasks(
        self,
        batch_codes: List[str],
        priority: int = TaskPriority.NORMAL,
        background_tasks: BackgroundTasks = None
    ) -> List[TaskResponse]:
        """批量启动任务"""
        tasks = []
        errors = []
        
        for batch_code in batch_codes:
            try:
                task = await self.start_calculation_task(
                    batch_code=batch_code,
                    priority=priority,
                    background_tasks=background_tasks
                )
                tasks.append(task)
            except Exception as e:
                logger.error(f"Error starting task for batch {batch_code}: {str(e)}")
                errors.append({"batch_code": batch_code, "error": str(e)})
        
        if errors:
            logger.warning(f"Failed to start {len(errors)} tasks out of {len(batch_codes)}")
        
        return tasks
    
    async def batch_cancel_tasks(self, task_ids: List[str]) -> int:
        """批量取消任务"""
        cancelled_count = 0
        
        for task_id in task_ids:
            try:
                if await self.cancel_task(task_id):
                    cancelled_count += 1
            except Exception as e:
                logger.error(f"Error cancelling task {task_id}: {str(e)}")
        
        logger.info(f"Batch cancelled {cancelled_count} out of {len(task_ids)} tasks")
        return cancelled_count
    
    async def batch_delete_tasks(self, task_ids: List[str], force: bool = False) -> int:
        """批量删除任务"""
        deleted_count = 0
        
        for task_id in task_ids:
            try:
                # 检查任务状态
                task_status = await self.get_task_status(task_id)
                if not task_status:
                    continue
                
                # 只能删除已完成、失败或取消的任务，除非强制删除
                if not force and task_status.status == TaskStatus.RUNNING:
                    continue
                
                # 如果是运行中的任务且要强制删除，先取消
                if task_status.status == TaskStatus.RUNNING:
                    await self.cancel_task(task_id)
                
                # 从缓存中删除
                if task_id in self._running_tasks:
                    del self._running_tasks[task_id]
                if task_id in self._task_progress:
                    del self._task_progress[task_id]
                
                # 从数据库删除
                if self.task_repo.delete(task_id):
                    deleted_count += 1
                    
            except Exception as e:
                logger.error(f"Error deleting task {task_id}: {str(e)}")
        
        logger.info(f"Batch deleted {deleted_count} out of {len(task_ids)} tasks (force={force})")
        return deleted_count
    
    async def list_tasks(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[TaskResponse]:
        """查询任务列表"""
        try:
            # 从数据库获取任务列表
            db_tasks = self.task_repo.get_paginated(
                filters=filters or {},
                limit=limit,
                offset=offset,
                order_by="started_at",
                order_direction="desc"
            )
            
            tasks = []
            for db_task in db_tasks:
                # 如果任务在内存中，使用内存中的最新状态
                if db_task.id in self._running_tasks:
                    tasks.append(self._convert_to_task_response(self._running_tasks[db_task.id]))
                else:
                    tasks.append(TaskResponse(
                        id=db_task.id,
                        batch_id=db_task.batch_id,
                        status=db_task.status,
                        progress=db_task.progress or 0.0,
                        started_at=db_task.started_at,
                        completed_at=db_task.completed_at,
                        error_message=db_task.error_message
                    ))
            
            return tasks
            
        except Exception as e:
            logger.error(f"Error listing tasks: {str(e)}")
            return []
    
    async def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            # 更新运行中任务数量
            running_count = len([t for t in self._running_tasks.values() if t["status"] == TaskStatus.RUNNING])
            self._system_stats["running_tasks"] = running_count
            
            # 计算系统运行时间
            uptime = datetime.now() - self._system_stats["system_start_time"]
            
            return {
                "system_status": "healthy",
                "uptime_seconds": uptime.total_seconds(),
                "memory_tasks": len(self._running_tasks),
                "cached_progress": len(self._task_progress),
                "statistics": self._system_stats.copy(),
                "last_updated": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting system status: {str(e)}")
            return {"system_status": "error", "error": str(e)}
    
    async def _execute_calculation_task(self, task_id: str) -> None:
        """执行计算任务（异步版本）"""
        try:
            task_info = self._running_tasks.get(task_id)
            if not task_info:
                logger.error(f"Task {task_id} not found in running tasks")
                return
            
            # 检查是否被取消
            if task_id in self._cancelled_tasks:
                logger.info(f"Task {task_id} was cancelled before execution")
                return
            
            # 更新任务状态为运行中
            task_info["status"] = TaskStatus.RUNNING
            self._update_task_progress(task_id, 0, "data_loading", "processing")
            
            # 获取批次信息
            batch = self.aggregation_repo.get_by_id(task_info["batch_id"])
            if not batch:
                await self._complete_task_with_error(task_id, "批次数据不存在")
                return
            
            # 第一阶段：数据加载
            logger.info(f"Task {task_id}: Starting data loading")
            await self._simulate_progress(task_id, 0, 33, "data_loading")
            
            if task_id in self._cancelled_tasks:
                return
            
            # 第二阶段：统计计算
            logger.info(f"Task {task_id}: Starting statistical calculation")
            start_time = datetime.now()
            
            try:
                # 调用计算服务，传入进度回调
                def progress_callback(progress: float, message: str):
                    # 将计算进度映射到 33-90% 的任务进度范围
                    task_progress = 33 + (progress / 100.0) * 57  # 33% + 57% = 90%
                    self._update_task_progress(task_id, task_progress, "statistical_calculation", "processing")
                    logger.debug(f"Task {task_id}: {message} ({task_progress:.1f}%)")
                
                if batch.aggregation_level == AggregationLevel.REGIONAL:
                    # 区域级计算（增强版本，自动生成学校数据）
                    result = await self.calculation_service.calculate_batch_statistics(
                        batch_code=batch.batch_code,
                        config=None,
                        progress_callback=progress_callback
                    )
                else:
                    # 学校级计算
                    result = await self.calculation_service.calculate_school_statistics(
                        batch_code=batch.batch_code,
                        school_id=batch.school_id
                    )
                    # 学校级计算不需要进度回调，直接更新到 90%
                    self._update_task_progress(task_id, 90, "statistical_calculation", "completed")
                
            except Exception as calc_error:
                await self._complete_task_with_error(task_id, f"计算失败: {str(calc_error)}")
                return
            
            if task_id in self._cancelled_tasks:
                return
            
            # 第三阶段：结果汇聚
            logger.info(f"Task {task_id}: Starting result aggregation")
            
            # 更新批次统计数据
            calculation_duration = (datetime.now() - start_time).total_seconds()
            
            # 根据计算类型更新不同的数据结构
            if batch.aggregation_level == AggregationLevel.REGIONAL:
                # 区域级计算返回的是增强结果，包含区域和学校数据
                statistics_data = result.get('regional_statistics', result)
                logger.info(f"Task {task_id}: 区域级计算完成，同时生成了 {result.get('school_statistics_summary', {}).get('successful_schools', 0)} 个学校数据")
            else:
                # 学校级计算返回的是单个学校数据
                statistics_data = result.get('statistics', result)
            
            self.aggregation_repo.update(batch.id, {
                "statistics_data": statistics_data,
                "calculation_status": CalculationStatus.COMPLETED,
                "calculation_duration": calculation_duration
            })
            
            await self._simulate_progress(task_id, 90, 100, "result_aggregation")
            
            # 完成任务
            await self._complete_task_successfully(task_id)
            
        except Exception as e:
            logger.error(f"Error executing task {task_id}: {str(e)}")
            await self._complete_task_with_error(task_id, str(e))
    
    def _execute_calculation_task_sync(self, task_id: str) -> None:
        """执行计算任务（同步版本，用于线程）"""
        asyncio.run(self._execute_calculation_task(task_id))
    
    def _update_task_progress(self, task_id: str, progress: float, stage: str, status: str) -> None:
        """更新任务进度"""
        if task_id not in self._task_progress:
            return
        
        # 更新整体进度
        self._task_progress[task_id]["overall_progress"] = progress
        self._task_progress[task_id]["last_updated"] = datetime.now()
        
        # 更新阶段状态
        for stage_info in self._task_progress[task_id]["stage_details"]:
            if stage_info["stage"] == stage:
                stage_info["status"] = status
                stage_info["progress"] = min(progress * 3, 100.0) if status == "processing" else (100.0 if status == "completed" else 0.0)
                break
        
        # 更新内存中的任务进度
        if task_id in self._running_tasks:
            self._running_tasks[task_id]["progress"] = progress
        
        # 更新数据库中的任务进度
        try:
            self.task_repo.update(task_id, {"progress": progress})
        except Exception as e:
            logger.error(f"Error updating task progress in database: {str(e)}")
    
    async def _simulate_progress(self, task_id: str, start_progress: float, end_progress: float, stage: str) -> None:
        """模拟进度更新"""
        steps = 10
        step_size = (end_progress - start_progress) / steps
        
        for i in range(steps + 1):
            if task_id in self._cancelled_tasks:
                return
            
            progress = start_progress + (step_size * i)
            status = "completed" if i == steps else "processing"
            self._update_task_progress(task_id, progress, stage, status)
            
            if i < steps:  # 最后一步不需要等待
                await asyncio.sleep(0.5)  # 模拟处理时间
    
    async def _complete_task_successfully(self, task_id: str) -> None:
        """成功完成任务"""
        if task_id not in self._running_tasks:
            return
        
        # 更新任务状态
        self._running_tasks[task_id]["status"] = TaskStatus.COMPLETED
        self._running_tasks[task_id]["progress"] = 100.0
        self._running_tasks[task_id]["completed_at"] = datetime.now()
        
        # 更新数据库
        self.task_repo.update(task_id, {
            "status": TaskStatus.COMPLETED,
            "progress": 100.0,
            "completed_at": datetime.now()
        })
        
        # 清理缓存（可选，保留一段时间用于查询）
        # 更新系统统计
        self._system_stats["completed_tasks"] += 1
        if self._system_stats["running_tasks"] > 0:
            self._system_stats["running_tasks"] -= 1
        
        logger.info(f"Task {task_id} completed successfully")
    
    async def _complete_task_with_error(self, task_id: str, error_message: str) -> None:
        """任务执行失败"""
        if task_id not in self._running_tasks:
            return
        
        # 更新任务状态
        self._running_tasks[task_id]["status"] = TaskStatus.FAILED
        self._running_tasks[task_id]["error_message"] = error_message
        self._running_tasks[task_id]["completed_at"] = datetime.now()
        
        # 更新批次状态
        batch_id = self._running_tasks[task_id]["batch_id"]
        self.aggregation_repo.update(batch_id, {
            "calculation_status": CalculationStatus.FAILED
        })
        
        # 更新数据库
        self.task_repo.update(task_id, {
            "status": TaskStatus.FAILED,
            "error_message": error_message,
            "completed_at": datetime.now()
        })
        
        # 更新系统统计
        self._system_stats["failed_tasks"] += 1
        if self._system_stats["running_tasks"] > 0:
            self._system_stats["running_tasks"] -= 1
        
        logger.error(f"Task {task_id} failed: {error_message}")
    
    def _find_running_task_by_batch(self, batch_code: str, school_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """根据批次查找正在运行的任务"""
        for task_id, task_info in self._running_tasks.items():
            if (task_info["batch_code"] == batch_code and 
                task_info.get("school_id") == school_id and
                task_info["status"] in [TaskStatus.PENDING, TaskStatus.RUNNING]):
                return task_info
        return None
    
    def _convert_to_task_response(self, task_info: Dict[str, Any]) -> TaskResponse:
        """转换为任务响应模型"""
        return TaskResponse(
            id=task_info["id"],
            batch_id=task_info["batch_id"],
            status=task_info["status"],
            progress=task_info["progress"],
            started_at=task_info["started_at"],
            completed_at=task_info.get("completed_at"),
            error_message=task_info.get("error_message")
        )
