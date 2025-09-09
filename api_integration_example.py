#!/usr/bin/env python3
"""
API集成示例
展示如何将新实现的简化汇聚服务集成到现有的API中
"""

import asyncio
import logging
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional, List

from app.database.connection import get_db_session
from app.services.simplified_aggregation_service import SimplifiedAggregationService
from app.repositories.simplified_aggregation_repository import SimplifiedAggregationRepository
from app.database.models import AggregationLevel, CalculationStatus

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建FastAPI应用
app = FastAPI(title="简化汇聚服务API示例", version="1.0.0")

# 请求/响应模型
class AggregationRequest(BaseModel):
    """汇聚请求模型"""
    batch_code: str
    school_id: Optional[str] = None
    school_name: Optional[str] = None

class BatchAggregationRequest(BaseModel):
    """批量汇聚请求模型"""
    batch_codes: List[str]

class AggregationResponse(BaseModel):
    """汇聚响应模型"""
    success: bool
    message: str
    data: Optional[dict] = None
    duration: Optional[float] = None

# API端点
@app.post("/api/aggregation/regional", response_model=AggregationResponse)
async def aggregate_regional(
    request: AggregationRequest,
    db: Session = Depends(get_db_session)
):
    """区域级汇聚API"""
    logger.info(f"开始区域级汇聚：{request.batch_code}")
    
    try:
        service = SimplifiedAggregationService(db)
        repository = SimplifiedAggregationRepository(db)
        
        # 执行区域级汇聚
        result = service.aggregate_batch_regional(request.batch_code)
        
        if result['success']:
            # 保存结果到数据库
            save_result = repository.save_aggregation_data(
                batch_code=request.batch_code,
                aggregation_level=AggregationLevel.REGIONAL,
                data=result['data'],
                calculation_duration=result['duration']
            )
            
            return AggregationResponse(
                success=True,
                message=f"区域级汇聚完成，处理了{result['subjects_count']}个科目",
                data={
                    "batch_code": request.batch_code,
                    "subjects_count": result['subjects_count'],
                    "total_schools": result['total_schools'],
                    "total_students": result['total_students'],
                    "record_id": save_result['record_id']
                },
                duration=result['duration']
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"区域级汇聚失败：{result['error']}"
            )
            
    except Exception as e:
        logger.error(f"区域级汇聚API错误：{str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/aggregation/school", response_model=AggregationResponse)
async def aggregate_school(
    request: AggregationRequest,
    db: Session = Depends(get_db_session)
):
    """学校级汇聚API"""
    if not request.school_id or not request.school_name:
        raise HTTPException(
            status_code=400,
            detail="学校级汇聚需要提供school_id和school_name"
        )
    
    logger.info(f"开始学校级汇聚：{request.batch_code} - {request.school_name}")
    
    try:
        service = SimplifiedAggregationService(db)
        repository = SimplifiedAggregationRepository(db)
        
        # 执行学校级汇聚
        result = service.aggregate_batch_school(
            request.batch_code,
            request.school_id,
            request.school_name
        )
        
        if result['success']:
            # 保存结果到数据库
            save_result = repository.save_aggregation_data(
                batch_code=request.batch_code,
                aggregation_level=AggregationLevel.SCHOOL,
                data=result['data'],
                school_id=request.school_id,
                school_name=request.school_name,
                calculation_duration=result['duration']
            )
            
            return AggregationResponse(
                success=True,
                message=f"学校级汇聚完成，处理了{result['subjects_count']}个科目",
                data={
                    "batch_code": request.batch_code,
                    "school_id": request.school_id,
                    "school_name": request.school_name,
                    "subjects_count": result['subjects_count'],
                    "total_students": result['total_students'],
                    "record_id": save_result['record_id']
                },
                duration=result['duration']
            )
        else:
            raise HTTPException(
                status_code=500,
                detail=f"学校级汇聚失败：{result['error']}"
            )
            
    except Exception as e:
        logger.error(f"学校级汇聚API错误：{str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/aggregation/batch", response_model=AggregationResponse)
async def aggregate_batch(
    request: BatchAggregationRequest,
    db: Session = Depends(get_db_session)
):
    """批量汇聚API"""
    logger.info(f"开始批量汇聚：{len(request.batch_codes)} 个批次")
    
    try:
        service = SimplifiedAggregationService(db)
        
        # 执行批量汇聚
        result = service.aggregate_all_batches(request.batch_codes)
        
        return AggregationResponse(
            success=True,
            message=f"批量汇聚完成，成功：{result['success_count']}，失败：{result['failed_count']}",
            data={
                "batch_codes": request.batch_codes,
                "success_count": result['success_count'],
                "failed_count": result['failed_count'],
                "errors": result['errors'][:10]  # 最多返回10个错误信息
            },
            duration=result['duration']
        )
            
    except Exception as e:
        logger.error(f"批量汇聚API错误：{str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/aggregation/{batch_code}/status")
async def get_batch_status(
    batch_code: str,
    db: Session = Depends(get_db_session)
):
    """获取批次汇聚状态"""
    try:
        repository = SimplifiedAggregationRepository(db)
        status_info = repository.get_batch_aggregation_status(batch_code)
        
        return JSONResponse(content=status_info)
        
    except Exception as e:
        logger.error(f"获取批次状态API错误：{str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/aggregation/{batch_code}/regional")
async def get_regional_data(
    batch_code: str,
    db: Session = Depends(get_db_session)
):
    """获取区域级汇聚数据"""
    try:
        repository = SimplifiedAggregationRepository(db)
        data = repository.get_aggregation_data(
            batch_code, AggregationLevel.REGIONAL
        )
        
        if data:
            return JSONResponse(content=data)
        else:
            raise HTTPException(
                status_code=404,
                detail=f"未找到批次 {batch_code} 的区域级汇聚数据"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取区域级数据API错误：{str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/aggregation/{batch_code}/school/{school_id}")
async def get_school_data(
    batch_code: str,
    school_id: str,
    db: Session = Depends(get_db_session)
):
    """获取学校级汇聚数据"""
    try:
        repository = SimplifiedAggregationRepository(db)
        data = repository.get_aggregation_data(
            batch_code, AggregationLevel.SCHOOL, school_id=school_id
        )
        
        if data:
            return JSONResponse(content=data)
        else:
            raise HTTPException(
                status_code=404,
                detail=f"未找到批次 {batch_code} 学校 {school_id} 的汇聚数据"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取学校级数据API错误：{str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/aggregation/{batch_code}")
async def delete_batch_data(
    batch_code: str,
    db: Session = Depends(get_db_session)
):
    """删除批次汇聚数据"""
    try:
        repository = SimplifiedAggregationRepository(db)
        result = repository.delete_batch_aggregations(batch_code)
        
        return JSONResponse(content=result)
        
    except Exception as e:
        logger.error(f"删除批次数据API错误：{str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/aggregation/recent")
async def get_recent_aggregations(
    limit: int = 20,
    level: Optional[str] = None,
    status: Optional[str] = None,
    db: Session = Depends(get_db_session)
):
    """获取最近的汇聚记录"""
    try:
        repository = SimplifiedAggregationRepository(db)
        
        # 转换参数
        aggregation_level = None
        if level:
            aggregation_level = AggregationLevel(level.upper())
        
        calc_status = None
        if status:
            calc_status = CalculationStatus(status.upper())
        
        records = repository.get_recent_aggregations(
            limit=limit,
            aggregation_level=aggregation_level,
            status=calc_status
        )
        
        return JSONResponse(content={
            "records": records,
            "total": len(records)
        })
        
    except Exception as e:
        logger.error(f"获取最近记录API错误：{str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# 主函数用于演示
if __name__ == "__main__":
    import uvicorn
    
    logger.info("🚀 启动简化汇聚服务API示例")
    logger.info("")
    logger.info("📝 可用端点:")
    logger.info("  POST /api/aggregation/regional - 区域级汇聚")
    logger.info("  POST /api/aggregation/school - 学校级汇聚") 
    logger.info("  POST /api/aggregation/batch - 批量汇聚")
    logger.info("  GET  /api/aggregation/{batch_code}/status - 批次状态")
    logger.info("  GET  /api/aggregation/{batch_code}/regional - 区域级数据")
    logger.info("  GET  /api/aggregation/{batch_code}/school/{school_id} - 学校级数据")
    logger.info("  DELETE /api/aggregation/{batch_code} - 删除批次数据")
    logger.info("  GET  /api/aggregation/recent - 最近记录")
    logger.info("")
    logger.info("💡 使用示例:")
    logger.info("  curl -X POST http://localhost:8000/api/aggregation/regional \\")
    logger.info("       -H 'Content-Type: application/json' \\")
    logger.info("       -d '{\"batch_code\": \"G7-2025\"}'")
    logger.info("")
    
    uvicorn.run(
        "api_integration_example:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )