#!/usr/bin/env python3
"""
增强汇聚 API v1.2
提供符合汇聚模块修复实施方案v1.2的API接口
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, Any, Optional
import logging
import datetime
from enhanced_aggregation_engine import EnhancedAggregationEngine, AggregationLevel
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1.2/aggregation", tags=["aggregation-v1.2"])

# 全局汇聚引擎实例
_aggregation_engine = None

def get_aggregation_engine() -> EnhancedAggregationEngine:
    """获取汇聚引擎实例"""
    global _aggregation_engine
    if _aggregation_engine is None:
        _aggregation_engine = EnhancedAggregationEngine()
    return _aggregation_engine


class AggregationRequest(BaseModel):
    """汇聚请求模型"""
    batch_code: str
    school_code: Optional[str] = None


class AggregationResponse(BaseModel):
    """汇聚响应模型"""
    aggregation_level: str
    batch_code: str
    school_code: Optional[str] = None
    subjects: list
    generated_at: str
    metadata: dict
    schema_version: str = "v1.2"


@router.post("/regional", response_model=Dict[str, Any])
async def aggregate_regional_level(
    request: AggregationRequest,
    engine: EnhancedAggregationEngine = Depends(get_aggregation_engine)
) -> Dict[str, Any]:
    """
    区域层级汇聚 (REGIONAL)
    
    返回的数据结构符合v1.2规范：
    - 所有数值保留两位小数
    - 百分比字段输出0-100的数值
    - subjects数组包含考试和问卷科目
    - 每个科目包含school_rankings
    - 每个维度包含rank字段
    """
    try:
        logger.info(f"开始区域层级汇聚: batch_code={request.batch_code}")
        
        result = engine.aggregate_regional_level(request.batch_code)
        
        if 'error' in result:
            raise HTTPException(status_code=400, detail=result['error'])
        
        logger.info(f"区域层级汇聚完成: 科目数={len(result.get('subjects', []))}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"区域层级汇聚异常: {str(e)}")
        raise HTTPException(status_code=500, detail=f"区域层级汇聚失败: {str(e)}")


@router.post("/school", response_model=Dict[str, Any])
async def aggregate_school_level(
    request: AggregationRequest,
    engine: EnhancedAggregationEngine = Depends(get_aggregation_engine)
) -> Dict[str, Any]:
    """
    学校层级汇聚 (SCHOOL)
    
    返回的数据结构符合v1.2规范：
    - 所有数值保留两位小数
    - 百分比字段输出0-100的数值
    - 每个科目包含region_rank和total_schools
    - 每个维度包含rank字段
    """
    try:
        if not request.school_code:
            raise HTTPException(status_code=400, detail="学校层级汇聚必须提供school_code")
        
        logger.info(f"开始学校层级汇聚: batch_code={request.batch_code}, school_code={request.school_code}")
        
        result = engine.aggregate_school_level(request.batch_code, request.school_code)
        
        if 'error' in result:
            raise HTTPException(status_code=400, detail=result['error'])
        
        logger.info(f"学校层级汇聚完成: 科目数={len(result.get('subjects', []))}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"学校层级汇聚异常: {str(e)}")
        raise HTTPException(status_code=500, detail=f"学校层级汇聚失败: {str(e)}")


@router.get("/batch/{batch_code}/regional")
async def get_regional_aggregation(
    batch_code: str,
    engine: EnhancedAggregationEngine = Depends(get_aggregation_engine)
) -> Dict[str, Any]:
    """
    GET方式获取区域层级汇聚
    """
    try:
        logger.info(f"GET请求区域层级汇聚: batch_code={batch_code}")
        
        result = engine.aggregate_regional_level(batch_code)
        
        if 'error' in result:
            raise HTTPException(status_code=400, detail=result['error'])
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GET区域层级汇聚异常: {str(e)}")
        raise HTTPException(status_code=500, detail=f"区域层级汇聚失败: {str(e)}")


@router.get("/batch/{batch_code}/school/{school_code}")
async def get_school_aggregation(
    batch_code: str,
    school_code: str,
    engine: EnhancedAggregationEngine = Depends(get_aggregation_engine)
) -> Dict[str, Any]:
    """
    GET方式获取学校层级汇聚
    """
    try:
        logger.info(f"GET请求学校层级汇聚: batch_code={batch_code}, school_code={school_code}")
        
        result = engine.aggregate_school_level(batch_code, school_code)
        
        if 'error' in result:
            raise HTTPException(status_code=400, detail=result['error'])
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"GET学校层级汇聚异常: {str(e)}")
        raise HTTPException(status_code=500, detail=f"学校层级汇聚失败: {str(e)}")


@router.get("/batch/{batch_code}/metadata")
async def get_batch_metadata(
    batch_code: str,
    engine: EnhancedAggregationEngine = Depends(get_aggregation_engine)
) -> Dict[str, Any]:
    """
    获取批次元数据信息
    """
    try:
        session = engine.get_session()
        try:
            metadata = engine._get_metadata(session, batch_code)
            return {
                'batch_code': batch_code,
                'metadata': metadata,
                'generated_at': datetime.datetime.now().isoformat()
            }
        finally:
            engine.close_session(session)
            
    except Exception as e:
        logger.error(f"获取批次元数据异常: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取批次元数据失败: {str(e)}")


@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    健康检查接口
    """
    return {
        "status": "healthy",
        "version": "v1.2",
        "description": "增强汇聚引擎 v1.2 - 符合修复实施方案要求",
        "features": [
            "精度统一处理（两位小数）",
            "百分比字段输出0-100数值",
            "科目层排名功能",
            "维度层排名功能",
            "问卷数据重构",
            "数据结构统一"
        ]
    }


@router.get("/schema")
async def get_schema_info() -> Dict[str, Any]:
    """
    获取数据结构规范信息
    """
    return {
        "schema_version": "v1.2",
        "data_structure": {
            "regional_level": {
                "aggregation_level": "REGIONAL",
                "batch_code": "string",
                "subjects": [
                    {
                        "name": "string",
                        "type": "exam|questionnaire|interaction",
                        "student_count": "int",
                        "avg_score": "float(2)",
                        "avg_score_rate_pct": "float(2) [0-100]",
                        "school_rankings": [
                            {
                                "school_code": "string",
                                "school_name": "string",
                                "avg_score": "float(2)",
                                "rank": "int"
                            }
                        ],
                        "dimensions": [
                            {
                                "code": "string",
                                "name": "string",
                                "avg_score": "float(2)",
                                "avg_score_rate_pct": "float(2) [0-100]"
                            }
                        ]
                    }
                ]
            },
            "school_level": {
                "aggregation_level": "SCHOOL",
                "batch_code": "string",
                "school_code": "string",
                "subjects": [
                    {
                        "name": "string",
                        "type": "exam|questionnaire|interaction",
                        "region_rank": "int",
                        "total_schools": "int",
                        "dimensions": [
                            {
                                "code": "string",
                                "name": "string",
                                "rank": "int"
                            }
                        ]
                    }
                ]
            }
        },
        "precision_rules": {
            "decimal_places": 2,
            "percentage_fields": "0-100 range",
            "ranking_method": "DENSE_RANK"
        }
    }


if __name__ == "__main__":
    # 简单的测试
    import asyncio
    import uvicorn
    from fastapi import FastAPI
    
    app = FastAPI(title="增强汇聚 API v1.2", version="1.2.0")
    app.include_router(router)
    
    # 测试函数
    async def test_api():
        engine = get_aggregation_engine()
        
        print("=== API功能测试 ===")
        
        # 测试区域层级汇聚
        try:
            result = await get_regional_aggregation("G4-2025", engine)
            print(f"区域层级汇聚成功: {len(result.get('subjects', []))}个科目")
        except Exception as e:
            print(f"区域层级汇聚失败: {e}")
        
        # 获取一个学校代码进行测试
        session = engine.get_session()
        try:
            from sqlalchemy import text
            result = session.execute(text("SELECT DISTINCT school_code FROM student_cleaned_scores WHERE batch_code = 'G4-2025' LIMIT 1"))
            row = result.fetchone()
            test_school_code = row[0] if row else None
        finally:
            engine.close_session(session)
        
        if test_school_code:
            try:
                result = await get_school_aggregation("G4-2025", test_school_code, engine)
                print(f"学校层级汇聚成功: 学校{test_school_code}, {len(result.get('subjects', []))}个科目")
            except Exception as e:
                print(f"学校层级汇聚失败: {e}")
        
        print("=== API测试完成 ===")
    
    # 运行API测试
    asyncio.run(test_api())
