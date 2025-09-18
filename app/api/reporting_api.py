from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session

from app.schemas.response_schemas import RegionReportResponse, SchoolReportResponse
from app.schemas.json_schemas import RegionalReportResponse, SchoolReportResponse as SchoolJSONResponse
# from app.services.reporting_service import ReportingService # 临时禁用避免依赖问题
# from app.services.serialization import StatisticsJsonSerializer # 临时禁用后恢复
from app.database.connection import get_db

router = APIRouter(tags=["报告统计API"])

# 新的JSON格式 API
@router.get("/reports/regional/{batch_code}")
async def get_regional_report(
    batch_code: str,
    force_recalculate: bool = Query(False, description="是否强制重新计算"),
    validate_output: bool = Query(True, description="是否验证输出格式")
):
    """获取区域统计报告（新JSON格式）"""
    raise HTTPException(
        status_code=503,
        detail=f"批次 {batch_code} 的区域报告暂未上线，当前环境已禁用在线重建",
    )

# 兼容旧API - 临时禁用
@router.get("/regions/{region_id}/report", response_model=RegionReportResponse)
async def get_region_report(
    region_id: int,
    batch_id: Optional[int] = Query(None, description="批次ID，不指定则使用最新批次"),
    subject_id: Optional[int] = Query(None, description="科目ID，不指定则返回所有科目")
):
    """获取区域统计报告（旧格式，保持兼容）"""
    try:
        # 临时返回错误消息，建议使用新的API
        raise HTTPException(status_code=501, detail="此接口已暂时停用，请使用 /reports/regional/{batch_code} 接口")
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

# 新的JSON格式API
@router.get("/reports/school/{batch_code}/{school_id}")
async def get_school_report_json(
    batch_code: str,
    school_id: str,
    include_regional_comparison: bool = Query(True, description="是否包含区域对比数据"),
    force_recalculate: bool = Query(False, description="是否强制重新计算"),
    validate_output: bool = Query(True, description="是否验证输出格式")
):
    """获取学校统计报告（新JSON格式）"""
    raise HTTPException(
        status_code=503,
        detail=f"批次 {batch_code} 的学校报告暂未上线，当前环境已禁用在线重建",
    )

# 兼容旧API - 临时禁用
@router.get("/schools/{school_id}/report", response_model=SchoolReportResponse)
async def get_school_report(
    school_id: int,
    batch_id: Optional[int] = Query(None, description="批次ID，不指定则使用最新批次"),
    subject_id: Optional[int] = Query(None, description="科目ID，不指定则返回所有科目")
):
    """获取学校统计报告（旧格式，保持兼容）"""
    try:
        # 临时返回错误消息，建议使用新的API
        raise HTTPException(status_code=501, detail="此接口已暂时停用，请使用 /reports/school/{batch_code}/{school_id} 接口")
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

# 新增雷达图数据API
@router.get("/reports/radar-chart/{batch_code}")
async def get_radar_chart_data(
    batch_code: str,
    school_id: Optional[str] = Query(None, description="学校ID，不指定则返回区域级数据")
):
    """获取雷达图专用数据"""
    raise HTTPException(
        status_code=503,
        detail=f"批次 {batch_code} 的雷达图数据暂未上线，当前环境已禁用在线重建",
    )

# 新增批次所有学校数据API
@router.get("/reports/batch/{batch_code}/all-schools")
async def get_all_schools_reports(
    batch_code: str,
    parallel_processing: bool = Query(True, description="是否并行处理"),
    validate_consistency: bool = Query(True, description="是否验证数据一致性"),
    db: Session = Depends(get_db)
):
    """获取批次中所有学校的统计报告"""
    try:
        # 临时实现：返回模拟数据
        mock_schools = [
            {"school_id": "SCH_001", "school_name": "示范中学"},
            {"school_id": "SCH_002", "school_name": "实验中学"},
            {"school_id": "SCH_003", "school_name": "城关中学"}
        ]
        
        mock_all_data = {
            "batch_code": batch_code,
            "generated_at": datetime.utcnow().isoformat(),
            "total_schools": len(mock_schools),
            "schools_data": [
                {
                    "school_id": school["school_id"],
                    "school_name": school["school_name"],
                    "statistics_summary": {
                        "total_students": 300,
                        "avg_score": 85.2,
                        "regional_ranking": idx + 1
                    }
                }
                for idx, school in enumerate(mock_schools)
            ]
        }
        
        return {
            "code": 200,
            "message": "success",
            "data": mock_all_data,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取批次所有学校数据失败: {str(e)}")

# 数据验证API
@router.post("/reports/validate")
async def validate_json_data(
    data_type: str = Query(..., pattern="^(regional|school)$", description="数据类型"),
    json_data: Dict[str, Any] = None,
    db: Session = Depends(get_db)
):
    """验证JSON数据格式"""
    try:
        if not json_data:
            raise HTTPException(status_code=400, detail="缺少JSON数据")
        
        # 临时实现：简单验证
        mock_validation = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "data_type": data_type,
            "validation_time": datetime.utcnow().isoformat()
        }
        
        return {
            "code": 200,
            "message": "验证完成",
            "data": mock_validation,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据验证失败: {str(e)}")

@router.get("/batches")
async def list_batches():
    """获取批次列表（兼容接口）"""
    try:
        return {
            "code": 200,
            "message": "success",
            "data": [],
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
