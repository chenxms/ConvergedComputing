from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session

from app.schemas.response_schemas import RegionReportResponse, SchoolReportResponse
from app.schemas.json_schemas import RegionalReportResponse, SchoolReportResponse as SchoolJSONResponse
from app.services.reporting_service import ReportingService
from app.services.serialization import StatisticsJsonSerializer
from app.database.connection import get_db

router = APIRouter(tags=["报告统计API"])

# 新的JSON格式API
@router.get("/api/v1/reports/regional/{batch_code}", response_model=RegionalReportResponse)
async def get_regional_report(
    batch_code: str,
    force_recalculate: bool = Query(False, description="是否强制重新计算"),
    validate_output: bool = Query(True, description="是否验证输出格式"),
    db: Session = Depends(get_db)
):
    """获取区域统计报告（新JSON格式）"""
    try:
        serializer = StatisticsJsonSerializer(db)
        statistics_data = await serializer.serialize_regional_data(
            batch_code, 
            force_recalculate=force_recalculate,
            validate_output=validate_output
        )
        
        return RegionalReportResponse(
            code=200,
            message="success",
            data={
                "batch_code": batch_code,
                "statistics": statistics_data
            },
            timestamp=datetime.utcnow().isoformat() + "Z"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取区域报告失败: {str(e)}")

# 兼容旧API
@router.get("/regions/{region_id}/report", response_model=RegionReportResponse)
async def get_region_report(
    region_id: int,
    batch_id: Optional[int] = Query(None, description="批次ID，不指定则使用最新批次"),
    subject_id: Optional[int] = Query(None, description="科目ID，不指定则返回所有科目")
):
    """获取区域统计报告（旧格式，保持兼容）"""
    try:
        reporting_service = ReportingService()
        report = await reporting_service.get_region_report(region_id, batch_id, subject_id)
        return report
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

# 新的JSON格式API
@router.get("/api/v1/reports/school/{batch_code}/{school_id}", response_model=SchoolJSONResponse)
async def get_school_report_json(
    batch_code: str,
    school_id: str,
    include_regional_comparison: bool = Query(True, description="是否包含区域对比数据"),
    force_recalculate: bool = Query(False, description="是否强制重新计算"),
    validate_output: bool = Query(True, description="是否验证输出格式"),
    db: Session = Depends(get_db)
):
    """获取学校统计报告（新JSON格式）"""
    try:
        serializer = StatisticsJsonSerializer(db)
        statistics_data = await serializer.serialize_school_data(
            batch_code,
            school_id,
            include_regional_comparison=include_regional_comparison,
            force_recalculate=force_recalculate,
            validate_output=validate_output
        )
        
        return SchoolJSONResponse(
            code=200,
            message="success",
            data={
                "batch_code": batch_code,
                "school_id": school_id,
                "statistics": statistics_data
            },
            timestamp=datetime.utcnow().isoformat() + "Z"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取学校报告失败: {str(e)}")

# 兼容旧API
@router.get("/schools/{school_id}/report", response_model=SchoolReportResponse)
async def get_school_report(
    school_id: int,
    batch_id: Optional[int] = Query(None, description="批次ID，不指定则使用最新批次"),
    subject_id: Optional[int] = Query(None, description="科目ID，不指定则返回所有科目")
):
    """获取学校统计报告（旧格式，保持兼容）"""
    try:
        reporting_service = ReportingService()
        report = await reporting_service.get_school_report(school_id, batch_id, subject_id)
        return report
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

# 新增雷达图数据API
@router.get("/api/v1/reports/radar-chart/{batch_code}")
async def get_radar_chart_data(
    batch_code: str,
    school_id: Optional[str] = Query(None, description="学校ID，不指定则返回区域级数据"),
    db: Session = Depends(get_db)
):
    """获取雷达图专用数据"""
    try:
        serializer = StatisticsJsonSerializer(db)
        radar_data = await serializer.get_radar_chart_data(batch_code, school_id)
        
        return {
            "code": 200,
            "message": "success",
            "data": {
                "batch_code": batch_code,
                "school_id": school_id,
                "radar_chart_data": radar_data
            },
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取雷达图数据失败: {str(e)}")

# 新增批次所有学校数据API
@router.get("/api/v1/reports/batch/{batch_code}/all-schools")
async def get_all_schools_reports(
    batch_code: str,
    parallel_processing: bool = Query(True, description="是否并行处理"),
    validate_consistency: bool = Query(True, description="是否验证数据一致性"),
    db: Session = Depends(get_db)
):
    """获取批次中所有学校的统计报告"""
    try:
        serializer = StatisticsJsonSerializer(db)
        all_data = await serializer.serialize_all_schools_data(
            batch_code,
            parallel_processing=parallel_processing,
            validate_consistency=validate_consistency
        )
        
        return {
            "code": 200,
            "message": "success",
            "data": all_data,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取批次所有学校数据失败: {str(e)}")

# 数据验证API
@router.post("/api/v1/reports/validate")
async def validate_json_data(
    data_type: str = Query(..., regex="^(regional|school)$", description="数据类型"),
    json_data: Dict[str, Any] = None,
    db: Session = Depends(get_db)
):
    """验证JSON数据格式"""
    try:
        if not json_data:
            raise HTTPException(status_code=400, detail="缺少JSON数据")
        
        serializer = StatisticsJsonSerializer(db)
        validation_result = await serializer.validate_json_data(json_data, data_type)
        
        return {
            "code": 200,
            "message": "验证完成",
            "data": validation_result.to_dict(),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据验证失败: {str(e)}")

@router.get("/batches")
async def list_batches():
    """获取批次列表（兼容接口）"""
    try:
        reporting_service = ReportingService()
        batches = await reporting_service.list_batches()
        return batches
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))