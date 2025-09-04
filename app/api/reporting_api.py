from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any

from app.schemas.response_schemas import RegionReportResponse, SchoolReportResponse
from app.services.reporting_service import ReportingService

router = APIRouter()

@router.get("/regions/{region_id}/report", response_model=RegionReportResponse)
async def get_region_report(
    region_id: int,
    batch_id: Optional[int] = Query(None, description="批次ID，不指定则使用最新批次"),
    subject_id: Optional[int] = Query(None, description="科目ID，不指定则返回所有科目")
):
    """获取区域统计报告"""
    try:
        reporting_service = ReportingService()
        report = await reporting_service.get_region_report(region_id, batch_id, subject_id)
        return report
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.get("/schools/{school_id}/report", response_model=SchoolReportResponse)
async def get_school_report(
    school_id: int,
    batch_id: Optional[int] = Query(None, description="批次ID，不指定则使用最新批次"),
    subject_id: Optional[int] = Query(None, description="科目ID，不指定则返回所有科目")
):
    """获取学校统计报告"""
    try:
        reporting_service = ReportingService()
        report = await reporting_service.get_school_report(school_id, batch_id, subject_id)
        return report
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.get("/batches")
async def list_batches():
    """获取批次列表"""
    try:
        reporting_service = ReportingService()
        batches = await reporting_service.list_batches()
        return batches
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))