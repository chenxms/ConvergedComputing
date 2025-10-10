from fastapi import APIRouter, HTTPException, Query
from typing import Any, Dict, Optional, List
import logging
from sqlalchemy.orm import Session

from app.database.connection import SessionLocal
from app.database.repositories import StatisticalAggregationRepository
from app.database.models import SchoolMasterData
from datetime import datetime

router = APIRouter()

logger = logging.getLogger(__name__)


def _fetch_v12_regional(db: Session, batch_code: str) -> Optional[Dict[str, Any]]:
    """仅从仓储读取区域级 v1.2 数据，不再触发重建或写入。"""
    repo = StatisticalAggregationRepository(db)
    regional = repo.get_regional_statistics(batch_code)
    if regional and isinstance(regional.statistics_data, dict):
        return regional.statistics_data

    logger.info("v1.2 区域级数据缺失，跳过自动重建: %s", batch_code)
    return None


def _fetch_v12_school(db: Session, batch_code: str, school_id: str) -> Optional[Dict[str, Any]]:
    """仅从仓储读取学校级 v1.2 数据，不再触发重建或写入。"""
    repo = StatisticalAggregationRepository(db)
    school = repo.get_school_statistics(batch_code, school_id)
    if school and isinstance(school.statistics_data, dict):
        return school.statistics_data

    logger.info("v1.2 学校级数据缺失，跳过自动重建: %s/%s", batch_code, school_id)
    return None


def _scaffold_regional_payload(batch_code: str) -> Dict[str, Any]:
    """Provide a minimal, schema-shaped regional payload so frontend can continue."""
    now = datetime.utcnow().isoformat() + "Z"
    return {
        "data_version": "1.0",
        "schema_version": "2025-09-04",
        "batch_info": {
            "batch_code": batch_code,
            "grade_level": "初中",
            "total_schools": 0,
            "total_students": 0,
            "calculation_time": now,
        },
        "academic_subjects": {},
        "non_academic_subjects": {},
        "radar_chart_data": {
            "academic_dimensions": [],
            "non_academic_dimensions": [],
        },
    }


def _resolve_school_name(db: Session, batch_code: str, school_id: str) -> Optional[str]:
    try:
        rec = (
            db.query(SchoolMasterData)
            .filter(
                SchoolMasterData.batch_code == batch_code,
                SchoolMasterData.school_id == school_id,
                SchoolMasterData.status == "ACTIVE",
            )
            .first()
        )
        return rec.standard_school_name if rec else None
    except Exception:
        return None


def _scaffold_school_payload(batch_code: str, school_id: str, school_name: Optional[str]) -> Dict[str, Any]:
    now = datetime.utcnow().isoformat() + "Z"
    return {
        "data_version": "1.0",
        "schema_version": "2025-09-04",
        "school_info": {
            "school_id": school_id,
            "school_name": school_name or "未知学校",
            "batch_code": batch_code,
            "total_students": 0,
            "calculation_time": now,
        },
        "academic_subjects": {},
        "non_academic_subjects": {},
        "radar_chart_data": {
            "academic_dimensions": [],
            "non_academic_dimensions": [],
        },
    }


@router.get("/batch/{batch_code}/regional")
def get_v12_regional(
    batch_code: str,
    include_detail: bool = False,
    allow_partial: bool = Query(True, description="缺失时返回空壳数据而非404")
):
    """返回指定批次的区域级 v1.2 subjects 数据（仅读取）。"""
    _ = include_detail  # 保留参数兼容旧调用
    db = SessionLocal()
    try:
        data = _fetch_v12_regional(db, batch_code)
        if data is None:
            if not allow_partial:
                raise HTTPException(status_code=404, detail="v1.2 区域级数据未准备")
            # 返回空壳结构，避免前端整体失败
            return {
                "success": True,
                "message": f"区域级数据缺失，返回空壳结构供渲染占位：{batch_code}",
                "data": _scaffold_regional_payload(batch_code),
                "code": 206,
                "partial": True,
            }
        return {
            "success": True,
            "message": f"v1.2 区域级 subjects 已生成 {batch_code}",
            "data": data,
            "code": 200,
        }
    finally:
        db.close()


@router.get("/batch/{batch_code}/school/{school_id}")
def get_v12_school(
    batch_code: str,
    school_id: str,
    allow_partial: bool = Query(True, description="缺失时返回空壳数据而非404")
):
    """返回指定学校的 v1.2 subjects 数据（仅读取）。"""
    db = SessionLocal()
    try:
        # 规范化 school_id，避免 5001.0/5001 等格式差异导致预计算缺失
        sid = str(school_id).strip()
        try:
            if "." in sid:
                head, tail = sid.split(".", 1)
                if tail and set(tail) <= {"0"} and head.lstrip("-+").isdigit():
                    sid = head
        except Exception:
            sid = str(school_id)

        data = _fetch_v12_school(db, batch_code, sid)
        if data is None:
            if not allow_partial:
                raise HTTPException(status_code=404, detail="v1.2 学校级数据未准备")
            school_name = _resolve_school_name(db, batch_code, sid)
            return {
                "success": True,
                "message": f"学校级数据缺失，返回空壳结构供渲染占位：{batch_code}/{sid}",
                "data": _scaffold_school_payload(batch_code, sid, school_name),
                "code": 206,
                "partial": True,
            }
        return {
            "success": True,
            "message": f"v1.2 学校级 subjects 已生成 {batch_code}/{sid}",
            "data": data,
            "code": 200,
        }
    finally:
        db.close()


@router.get("/batch/{batch_code}/schools/available")
def list_available_schools(batch_code: str) -> Dict[str, Any]:
    """列出当前批次已生成v1.2数据的学校清单，便于前端只拉取可用数据。"""
    db = SessionLocal()
    try:
        repo = StatisticalAggregationRepository(db)
        records: List[Any] = repo.get_all_school_statistics(batch_code)
        schools = [
            {
                "school_id": r.school_id,
                "school_name": r.school_name,
                "updated_at": (r.updated_at.isoformat() + "Z") if getattr(r, "updated_at", None) else None,
            }
            for r in records or []
            if getattr(r, "school_id", None)
        ]
        return {
            "success": True,
            "message": f"已生成的学校清单：{batch_code}",
            "data": {
                "batch_code": batch_code,
                "total": len(schools),
                "schools": schools,
            },
            "code": 200,
        }
    finally:
        db.close()
