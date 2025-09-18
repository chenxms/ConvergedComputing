from fastapi import APIRouter, HTTPException
from typing import Any, Dict, Optional
import logging
from sqlalchemy.orm import Session

from app.database.connection import SessionLocal
from app.database.repositories import StatisticalAggregationRepository

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


@router.get("/batch/{batch_code}/regional")
def get_v12_regional(batch_code: str, include_detail: bool = False):
    """返回指定批次的区域级 v1.2 subjects 数据（仅读取）。"""
    _ = include_detail  # 保留参数兼容旧调用
    db = SessionLocal()
    try:
        data = _fetch_v12_regional(db, batch_code)
        if data is None:
            raise HTTPException(status_code=404, detail="v1.2 区域级数据未准备")
        return {
            "success": True,
            "message": f"v1.2 区域级 subjects 已生成 {batch_code}",
            "data": data,
            "code": 200,
        }
    finally:
        db.close()


@router.get("/batch/{batch_code}/school/{school_id}")
def get_v12_school(batch_code: str, school_id: str):
    """返回指定学校的 v1.2 subjects 数据（仅读取）。"""
    db = SessionLocal()
    try:
        data = _fetch_v12_school(db, batch_code, school_id)
        if data is None:
            raise HTTPException(status_code=404, detail="v1.2 学校级数据未准备")
        return {
            "success": True,
            "message": f"v1.2 学校级 subjects 已生成 {batch_code}/{school_id}",
            "data": data,
            "code": 200,
        }
    finally:
        db.close()
