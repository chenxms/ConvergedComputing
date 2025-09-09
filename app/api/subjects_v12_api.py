from fastapi import APIRouter, HTTPException
from typing import Any, Dict
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database.connection import SessionLocal
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus
from app.services.subjects_builder import SubjectsBuilder
from app.utils.precision import round2_json


router = APIRouter()


def get_db_session() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _fetch_v12_regional(db: Session, batch_code: str) -> Dict[str, Any]:
    repo = StatisticalAggregationRepository(db)
    regional = repo.get_regional_statistics(batch_code)
    if regional and isinstance(regional.statistics_data, dict):
        data = regional.statistics_data
        if data.get("subjects"):
            return data
    # 构建并保存
    subjects = SubjectsBuilder().build_regional_subjects(batch_code)
    result = {
        "schema_version": "v1.2",
        "batch_code": batch_code,
        "aggregation_level": "REGIONAL",
        "subjects": subjects,
    }
    processed = round2_json(result)
    repo.upsert_statistics({
        "batch_code": batch_code,
        "aggregation_level": DBAggregationLevel.REGIONAL,
        "school_id": None,
        "school_name": None,
        "statistics_data": processed,
        "calculation_status": CalculationStatus.COMPLETED,
    })
    return processed


def _fetch_v12_school(db: Session, batch_code: str, school_code: str) -> Dict[str, Any]:
    repo = StatisticalAggregationRepository(db)
    rec = repo.get_school_statistics(batch_code, school_code)
    if rec and isinstance(rec.statistics_data, dict):
        data = rec.statistics_data
        if data.get("subjects"):
            return data
    subjects = SubjectsBuilder().build_school_subjects(batch_code, school_code)
    result = {
        "schema_version": "v1.2",
        "batch_code": batch_code,
        "aggregation_level": "SCHOOL",
        "school_code": school_code,
        "subjects": subjects,
    }
    processed = round2_json(result)
    repo.upsert_statistics({
        "batch_code": batch_code,
        "aggregation_level": DBAggregationLevel.SCHOOL,
        "school_id": school_code,
        "school_name": None,
        "statistics_data": processed,
        "calculation_status": CalculationStatus.COMPLETED,
    })
    return processed


@router.get("/batch/{batch_code}/regional")
def get_v12_regional(batch_code: str):
    try:
        db = next(get_db_session())
        try:
            data = _fetch_v12_regional(db, batch_code)
            return {"success": True, "message": f"v1.2 区域级 subjects 已生成 {batch_code}", "data": data, "code": 200}
        finally:
            db.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成 v1.2 区域级失败: {str(e)}")


@router.get("/batch/{batch_code}/school/{school_code}")
def get_v12_school(batch_code: str, school_code: str):
    try:
        db = next(get_db_session())
        try:
            data = _fetch_v12_school(db, batch_code, school_code)
            return {"success": True, "message": f"v1.2 学校级 subjects 已生成 {batch_code}/{school_code}", "data": data, "code": 200}
        finally:
            db.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成 v1.2 学校级失败: {str(e)}")


@router.post("/batch/{batch_code}/materialize")
def materialize_v12(batch_code: str):
    try:
        db = next(get_db_session())
        try:
            # 触发区域
            _fetch_v12_regional(db, batch_code)
            # 触发学校级（批量）
            rows = db.execute(text("SELECT DISTINCT school_code FROM student_cleaned_scores WHERE batch_code=:b"), {"b": batch_code}).fetchall()
            count = 0
            for (school_code,) in rows:
                _fetch_v12_school(db, batch_code, school_code)
                count += 1
            return {"success": True, "message": "v1.2 subjects 全量生成完成", "data": {"batch_code": batch_code, "schools_materialized": count}, "code": 200}
        finally:
            db.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"v1.2 全量生成失败: {str(e)}")

