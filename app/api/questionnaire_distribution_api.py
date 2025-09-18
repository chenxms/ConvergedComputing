# 问卷题目选项分布API
from fastapi import APIRouter, HTTPException, Path
from typing import Dict, Any
import logging
from ..services.question_option_distribution_service import QuestionOptionDistributionService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["questionnaire-distributions"])

# 创建服务实例
distribution_service = QuestionOptionDistributionService()


@router.get("/questionnaire-distributions/{batch_code}/{subject_name}/regional")
async def get_regional_questionnaire_distributions(
    batch_code: str = Path(..., description="批次代码"),
    subject_name: str = Path(..., description="科目名称")
) -> Dict[str, Any]:
    """获取区域级问卷题目选项分布
    
    Args:
        batch_code: 批次代码
        subject_name: 科目名称
        
    Returns:
        区域级题目选项分布数据
    """
    try:
        logger.info(f"查询区域级题目选项分布 - 批次:{batch_code}, 科目:{subject_name}")
        
        result = distribution_service.get_regional_option_distributions(batch_code, subject_name)
        
        if not result['questions']:
            raise HTTPException(
                status_code=404, 
                detail=f"未找到批次 {batch_code} 科目 {subject_name} 的题目选项分布数据"
            )
        
        return {
            "code": 200,
            "message": "查询成功",
            "data": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"查询区域级题目选项分布失败: {e}")
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/questionnaire-distributions/{batch_code}/{subject_name}/school/{school_id}")
async def get_school_questionnaire_distributions(
    batch_code: str = Path(..., description="批次代码"),
    subject_name: str = Path(..., description="科目名称"),
    school_id: str = Path(..., description="学校ID")
) -> Dict[str, Any]:
    """获取学校级问卷题目选项分布
    
    Args:
        batch_code: 批次代码
        subject_name: 科目名称
        school_id: 学校ID
        
    Returns:
        学校级题目选项分布数据
    """
    try:
        logger.info(f"查询学校级题目选项分布 - 批次:{batch_code}, 科目:{subject_name}, 学校:{school_id}")
        
        result = distribution_service.get_school_option_distributions(batch_code, subject_name, school_id)
        
        if not result['questions']:
            raise HTTPException(
                status_code=404, 
                detail=f"未找到学校 {school_id} 在批次 {batch_code} 科目 {subject_name} 的题目选项分布数据"
            )
        
        return {
            "code": 200,
            "message": "查询成功",
            "data": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"查询学校级题目选项分布失败: {e}")
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/questionnaire-distributions/{batch_code}/subjects")
async def list_questionnaire_subjects(
    batch_code: str = Path(..., description="批次代码")
) -> Dict[str, Any]:
    """获取批次中的问卷科目列表
    
    Args:
        batch_code: 批次代码
        
    Returns:
        问卷科目列表
    """
    try:
        from ..database.connection import get_db_context
        from sqlalchemy import text
        
        with get_db_context() as db:
            sql = text("""
                SELECT DISTINCT 
                    subject_name,
                    COUNT(DISTINCT school_code) as school_count,
                    COUNT(DISTINCT student_id) as student_count
                FROM student_cleaned_scores 
                WHERE batch_code = :batch 
                  AND subject_type = 'questionnaire'
                GROUP BY subject_name
                ORDER BY subject_name
            """)
            
            rows = db.execute(sql, {'batch': batch_code}).fetchall()
            
            subjects = []
            for row in rows:
                subjects.append({
                    'subject_name': row[0],
                    'school_count': int(row[1]),
                    'student_count': int(row[2])
                })
        
        return {
            "code": 200,
            "message": "查询成功",
            "data": {
                "batch_code": batch_code,
                "questionnaire_subjects": subjects,
                "total_subjects": len(subjects)
            }
        }
        
    except Exception as e:
        logger.error(f"获取问卷科目列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/questionnaire-distributions/{batch_code}/{subject_name}/schools")
async def list_schools_with_questionnaire_data(
    batch_code: str = Path(..., description="批次代码"),
    subject_name: str = Path(..., description="科目名称")
) -> Dict[str, Any]:
    """获取有问卷数据的学校列表
    
    Args:
        batch_code: 批次代码
        subject_name: 科目名称
        
    Returns:
        学校列表及统计信息
    """
    try:
        from ..database.connection import get_db_context
        from sqlalchemy import text
        
        with get_db_context() as db:
            sql = text("""
                SELECT DISTINCT 
                    smd.school_id,
                    smd.standard_school_name,
                    COUNT(DISTINCT scs.student_id) as student_count,
                    EXISTS(
                        SELECT 1 FROM questionnaire_option_distribution qod
                        WHERE qod.batch_code = scs.batch_code
                          AND qod.subject_name = scs.subject_name 
                          AND qod.school_id = smd.school_id
                    ) as has_distribution_data
                FROM school_master_data smd
                JOIN student_cleaned_scores scs 
                  ON smd.batch_code = scs.batch_code 
                 AND smd.school_id = scs.school_code
                WHERE smd.batch_code = :batch 
                  AND scs.subject_name = :subject
                  AND scs.subject_type = 'questionnaire'
                  AND smd.status = 'ACTIVE'
                GROUP BY smd.school_id, smd.standard_school_name
                ORDER BY smd.standard_school_name
            """)
            
            rows = db.execute(sql, {'batch': batch_code, 'subject': subject_name}).fetchall()
            
            schools = []
            for row in rows:
                schools.append({
                    'school_id': row[0],
                    'school_name': row[1],
                    'student_count': int(row[2]),
                    'has_distribution_data': bool(row[3])
                })
        
        return {
            "code": 200,
            "message": "查询成功",
            "data": {
                "batch_code": batch_code,
                "subject_name": subject_name,
                "schools": schools,
                "total_schools": len(schools)
            }
        }
        
    except Exception as e:
        logger.error(f"获取学校列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")
