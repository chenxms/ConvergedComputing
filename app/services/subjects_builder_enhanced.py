"""
SubjectsBuilder增强模块

为SubjectsBuilder添加增强的学校信息处理功能：
- 更好的错误处理和日志记录
- 学校信息一致性验证
- 性能优化的批量查询
- 缓存机制改进
"""

from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.orm import Session
import logging
import json
from app.database.connection import get_db
from app.utils.precision import round2

logger = logging.getLogger(__name__)


class SchoolInfoHandler:
    """学校信息处理器"""
    
    def __init__(self):
        # 学校信息缓存 {batch_code: {school_id: {name, status}}}
        self._school_info_cache = {}
    
    def get_school_info(self, batch_code: str, school_id: str, db_session: Session = None) -> Optional[Dict[str, str]]:
        """
        获取学校信息，优先从缓存读取
        
        Returns:
            Dict包含 name, status 或 None
        """
        # 检查缓存
        cache_key = f"{batch_code}:{school_id}"
        if batch_code in self._school_info_cache and school_id in self._school_info_cache[batch_code]:
            return self._school_info_cache[batch_code][school_id]
        
        # 从数据库查询
        if not db_session:
            db_session = next(get_db())
            should_close = True
        else:
            should_close = False
            
        try:
            result = db_session.execute(text("""
                SELECT standard_school_name, status
                FROM school_master_data
                WHERE batch_code = :batch_code 
                    AND school_id = :school_id
            """), {
                "batch_code": batch_code,
                "school_id": school_id
            }).fetchone()
            
            if result:
                info = {
                    "name": result[0],
                    "status": result[1]
                }
                
                # 更新缓存
                if batch_code not in self._school_info_cache:
                    self._school_info_cache[batch_code] = {}
                self._school_info_cache[batch_code][school_id] = info
                
                return info
            else:
                logger.warning(f"School {school_id} not found in batch {batch_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting school info for {batch_code}:{school_id}: {str(e)}")
            return None
        finally:
            if should_close:
                db_session.close()
    
    def batch_load_school_info(self, batch_code: str, school_ids: List[str], db_session: Session = None) -> Dict[str, Dict[str, str]]:
        """
        批量加载学校信息
        
        Returns:
            Dict[school_id, {name, status}]
        """
        if not school_ids:
            return {}
            
        if not db_session:
            db_session = next(get_db())
            should_close = True
        else:
            should_close = False
            
        try:
            # 构建IN查询
            placeholders = ','.join([f':school_id_{i}' for i in range(len(school_ids))])
            params = {"batch_code": batch_code}
            for i, school_id in enumerate(school_ids):
                params[f"school_id_{i}"] = school_id
            
            results = db_session.execute(text(f"""
                SELECT school_id, standard_school_name, status
                FROM school_master_data
                WHERE batch_code = :batch_code 
                    AND school_id IN ({placeholders})
            """), params).fetchall()
            
            # 处理结果
            school_info_map = {}
            for row in results:
                school_id, name, status = row
                info = {"name": name, "status": status}
                school_info_map[school_id] = info
                
                # 更新缓存
                if batch_code not in self._school_info_cache:
                    self._school_info_cache[batch_code] = {}
                self._school_info_cache[batch_code][school_id] = info
            
            # 记录未找到的学校
            missing_schools = set(school_ids) - set(school_info_map.keys())
            if missing_schools:
                logger.warning(f"Schools not found in batch {batch_code}: {missing_schools}")
            
            return school_info_map
            
        except Exception as e:
            logger.error(f"Error batch loading school info for batch {batch_code}: {str(e)}")
            return {}
        finally:
            if should_close:
                db_session.close()
    
    def validate_school_exists(self, batch_code: str, school_id: str, db_session: Session = None) -> bool:
        """验证学校是否存在且为活跃状态"""
        info = self.get_school_info(batch_code, school_id, db_session)
        return info is not None and info.get("status") == "ACTIVE"
    
    def clear_cache(self, batch_code: str = None):
        """清理缓存"""
        if batch_code:
            self._school_info_cache.pop(batch_code, None)
        else:
            self._school_info_cache.clear()


class EnhancedSchoolRankingCalculator:
    """增强的学校排名计算器"""
    
    def __init__(self):
        self.school_info_handler = SchoolInfoHandler()
    
    def compute_school_rankings_enhanced(self, batch_code: str, subject_name: str, db_session: Session = None) -> List[Dict[str, Any]]:
        """
        增强的学校排名计算，包含错误处理和数据验证
        """
        if not db_session:
            db_session = next(get_db())
            should_close = True
        else:
            should_close = False
            
        try:
            # 首先验证批次和科目是否存在数据
            data_check = db_session.execute(text("""
                SELECT COUNT(DISTINCT scs.school_code) as school_count,
                       COUNT(*) as total_records
                FROM student_cleaned_scores scs
                JOIN school_master_data smd 
                  ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
                 AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
                 AND smd.status = 'ACTIVE'
                WHERE scs.batch_code = :batch_code 
                    AND scs.subject_name = :subject_name
                    AND scs.subject_type IN ('exam','questionnaire')
            """), {
                "batch_code": batch_code,
                "subject_name": subject_name
            }).fetchone()
            
            if not data_check or data_check.total_records == 0:
                logger.warning(f"No data found for batch {batch_code}, subject {subject_name}")
                return []
            
            logger.info(f"Computing rankings for {data_check.school_count} schools in batch {batch_code}, subject {subject_name}")
            
            # 计算学校排名
            sql = text("""
                SELECT scs.school_code,
                       smd.standard_school_name AS school_name,
                       ROUND(AVG(scs.total_score), 2) AS avg,
                       COUNT(DISTINCT scs.student_id) AS student_count,
                       ROUND(MAX(scs.max_score), 2) AS max_score,
                       DENSE_RANK() OVER (ORDER BY AVG(scs.total_score) DESC, scs.school_code ASC) AS rnk
                FROM student_cleaned_scores scs
                JOIN school_master_data smd 
                  ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
                 AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
                 AND smd.status = 'ACTIVE'
                WHERE scs.batch_code = :batch_code 
                    AND scs.subject_name = :subject_name
                    AND scs.subject_type IN ('exam','questionnaire')
                GROUP BY scs.school_code, smd.standard_school_name
                HAVING COUNT(DISTINCT scs.student_id) >= 1  -- 至少有1个学生
                ORDER BY avg DESC, scs.school_code ASC
            """)
            
            rows = db_session.execute(sql, {
                "batch_code": batch_code,
                "subject_name": subject_name
            }).fetchall()
            
            rankings = []
            for row in rows:
                school_code, school_name, avg, student_count, max_score, rank = row
                
                # 数据验证
                if not school_name:
                    logger.warning(f"School {school_code} has no name, using fallback")
                    school_name = f"学校_{school_code}"
                
                if avg is None or avg < 0:
                    logger.warning(f"Invalid average score for school {school_code}: {avg}")
                    avg = 0.0
                
                # 计算得分率
                score_rate = round2((avg / max_score * 100.0) if max_score and max_score > 0 else None)
                
                rankings.append({
                    "school_code": school_code,
                    "school_name": school_name,
                    "avg": float(avg or 0),
                    "rank": int(rank),
                    "student_count": int(student_count),
                    "score_rate": score_rate,
                    "max_score": float(max_score or 0)
                })
            
            logger.info(f"Successfully computed rankings for {len(rankings)} schools")
            return rankings
            
        except Exception as e:
            logger.error(f"Error computing school rankings for batch {batch_code}, subject {subject_name}: {str(e)}")
            raise
        finally:
            if should_close:
                db_session.close()
    
    def compute_school_region_rank_enhanced(self, batch_code: str, subject_name: str, school_code: str, db_session: Session = None) -> Dict[str, Any]:
        """
        增强的学校区域排名计算
        """
        if not db_session:
            db_session = next(get_db())
            should_close = True
        else:
            should_close = False
            
        try:
            # 验证学校是否存在
            if not self.school_info_handler.validate_school_exists(batch_code, school_code, db_session):
                logger.warning(f"School {school_code} not found or inactive in batch {batch_code}")
                return {"region_rank": None, "total_schools": 0, "error": "school_not_found"}
            
            sql = text("""
                WITH ranks AS (
                  SELECT scs.school_code,
                         AVG(scs.total_score) as avg_score,
                         COUNT(DISTINCT scs.student_id) as student_count,
                         DENSE_RANK() OVER (ORDER BY AVG(scs.total_score) DESC, scs.school_code ASC) AS r
                  FROM student_cleaned_scores scs
                  JOIN school_master_data smd 
                    ON smd.batch_code COLLATE utf8mb4_unicode_ci = scs.batch_code COLLATE utf8mb4_unicode_ci
                   AND smd.school_id COLLATE utf8mb4_unicode_ci = scs.school_code COLLATE utf8mb4_unicode_ci
                   AND smd.status = 'ACTIVE'
                  WHERE scs.batch_code = :batch_code 
                    AND scs.subject_name = :subject_name
                    AND scs.subject_type IN ('exam','questionnaire')
                  GROUP BY scs.school_code
                  HAVING COUNT(DISTINCT scs.student_id) >= 1
                )
                SELECT 
                    r.r AS region_rank,
                    r.avg_score,
                    r.student_count,
                    (SELECT COUNT(*) FROM ranks) AS total_schools
                FROM ranks r 
                WHERE r.school_code = :school_code
            """)
            
            result = db_session.execute(sql, {
                "batch_code": batch_code,
                "subject_name": subject_name,
                "school_code": school_code
            }).fetchone()
            
            if not result:
                logger.warning(f"No ranking data found for school {school_code} in batch {batch_code}, subject {subject_name}")
                return {"region_rank": None, "total_schools": 0, "error": "no_data"}
            
            return {
                "region_rank": int(result[0] or 0),
                "avg_score": float(result[1] or 0),
                "student_count": int(result[2] or 0),
                "total_schools": int(result[3] or 0)
            }
            
        except Exception as e:
            logger.error(f"Error computing region rank for school {school_code}: {str(e)}")
            return {"region_rank": None, "total_schools": 0, "error": str(e)}
        finally:
            if should_close:
                db_session.close()


class SchoolDataValidator:
    """学校数据验证器（用于构建前验证）"""
    
    def __init__(self):
        self.school_info_handler = SchoolInfoHandler()
    
    def validate_school_data_for_subject(self, batch_code: str, subject_name: str, school_code: str = None, db_session: Session = None) -> Dict[str, Any]:
        """
        验证指定批次科目的学校数据质量
        """
        if not db_session:
            db_session = next(get_db())
            should_close = True
        else:
            should_close = False
            
        validation_result = {
            "is_valid": True,
            "warnings": [],
            "errors": [],
            "stats": {}
        }
        
        try:
            # 基础数据统计
            where_conditions = ["scs.batch_code = :batch_code", "scs.subject_name = :subject_name"]
            params = {"batch_code": batch_code, "subject_name": subject_name}
            
            if school_code:
                where_conditions.append("scs.school_code = :school_code")
                params["school_code"] = school_code
            
            where_clause = " AND ".join(where_conditions)
            
            stats = db_session.execute(text(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT scs.school_code) as unique_schools,
                    COUNT(DISTINCT scs.student_id) as unique_students,
                    COUNT(CASE WHEN scs.school_name IS NULL OR scs.school_name = '' THEN 1 END) as null_school_names,
                    COUNT(CASE WHEN scs.total_score IS NULL THEN 1 END) as null_scores,
                    AVG(scs.total_score) as avg_total_score,
                    MAX(scs.max_score) as max_possible_score
                FROM student_cleaned_scores scs
                WHERE {where_clause}
                    AND scs.subject_type IN ('exam', 'questionnaire')
            """), params).fetchone()
            
            validation_result["stats"] = {
                "total_records": int(stats[0] or 0),
                "unique_schools": int(stats[1] or 0),
                "unique_students": int(stats[2] or 0),
                "null_school_names": int(stats[3] or 0),
                "null_scores": int(stats[4] or 0),
                "avg_total_score": float(stats[5] or 0),
                "max_possible_score": float(stats[6] or 0)
            }
            
            # 验证数据质量
            if validation_result["stats"]["total_records"] == 0:
                validation_result["errors"].append("No data found for specified criteria")
                validation_result["is_valid"] = False
            
            if validation_result["stats"]["null_school_names"] > 0:
                validation_result["warnings"].append(f"{validation_result['stats']['null_school_names']} records have null school names")
            
            if validation_result["stats"]["null_scores"] > 0:
                validation_result["errors"].append(f"{validation_result['stats']['null_scores']} records have null scores")
                validation_result["is_valid"] = False
            
            # 验证与主数据匹配
            if validation_result["stats"]["unique_schools"] > 0:
                master_match = db_session.execute(text(f"""
                    SELECT 
                        COUNT(DISTINCT scs.school_code) as cleaned_schools,
                        COUNT(DISTINCT smd.school_id) as matched_schools
                    FROM student_cleaned_scores scs
                    LEFT JOIN school_master_data smd 
                        ON scs.batch_code = smd.batch_code 
                        AND scs.school_code = smd.school_id
                        AND smd.status = 'ACTIVE'
                    WHERE {where_clause}
                        AND scs.subject_type IN ('exam', 'questionnaire')
                """), params).fetchone()
                
                cleaned_schools = int(master_match[0] or 0)
                matched_schools = int(master_match[1] or 0)
                
                validation_result["stats"]["master_data_match"] = {
                    "cleaned_schools": cleaned_schools,
                    "matched_schools": matched_schools,
                    "match_rate": (matched_schools / cleaned_schools * 100) if cleaned_schools > 0 else 0
                }
                
                if matched_schools < cleaned_schools:
                    unmatched = cleaned_schools - matched_schools
                    validation_result["errors"].append(f"{unmatched} schools not found in master data")
                    validation_result["is_valid"] = False
            
            return validation_result
            
        except Exception as e:
            validation_result["is_valid"] = False
            validation_result["errors"].append(f"Validation error: {str(e)}")
            logger.error(f"School data validation error: {str(e)}")
            return validation_result
        finally:
            if should_close:
                db_session.close()


# 全局实例
school_info_handler = SchoolInfoHandler()
enhanced_ranking_calculator = EnhancedSchoolRankingCalculator()
school_data_validator = SchoolDataValidator()


# 便捷函数
def get_school_name(batch_code: str, school_id: str, db_session: Session = None) -> str:
    """便捷函数：获取学校名称，失败时返回默认名称"""
    info = school_info_handler.get_school_info(batch_code, school_id, db_session)
    if info and info.get("name"):
        return info["name"]
    else:
        return f"学校_{school_id}"


def validate_school_before_build(batch_code: str, school_code: str, subject_name: str = None, db_session: Session = None) -> bool:
    """便捷函数：构建前验证学校是否可用"""
    # 验证学校存在性
    if not school_info_handler.validate_school_exists(batch_code, school_code, db_session):
        logger.warning(f"School validation failed: {school_code} not found or inactive in batch {batch_code}")
        return False
    
    # 如果指定了科目，验证该科目的数据质量
    if subject_name:
        validation = school_data_validator.validate_school_data_for_subject(
            batch_code, subject_name, school_code, db_session
        )
        if not validation["is_valid"]:
            logger.warning(f"School data validation failed for {school_code}: {validation['errors']}")
            return False
    
    return True