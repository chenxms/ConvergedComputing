"""
学校信息验证模块

提供学校数据一致性和完整性验证功能，确保汇聚前数据质量
"""

from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """验证级别"""
    ERROR = "error"      # 严重错误，阻止汇聚
    WARNING = "warning"  # 警告，允许汇聚但需注意
    INFO = "info"        # 信息提示


@dataclass
class ValidationResult:
    """验证结果"""
    level: ValidationLevel
    message: str
    details: Optional[Dict[str, Any]] = None
    affected_records: int = 0


class SchoolValidationReport:
    """学校验证报告"""
    
    def __init__(self, batch_code: str):
        self.batch_code = batch_code
        self.results: List[ValidationResult] = []
        self.summary: Dict[str, int] = {"error": 0, "warning": 0, "info": 0}
    
    def add_result(self, result: ValidationResult):
        """添加验证结果"""
        self.results.append(result)
        self.summary[result.level.value] += 1
    
    def has_errors(self) -> bool:
        """是否存在错误"""
        return self.summary["error"] > 0
    
    def has_warnings(self) -> bool:
        """是否存在警告"""
        return self.summary["warning"] > 0
    
    def is_valid_for_aggregation(self) -> bool:
        """是否可以进行汇聚"""
        return not self.has_errors()
    
    def get_report(self) -> Dict[str, Any]:
        """获取完整报告"""
        return {
            "batch_code": self.batch_code,
            "summary": self.summary,
            "is_valid_for_aggregation": self.is_valid_for_aggregation(),
            "results": [
                {
                    "level": r.level.value,
                    "message": r.message,
                    "details": r.details,
                    "affected_records": r.affected_records
                }
                for r in self.results
            ]
        }


class SchoolDataValidator:
    """学校数据验证器"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def validate_batch(self, batch_code: str) -> SchoolValidationReport:
        """验证指定批次的学校数据"""
        report = SchoolValidationReport(batch_code)
        
        # 1. 验证school_master_data完整性
        self._validate_master_data(batch_code, report)
        
        # 2. 验证清洗数据与主数据一致性
        self._validate_cleaned_data_consistency(batch_code, report)
        
        # 3. 验证学校名称完整性
        self._validate_school_names(batch_code, report)
        
        # 4. 验证学校ID格式
        self._validate_school_id_format(batch_code, report)
        
        # 5. 验证数据完整性
        self._validate_data_completeness(batch_code, report)
        
        # 6. 验证孤儿数据
        self._validate_orphaned_data(batch_code, report)
        
        return report
    
    def _validate_master_data(self, batch_code: str, report: SchoolValidationReport):
        """验证school_master_data表完整性"""
        try:
            # 检查是否存在master data
            master_count = self.db.execute(text("""
                SELECT COUNT(*) 
                FROM school_master_data 
                WHERE batch_code = :batch_code
            """), {"batch_code": batch_code}).scalar() or 0
            
            if master_count == 0:
                report.add_result(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message=f"批次 {batch_code} 在 school_master_data 中没有记录",
                    affected_records=0
                ))
                return
            
            # 检查活跃学校数量
            active_count = self.db.execute(text("""
                SELECT COUNT(*) 
                FROM school_master_data 
                WHERE batch_code = :batch_code AND status = 'ACTIVE'
            """), {"batch_code": batch_code}).scalar() or 0
            
            if active_count == 0:
                report.add_result(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message=f"批次 {batch_code} 没有活跃状态的学校",
                    details={"total_schools": master_count, "active_schools": active_count},
                    affected_records=master_count
                ))
            elif active_count < master_count:
                inactive_count = master_count - active_count
                report.add_result(ValidationResult(
                    level=ValidationLevel.WARNING,
                    message=f"批次 {batch_code} 存在 {inactive_count} 所非活跃学校",
                    details={"total_schools": master_count, "active_schools": active_count, "inactive_schools": inactive_count},
                    affected_records=inactive_count
                ))
            
            # 检查school_id重复
            duplicate_query = text("""
                SELECT school_id, COUNT(*) as count 
                FROM school_master_data 
                WHERE batch_code = :batch_code
                GROUP BY school_id 
                HAVING COUNT(*) > 1
            """)
            duplicates = self.db.execute(duplicate_query, {"batch_code": batch_code}).fetchall()
            
            if duplicates:
                duplicate_ids = [row[0] for row in duplicates]
                total_duplicates = sum(row[1] for row in duplicates)
                report.add_result(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message=f"school_master_data 存在重复的 school_id",
                    details={"duplicate_school_ids": duplicate_ids},
                    affected_records=total_duplicates
                ))
            
            # 检查standard_school_name完整性
            null_names = self.db.execute(text("""
                SELECT COUNT(*) 
                FROM school_master_data 
                WHERE batch_code = :batch_code 
                    AND status = 'ACTIVE'
                    AND (standard_school_name IS NULL OR standard_school_name = '')
            """), {"batch_code": batch_code}).scalar() or 0
            
            if null_names > 0:
                report.add_result(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message=f"school_master_data 存在 {null_names} 条空的 standard_school_name",
                    affected_records=null_names
                ))
            else:
                report.add_result(ValidationResult(
                    level=ValidationLevel.INFO,
                    message=f"school_master_data 数据完整性检查通过: {active_count} 所活跃学校",
                    affected_records=active_count
                ))
                
        except Exception as e:
            report.add_result(ValidationResult(
                level=ValidationLevel.ERROR,
                message=f"验证 school_master_data 时出错: {str(e)}",
                details={"error": str(e)}
            ))
    
    def _validate_cleaned_data_consistency(self, batch_code: str, report: SchoolValidationReport):
        """验证清洗数据与主数据一致性"""
        try:
            # 检查清洗数据是否存在
            cleaned_count = self.db.execute(text("""
                SELECT COUNT(DISTINCT school_code) 
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
            """), {"batch_code": batch_code}).scalar() or 0
            
            if cleaned_count == 0:
                report.add_result(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message=f"批次 {batch_code} 没有清洗后的学生数据",
                    affected_records=0
                ))
                return
            
            # 检查清洗数据中的学校与主数据匹配情况
            consistency_check = self.db.execute(text("""
                SELECT 
                    COUNT(DISTINCT scs.school_code) as cleaned_schools,
                    COUNT(DISTINCT smd.school_id) as master_schools,
                    COUNT(DISTINCT CASE 
                        WHEN smd.school_id IS NOT NULL THEN scs.school_code 
                    END) as matched_schools
                FROM student_cleaned_scores scs
                LEFT JOIN school_master_data smd 
                    ON scs.batch_code COLLATE utf8mb4_unicode_ci = smd.batch_code COLLATE utf8mb4_unicode_ci
                    AND scs.school_code COLLATE utf8mb4_unicode_ci = smd.school_id COLLATE utf8mb4_unicode_ci
                    AND smd.status = 'ACTIVE'
                WHERE scs.batch_code = :batch_code
            """), {"batch_code": batch_code}).fetchone()
            
            cleaned_schools = consistency_check.cleaned_schools
            master_schools = consistency_check.master_schools
            matched_schools = consistency_check.matched_schools
            unmatched_schools = cleaned_schools - matched_schools
            
            if unmatched_schools > 0:
                # 获取未匹配的学校详情
                unmatched_query = text("""
                    SELECT DISTINCT 
                        scs.school_code,
                        scs.school_name,
                        COUNT(DISTINCT scs.student_id) as student_count
                    FROM student_cleaned_scores scs
                    LEFT JOIN school_master_data smd 
                        ON scs.batch_code COLLATE utf8mb4_unicode_ci = smd.batch_code COLLATE utf8mb4_unicode_ci
                        AND scs.school_code COLLATE utf8mb4_unicode_ci = smd.school_id COLLATE utf8mb4_unicode_ci
                        AND smd.status = 'ACTIVE'
                    WHERE scs.batch_code = :batch_code 
                        AND smd.school_id IS NULL
                    GROUP BY scs.school_code, scs.school_name
                    ORDER BY student_count DESC
                """)
                unmatched_details = self.db.execute(unmatched_query, {"batch_code": batch_code}).fetchall()
                unmatched_list = [
                    {"school_code": row[0], "school_name": row[1], "student_count": row[2]} 
                    for row in unmatched_details
                ]
                
                report.add_result(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message=f"清洗数据中有 {unmatched_schools} 所学校不在 school_master_data 中",
                    details={
                        "cleaned_schools": cleaned_schools,
                        "master_schools": master_schools,
                        "matched_schools": matched_schools,
                        "unmatched_schools": unmatched_list
                    },
                    affected_records=unmatched_schools
                ))
            else:
                report.add_result(ValidationResult(
                    level=ValidationLevel.INFO,
                    message=f"清洗数据与主数据匹配检查通过: {matched_schools}/{cleaned_schools} 学校匹配",
                    details={
                        "cleaned_schools": cleaned_schools,
                        "master_schools": master_schools,
                        "matched_schools": matched_schools
                    },
                    affected_records=matched_schools
                ))
                
        except Exception as e:
            report.add_result(ValidationResult(
                level=ValidationLevel.ERROR,
                message=f"验证数据一致性时出错: {str(e)}",
                details={"error": str(e)}
            ))
    
    def _validate_school_names(self, batch_code: str, report: SchoolValidationReport):
        """验证学校名称完整性"""
        try:
            # 检查清洗数据中的学校名称
            name_stats = self.db.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN school_name IS NOT NULL AND school_name != '' THEN 1 END) as with_names,
                    COUNT(CASE WHEN school_name IS NULL OR school_name = '' THEN 1 END) as null_names,
                    COUNT(DISTINCT school_name) as unique_names
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
            """), {"batch_code": batch_code}).fetchone()
            
            if name_stats.null_names > 0:
                report.add_result(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message=f"清洗数据中有 {name_stats.null_names} 条记录缺少学校名称",
                    details={
                        "total_records": name_stats.total_records,
                        "with_names": name_stats.with_names,
                        "null_names": name_stats.null_names,
                        "unique_names": name_stats.unique_names
                    },
                    affected_records=name_stats.null_names
                ))
            else:
                report.add_result(ValidationResult(
                    level=ValidationLevel.INFO,
                    message=f"学校名称完整性检查通过: {name_stats.with_names} 条记录都有名称",
                    affected_records=name_stats.with_names
                ))
                
        except Exception as e:
            report.add_result(ValidationResult(
                level=ValidationLevel.ERROR,
                message=f"验证学校名称时出错: {str(e)}",
                details={"error": str(e)}
            ))
    
    def _validate_school_id_format(self, batch_code: str, report: SchoolValidationReport):
        """验证学校ID格式"""
        try:
            # 检查school_id格式分布
            id_format_query = text("""
                SELECT 
                    CASE 
                        WHEN school_id REGEXP '^[0-9]+$' THEN '数字ID'
                        WHEN school_id REGEXP '^[0-9]+-[0-9]+$' THEN '连字符数字ID'
                        WHEN school_id LIKE 'SCH_%' THEN '自动编号ID'
                        WHEN school_id IS NULL THEN 'NULL'
                        ELSE '其他格式'
                    END as id_format,
                    COUNT(DISTINCT school_id) as school_count
                FROM school_master_data 
                WHERE batch_code = :batch_code AND status = 'ACTIVE'
                GROUP BY id_format
                ORDER BY school_count DESC
            """)
            
            format_stats = self.db.execute(id_format_query, {"batch_code": batch_code}).fetchall()
            format_dict = {row[0]: row[1] for row in format_stats}
            
            # 检查是否有问题格式
            problematic_formats = ['自动编号ID', 'NULL', '其他格式']
            issues = {fmt: count for fmt, count in format_dict.items() if fmt in problematic_formats}
            
            if issues:
                report.add_result(ValidationResult(
                    level=ValidationLevel.WARNING,
                    message=f"school_id 格式存在问题",
                    details={"format_distribution": format_dict, "problematic_formats": issues},
                    affected_records=sum(issues.values())
                ))
            else:
                report.add_result(ValidationResult(
                    level=ValidationLevel.INFO,
                    message=f"school_id 格式检查通过",
                    details={"format_distribution": format_dict},
                    affected_records=sum(format_dict.values())
                ))
                
        except Exception as e:
            report.add_result(ValidationResult(
                level=ValidationLevel.ERROR,
                message=f"验证学校ID格式时出错: {str(e)}",
                details={"error": str(e)}
            ))
    
    def _validate_data_completeness(self, batch_code: str, report: SchoolValidationReport):
        """验证数据完整性"""
        try:
            # 检查每个学校的数据完整性
            completeness_query = text("""
                SELECT 
                    smd.school_id,
                    smd.standard_school_name,
                    COUNT(DISTINCT scs.student_id) as student_count,
                    COUNT(DISTINCT scs.subject_name) as subject_count,
                    COUNT(*) as total_records
                FROM school_master_data smd
                LEFT JOIN student_cleaned_scores scs 
                    ON smd.batch_code = scs.batch_code 
                    AND smd.school_id = scs.school_code
                WHERE smd.batch_code = :batch_code AND smd.status = 'ACTIVE'
                GROUP BY smd.school_id, smd.standard_school_name
                ORDER BY student_count DESC
            """)
            
            completeness_data = self.db.execute(completeness_query, {"batch_code": batch_code}).fetchall()
            
            schools_without_data = [
                {"school_id": row[0], "school_name": row[1]} 
                for row in completeness_data if row[2] == 0  # student_count == 0
            ]
            
            if schools_without_data:
                report.add_result(ValidationResult(
                    level=ValidationLevel.WARNING,
                    message=f"有 {len(schools_without_data)} 所学校没有学生数据",
                    details={"schools_without_data": schools_without_data},
                    affected_records=len(schools_without_data)
                ))
            
            # 统计总体完整性
            total_schools = len(completeness_data)
            schools_with_data = total_schools - len(schools_without_data)
            total_students = sum(row[2] for row in completeness_data)
            
            report.add_result(ValidationResult(
                level=ValidationLevel.INFO,
                message=f"数据完整性统计: {schools_with_data}/{total_schools} 学校有数据，共 {total_students} 名学生",
                details={
                    "total_schools": total_schools,
                    "schools_with_data": schools_with_data,
                    "total_students": total_students
                },
                affected_records=total_students
            ))
                
        except Exception as e:
            report.add_result(ValidationResult(
                level=ValidationLevel.ERROR,
                message=f"验证数据完整性时出错: {str(e)}",
                details={"error": str(e)}
            ))
    
    def _validate_orphaned_data(self, batch_code: str, report: SchoolValidationReport):
        """验证孤儿数据"""
        try:
            # 检查统计汇聚表中的孤儿记录
            orphaned_agg_query = text("""
                SELECT 
                    sa.school_id,
                    sa.school_name,
                    sa.aggregation_level,
                    COUNT(*) as record_count
                FROM statistical_aggregations sa
                LEFT JOIN school_master_data smd 
                    ON sa.batch_code COLLATE utf8mb4_unicode_ci = smd.batch_code COLLATE utf8mb4_unicode_ci
                    AND sa.school_id COLLATE utf8mb4_unicode_ci = smd.school_id COLLATE utf8mb4_unicode_ci
                    AND smd.status = 'ACTIVE'
                WHERE sa.batch_code = :batch_code 
                    AND sa.aggregation_level = 'SCHOOL'
                    AND sa.school_id IS NOT NULL
                    AND smd.school_id IS NULL
                GROUP BY sa.school_id, sa.school_name, sa.aggregation_level
                ORDER BY record_count DESC
            """)
            
            orphaned_agg = self.db.execute(orphaned_agg_query, {"batch_code": batch_code}).fetchall()
            
            if orphaned_agg:
                orphaned_list = [
                    {"school_id": row[0], "school_name": row[1], "record_count": row[3]} 
                    for row in orphaned_agg
                ]
                total_orphaned = sum(row[3] for row in orphaned_agg)
                
                report.add_result(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message=f"统计汇聚表中存在 {len(orphaned_agg)} 个孤儿学校记录",
                    details={"orphaned_records": orphaned_list},
                    affected_records=total_orphaned
                ))
            else:
                # 检查是否有统计汇聚数据
                agg_count = self.db.execute(text("""
                    SELECT COUNT(*) 
                    FROM statistical_aggregations 
                    WHERE batch_code = :batch_code
                """), {"batch_code": batch_code}).scalar() or 0
                
                if agg_count > 0:
                    report.add_result(ValidationResult(
                        level=ValidationLevel.INFO,
                        message=f"孤儿数据检查通过: 统计汇聚表中无孤儿记录",
                        affected_records=agg_count
                    ))
                
        except Exception as e:
            report.add_result(ValidationResult(
                level=ValidationLevel.ERROR,
                message=f"验证孤儿数据时出错: {str(e)}",
                details={"error": str(e)}
            ))


def validate_school_data_before_aggregation(db_session: Session, batch_code: str) -> SchoolValidationReport:
    """
    汇聚前学校数据验证入口函数
    
    Args:
        db_session: 数据库会话
        batch_code: 批次代码
    
    Returns:
        SchoolValidationReport: 验证报告
    """
    validator = SchoolDataValidator(db_session)
    return validator.validate_batch(batch_code)


def print_validation_report(report: SchoolValidationReport):
    """打印验证报告"""
    print(f"\n=== 学校数据验证报告: {report.batch_code} ===")
    print(f"总计: {len(report.results)} 个检查项")
    print(f"错误: {report.summary['error']} 个")
    print(f"警告: {report.summary['warning']} 个")
    print(f"信息: {report.summary['info']} 个")
    print(f"可否汇聚: {'✓ 是' if report.is_valid_for_aggregation() else '✗ 否'}")
    
    for i, result in enumerate(report.results, 1):
        level_icon = {"error": "✗", "warning": "⚠", "info": "ℹ"}
        print(f"\n{i}. [{level_icon[result.level.value]}] {result.message}")
        if result.affected_records > 0:
            print(f"   影响记录数: {result.affected_records}")
        if result.details:
            print(f"   详细信息: {result.details}")