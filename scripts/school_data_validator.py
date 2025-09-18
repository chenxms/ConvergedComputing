#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
学校数据验证器 - 专门验证学校数据的一致性和完整性
确保school_master_data作为唯一权威数据源
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from sqlalchemy import text
from sqlalchemy.orm import Session

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database.connection import get_db

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('school_data_validator.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class SchoolValidationIssue:
    """学校验证问题数据类"""
    issue_type: str
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    school_code: str
    school_name: Optional[str]
    description: str
    details: Dict[str, Any]
    fix_suggestions: List[str]


@dataclass
class SchoolValidationReport:
    """学校验证报告数据类"""
    batch_code: str
    validation_timestamp: datetime
    total_schools_checked: int
    issues_found: int
    critical_issues: int
    validation_status: str  # PASS, WARN, FAIL
    issues: List[SchoolValidationIssue]
    summary: Dict[str, Any]
    recommendations: List[str]


class SchoolDataValidator:
    """学校数据专用验证器"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.school_id_pattern = r'^[0-9]{4}$'  # 4位数字格式
        self.school_id_range = (5044, 5099)  # 有效范围
    
    def validate_batch_schools(self, batch_code: str) -> SchoolValidationReport:
        """对批次中的学校数据进行全面验证"""
        logger.info(f"开始验证批次 {batch_code} 的学校数据")
        
        start_time = datetime.now()
        issues = []
        
        # 1. 验证school_master_data完整性
        issues.extend(self._validate_master_data_completeness(batch_code))
        
        # 2. 验证学校ID格式和范围
        issues.extend(self._validate_school_id_format(batch_code))
        
        # 3. 验证学校名称标准化
        issues.extend(self._validate_school_name_standardization(batch_code))
        
        # 4. 验证跨表一致性
        issues.extend(self._validate_cross_table_consistency(batch_code))
        
        # 5. 验证数据源一致性
        issues.extend(self._validate_data_source_consistency(batch_code))
        
        # 6. 验证学校状态
        issues.extend(self._validate_school_status(batch_code))
        
        # 7. 检查孤立学校数据
        issues.extend(self._find_orphaned_schools(batch_code))
        
        # 8. 检查重复学校
        issues.extend(self._find_duplicate_schools(batch_code))
        
        # 生成报告
        report = self._generate_validation_report(batch_code, issues, start_time)
        
        logger.info(f"批次 {batch_code} 学校数据验证完成 - 状态: {report.validation_status}")
        return report
    
    def _validate_master_data_completeness(self, batch_code: str) -> List[SchoolValidationIssue]:
        """验证school_master_data的数据完整性"""
        issues = []
        
        try:
            # 检查必要字段的完整性
            query = text("""
                SELECT 
                    school_id,
                    standard_school_name,
                    school_type,
                    status,
                    data_source,
                    CASE 
                        WHEN school_id IS NULL OR school_id = '' THEN 'MISSING_SCHOOL_ID'
                        WHEN standard_school_name IS NULL OR standard_school_name = '' THEN 'MISSING_SCHOOL_NAME'
                        WHEN status IS NULL OR status = '' THEN 'MISSING_STATUS'
                        ELSE 'COMPLETE'
                    END as completeness_check
                FROM school_master_data 
                WHERE batch_code = :batch_code
                ORDER BY school_id
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            
            for row in result:
                school_id = row[0] or 'UNKNOWN'
                school_name = row[1] or 'UNKNOWN'
                school_type = row[2]
                status = row[3]
                data_source = row[4]
                completeness = row[5]
                
                if completeness != 'COMPLETE':
                    issue_type = completeness
                    if completeness == 'MISSING_SCHOOL_NAME':
                        severity = "CRITICAL"
                        description = "学校名称缺失，这会导致前端显示错误"
                        fix_suggestions = [
                            "立即修复缺失的学校名称",
                            "检查原始数据源",
                            "暂停此学校的数据处理"
                        ]
                    elif completeness == 'MISSING_SCHOOL_ID':
                        severity = "CRITICAL" 
                        description = "学校ID缺失，无法进行数据关联"
                        fix_suggestions = [
                            "补充正确的学校ID",
                            "检查数据导入流程",
                            "验证学校编码规则"
                        ]
                    else:
                        severity = "MEDIUM"
                        description = f"字段缺失: {completeness}"
                        fix_suggestions = ["补充缺失字段", "检查数据导入规则"]
                    
                    issues.append(SchoolValidationIssue(
                        issue_type=issue_type,
                        severity=severity,
                        school_code=school_id,
                        school_name=school_name,
                        description=description,
                        details={
                            'school_type': school_type,
                            'status': status,
                            'data_source': data_source,
                            'missing_field': completeness
                        },
                        fix_suggestions=fix_suggestions
                    ))
            
        except Exception as e:
            logger.error(f"验证master_data完整性时出错: {e}")
            issues.append(SchoolValidationIssue(
                issue_type="VALIDATION_ERROR",
                severity="HIGH",
                school_code="SYSTEM",
                school_name="SYSTEM",
                description=f"验证过程出错: {str(e)}",
                details={'error': str(e)},
                fix_suggestions=["检查数据库连接", "重新运行验证"]
            ))
        
        return issues
    
    def _validate_school_id_format(self, batch_code: str) -> List[SchoolValidationIssue]:
        """验证学校ID格式"""
        issues = []
        
        try:
            query = text("""
                SELECT 
                    school_id,
                    standard_school_name,
                    CASE 
                        WHEN school_id IS NULL THEN 'NULL_ID'
                        WHEN LENGTH(school_id) != 4 THEN 'INVALID_LENGTH'
                        WHEN school_id NOT REGEXP '^[0-9]{4}$' THEN 'INVALID_FORMAT'
                        WHEN CAST(school_id AS UNSIGNED) < :min_id THEN 'OUT_OF_RANGE_LOW'
                        WHEN CAST(school_id AS UNSIGNED) > :max_id THEN 'OUT_OF_RANGE_HIGH'
                        ELSE 'VALID'
                    END as format_check
                FROM school_master_data 
                WHERE batch_code = :batch_code
                AND status = 'ACTIVE'
                ORDER BY school_id
            """)
            
            result = self.db_session.execute(query, {
                'batch_code': batch_code,
                'min_id': self.school_id_range[0],
                'max_id': self.school_id_range[1]
            })
            
            for row in result:
                school_id = row[0]
                school_name = row[1]
                format_check = row[2]
                
                if format_check != 'VALID':
                    severity = "HIGH" if format_check in ['NULL_ID', 'INVALID_FORMAT'] else "MEDIUM"
                    
                    format_descriptions = {
                        'NULL_ID': '学校ID为空',
                        'INVALID_LENGTH': f'学校ID长度错误(应为4位): {school_id}',
                        'INVALID_FORMAT': f'学校ID格式错误(应为4位数字): {school_id}',
                        'OUT_OF_RANGE_LOW': f'学校ID过小(应在{self.school_id_range[0]}-{self.school_id_range[1]}范围内): {school_id}',
                        'OUT_OF_RANGE_HIGH': f'学校ID过大(应在{self.school_id_range[0]}-{self.school_id_range[1]}范围内): {school_id}'
                    }
                    
                    issues.append(SchoolValidationIssue(
                        issue_type="INVALID_SCHOOL_ID_FORMAT",
                        severity=severity,
                        school_code=school_id or 'UNKNOWN',
                        school_name=school_name,
                        description=format_descriptions.get(format_check, f"ID格式问题: {format_check}"),
                        details={
                            'format_issue': format_check,
                            'expected_pattern': self.school_id_pattern,
                            'expected_range': f"{self.school_id_range[0]}-{self.school_id_range[1]}"
                        },
                        fix_suggestions=[
                            "修正学校ID格式为4位数字",
                            f"确保学校ID在{self.school_id_range[0]}-{self.school_id_range[1]}范围内",
                            "检查学校编码规则",
                            "重新导入正确的学校数据"
                        ]
                    ))
        
        except Exception as e:
            logger.error(f"验证学校ID格式时出错: {e}")
            issues.append(SchoolValidationIssue(
                issue_type="VALIDATION_ERROR",
                severity="HIGH",
                school_code="SYSTEM",
                school_name="SYSTEM",
                description=f"ID格式验证出错: {str(e)}",
                details={'error': str(e)},
                fix_suggestions=["检查数据库连接", "重新运行验证"]
            ))
        
        return issues
    
    def _validate_school_name_standardization(self, batch_code: str) -> List[SchoolValidationIssue]:
        """验证学校名称标准化"""
        issues = []
        
        try:
            # 检查名称格式问题
            query = text("""
                SELECT 
                    school_id,
                    standard_school_name,
                    CASE 
                        WHEN standard_school_name IS NULL THEN 'NULL_NAME'
                        WHEN TRIM(standard_school_name) = '' THEN 'EMPTY_NAME'
                        WHEN LENGTH(standard_school_name) > 100 THEN 'NAME_TOO_LONG'
                        WHEN LENGTH(standard_school_name) < 3 THEN 'NAME_TOO_SHORT'
                        WHEN standard_school_name REGEXP '[0-9]+$' THEN 'NAME_ENDS_WITH_NUMBER'
                        WHEN standard_school_name LIKE '%学校%学校%' THEN 'DUPLICATE_SUFFIX'
                        ELSE 'VALID'
                    END as name_check
                FROM school_master_data 
                WHERE batch_code = :batch_code
                AND status = 'ACTIVE'
                ORDER BY school_id
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            
            for row in result:
                school_id = row[0]
                school_name = row[1]
                name_check = row[2]
                
                if name_check != 'VALID':
                    if name_check in ['NULL_NAME', 'EMPTY_NAME']:
                        severity = "CRITICAL"
                    elif name_check in ['NAME_TOO_LONG', 'NAME_TOO_SHORT']:
                        severity = "HIGH"
                    else:
                        severity = "MEDIUM"
                    
                    name_descriptions = {
                        'NULL_NAME': '学校名称为NULL',
                        'EMPTY_NAME': '学校名称为空字符串',
                        'NAME_TOO_LONG': f'学校名称过长(>100字符): {school_name}',
                        'NAME_TOO_SHORT': f'学校名称过短(<3字符): {school_name}',
                        'NAME_ENDS_WITH_NUMBER': f'学校名称以数字结尾: {school_name}',
                        'DUPLICATE_SUFFIX': f'学校名称有重复后缀: {school_name}'
                    }
                    
                    fix_suggestions = []
                    if name_check in ['NULL_NAME', 'EMPTY_NAME']:
                        fix_suggestions = [
                            "立即停止处理！NULL/空名称会导致前端错误",
                            "从原始数据源获取正确的学校名称",
                            "重新运行数据清洗流程"
                        ]
                    else:
                        fix_suggestions = [
                            "标准化学校名称格式",
                            "检查命名规则",
                            "修正格式问题"
                        ]
                    
                    issues.append(SchoolValidationIssue(
                        issue_type="INVALID_SCHOOL_NAME",
                        severity=severity,
                        school_code=school_id,
                        school_name=school_name,
                        description=name_descriptions.get(name_check, f"名称问题: {name_check}"),
                        details={
                            'name_issue': name_check,
                            'name_length': len(school_name) if school_name else 0
                        },
                        fix_suggestions=fix_suggestions
                    ))
        
        except Exception as e:
            logger.error(f"验证学校名称标准化时出错: {e}")
            issues.append(SchoolValidationIssue(
                issue_type="VALIDATION_ERROR",
                severity="HIGH",
                school_code="SYSTEM",
                school_name="SYSTEM",
                description=f"名称验证出错: {str(e)}",
                details={'error': str(e)},
                fix_suggestions=["检查数据库连接", "重新运行验证"]
            ))
        
        return issues
    
    def _validate_cross_table_consistency(self, batch_code: str) -> List[SchoolValidationIssue]:
        """验证跨表数据一致性"""
        issues = []
        
        try:
            # 检查student_cleaned_scores中学校名称与master_data不一致的情况
            query = text("""
                SELECT DISTINCT
                    scs.school_code,
                    scs.school_name as cleaned_name,
                    smd.standard_school_name as master_name,
                    COUNT(*) as affected_students
                FROM student_cleaned_scores scs
                INNER JOIN school_master_data smd 
                    ON smd.batch_code = scs.batch_code
                    AND smd.school_id = scs.school_code
                    AND smd.status = 'ACTIVE'
                WHERE scs.batch_code = :batch_code
                AND scs.school_name != smd.standard_school_name
                GROUP BY scs.school_code, scs.school_name, smd.standard_school_name
                ORDER BY affected_students DESC
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            
            for row in result:
                school_code = row[0]
                cleaned_name = row[1]
                master_name = row[2]
                affected_students = row[3]
                
                issues.append(SchoolValidationIssue(
                    issue_type="CROSS_TABLE_NAME_INCONSISTENCY",
                    severity="MEDIUM",
                    school_code=school_code,
                    school_name=cleaned_name,
                    description=f"学校名称不一致 - 清洗表: '{cleaned_name}' vs 主表: '{master_name}'",
                    details={
                        'cleaned_name': cleaned_name,
                        'master_name': master_name,
                        'affected_students': affected_students
                    },
                    fix_suggestions=[
                        "统一使用school_master_data中的标准名称",
                        "更新student_cleaned_scores中的学校名称",
                        "检查数据清洗规则"
                    ]
                ))
            
            # 检查在其他表中存在但在master_data中不存在的学校
            orphan_query = text("""
                SELECT DISTINCT
                    scs.school_code,
                    scs.school_name,
                    COUNT(*) as student_count
                FROM student_cleaned_scores scs
                LEFT JOIN school_master_data smd 
                    ON smd.batch_code = scs.batch_code
                    AND smd.school_id = scs.school_code
                    AND smd.status = 'ACTIVE'
                WHERE scs.batch_code = :batch_code
                AND smd.school_id IS NULL
                GROUP BY scs.school_code, scs.school_name
                ORDER BY student_count DESC
            """)
            
            result = self.db_session.execute(orphan_query, {'batch_code': batch_code})
            
            for row in result:
                school_code = row[0]
                school_name = row[1]
                student_count = row[2]
                
                issues.append(SchoolValidationIssue(
                    issue_type="ORPHANED_SCHOOL",
                    severity="CRITICAL",
                    school_code=school_code,
                    school_name=school_name,
                    description=f"学校在student_cleaned_scores中存在但不在school_master_data中",
                    details={
                        'student_count': student_count,
                        'table': 'student_cleaned_scores'
                    },
                    fix_suggestions=[
                        "立即停止处理！孤立学校会导致汇聚错误",
                        "将学校添加到school_master_data",
                        "或从student_cleaned_scores中删除无效数据",
                        "检查数据导入流程"
                    ]
                ))
        
        except Exception as e:
            logger.error(f"验证跨表一致性时出错: {e}")
            issues.append(SchoolValidationIssue(
                issue_type="VALIDATION_ERROR",
                severity="HIGH",
                school_code="SYSTEM",
                school_name="SYSTEM",
                description=f"跨表验证出错: {str(e)}",
                details={'error': str(e)},
                fix_suggestions=["检查数据库连接", "重新运行验证"]
            ))
        
        return issues
    
    def _validate_data_source_consistency(self, batch_code: str) -> List[SchoolValidationIssue]:
        """验证数据源一致性"""
        issues = []
        
        try:
            query = text("""
                SELECT 
                    school_id,
                    standard_school_name,
                    data_source,
                    COUNT(*) as count
                FROM school_master_data 
                WHERE batch_code = :batch_code
                AND status = 'ACTIVE'
                GROUP BY school_id, standard_school_name, data_source
                HAVING COUNT(*) > 1
                ORDER BY school_id
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            
            for row in result:
                school_id = row[0]
                school_name = row[1]
                data_source = row[2]
                count = row[3]
                
                if count > 1:
                    issues.append(SchoolValidationIssue(
                        issue_type="DUPLICATE_SCHOOL_ENTRY",
                        severity="HIGH",
                        school_code=school_id,
                        school_name=school_name,
                        description=f"学校在master_data中有 {count} 条记录",
                        details={
                            'data_source': data_source,
                            'duplicate_count': count
                        },
                        fix_suggestions=[
                            "删除重复的学校记录",
                            "保留最新或最准确的记录",
                            "检查数据导入逻辑"
                        ]
                    ))
            
            # 检查data_source字段的多样性
            source_query = text("""
                SELECT 
                    data_source,
                    COUNT(*) as school_count
                FROM school_master_data 
                WHERE batch_code = :batch_code
                AND status = 'ACTIVE'
                GROUP BY data_source
                ORDER BY school_count DESC
            """)
            
            result = self.db_session.execute(source_query, {'batch_code': batch_code})
            data_sources = []
            
            for row in result:
                data_sources.append({
                    'source': row[0] or 'NULL',
                    'count': row[1]
                })
            
            if len(data_sources) > 3:  # 如果有超过3个不同的数据源，可能有问题
                issues.append(SchoolValidationIssue(
                    issue_type="MULTIPLE_DATA_SOURCES",
                    severity="MEDIUM",
                    school_code="BATCH",
                    school_name="BATCH",
                    description=f"批次中有 {len(data_sources)} 个不同的数据源",
                    details={'data_sources': data_sources},
                    fix_suggestions=[
                        "统一数据源标识",
                        "检查是否有异常的数据导入",
                        "验证数据来源的合理性"
                    ]
                ))
        
        except Exception as e:
            logger.error(f"验证数据源一致性时出错: {e}")
            issues.append(SchoolValidationIssue(
                issue_type="VALIDATION_ERROR",
                severity="HIGH",
                school_code="SYSTEM",
                school_name="SYSTEM",
                description=f"数据源验证出错: {str(e)}",
                details={'error': str(e)},
                fix_suggestions=["检查数据库连接", "重新运行验证"]
            ))
        
        return issues
    
    def _validate_school_status(self, batch_code: str) -> List[SchoolValidationIssue]:
        """验证学校状态"""
        issues = []
        
        try:
            query = text("""
                SELECT 
                    school_id,
                    standard_school_name,
                    status,
                    COUNT(*) as count
                FROM school_master_data 
                WHERE batch_code = :batch_code
                GROUP BY school_id, standard_school_name, status
                ORDER BY school_id, status
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            
            for row in result:
                school_id = row[0]
                school_name = row[1]
                status = row[2]
                count = row[3]
                
                if status != 'ACTIVE':
                    severity = "HIGH" if status in ['INACTIVE', 'DELETED'] else "MEDIUM"
                    issues.append(SchoolValidationIssue(
                        issue_type="INVALID_SCHOOL_STATUS",
                        severity=severity,
                        school_code=school_id,
                        school_name=school_name,
                        description=f"学校状态异常: {status}",
                        details={'status': status, 'count': count},
                        fix_suggestions=[
                            "检查学校状态是否应该为ACTIVE",
                            "如果学校确实无效，从处理范围中排除",
                            "更新学校状态"
                        ]
                    ))
        
        except Exception as e:
            logger.error(f"验证学校状态时出错: {e}")
            issues.append(SchoolValidationIssue(
                issue_type="VALIDATION_ERROR",
                severity="HIGH",
                school_code="SYSTEM",
                school_name="SYSTEM",
                description=f"状态验证出错: {str(e)}",
                details={'error': str(e)},
                fix_suggestions=["检查数据库连接", "重新运行验证"]
            ))
        
        return issues
    
    def _find_orphaned_schools(self, batch_code: str) -> List[SchoolValidationIssue]:
        """查找孤立的学校数据"""
        issues = []
        
        try:
            # 在statistical_aggregations中查找孤立学校
            query = text("""
                SELECT DISTINCT
                    sa.school_id,
                    sa.school_name,
                    COUNT(*) as aggregation_count
                FROM statistical_aggregations sa
                LEFT JOIN school_master_data smd 
                    ON smd.batch_code = sa.batch_code
                    AND smd.school_id = sa.school_id
                    AND smd.status = 'ACTIVE'
                WHERE sa.batch_code = :batch_code
                AND smd.school_id IS NULL
                GROUP BY sa.school_id, sa.school_name
                ORDER BY aggregation_count DESC
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            
            for row in result:
                school_id = row[0]
                school_name = row[1]
                aggregation_count = row[2]
                
                issues.append(SchoolValidationIssue(
                    issue_type="ORPHANED_AGGREGATION",
                    severity="CRITICAL",
                    school_code=school_id,
                    school_name=school_name,
                    description=f"学校在statistical_aggregations中存在但不在school_master_data中",
                    details={
                        'aggregation_count': aggregation_count,
                        'table': 'statistical_aggregations'
                    },
                    fix_suggestions=[
                        "立即检查！这表明数据汇聚过程有问题",
                        "添加学校到school_master_data",
                        "或删除无效的汇聚记录",
                        "检查汇聚逻辑是否正确"
                    ]
                ))
        
        except Exception as e:
            # statistical_aggregations表可能不存在，这是正常的
            logger.warning(f"查找孤立学校时出错（可能表不存在）: {e}")
        
        return issues
    
    def _find_duplicate_schools(self, batch_code: str) -> List[SchoolValidationIssue]:
        """查找重复的学校"""
        issues = []
        
        try:
            # 按学校名称查找可能的重复
            query = text("""
                SELECT 
                    standard_school_name,
                    GROUP_CONCAT(school_id ORDER BY school_id) as school_ids,
                    COUNT(*) as count
                FROM school_master_data 
                WHERE batch_code = :batch_code
                AND status = 'ACTIVE'
                GROUP BY standard_school_name
                HAVING COUNT(*) > 1
                ORDER BY count DESC, standard_school_name
            """)
            
            result = self.db_session.execute(query, {'batch_code': batch_code})
            
            for row in result:
                school_name = row[0]
                school_ids = row[1]
                count = row[2]
                
                issues.append(SchoolValidationIssue(
                    issue_type="DUPLICATE_SCHOOL_NAME",
                    severity="HIGH",
                    school_code="MULTIPLE",
                    school_name=school_name,
                    description=f"学校名称重复 ({count}次): {school_ids}",
                    details={
                        'duplicate_school_ids': school_ids.split(','),
                        'duplicate_count': count
                    },
                    fix_suggestions=[
                        "检查是否为同一学校的不同编码",
                        "标准化学校名称",
                        "合并重复的学校记录",
                        "检查学校编码规则"
                    ]
                ))
        
        except Exception as e:
            logger.error(f"查找重复学校时出错: {e}")
            issues.append(SchoolValidationIssue(
                issue_type="VALIDATION_ERROR",
                severity="HIGH",
                school_code="SYSTEM",
                school_name="SYSTEM",
                description=f"重复检查出错: {str(e)}",
                details={'error': str(e)},
                fix_suggestions=["检查数据库连接", "重新运行验证"]
            ))
        
        return issues
    
    def _generate_validation_report(self, batch_code: str, issues: List[SchoolValidationIssue], start_time: datetime) -> SchoolValidationReport:
        """生成验证报告"""
        end_time = datetime.now()
        
        # 统计结果
        total_issues = len(issues)
        critical_issues = len([i for i in issues if i.severity == "CRITICAL"])
        
        # 确定验证状态
        if critical_issues > 0:
            validation_status = "FAIL"
        elif any(i.severity in ["HIGH", "MEDIUM"] for i in issues):
            validation_status = "WARN"
        else:
            validation_status = "PASS"
        
        # 获取学校总数
        try:
            query = text("SELECT COUNT(DISTINCT school_id) FROM school_master_data WHERE batch_code = :batch_code AND status = 'ACTIVE'")
            total_schools = self.db_session.execute(query, {'batch_code': batch_code}).scalar() or 0
        except:
            total_schools = 0
        
        # 生成统计摘要
        severity_counts = {}
        issue_type_counts = {}
        
        for issue in issues:
            severity_counts[issue.severity] = severity_counts.get(issue.severity, 0) + 1
            issue_type_counts[issue.issue_type] = issue_type_counts.get(issue.issue_type, 0) + 1
        
        summary = {
            'processing_time_seconds': (end_time - start_time).total_seconds(),
            'severity_distribution': severity_counts,
            'issue_type_distribution': issue_type_counts,
            'schools_with_issues': len(set(i.school_code for i in issues if i.school_code != 'SYSTEM')),
            'issue_rate_percentage': round((total_issues / total_schools * 100) if total_schools > 0 else 0, 2)
        }
        
        # 生成综合建议
        recommendations = []
        if critical_issues > 0:
            recommendations.extend([
                "立即停止数据处理！发现关键问题",
                "修复所有CRITICAL级别的问题",
                "重新运行数据清洗和验证流程"
            ])
        
        if any(i.issue_type == "ORPHANED_SCHOOL" for i in issues):
            recommendations.append("修复school_master_data中缺失的学校记录")
        
        if any(i.issue_type.startswith("INVALID_SCHOOL") for i in issues):
            recommendations.append("检查并修正学校基础信息格式")
        
        if any(i.issue_type == "CROSS_TABLE_NAME_INCONSISTENCY" for i in issues):
            recommendations.append("统一所有表中的学校名称标准")
        
        return SchoolValidationReport(
            batch_code=batch_code,
            validation_timestamp=end_time,
            total_schools_checked=total_schools,
            issues_found=total_issues,
            critical_issues=critical_issues,
            validation_status=validation_status,
            issues=issues,
            summary=summary,
            recommendations=recommendations
        )
    
    def generate_fix_script(self, report: SchoolValidationReport, output_file: str = None) -> str:
        """生成修复脚本"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"school_data_fix_script_{report.batch_code}_{timestamp}.sql"
        
        script_lines = [
            "-- 学校数据修复脚本",
            f"-- 批次: {report.batch_code}",
            f"-- 生成时间: {report.validation_timestamp}",
            f"-- 发现问题: {report.issues_found}个",
            "",
            "-- 注意: 执行前请备份相关数据表",
            "-- 建议在测试环境先验证修复效果",
            "",
            "START TRANSACTION;",
            ""
        ]
        
        # 按问题类型生成修复语句
        for issue in report.issues:
            if issue.issue_type == "MISSING_SCHOOL_NAME" and issue.severity == "CRITICAL":
                script_lines.extend([
                    f"-- 修复学校 {issue.school_code} 的缺失名称",
                    f"-- 当前状态: {issue.description}",
                    f"UPDATE school_master_data SET standard_school_name = '待补充学校名称_{issue.school_code}' WHERE batch_code = '{report.batch_code}' AND school_id = '{issue.school_code}' AND (standard_school_name IS NULL OR standard_school_name = '');",
                    ""
                ])
            
            elif issue.issue_type == "ORPHANED_SCHOOL" and issue.severity == "CRITICAL":
                script_lines.extend([
                    f"-- 添加孤立学校 {issue.school_code} 到 school_master_data",
                    f"INSERT INTO school_master_data (batch_code, school_id, standard_school_name, school_type, status, data_source, created_at, updated_at)",
                    f"VALUES ('{report.batch_code}', '{issue.school_code}', '{issue.school_name or '待确认学校名称'}', 'MIDDLE_SCHOOL', 'ACTIVE', 'FIX_SCRIPT', NOW(), NOW())",
                    f"ON DUPLICATE KEY UPDATE updated_at = NOW();",
                    ""
                ])
        
        script_lines.extend([
            "-- 提交更改 (如果确认无误，可以取消注释下一行)",
            "-- COMMIT;",
            "",
            "-- 如果有问题，可以回滚",
            "-- ROLLBACK;",
            "",
            f"-- 修复完成后，请重新运行验证: python school_data_validator.py {report.batch_code}"
        ])
        
        script_content = "\n".join(script_lines)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(script_content)
        
        logger.info(f"修复脚本已生成: {output_file}")
        return output_file


def main():
    """主函数 - 命令行接口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="学校数据验证工具")
    parser.add_argument("batch_code", help="批次代码，如 G4-2025")
    parser.add_argument("--save-report", action="store_true", help="保存详细报告到JSON文件")
    parser.add_argument("--generate-fix-script", action="store_true", help="生成修复脚本")
    parser.add_argument("--verbose", action="store_true", help="显示详细信息")
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    try:
        with next(get_db()) as db:
            validator = SchoolDataValidator(db)
            report = validator.validate_batch_schools(args.batch_code)
            
            # 打印报告摘要
            print("="*80)
            print(f"学校数据验证报告 - 批次: {report.batch_code}")
            print("="*80)
            print(f"验证时间: {report.validation_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"验证状态: {report.validation_status}")
            print(f"处理时间: {report.summary['processing_time_seconds']:.2f}秒")
            print()
            
            print(f"学校总数: {report.total_schools_checked}")
            print(f"发现问题: {report.issues_found}个")
            print(f"关键问题: {report.critical_issues}个")
            print(f"问题率: {report.summary['issue_rate_percentage']}%")
            print()
            
            # 按严重程度显示问题统计
            if report.summary['severity_distribution']:
                print("问题严重程度分布:")
                severity_order = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
                for severity in severity_order:
                    count = report.summary['severity_distribution'].get(severity, 0)
                    if count > 0:
                        severity_icon = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}[severity]
                        print(f"  {severity_icon} {severity}: {count}个")
                print()
            
            # 显示关键和高级问题
            critical_and_high = [i for i in report.issues if i.severity in ['CRITICAL', 'HIGH']]
            if critical_and_high:
                print(f"⚠️  关键和高级问题 ({len(critical_and_high)}个):")
                print("-" * 80)
                for issue in critical_and_high[:10]:  # 只显示前10个
                    severity_icon = {"CRITICAL": "🔴", "HIGH": "🟠"}[issue.severity]
                    print(f"{severity_icon} [{issue.severity}] {issue.school_code}: {issue.description}")
                
                if len(critical_and_high) > 10:
                    print(f"... 还有 {len(critical_and_high) - 10} 个问题")
                print()
            
            # 显示建议
            if report.recommendations:
                print("💡 修复建议:")
                for i, rec in enumerate(report.recommendations, 1):
                    print(f"  {i}. {rec}")
                print()
            
            # 显示问题类型分布
            if args.verbose and report.summary['issue_type_distribution']:
                print("问题类型分布:")
                for issue_type, count in sorted(report.summary['issue_type_distribution'].items(), key=lambda x: x[1], reverse=True):
                    print(f"  • {issue_type}: {count}个")
                print()
            
            # 保存报告
            if args.save_report:
                timestamp = report.validation_timestamp.strftime("%Y%m%d_%H%M%S")
                filename = f"school_validation_report_{report.batch_code}_{timestamp}.json"
                
                # 序列化报告
                report_dict = asdict(report)
                report_dict['validation_timestamp'] = report.validation_timestamp.isoformat()
                
                os.makedirs("validation_reports", exist_ok=True)
                filepath = os.path.join("validation_reports", filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(report_dict, f, ensure_ascii=False, indent=2)
                
                print(f"详细报告已保存到: {filepath}")
            
            # 生成修复脚本
            if args.generate_fix_script and report.critical_issues > 0:
                fix_script_path = validator.generate_fix_script(report)
                print(f"修复脚本已生成: {fix_script_path}")
            
            # 设置退出代码
            if report.critical_issues > 0:
                print("\n❌ 发现关键问题，建议立即修复！")
                sys.exit(2)  # 关键问题
            elif report.validation_status == "FAIL":
                print("\n⚠️  验证失败，需要处理问题后重新验证")
                sys.exit(1)  # 一般失败
            elif report.validation_status == "WARN":
                print("\n⚠️  验证通过但有警告，建议处理后再继续")
                sys.exit(0)  # 警告但可继续
            else:
                print("\n✅ 学校数据验证通过！")
                sys.exit(0)  # 成功
                
    except Exception as e:
        logger.error(f"验证过程失败: {e}")
        sys.exit(3)


if __name__ == "__main__":
    main()