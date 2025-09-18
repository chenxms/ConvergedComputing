#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据治理配置文件 - 定义数据质量规范和检查规则
基于G4数据问题的经验教训制定的标准化配置
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum
import json


class Severity(Enum):
    """问题严重程度枚举"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ActionType(Enum):
    """处理动作类型枚举"""
    BLOCK = "BLOCK"  # 阻断处理
    WARN = "WARN"   # 发出警告但继续
    LOG = "LOG"     # 仅记录日志
    FIX = "FIX"     # 自动修复


@dataclass
class QualityRule:
    """数据质量规则定义"""
    rule_id: str
    name: str
    description: str
    severity: Severity
    action: ActionType
    threshold_value: Optional[float]
    threshold_operator: str  # "gt", "lt", "eq", "ne", "gte", "lte"
    check_sql: Optional[str] = None
    fix_sql: Optional[str] = None
    recommendations: List[str] = None
    
    def __post_init__(self):
        if self.recommendations is None:
            self.recommendations = []


@dataclass
class ProcessingStage:
    """处理阶段定义"""
    stage_id: str
    name: str
    description: str
    required_tables: List[str]
    quality_rules: List[str]  # rule_ids
    blocking_rules: List[str]  # rule_ids that block processing
    dependencies: List[str] = None  # other stage_ids
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []


@dataclass
class DataGovernanceConfig:
    """数据治理配置"""
    version: str
    description: str
    quality_rules: Dict[str, QualityRule]
    processing_stages: Dict[str, ProcessingStage]
    global_settings: Dict[str, Any]
    alert_settings: Dict[str, Any]


class DataGovernanceConfigManager:
    """数据治理配置管理器"""
    
    def __init__(self):
        self.config = self._create_default_config()
    
    def _create_default_config(self) -> DataGovernanceConfig:
        """创建默认的数据治理配置"""
        
        # 定义质量规则
        quality_rules = {
            # 关键规则：NULL学校名称（零容忍）
            "NULL_SCHOOL_NAMES": QualityRule(
                rule_id="NULL_SCHOOL_NAMES",
                name="NULL学校名称检查",
                description="检查各表中是否存在NULL或空的学校名称，这会导致前端显示错误",
                severity=Severity.CRITICAL,
                action=ActionType.BLOCK,
                threshold_value=0.0,
                threshold_operator="gt",
                check_sql="""
                    SELECT 
                        'student_cleaned_scores' as table_name,
                        COUNT(*) as null_count
                    FROM student_cleaned_scores 
                    WHERE batch_code = %(batch_code)s
                    AND (school_name IS NULL OR school_name = '' OR TRIM(school_name) = '')
                    
                    UNION ALL
                    
                    SELECT 
                        'school_master_data' as table_name,
                        COUNT(*) as null_count
                    FROM school_master_data 
                    WHERE batch_code = %(batch_code)s 
                    AND (standard_school_name IS NULL OR standard_school_name = '' OR TRIM(standard_school_name) = '')
                """,
                recommendations=[
                    "立即停止处理！NULL学校名称会导致前端显示错误",
                    "从原始数据源获取正确的学校名称",
                    "更新school_master_data中的标准化名称",
                    "重新运行数据清洗流程",
                    "添加数据库约束防止NULL值"
                ]
            ),
            
            # 关键规则：孤立学校（零容忍）
            "ORPHANED_SCHOOLS": QualityRule(
                rule_id="ORPHANED_SCHOOLS",
                name="孤立学校检查",
                description="检查在student_cleaned_scores中存在但不在school_master_data中的学校",
                severity=Severity.CRITICAL,
                action=ActionType.BLOCK,
                threshold_value=0.0,
                threshold_operator="gt",
                check_sql="""
                    SELECT COUNT(DISTINCT scs.school_code) as orphaned_count
                    FROM student_cleaned_scores scs
                    LEFT JOIN school_master_data smd 
                        ON smd.batch_code = scs.batch_code
                        AND smd.school_id = scs.school_code
                        AND smd.status = 'ACTIVE'
                    WHERE scs.batch_code = %(batch_code)s
                        AND smd.school_id IS NULL
                """,
                recommendations=[
                    "立即停止处理！孤立学校会导致汇聚错误",
                    "将缺失的学校添加到school_master_data",
                    "或从student_cleaned_scores中删除无效数据",
                    "检查数据导入流程",
                    "验证学校编码映射的正确性"
                ]
            ),
            
            # 高级规则：学校ID格式
            "INVALID_SCHOOL_ID_FORMAT": QualityRule(
                rule_id="INVALID_SCHOOL_ID_FORMAT",
                name="学校ID格式检查",
                description="检查学校ID是否符合4位数字格式且在有效范围内(5044-5099)",
                severity=Severity.HIGH,
                action=ActionType.WARN,
                threshold_value=0.0,
                threshold_operator="gt",
                check_sql="""
                    SELECT COUNT(*) as invalid_count
                    FROM school_master_data 
                    WHERE batch_code = %(batch_code)s
                        AND status = 'ACTIVE'
                        AND (
                            LENGTH(school_id) != 4 
                            OR school_id NOT REGEXP '^[0-9]{4}$'
                            OR CAST(school_id AS UNSIGNED) < 5044 
                            OR CAST(school_id AS UNSIGNED) > 5099
                        )
                """,
                recommendations=[
                    "修正学校ID格式为4位数字",
                    "确保学校ID在5044-5099范围内",
                    "检查学校编码规则",
                    "重新导入正确的学校数据"
                ]
            ),
            
            # 中级规则：学校数量偏差
            "SCHOOL_COUNT_DEVIATION": QualityRule(
                rule_id="SCHOOL_COUNT_DEVIATION",
                name="学校数量偏差检查",
                description="检查当前批次学校数量与历史平均值的偏差",
                severity=Severity.MEDIUM,
                action=ActionType.WARN,
                threshold_value=5.0,  # 5%偏差阈值
                threshold_operator="gt",
                recommendations=[
                    "检查是否有学校数据缺失",
                    "验证批次范围是否正确",
                    "确认是否有新增或减少的学校",
                    "与历史批次进行对比分析"
                ]
            ),
            
            # 中级规则：学生数据分布
            "LOW_STUDENT_SCHOOLS": QualityRule(
                rule_id="LOW_STUDENT_SCHOOLS",
                name="学生数据分布检查",
                description="检查学生数过少的学校",
                severity=Severity.MEDIUM,
                action=ActionType.WARN,
                threshold_value=10.0,  # 每校最少10个学生
                threshold_operator="lt",
                check_sql="""
                    SELECT COUNT(*) as low_student_schools
                    FROM (
                        SELECT 
                            school_code,
                            COUNT(*) as student_count
                        FROM student_cleaned_scores 
                        WHERE batch_code = %(batch_code)s
                        GROUP BY school_code
                        HAVING student_count < %(threshold)s
                    ) as school_stats
                """,
                recommendations=[
                    "检查学生数过少的学校是否正常",
                    "验证学校规模是否合理",
                    "确认数据完整性",
                    "考虑是否需要排除小规模学校"
                ]
            ),
            
            # 中级规则：跨表名称一致性
            "NAME_INCONSISTENCY": QualityRule(
                rule_id="NAME_INCONSISTENCY",
                name="跨表学校名称一致性检查",
                description="检查不同表中学校名称的一致性",
                severity=Severity.MEDIUM,
                action=ActionType.WARN,
                threshold_value=0.0,
                threshold_operator="gt",
                check_sql="""
                    SELECT COUNT(*) as inconsistent_count
                    FROM student_cleaned_scores scs
                    INNER JOIN school_master_data smd 
                        ON smd.batch_code = scs.batch_code
                        AND smd.school_id = scs.school_code
                        AND smd.status = 'ACTIVE'
                    WHERE scs.batch_code = %(batch_code)s
                    AND scs.school_name != smd.standard_school_name
                """,
                recommendations=[
                    "统一使用school_master_data中的标准名称",
                    "更新student_cleaned_scores中的学校名称",
                    "检查数据清洗规则",
                    "标准化学校名称格式"
                ]
            ),
            
            # 高级规则：汇聚完整性
            "AGGREGATION_COMPLETENESS": QualityRule(
                rule_id="AGGREGATION_COMPLETENESS",
                name="汇聚完整性检查",
                description="检查是否所有学校都完成了统计汇聚",
                severity=Severity.HIGH,
                action=ActionType.WARN,
                threshold_value=100.0,  # 100%完整率
                threshold_operator="lt",
                check_sql="""
                    SELECT 
                        (COUNT(DISTINCT sa.school_id) / COUNT(DISTINCT smd.school_id) * 100) as completeness_rate
                    FROM school_master_data smd
                    LEFT JOIN statistical_aggregations sa 
                        ON sa.batch_code = smd.batch_code
                        AND sa.school_id = smd.school_id
                    WHERE smd.batch_code = %(batch_code)s
                        AND smd.status = 'ACTIVE'
                """,
                recommendations=[
                    "检查汇聚过程是否完整",
                    "重新运行未完成的学校汇聚",
                    "验证汇聚逻辑是否正确",
                    "检查是否有处理失败的学校"
                ]
            ),
            
            # 高级规则：JSON格式验证
            "INVALID_JSON_FORMAT": QualityRule(
                rule_id="INVALID_JSON_FORMAT",
                name="JSON格式验证",
                description="检查统计结果JSON格式的有效性",
                severity=Severity.HIGH,
                action=ActionType.WARN,
                threshold_value=0.0,
                threshold_operator="gt",
                recommendations=[
                    "修复无效的JSON格式",
                    "重新生成统计结果",
                    "检查序列化过程",
                    "验证数据类型转换"
                ]
            ),
            
            # 关键规则：后处理NULL名称
            "POST_PROCESSING_NULL_NAMES": QualityRule(
                rule_id="POST_PROCESSING_NULL_NAMES",
                name="处理后NULL名称检查",
                description="检查处理完成后是否仍有NULL学校名称",
                severity=Severity.CRITICAL,
                action=ActionType.BLOCK,
                threshold_value=0.0,
                threshold_operator="gt",
                check_sql="""
                    SELECT COUNT(*) as null_count
                    FROM statistical_aggregations 
                    WHERE batch_code = %(batch_code)s
                    AND (school_name IS NULL OR school_name = '' OR TRIM(school_name) = '')
                """,
                recommendations=[
                    "处理失败！输出结果中仍有NULL学校名称",
                    "检查数据处理管道",
                    "重新运行完整的处理流程",
                    "修复数据源问题"
                ]
            )
        }
        
        # 定义处理阶段
        processing_stages = {
            "DATA_PREPARATION": ProcessingStage(
                stage_id="DATA_PREPARATION",
                name="数据准备阶段",
                description="准备和验证原始数据",
                required_tables=["school_master_data", "student_score_detail"],
                quality_rules=["NULL_SCHOOL_NAMES", "INVALID_SCHOOL_ID_FORMAT", "SCHOOL_COUNT_DEVIATION"],
                blocking_rules=["NULL_SCHOOL_NAMES"]
            ),
            
            "DATA_CLEANING": ProcessingStage(
                stage_id="DATA_CLEANING",
                name="数据清洗阶段",
                description="清洗和标准化学生数据",
                required_tables=["student_cleaned_scores"],
                quality_rules=["ORPHANED_SCHOOLS", "LOW_STUDENT_SCHOOLS", "NAME_INCONSISTENCY"],
                blocking_rules=["ORPHANED_SCHOOLS"],
                dependencies=["DATA_PREPARATION"]
            ),
            
            "DATA_AGGREGATION": ProcessingStage(
                stage_id="DATA_AGGREGATION",
                name="数据汇聚阶段",
                description="进行统计计算和数据汇聚",
                required_tables=["statistical_aggregations"],
                quality_rules=["AGGREGATION_COMPLETENESS"],
                blocking_rules=[],
                dependencies=["DATA_CLEANING"]
            ),
            
            "RESULT_SERIALIZATION": ProcessingStage(
                stage_id="RESULT_SERIALIZATION",
                name="结果序列化阶段",
                description="生成JSON格式的统计结果",
                required_tables=["statistical_aggregations"],
                quality_rules=["INVALID_JSON_FORMAT", "POST_PROCESSING_NULL_NAMES"],
                blocking_rules=["POST_PROCESSING_NULL_NAMES"],
                dependencies=["DATA_AGGREGATION"]
            )
        }
        
        # 全局设置
        global_settings = {
            "school_id_pattern": r"^[0-9]{4}$",
            "school_id_range": [5044, 5099],
            "min_student_per_school": 10,
            "max_school_count_deviation_percent": 5.0,
            "min_data_completeness_rate": 95.0,
            "processing_timeout_minutes": 30,
            "retry_count": 3,
            "backup_before_fix": True
        }
        
        # 警报设置
        alert_settings = {
            "email_enabled": False,
            "email_recipients": [],
            "sms_enabled": False,
            "sms_recipients": [],
            "log_level": "INFO",
            "alert_threshold": Severity.HIGH.value,
            "notification_cooldown_minutes": 60
        }
        
        return DataGovernanceConfig(
            version="1.0.0",
            description="基于G4数据问题经验教训制定的数据治理配置",
            quality_rules=quality_rules,
            processing_stages=processing_stages,
            global_settings=global_settings,
            alert_settings=alert_settings
        )
    
    def get_rules_for_stage(self, stage_id: str) -> List[QualityRule]:
        """获取指定阶段的质量规则"""
        if stage_id not in self.config.processing_stages:
            return []
        
        stage = self.config.processing_stages[stage_id]
        return [self.config.quality_rules[rule_id] for rule_id in stage.quality_rules if rule_id in self.config.quality_rules]
    
    def get_blocking_rules_for_stage(self, stage_id: str) -> List[QualityRule]:
        """获取指定阶段的阻断规则"""
        if stage_id not in self.config.processing_stages:
            return []
        
        stage = self.config.processing_stages[stage_id]
        return [self.config.quality_rules[rule_id] for rule_id in stage.blocking_rules if rule_id in self.config.quality_rules]
    
    def get_stage_dependencies(self, stage_id: str) -> List[str]:
        """获取指定阶段的依赖"""
        if stage_id not in self.config.processing_stages:
            return []
        
        return self.config.processing_stages[stage_id].dependencies
    
    def validate_rule_execution_order(self) -> List[str]:
        """验证规则执行顺序的合理性"""
        issues = []
        
        # 检查阶段依赖的循环
        def has_circular_dependency(stage_id: str, visited: set, path: list) -> bool:
            if stage_id in path:
                return True
            if stage_id in visited:
                return False
            
            visited.add(stage_id)
            path.append(stage_id)
            
            for dep in self.get_stage_dependencies(stage_id):
                if has_circular_dependency(dep, visited, path):
                    return True
            
            path.remove(stage_id)
            return False
        
        for stage_id in self.config.processing_stages:
            if has_circular_dependency(stage_id, set(), []):
                issues.append(f"阶段 {stage_id} 存在循环依赖")
        
        # 检查规则引用的有效性
        for stage_id, stage in self.config.processing_stages.items():
            for rule_id in stage.quality_rules:
                if rule_id not in self.config.quality_rules:
                    issues.append(f"阶段 {stage_id} 引用了不存在的规则 {rule_id}")
            
            for rule_id in stage.blocking_rules:
                if rule_id not in self.config.quality_rules:
                    issues.append(f"阶段 {stage_id} 引用了不存在的阻断规则 {rule_id}")
                elif rule_id not in stage.quality_rules:
                    issues.append(f"阶段 {stage_id} 的阻断规则 {rule_id} 不在质量规则列表中")
        
        return issues
    
    def save_to_file(self, filepath: str):
        """保存配置到文件"""
        config_dict = {
            'version': self.config.version,
            'description': self.config.description,
            'quality_rules': {k: asdict(v) for k, v in self.config.quality_rules.items()},
            'processing_stages': {k: asdict(v) for k, v in self.config.processing_stages.items()},
            'global_settings': self.config.global_settings,
            'alert_settings': self.config.alert_settings
        }
        
        # 转换枚举值为字符串
        for rule_dict in config_dict['quality_rules'].values():
            rule_dict['severity'] = rule_dict['severity'].value if hasattr(rule_dict['severity'], 'value') else rule_dict['severity']
            rule_dict['action'] = rule_dict['action'].value if hasattr(rule_dict['action'], 'value') else rule_dict['action']
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(config_dict, f, ensure_ascii=False, indent=2)
    
    def load_from_file(self, filepath: str):
        """从文件加载配置"""
        with open(filepath, 'r', encoding='utf-8') as f:
            config_dict = json.load(f)
        
        # 重建质量规则对象
        quality_rules = {}
        for rule_id, rule_dict in config_dict['quality_rules'].items():
            rule_dict['severity'] = Severity(rule_dict['severity'])
            rule_dict['action'] = ActionType(rule_dict['action'])
            quality_rules[rule_id] = QualityRule(**rule_dict)
        
        # 重建处理阶段对象
        processing_stages = {}
        for stage_id, stage_dict in config_dict['processing_stages'].items():
            processing_stages[stage_id] = ProcessingStage(**stage_dict)
        
        self.config = DataGovernanceConfig(
            version=config_dict['version'],
            description=config_dict['description'],
            quality_rules=quality_rules,
            processing_stages=processing_stages,
            global_settings=config_dict['global_settings'],
            alert_settings=config_dict['alert_settings']
        )
    
    def get_processing_order(self) -> List[str]:
        """获取处理阶段的正确执行顺序"""
        stages = list(self.config.processing_stages.keys())
        ordered_stages = []
        remaining_stages = set(stages)
        
        while remaining_stages:
            # 找到没有未满足依赖的阶段
            ready_stages = []
            for stage in remaining_stages:
                dependencies = self.get_stage_dependencies(stage)
                if all(dep in ordered_stages for dep in dependencies):
                    ready_stages.append(stage)
            
            if not ready_stages:
                # 如果没有就绪的阶段，可能有循环依赖
                raise ValueError(f"无法确定处理顺序，可能存在循环依赖。剩余阶段: {remaining_stages}")
            
            # 按阶段ID排序以确保一致性
            ready_stages.sort()
            ordered_stages.extend(ready_stages)
            remaining_stages -= set(ready_stages)
        
        return ordered_stages


def main():
    """主函数 - 生成和验证配置"""
    import argparse
    
    parser = argparse.ArgumentParser(description="数据治理配置管理工具")
    parser.add_argument("--generate", action="store_true", help="生成默认配置文件")
    parser.add_argument("--validate", action="store_true", help="验证配置文件")
    parser.add_argument("--config-file", default="data_governance_config.json", help="配置文件路径")
    parser.add_argument("--show-order", action="store_true", help="显示处理阶段执行顺序")
    
    args = parser.parse_args()
    
    config_manager = DataGovernanceConfigManager()
    
    if args.generate:
        config_manager.save_to_file(args.config_file)
        print(f"配置文件已生成: {args.config_file}")
        print(f"配置版本: {config_manager.config.version}")
        print(f"质量规则数量: {len(config_manager.config.quality_rules)}")
        print(f"处理阶段数量: {len(config_manager.config.processing_stages)}")
    
    if args.validate:
        try:
            if args.generate:
                # 如果刚生成了配置，就不需要重新加载
                pass
            else:
                config_manager.load_from_file(args.config_file)
            
            issues = config_manager.validate_rule_execution_order()
            
            if issues:
                print("配置验证发现问题:")
                for issue in issues:
                    print(f"  - {issue}")
            else:
                print("配置验证通过！")
                
        except Exception as e:
            print(f"配置验证失败: {e}")
    
    if args.show_order:
        try:
            if not args.generate and not args.validate:
                config_manager.load_from_file(args.config_file)
            
            order = config_manager.get_processing_order()
            print("处理阶段执行顺序:")
            for i, stage_id in enumerate(order, 1):
                stage = config_manager.config.processing_stages[stage_id]
                print(f"  {i}. {stage.name} ({stage_id})")
                
                # 显示该阶段的规则
                rules = config_manager.get_rules_for_stage(stage_id)
                blocking_rules = config_manager.get_blocking_rules_for_stage(stage_id)
                
                if rules:
                    print(f"     质量规则: {len(rules)}个")
                    for rule in rules:
                        blocking_indicator = " [阻断]" if rule in blocking_rules else ""
                        print(f"       - {rule.name} ({rule.severity.value}){blocking_indicator}")
                print()
                
        except Exception as e:
            print(f"无法显示执行顺序: {e}")


if __name__ == "__main__":
    main()