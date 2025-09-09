# 查询构建器模式实现
from typing import Dict, Any, List, Optional, Union, Callable
from sqlalchemy.orm import Query
from sqlalchemy import and_, or_, desc, asc, func, text
from datetime import datetime
import logging

from .models import StatisticalAggregation, AggregationLevel, CalculationStatus

logger = logging.getLogger(__name__)


class QueryResult:
    """查询结果封装类"""
    
    def __init__(
        self,
        data: List[Any],
        total_count: int,
        offset: int = 0,
        limit: int = 100,
        has_more: bool = False
    ):
        self.data = data
        self.total_count = total_count
        self.offset = offset
        self.limit = limit
        self.has_more = has_more
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "data": self.data,
            "total_count": self.total_count,
            "offset": self.offset,
            "limit": self.limit,
            "has_more": self.has_more,
            "page": (self.offset // self.limit) + 1 if self.limit > 0 else 1,
            "total_pages": (self.total_count + self.limit - 1) // self.limit if self.limit > 0 else 1
        }


class StatisticalQueryBuilder:
    """统计数据查询构建器"""
    
    def __init__(self, base_query: Query):
        self.query = base_query
        self.conditions = []
        self.order_clauses = []
        self._joins_added = set()
    
    def filter_by_batch_codes(self, batch_codes: List[str]) -> 'StatisticalQueryBuilder':
        """按批次代码过滤"""
        if batch_codes:
            self.conditions.append(StatisticalAggregation.batch_code.in_(batch_codes))
        return self
    
    def filter_by_aggregation_level(self, level: AggregationLevel) -> 'StatisticalQueryBuilder':
        """按汇聚级别过滤"""
        self.conditions.append(StatisticalAggregation.aggregation_level == level)
        return self
    
    def filter_by_school_ids(self, school_ids: List[str]) -> 'StatisticalQueryBuilder':
        """按学校ID过滤"""
        if school_ids:
            self.conditions.append(StatisticalAggregation.school_id.in_(school_ids))
        return self
    
    def filter_by_calculation_status(self, status: CalculationStatus) -> 'StatisticalQueryBuilder':
        """按计算状态过滤"""
        self.conditions.append(StatisticalAggregation.calculation_status == status)
        return self
    
    def filter_by_date_range(self, start_date: datetime, end_date: datetime) -> 'StatisticalQueryBuilder':
        """按时间范围过滤"""
        self.conditions.append(
            and_(
                StatisticalAggregation.created_at >= start_date,
                StatisticalAggregation.created_at <= end_date
            )
        )
        return self
    
    def filter_by_student_count_range(self, min_students: int, max_students: int = None) -> 'StatisticalQueryBuilder':
        """按学生数量范围过滤"""
        self.conditions.append(StatisticalAggregation.total_students >= min_students)
        if max_students:
            self.conditions.append(StatisticalAggregation.total_students <= max_students)
        return self
    
    def filter_by_json_criteria(self, json_path: str, operator: str, value: Any) -> 'StatisticalQueryBuilder':
        """按JSON字段条件过滤"""
        json_extract = func.json_extract(StatisticalAggregation.statistics_data, json_path)
        
        if operator == '>=':
            self.conditions.append(json_extract >= value)
        elif operator == '<=':
            self.conditions.append(json_extract <= value)
        elif operator == '=':
            self.conditions.append(json_extract == value)
        elif operator == '!=':
            self.conditions.append(json_extract != value)
        elif operator == 'in':
            if isinstance(value, (list, tuple)):
                self.conditions.append(json_extract.in_(value))
        elif operator == 'like':
            self.conditions.append(json_extract.like(f"%{value}%"))
        elif operator == 'is_null':
            self.conditions.append(json_extract.is_(None))
        elif operator == 'is_not_null':
            self.conditions.append(json_extract.is_not(None))
        else:
            logger.warning(f"Unknown operator for JSON filter: {operator}")
        
        return self
    
    def filter_by_calculation_duration_range(self, min_duration: float, max_duration: float = None) -> 'StatisticalQueryBuilder':
        """按计算耗时范围过滤"""
        self.conditions.append(StatisticalAggregation.calculation_duration >= min_duration)
        if max_duration:
            self.conditions.append(StatisticalAggregation.calculation_duration <= max_duration)
        return self
    
    def filter_by_school_name_pattern(self, pattern: str) -> 'StatisticalQueryBuilder':
        """按学校名称模式过滤"""
        if pattern:
            self.conditions.append(StatisticalAggregation.school_name.like(f"%{pattern}%"))
        return self
    
    def filter_by_data_version(self, version: str) -> 'StatisticalQueryBuilder':
        """按数据版本过滤"""
        self.conditions.append(StatisticalAggregation.data_version == version)
        return self
    
    def order_by_created_at(self, direction: str = 'desc') -> 'StatisticalQueryBuilder':
        """按创建时间排序"""
        if direction.lower() == 'desc':
            self.order_clauses.append(desc(StatisticalAggregation.created_at))
        else:
            self.order_clauses.append(asc(StatisticalAggregation.created_at))
        return self
    
    def order_by_updated_at(self, direction: str = 'desc') -> 'StatisticalQueryBuilder':
        """按更新时间排序"""
        if direction.lower() == 'desc':
            self.order_clauses.append(desc(StatisticalAggregation.updated_at))
        else:
            self.order_clauses.append(asc(StatisticalAggregation.updated_at))
        return self
    
    def order_by_student_count(self, direction: str = 'desc') -> 'StatisticalQueryBuilder':
        """按学生数量排序"""
        if direction.lower() == 'desc':
            self.order_clauses.append(desc(StatisticalAggregation.total_students))
        else:
            self.order_clauses.append(asc(StatisticalAggregation.total_students))
        return self
    
    def order_by_calculation_duration(self, direction: str = 'desc') -> 'StatisticalQueryBuilder':
        """按计算耗时排序"""
        if direction.lower() == 'desc':
            self.order_clauses.append(desc(StatisticalAggregation.calculation_duration))
        else:
            self.order_clauses.append(asc(StatisticalAggregation.calculation_duration))
        return self
    
    def order_by_json_field(self, json_path: str, direction: str = 'desc') -> 'StatisticalQueryBuilder':
        """按JSON字段排序"""
        json_extract = func.json_extract(StatisticalAggregation.statistics_data, json_path)
        if direction.lower() == 'desc':
            self.order_clauses.append(desc(json_extract))
        else:
            self.order_clauses.append(asc(json_extract))
        return self
    
    def order_by_field(self, field_name: str, direction: str = 'desc') -> 'StatisticalQueryBuilder':
        """按指定字段排序"""
        if hasattr(StatisticalAggregation, field_name):
            field = getattr(StatisticalAggregation, field_name)
            if direction.lower() == 'desc':
                self.order_clauses.append(desc(field))
            else:
                self.order_clauses.append(asc(field))
        else:
            logger.warning(f"Unknown field for ordering: {field_name}")
        return self
    
    def add_custom_filter(self, filter_condition) -> 'StatisticalQueryBuilder':
        """添加自定义过滤条件"""
        self.conditions.append(filter_condition)
        return self
    
    def add_custom_order(self, order_clause) -> 'StatisticalQueryBuilder':
        """添加自定义排序"""
        self.order_clauses.append(order_clause)
        return self
    
    def build(self) -> Query:
        """构建最终查询"""
        # 应用所有条件
        if self.conditions:
            self.query = self.query.filter(and_(*self.conditions))
        
        # 应用排序
        if self.order_clauses:
            self.query = self.query.order_by(*self.order_clauses)
        else:
            # 默认按创建时间降序排序
            self.query = self.query.order_by(desc(StatisticalAggregation.created_at))
        
        return self.query
    
    def paginate(self, offset: int, limit: int) -> Query:
        """分页查询"""
        return self.build().offset(offset).limit(limit)
    
    def count(self) -> int:
        """获取查询结果总数"""
        # 创建计数查询，去除排序以提高性能
        count_query = self.query
        if self.conditions:
            count_query = count_query.filter(and_(*self.conditions))
        return count_query.count()
    
    def first(self) -> Optional[StatisticalAggregation]:
        """获取第一个结果"""
        return self.build().first()
    
    def all(self) -> List[StatisticalAggregation]:
        """获取所有结果"""
        return self.build().all()
    
    def get_query_info(self) -> Dict[str, Any]:
        """获取查询信息（用于调试）"""
        return {
            "conditions_count": len(self.conditions),
            "order_clauses_count": len(self.order_clauses),
            "joins_added": list(self._joins_added)
        }


class AdvancedStatisticalQueryBuilder(StatisticalQueryBuilder):
    """高级统计查询构建器，支持复杂查询场景"""
    
    def filter_by_performance_percentile(
        self, 
        subject: str, 
        percentile: float, 
        comparison: str = '>='
    ) -> 'AdvancedStatisticalQueryBuilder':
        """按学科性能百分位数过滤"""
        json_path = f'$.academic_subjects.{subject}.percentile_data.{int(percentile*100)}'
        return self.filter_by_json_criteria(json_path, comparison, 0)
    
    def filter_by_grade_distribution(
        self, 
        subject: str, 
        grade_level: str, 
        min_percentage: float
    ) -> 'AdvancedStatisticalQueryBuilder':
        """按年级分布过滤"""
        json_path = f'$.academic_subjects.{subject}.grade_distribution.{grade_level}.percentage'
        return self.filter_by_json_criteria(json_path, '>=', min_percentage)
    
    def filter_by_avg_score_range(
        self, 
        subject: str, 
        min_score: float, 
        max_score: float = None
    ) -> 'AdvancedStatisticalQueryBuilder':
        """按平均分范围过滤"""
        json_path = f'$.academic_subjects.{subject}.school_stats.avg_score'
        self.filter_by_json_criteria(json_path, '>=', min_score)
        if max_score:
            self.filter_by_json_criteria(json_path, '<=', max_score)
        return self
    
    def filter_by_difficulty_coefficient_range(
        self, 
        subject: str, 
        min_difficulty: float, 
        max_difficulty: float = None
    ) -> 'AdvancedStatisticalQueryBuilder':
        """按难度系数范围过滤"""
        json_path = f'$.academic_subjects.{subject}.statistical_indicators.difficulty_coefficient'
        self.filter_by_json_criteria(json_path, '>=', min_difficulty)
        if max_difficulty:
            self.filter_by_json_criteria(json_path, '<=', max_difficulty)
        return self
    
    def filter_by_discrimination_index_range(
        self, 
        subject: str, 
        min_discrimination: float, 
        max_discrimination: float = None
    ) -> 'AdvancedStatisticalQueryBuilder':
        """按区分度范围过滤"""
        json_path = f'$.academic_subjects.{subject}.statistical_indicators.discrimination_index'
        self.filter_by_json_criteria(json_path, '>=', min_discrimination)
        if max_discrimination:
            self.filter_by_json_criteria(json_path, '<=', max_discrimination)
        return self
    
    def order_by_subject_avg_score(self, subject: str, direction: str = 'desc') -> 'AdvancedStatisticalQueryBuilder':
        """按学科平均分排序"""
        json_path = f'$.academic_subjects.{subject}.school_stats.avg_score'
        return self.order_by_json_field(json_path, direction)
    
    def order_by_excellent_rate(self, subject: str, direction: str = 'desc') -> 'AdvancedStatisticalQueryBuilder':
        """按优秀率排序"""
        json_path = f'$.academic_subjects.{subject}.grade_distribution.excellent.percentage'
        return self.order_by_json_field(json_path, direction)
    
    def filter_schools_above_regional_average(
        self, 
        subject: str, 
        regional_avg: float
    ) -> 'AdvancedStatisticalQueryBuilder':
        """过滤高于区域平均分的学校"""
        json_path = f'$.academic_subjects.{subject}.school_stats.avg_score'
        return self.filter_by_json_criteria(json_path, '>', regional_avg)
    
    def filter_by_composite_performance(
        self, 
        subjects: List[str], 
        min_avg_performance: float
    ) -> 'AdvancedStatisticalQueryBuilder':
        """按综合成绩过滤（多学科平均）"""
        # 这需要使用复杂的JSON函数，简化实现
        if subjects:
            # 构建复杂的JSON查询条件
            for subject in subjects:
                json_path = f'$.academic_subjects.{subject}.school_stats.avg_score'
                self.filter_by_json_criteria(json_path, '>=', min_avg_performance * 0.8)
        return self


def create_statistical_query_builder(base_query: Query, advanced: bool = False) -> StatisticalQueryBuilder:
    """创建查询构建器工厂函数"""
    if advanced:
        return AdvancedStatisticalQueryBuilder(base_query)
    return StatisticalQueryBuilder(base_query)


def build_complex_query_from_dict(
    base_query: Query, 
    criteria: Dict[str, Any]
) -> StatisticalQueryBuilder:
    """从字典参数构建复杂查询"""
    builder = StatisticalQueryBuilder(base_query)
    
    # 批次过滤
    if 'batch_codes' in criteria:
        builder.filter_by_batch_codes(criteria['batch_codes'])
    
    # 聚合级别过滤
    if 'aggregation_level' in criteria:
        builder.filter_by_aggregation_level(criteria['aggregation_level'])
    
    # 学校ID过滤
    if 'school_ids' in criteria:
        builder.filter_by_school_ids(criteria['school_ids'])
    
    # 计算状态过滤
    if 'calculation_status' in criteria:
        builder.filter_by_calculation_status(criteria['calculation_status'])
    
    # 时间范围过滤
    if 'start_date' in criteria and 'end_date' in criteria:
        builder.filter_by_date_range(criteria['start_date'], criteria['end_date'])
    
    # 学生数量范围过滤
    if 'min_students' in criteria:
        builder.filter_by_student_count_range(
            criteria['min_students'], 
            criteria.get('max_students')
        )
    
    # 计算耗时范围过滤
    if 'min_duration' in criteria:
        builder.filter_by_calculation_duration_range(
            criteria['min_duration'],
            criteria.get('max_duration')
        )
    
    # 学校名称模式过滤
    if 'school_name_pattern' in criteria:
        builder.filter_by_school_name_pattern(criteria['school_name_pattern'])
    
    # 数据版本过滤
    if 'data_version' in criteria:
        builder.filter_by_data_version(criteria['data_version'])
    
    # JSON字段过滤
    if 'json_filters' in criteria:
        for json_filter in criteria['json_filters']:
            builder.filter_by_json_criteria(
                json_filter['path'],
                json_filter['operator'],
                json_filter['value']
            )
    
    # 排序
    order_by = criteria.get('order_by', 'created_at')
    direction = criteria.get('order_direction', 'desc')
    
    if order_by == 'created_at':
        builder.order_by_created_at(direction)
    elif order_by == 'updated_at':
        builder.order_by_updated_at(direction)
    elif order_by == 'total_students':
        builder.order_by_student_count(direction)
    elif order_by == 'calculation_duration':
        builder.order_by_calculation_duration(direction)
    elif order_by.startswith('json:'):
        # JSON字段排序，格式: "json:$.path"
        json_path = order_by[5:]
        builder.order_by_json_field(json_path, direction)
    else:
        builder.order_by_field(order_by, direction)
    
    return builder