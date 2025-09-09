# 通用学校排名服务
import pandas as pd
import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..utils.precision_handler import format_decimal, batch_format_dict
from ..database.enums import AggregationLevel

logger = logging.getLogger(__name__)


class RankingService:
    """学校排名服务，提供各种排名算法"""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def calculate_school_rankings(self, batch_code: str, ranking_field: str = 'avg_score',
                                ranking_order: str = 'desc', subject_name: Optional[str] = None) -> Dict[str, Any]:
        """
        计算批次中所有学校的排名
        
        Args:
            batch_code: 批次代码
            ranking_field: 排名依据字段（avg_score, excellent_rate, pass_rate等）
            ranking_order: 排名顺序（desc降序，asc升序）
            subject_name: 科目名称（可选，为空时使用综合排名）
            
        Returns:
            包含排名信息的字典
        """
        try:
            logger.info(f"开始计算学校排名: 批次={batch_code}, 字段={ranking_field}")
            
            # 获取学校数据
            schools_data = self._fetch_schools_data(batch_code, subject_name)
            if not schools_data:
                logger.warning(f"未找到批次数据: {batch_code}")
                return self._create_empty_ranking_result()
            
            # 提取排名字段的值
            ranking_values = []
            for school_data in schools_data:
                value = self._extract_ranking_value(school_data, ranking_field, subject_name)
                ranking_values.append({
                    'school_id': school_data['school_id'],
                    'school_name': school_data['school_name'],
                    'ranking_value': value,
                    'data': school_data
                })
            
            # 排序并计算排名
            rankings = self._calculate_rankings(ranking_values, ranking_order)
            
            result = {
                'batch_code': batch_code,
                'ranking_field': ranking_field,
                'ranking_order': ranking_order,
                'subject_name': subject_name,
                'total_schools': len(rankings),
                'rankings': rankings,
                'ranking_stats': self._calculate_ranking_stats(rankings)
            }
            
            return batch_format_dict(result, 2)
        
        except Exception as e:
            logger.error(f"计算学校排名失败: {str(e)}")
            return self._create_empty_ranking_result()
    
    def get_school_rank(self, batch_code: str, school_id: str, 
                       ranking_field: str = 'avg_score',
                       ranking_order: str = 'desc',
                       subject_name: Optional[str] = None) -> Dict[str, Any]:
        """
        获取特定学校的排名信息
        
        Args:
            batch_code: 批次代码
            school_id: 学校ID
            ranking_field: 排名依据字段
            ranking_order: 排名顺序
            subject_name: 科目名称
            
        Returns:
            学校排名信息
        """
        try:
            # 计算所有学校排名
            all_rankings = self.calculate_school_rankings(
                batch_code, ranking_field, ranking_order, subject_name
            )
            
            if not all_rankings['rankings']:
                return self._create_empty_school_rank()
            
            # 查找目标学校
            target_school = None
            for ranking in all_rankings['rankings']:
                if ranking['school_id'] == school_id:
                    target_school = ranking
                    break
            
            if not target_school:
                logger.warning(f"未找到学校: {school_id}")
                return self._create_empty_school_rank()
            
            # 计算相对位置信息
            rank_info = self._calculate_rank_position_info(
                target_school, all_rankings['rankings']
            )
            
            return {
                'batch_code': batch_code,
                'school_id': school_id,
                'school_name': target_school['school_name'],
                'rank': target_school['rank'],
                'ranking_value': target_school['ranking_value'],
                'total_schools': all_rankings['total_schools'],
                'percentile': rank_info['percentile'],
                'rank_category': rank_info['category'],
                'nearby_schools': rank_info['nearby_schools']
            }
        
        except Exception as e:
            logger.error(f"获取学校排名失败: {str(e)}")
            return self._create_empty_school_rank()
    
    def calculate_multi_field_rankings(self, batch_code: str, 
                                     ranking_fields: List[Dict[str, Any]],
                                     subject_name: Optional[str] = None) -> Dict[str, Any]:
        """
        多字段综合排名
        
        Args:
            batch_code: 批次代码
            ranking_fields: 排名字段配置列表，格式：
                [{'field': 'avg_score', 'weight': 0.4, 'order': 'desc'},
                 {'field': 'excellent_rate', 'weight': 0.3, 'order': 'desc'},
                 {'field': 'pass_rate', 'weight': 0.3, 'order': 'desc'}]
            subject_name: 科目名称
            
        Returns:
            综合排名结果
        """
        try:
            logger.info(f"开始计算多字段排名: 批次={batch_code}")
            
            # 验证权重总和
            total_weight = sum(field_config['weight'] for field_config in ranking_fields)
            if abs(total_weight - 1.0) > 0.01:
                logger.warning(f"权重总和不为1: {total_weight}")
            
            # 获取学校数据
            schools_data = self._fetch_schools_data(batch_code, subject_name)
            if not schools_data:
                return self._create_empty_ranking_result()
            
            # 计算每个字段的分值并标准化
            schools_with_scores = []
            for school_data in schools_data:
                composite_score = 0.0
                field_scores = {}
                
                for field_config in ranking_fields:
                    field_name = field_config['field']
                    weight = field_config['weight']
                    order = field_config.get('order', 'desc')
                    
                    # 提取字段值
                    value = self._extract_ranking_value(school_data, field_name, subject_name)
                    if value is not None:
                        field_scores[field_name] = value
                        composite_score += value * weight
                    else:
                        field_scores[field_name] = 0.0
                
                schools_with_scores.append({
                    'school_id': school_data['school_id'],
                    'school_name': school_data['school_name'],
                    'composite_score': composite_score,
                    'field_scores': field_scores,
                    'data': school_data
                })
            
            # 排序并计算排名
            schools_with_scores.sort(
                key=lambda x: x['composite_score'], 
                reverse=True  # 综合分越高排名越靠前
            )
            
            # 分配排名（处理并列）
            rankings = self._assign_ranks([
                {
                    'school_id': item['school_id'],
                    'school_name': item['school_name'],
                    'ranking_value': item['composite_score'],
                    'field_scores': item['field_scores'],
                    'data': item['data']
                }
                for item in schools_with_scores
            ])
            
            result = {
                'batch_code': batch_code,
                'ranking_type': 'multi_field',
                'ranking_fields': ranking_fields,
                'subject_name': subject_name,
                'total_schools': len(rankings),
                'rankings': rankings
            }
            
            return batch_format_dict(result, 2)
        
        except Exception as e:
            logger.error(f"多字段排名失败: {str(e)}")
            return self._create_empty_ranking_result()
    
    def calculate_rank_distribution(self, batch_code: str, ranking_field: str = 'avg_score',
                                  subject_name: Optional[str] = None) -> Dict[str, Any]:
        """
        计算排名分布统计
        
        Args:
            batch_code: 批次代码
            ranking_field: 排名字段
            subject_name: 科目名称
            
        Returns:
            排名分布统计
        """
        try:
            rankings_result = self.calculate_school_rankings(
                batch_code, ranking_field, 'desc', subject_name
            )
            
            if not rankings_result['rankings']:
                return {}
            
            rankings = rankings_result['rankings']
            total_schools = len(rankings)
            
            # 分位数统计
            values = [r['ranking_value'] for r in rankings if r['ranking_value'] is not None]
            if not values:
                return {}
            
            values_series = pd.Series(values)
            
            distribution = {
                'total_schools': total_schools,
                'field_stats': {
                    'mean': format_decimal(values_series.mean()),
                    'median': format_decimal(values_series.median()),
                    'std': format_decimal(values_series.std()),
                    'min': format_decimal(values_series.min()),
                    'max': format_decimal(values_series.max()),
                    'Q1': format_decimal(values_series.quantile(0.25)),
                    'Q3': format_decimal(values_series.quantile(0.75))
                },
                'rank_categories': {
                    'top_10_percent': self._get_schools_in_percentile_range(rankings, 0, 0.1),
                    'top_25_percent': self._get_schools_in_percentile_range(rankings, 0, 0.25),
                    'middle_50_percent': self._get_schools_in_percentile_range(rankings, 0.25, 0.75),
                    'bottom_25_percent': self._get_schools_in_percentile_range(rankings, 0.75, 1.0)
                }
            }
            
            return batch_format_dict(distribution, 2)
        
        except Exception as e:
            logger.error(f"计算排名分布失败: {str(e)}")
            return {}
    
    def _fetch_schools_data(self, batch_code: str, subject_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取学校统计数据"""
        try:
            # 从statistical_aggregations表获取学校级数据
            query = text("""
                SELECT 
                    school_id,
                    school_name,
                    statistics_data
                FROM statistical_aggregations
                WHERE batch_code = :batch_code 
                AND aggregation_level = 'school'
                AND calculation_status = 'completed'
            """)
            
            result = self.db.execute(query, {'batch_code': batch_code})
            rows = result.fetchall()
            
            schools_data = []
            for row in rows:
                schools_data.append({
                    'school_id': row.school_id,
                    'school_name': row.school_name,
                    'statistics_data': row.statistics_data
                })
            
            return schools_data
        
        except Exception as e:
            logger.error(f"获取学校数据失败: {str(e)}")
            return []
    
    def _extract_ranking_value(self, school_data: Dict[str, Any], 
                             ranking_field: str, subject_name: Optional[str] = None) -> Optional[float]:
        """从学校数据中提取排名字段值"""
        try:
            statistics_data = school_data['statistics_data']
            if not statistics_data:
                return None
            
            if subject_name:
                # 科目级排名
                subjects = statistics_data.get('academic_subjects', {})
                if subject_name not in subjects:
                    return None
                
                subject_data = subjects[subject_name]
                
                # 从school_stats中提取
                if 'school_stats' in subject_data:
                    value = subject_data['school_stats'].get(ranking_field)
                    return float(value) if value is not None else None
                
                # 从statistical_indicators中提取
                if 'statistical_indicators' in subject_data:
                    value = subject_data['statistical_indicators'].get(ranking_field)
                    return float(value) if value is not None else None
            else:
                # 综合排名（可能需要计算多科目平均值）
                subjects = statistics_data.get('academic_subjects', {})
                if not subjects:
                    return None
                
                values = []
                for subj_name, subj_data in subjects.items():
                    school_stats = subj_data.get('school_stats', {})
                    if ranking_field in school_stats:
                        value = school_stats[ranking_field]
                        if value is not None:
                            values.append(float(value))
                
                if values:
                    return sum(values) / len(values)  # 平均值
            
            return None
        
        except Exception as e:
            logger.warning(f"提取排名字段值失败: {ranking_field}, 错误: {str(e)}")
            return None
    
    def _calculate_rankings(self, schools_with_values: List[Dict[str, Any]], 
                          ranking_order: str) -> List[Dict[str, Any]]:
        """计算排名（处理并列情况）"""
        try:
            # 过滤掉无效值
            valid_schools = [
                school for school in schools_with_values 
                if school['ranking_value'] is not None
            ]
            
            if not valid_schools:
                return []
            
            # 排序
            reverse_order = (ranking_order.lower() == 'desc')
            valid_schools.sort(key=lambda x: x['ranking_value'], reverse=reverse_order)
            
            # 分配排名
            return self._assign_ranks(valid_schools)
        
        except Exception as e:
            logger.error(f"计算排名失败: {str(e)}")
            return []
    
    def _assign_ranks(self, sorted_schools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """分配排名，处理并列情况"""
        try:
            if not sorted_schools:
                return []
            
            rankings = []
            current_rank = 1
            
            for i, school in enumerate(sorted_schools):
                # 检查是否与前一个学校分数相同（并列）
                if i > 0 and sorted_schools[i-1]['ranking_value'] == school['ranking_value']:
                    # 并列，使用相同排名
                    rank = rankings[-1]['rank']
                else:
                    # 新的排名
                    rank = current_rank
                
                rankings.append({
                    'rank': rank,
                    'school_id': school['school_id'],
                    'school_name': school['school_name'],
                    'ranking_value': format_decimal(school['ranking_value']),
                    'field_scores': school.get('field_scores', {}),
                    'data': school.get('data', {})
                })
                
                current_rank = i + 2  # 下一个非并列排名
            
            return rankings
        
        except Exception as e:
            logger.error(f"分配排名失败: {str(e)}")
            return []
    
    def _calculate_ranking_stats(self, rankings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算排名统计信息"""
        try:
            if not rankings:
                return {}
            
            values = [r['ranking_value'] for r in rankings if r['ranking_value'] is not None]
            if not values:
                return {}
            
            values_series = pd.Series(values)
            
            return {
                'mean': format_decimal(values_series.mean()),
                'median': format_decimal(values_series.median()),
                'std': format_decimal(values_series.std()),
                'min': format_decimal(values_series.min()),
                'max': format_decimal(values_series.max()),
                'range': format_decimal(values_series.max() - values_series.min())
            }
        
        except Exception as e:
            logger.warning(f"计算排名统计失败: {str(e)}")
            return {}
    
    def _calculate_rank_position_info(self, target_school: Dict[str, Any], 
                                    all_rankings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算学校排名位置信息"""
        try:
            total_schools = len(all_rankings)
            target_rank = target_school['rank']
            
            # 计算百分位数
            percentile = (total_schools - target_rank + 1) / total_schools
            
            # 分类
            if percentile >= 0.9:
                category = "top_10_percent"
            elif percentile >= 0.75:
                category = "top_25_percent"
            elif percentile >= 0.5:
                category = "above_average"
            elif percentile >= 0.25:
                category = "below_average"
            else:
                category = "bottom_25_percent"
            
            # 找到附近的学校（前后各2名）
            target_index = next(
                i for i, school in enumerate(all_rankings) 
                if school['school_id'] == target_school['school_id']
            )
            
            nearby_schools = []
            for i in range(max(0, target_index - 2), min(len(all_rankings), target_index + 3)):
                if i != target_index:
                    nearby_schools.append({
                        'rank': all_rankings[i]['rank'],
                        'school_name': all_rankings[i]['school_name'],
                        'ranking_value': all_rankings[i]['ranking_value']
                    })
            
            return {
                'percentile': format_decimal(percentile * 100),  # 转换为百分比
                'category': category,
                'nearby_schools': nearby_schools
            }
        
        except Exception as e:
            logger.warning(f"计算排名位置信息失败: {str(e)}")
            return {'percentile': None, 'category': 'unknown', 'nearby_schools': []}
    
    def _get_schools_in_percentile_range(self, rankings: List[Dict[str, Any]], 
                                       start_pct: float, end_pct: float) -> List[Dict[str, Any]]:
        """获取指定百分位范围内的学校"""
        try:
            total_schools = len(rankings)
            start_index = int(start_pct * total_schools)
            end_index = int(end_pct * total_schools)
            
            schools_in_range = rankings[start_index:end_index]
            
            return [
                {
                    'rank': school['rank'],
                    'school_name': school['school_name'],
                    'ranking_value': school['ranking_value']
                }
                for school in schools_in_range
            ]
        
        except Exception as e:
            logger.warning(f"获取百分位范围学校失败: {str(e)}")
            return []
    
    def _create_empty_ranking_result(self) -> Dict[str, Any]:
        """创建空的排名结果"""
        return {
            'batch_code': '',
            'ranking_field': '',
            'ranking_order': 'desc',
            'subject_name': None,
            'total_schools': 0,
            'rankings': [],
            'ranking_stats': {}
        }
    
    def _create_empty_school_rank(self) -> Dict[str, Any]:
        """创建空的学校排名结果"""
        return {
            'batch_code': '',
            'school_id': '',
            'school_name': '',
            'rank': None,
            'ranking_value': None,
            'total_schools': 0,
            'percentile': None,
            'rank_category': 'unknown',
            'nearby_schools': []
        }


# 便捷函数
def get_school_rankings(db_session: Session, batch_code: str, 
                       ranking_field: str = 'avg_score',
                       subject_name: Optional[str] = None) -> Dict[str, Any]:
    """
    便捷函数：获取学校排名
    """
    ranking_service = RankingService(db_session)
    return ranking_service.calculate_school_rankings(batch_code, ranking_field, 'desc', subject_name)


def get_school_rank_info(db_session: Session, batch_code: str, school_id: str,
                        ranking_field: str = 'avg_score',
                        subject_name: Optional[str] = None) -> Dict[str, Any]:
    """
    便捷函数：获取特定学校排名信息
    """
    ranking_service = RankingService(db_session)
    return ranking_service.get_school_rank(batch_code, school_id, ranking_field, 'desc', subject_name)