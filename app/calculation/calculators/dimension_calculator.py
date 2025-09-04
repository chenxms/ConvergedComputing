# 多维度统计聚合计算器
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..engine import StatisticalStrategy
from ...database.connection import SessionLocal

logger = logging.getLogger(__name__)


class DimensionType(Enum):
    """维度类型枚举"""
    KNOWLEDGE_POINT = "knowledge_point"      # 知识点维度
    ABILITY = "ability"                      # 能力维度  
    QUESTION_TYPE = "question_type"          # 题型维度
    DIFFICULTY = "difficulty"                # 难度维度
    CUSTOM = "custom"                        # 自定义维度


@dataclass
class DimensionMapping:
    """维度映射数据结构"""
    question_id: str
    dimension_type: str
    dimension_value: str
    hierarchy_level: int
    parent_dimension: Optional[str] = None
    weight: float = 1.0
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class DimensionStats:
    """维度统计结果数据结构"""
    dimension_id: str
    dimension_name: str
    dimension_type: str
    hierarchy_level: int
    total_score: float
    total_questions: int
    student_count: int
    avg_score: float
    score_rate: float
    std_dev: float
    min_score: float
    max_score: float
    difficulty_coefficient: float
    discrimination_index: float
    grade_distribution: Dict[str, Any]
    percentiles: Dict[str, float]
    regional_ranking: Optional[int] = None
    parent_stats: Optional[Dict[str, Any]] = None
    children_stats: List[Dict[str, Any]] = None


class DimensionDataProvider:
    """维度数据提供者"""
    
    def __init__(self, db_session: Session = None):
        self.db_session = db_session or SessionLocal()
    
    def get_dimension_mappings(self, batch_code: str, 
                             dimension_types: Optional[List[str]] = None) -> List[DimensionMapping]:
        """获取维度映射关系"""
        try:
            # 构建基础查询
            query = """
            SELECT 
                qdm.question_id,
                qdm.dimension_type,
                qdm.dimension_value,
                qdm.hierarchy_level,
                qdm.parent_dimension,
                COALESCE(qdm.weight, 1.0) as weight,
                qdm.metadata
            FROM question_dimension_mapping qdm
            JOIN student_score_detail ssd ON ssd.question_id = qdm.question_id
            WHERE ssd.batch_code = :batch_code
            """
            
            params = {'batch_code': batch_code}
            
            # 添加维度类型过滤
            if dimension_types:
                placeholders = ','.join([f':type_{i}' for i in range(len(dimension_types))])
                query += f" AND qdm.dimension_type IN ({placeholders})"
                for i, dtype in enumerate(dimension_types):
                    params[f'type_{i}'] = dtype
            
            query += " ORDER BY qdm.dimension_type, qdm.hierarchy_level, qdm.dimension_value"
            
            result = self.db_session.execute(text(query), params).fetchall()
            
            mappings = []
            for row in result:
                mappings.append(DimensionMapping(
                    question_id=row.question_id,
                    dimension_type=row.dimension_type,
                    dimension_value=row.dimension_value,
                    hierarchy_level=row.hierarchy_level,
                    parent_dimension=row.parent_dimension,
                    weight=float(row.weight or 1.0),
                    metadata=row.metadata if hasattr(row, 'metadata') else None
                ))
            
            logger.info(f"获取到{len(mappings)}条维度映射关系")
            return mappings
            
        except Exception as e:
            logger.error(f"获取维度映射关系失败: {e}")
            raise
    
    def get_student_score_data(self, batch_code: str, 
                             question_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """获取学生答题数据"""
        try:
            query = """
            SELECT 
                ssd.student_id,
                ssd.question_id,
                ssd.score,
                sqc.max_score,
                gam.grade_level,
                gam.school_id,
                gam.school_name
            FROM student_score_detail ssd
            JOIN subject_question_config sqc ON sqc.question_id = ssd.question_id
            JOIN grade_aggregation_main gam ON gam.student_id = ssd.student_id 
                AND gam.batch_code = ssd.batch_code
            WHERE ssd.batch_code = :batch_code
            """
            
            params = {'batch_code': batch_code}
            
            if question_ids:
                placeholders = ','.join([f':qid_{i}' for i in range(len(question_ids))])
                query += f" AND ssd.question_id IN ({placeholders})"
                for i, qid in enumerate(question_ids):
                    params[f'qid_{i}'] = qid
            
            df = pd.read_sql(query, self.db_session.bind, params=params)
            
            # 数据类型转换和清洗
            df['score'] = pd.to_numeric(df['score'], errors='coerce')
            df['max_score'] = pd.to_numeric(df['max_score'], errors='coerce')
            
            logger.info(f"获取到{len(df)}条学生答题数据")
            return df
            
        except Exception as e:
            logger.error(f"获取学生答题数据失败: {e}")
            raise


class DimensionCalculator:
    """多维度统计计算器"""
    
    def __init__(self, data_provider: DimensionDataProvider = None):
        self.data_provider = data_provider or DimensionDataProvider()
        self.anomaly_detector = self._create_anomaly_detector()
    
    def calculate_dimension_statistics(self, batch_code: str,
                                     dimension_types: Optional[List[str]] = None,
                                     aggregation_level: str = 'regional') -> Dict[str, Any]:
        """计算多维度统计"""
        try:
            logger.info(f"开始计算维度统计: batch_code={batch_code}, types={dimension_types}")
            
            # 1. 获取维度映射和数据
            mappings = self.data_provider.get_dimension_mappings(batch_code, dimension_types)
            if not mappings:
                logger.warning(f"批次 {batch_code} 没有找到维度映射数据")
                return {}
            
            # 获取相关题目的学生数据
            question_ids = list(set(m.question_id for m in mappings))
            score_data = self.data_provider.get_student_score_data(batch_code, question_ids)
            
            if score_data.empty:
                logger.warning(f"批次 {batch_code} 没有找到学生答题数据")
                return {}
            
            # 2. 按维度类型分组计算
            dimension_results = {}
            
            # 按维度类型分组映射关系
            mappings_by_type = self._group_mappings_by_type(mappings)
            
            for dim_type, type_mappings in mappings_by_type.items():
                logger.info(f"计算维度类型: {dim_type}")
                
                # 计算该维度类型下的所有维度统计
                type_stats = self._calculate_dimension_type_stats(
                    dim_type, type_mappings, score_data, aggregation_level
                )
                
                dimension_results[dim_type] = type_stats
            
            # 3. 计算交叉维度分析
            cross_analysis = self._calculate_cross_dimension_analysis(mappings, score_data)
            
            # 4. 生成数据透视表
            pivot_data = self._generate_dimension_pivot_table(mappings, score_data)
            
            result = {
                'batch_code': batch_code,
                'aggregation_level': aggregation_level,
                'dimension_statistics': dimension_results,
                'cross_dimension_analysis': cross_analysis,
                'pivot_table': pivot_data,
                'summary': self._generate_summary_stats(dimension_results),
                'metadata': {
                    'total_dimensions': len([m for mappings_list in mappings_by_type.values() 
                                           for m in mappings_list]),
                    'dimension_types_count': len(mappings_by_type),
                    'total_questions': len(question_ids),
                    'total_students': score_data['student_id'].nunique()
                }
            }
            
            logger.info(f"维度统计计算完成: {len(dimension_results)}个维度类型")
            return result
            
        except Exception as e:
            logger.error(f"计算维度统计失败: {e}")
            raise
    
    def _group_mappings_by_type(self, mappings: List[DimensionMapping]) -> Dict[str, List[DimensionMapping]]:
        """按维度类型分组映射关系"""
        grouped = {}
        for mapping in mappings:
            dim_type = mapping.dimension_type
            if dim_type not in grouped:
                grouped[dim_type] = []
            grouped[dim_type].append(mapping)
        return grouped
    
    def _calculate_dimension_type_stats(self, dimension_type: str,
                                      mappings: List[DimensionMapping],
                                      score_data: pd.DataFrame,
                                      aggregation_level: str) -> Dict[str, Any]:
        """计算特定维度类型的统计数据"""
        try:
            # 按维度值和层级分组
            dimensions_by_value = self._group_mappings_by_value_and_level(mappings)
            
            type_results = {
                'dimension_type': dimension_type,
                'total_dimensions': len(dimensions_by_value),
                'dimensions': {}
            }
            
            for dim_key, dim_mappings in dimensions_by_value.items():
                dimension_value, hierarchy_level = dim_key
                
                # 计算该维度的统计数据
                dim_stats = self._calculate_single_dimension_stats(
                    dimension_value, hierarchy_level, dim_mappings, score_data, aggregation_level
                )
                
                type_results['dimensions'][f"{dimension_value}_L{hierarchy_level}"] = dim_stats
            
            # 计算层次化统计
            type_results['hierarchy_analysis'] = self._calculate_hierarchy_analysis(
                dimensions_by_value, score_data
            )
            
            return type_results
            
        except Exception as e:
            logger.error(f"计算维度类型统计失败 {dimension_type}: {e}")
            raise
    
    def _group_mappings_by_value_and_level(self, mappings: List[DimensionMapping]) -> Dict[Tuple[str, int], List[DimensionMapping]]:
        """按维度值和层级分组"""
        grouped = {}
        for mapping in mappings:
            key = (mapping.dimension_value, mapping.hierarchy_level)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(mapping)
        return grouped
    
    def _calculate_single_dimension_stats(self, dimension_value: str, 
                                        hierarchy_level: int,
                                        mappings: List[DimensionMapping],
                                        score_data: pd.DataFrame,
                                        aggregation_level: str) -> DimensionStats:
        """计算单个维度的统计数据"""
        try:
            # 获取该维度相关的题目数据
            question_ids = [m.question_id for m in mappings]
            dim_data = score_data[score_data['question_id'].isin(question_ids)].copy()
            
            if dim_data.empty:
                logger.warning(f"维度 {dimension_value} 没有数据")
                return self._create_empty_dimension_stats(dimension_value, hierarchy_level)
            
            # 按学生聚合分数（考虑题目权重）
            student_scores = self._aggregate_student_scores_with_weights(dim_data, mappings)
            
            # 计算总分（所有题目的满分总和）
            total_score = dim_data.groupby('question_id')['max_score'].first().sum()
            
            # 基础统计计算
            basic_stats = self._calculate_basic_stats(student_scores['total_score'])
            
            # 教育指标计算
            educational_stats = self._calculate_educational_metrics(
                student_scores['total_score'], total_score, 
                student_scores['grade_level'].iloc[0] if not student_scores.empty else '1st_grade'
            )
            
            # 百分位数计算
            percentiles = self._calculate_percentiles(student_scores['total_score'])
            
            # 区分度计算
            discrimination = self._calculate_discrimination(student_scores['total_score'], total_score)
            
            # 构建结果
            stats = DimensionStats(
                dimension_id=f"{dimension_value}_L{hierarchy_level}",
                dimension_name=dimension_value,
                dimension_type=mappings[0].dimension_type,
                hierarchy_level=hierarchy_level,
                total_score=float(total_score),
                total_questions=len(question_ids),
                student_count=len(student_scores),
                avg_score=basic_stats['mean'],
                score_rate=basic_stats['mean'] / total_score if total_score > 0 else 0,
                std_dev=basic_stats['std'],
                min_score=basic_stats['min'],
                max_score=basic_stats['max'],
                difficulty_coefficient=educational_stats['difficulty_coefficient'],
                discrimination_index=discrimination['discrimination_index'],
                grade_distribution=educational_stats['grade_distribution'],
                percentiles=percentiles
            )
            
            logger.debug(f"维度 {dimension_value} 统计计算完成: 学生数={len(student_scores)}")
            return stats
            
        except Exception as e:
            logger.error(f"计算单维度统计失败 {dimension_value}: {e}")
            raise
    
    def _aggregate_student_scores_with_weights(self, data: pd.DataFrame, 
                                             mappings: List[DimensionMapping]) -> pd.DataFrame:
        """按权重聚合学生维度分数"""
        # 创建数据副本以避免pandas警告
        data = data.copy()
        
        # 创建权重映射
        weight_map = {m.question_id: m.weight for m in mappings}
        data['weight'] = data['question_id'].map(weight_map).fillna(1.0)
        
        # 计算加权分数
        data['weighted_score'] = data['score'] * data['weight']
        data['weighted_max_score'] = data['max_score'] * data['weight']
        
        # 按学生聚合
        student_agg = data.groupby(['student_id', 'grade_level', 'school_id']).agg({
            'weighted_score': 'sum',
            'weighted_max_score': 'sum',
            'weight': 'sum'
        }).reset_index()
        
        # 计算总分（考虑权重）
        student_agg['total_score'] = student_agg['weighted_score']
        student_agg['total_max_score'] = student_agg['weighted_max_score']
        
        return student_agg
    
    def _calculate_basic_stats(self, scores: pd.Series) -> Dict[str, float]:
        """计算基础统计指标"""
        if scores.empty:
            return {'count': 0, 'mean': 0, 'std': 0, 'min': 0, 'max': 0, 'median': 0}
        
        return {
            'count': len(scores),
            'mean': float(scores.mean()),
            'std': float(scores.std(ddof=1)) if len(scores) > 1 else 0.0,
            'min': float(scores.min()),
            'max': float(scores.max()),
            'median': float(scores.median())
        }
    
    def _calculate_educational_metrics(self, scores: pd.Series, max_score: float, 
                                     grade_level: str) -> Dict[str, Any]:
        """计算教育指标"""
        if scores.empty or max_score <= 0:
            return {
                'difficulty_coefficient': 0,
                'grade_distribution': {},
                'pass_rate': 0,
                'excellent_rate': 0
            }
        
        # 难度系数 = 平均分 / 满分
        difficulty_coefficient = float(scores.mean() / max_score)
        
        # 等级分布计算
        if self._is_primary_grade(grade_level):
            # 小学标准
            excellent_mask = scores >= (max_score * 0.90)
            good_mask = (scores >= (max_score * 0.80)) & (scores < (max_score * 0.90))
            pass_mask = (scores >= (max_score * 0.60)) & (scores < (max_score * 0.80))
            fail_mask = scores < (max_score * 0.60)
            
            grade_distribution = {
                'excellent': {'count': int(excellent_mask.sum()), 'percentage': float(excellent_mask.mean())},
                'good': {'count': int(good_mask.sum()), 'percentage': float(good_mask.mean())},
                'pass': {'count': int(pass_mask.sum()), 'percentage': float(pass_mask.mean())},
                'fail': {'count': int(fail_mask.sum()), 'percentage': float(fail_mask.mean())}
            }
        else:
            # 初中标准
            a_mask = scores >= (max_score * 0.85)
            b_mask = (scores >= (max_score * 0.70)) & (scores < (max_score * 0.85))
            c_mask = (scores >= (max_score * 0.60)) & (scores < (max_score * 0.70))
            d_mask = scores < (max_score * 0.60)
            
            grade_distribution = {
                'A': {'count': int(a_mask.sum()), 'percentage': float(a_mask.mean())},
                'B': {'count': int(b_mask.sum()), 'percentage': float(b_mask.mean())},
                'C': {'count': int(c_mask.sum()), 'percentage': float(c_mask.mean())},
                'D': {'count': int(d_mask.sum()), 'percentage': float(d_mask.mean())}
            }
        
        pass_rate = float((scores >= (max_score * 0.60)).mean())
        excellent_rate = float((scores >= (max_score * 0.85)).mean())
        
        return {
            'difficulty_coefficient': difficulty_coefficient,
            'grade_distribution': grade_distribution,
            'pass_rate': pass_rate,
            'excellent_rate': excellent_rate
        }
    
    def _calculate_percentiles(self, scores: pd.Series) -> Dict[str, float]:
        """计算百分位数"""
        if scores.empty:
            return {}
        
        scores_sorted = scores.sort_values()
        n = len(scores_sorted)
        percentiles = [10, 25, 50, 75, 90]
        
        result = {}
        for p in percentiles:
            rank = int(np.floor(n * p / 100.0))
            rank = max(0, min(rank, n - 1))
            result[f'P{p}'] = float(scores_sorted.iloc[rank])
        
        return result
    
    def _calculate_discrimination(self, scores: pd.Series, max_score: float) -> Dict[str, Any]:
        """计算区分度"""
        if len(scores) < 10 or max_score <= 0:
            return {'discrimination_index': 0, 'interpretation': 'insufficient_data'}
        
        scores_sorted = scores.sort_values(ascending=False)
        n = len(scores_sorted)
        
        # 前27%和后27%分组
        high_group_size = max(1, int(n * 0.27))
        low_group_size = max(1, int(n * 0.27))
        
        high_group_mean = scores_sorted.iloc[:high_group_size].mean()
        low_group_mean = scores_sorted.iloc[-low_group_size:].mean()
        
        discrimination_index = (high_group_mean - low_group_mean) / max_score
        
        # 区分度解释
        if discrimination_index >= 0.4:
            interpretation = 'excellent'
        elif discrimination_index >= 0.3:
            interpretation = 'good'
        elif discrimination_index >= 0.2:
            interpretation = 'acceptable'
        else:
            interpretation = 'poor'
        
        return {
            'discrimination_index': float(discrimination_index),
            'interpretation': interpretation,
            'high_group_mean': float(high_group_mean),
            'low_group_mean': float(low_group_mean)
        }
    
    def _calculate_cross_dimension_analysis(self, mappings: List[DimensionMapping],
                                          score_data: pd.DataFrame) -> Dict[str, Any]:
        """计算交叉维度分析"""
        try:
            # 获取不同维度类型的组合
            dim_types = list(set(m.dimension_type for m in mappings))
            
            if len(dim_types) < 2:
                return {'message': '维度类型不足，无法进行交叉分析'}
            
            cross_results = {}
            
            # 两两维度类型交叉分析
            for i, type1 in enumerate(dim_types):
                for type2 in dim_types[i+1:]:
                    cross_key = f"{type1}_x_{type2}"
                    
                    # 计算交叉统计
                    cross_stats = self._calculate_dimension_pair_correlation(
                        type1, type2, mappings, score_data
                    )
                    
                    cross_results[cross_key] = cross_stats
            
            return cross_results
            
        except Exception as e:
            logger.error(f"交叉维度分析失败: {e}")
            return {'error': str(e)}
    
    def _calculate_dimension_pair_correlation(self, type1: str, type2: str,
                                            mappings: List[DimensionMapping],
                                            score_data: pd.DataFrame) -> Dict[str, Any]:
        """计算维度对相关性"""
        try:
            # 获取两个维度类型的题目
            type1_questions = [m.question_id for m in mappings if m.dimension_type == type1]
            type2_questions = [m.question_id for m in mappings if m.dimension_type == type2]
            
            # 计算学生在两个维度上的表现
            type1_scores = self._calculate_student_dimension_scores(type1_questions, score_data)
            type2_scores = self._calculate_student_dimension_scores(type2_questions, score_data)
            
            # 合并数据计算相关性
            merged_scores = pd.merge(type1_scores, type2_scores, on='student_id', suffixes=('_1', '_2'))
            
            if len(merged_scores) < 10:
                return {'correlation': 0, 'message': '样本数量不足'}
            
            correlation = merged_scores['score_rate_1'].corr(merged_scores['score_rate_2'])
            
            return {
                'correlation': float(correlation) if not pd.isna(correlation) else 0,
                'sample_size': len(merged_scores),
                'type1_avg_rate': float(merged_scores['score_rate_1'].mean()),
                'type2_avg_rate': float(merged_scores['score_rate_2'].mean())
            }
            
        except Exception as e:
            logger.error(f"计算维度对相关性失败 {type1} x {type2}: {e}")
            return {'error': str(e)}
    
    def _calculate_student_dimension_scores(self, question_ids: List[str], 
                                          score_data: pd.DataFrame) -> pd.DataFrame:
        """计算学生在特定维度上的得分"""
        dim_data = score_data[score_data['question_id'].isin(question_ids)]
        
        student_scores = dim_data.groupby('student_id').agg({
            'score': 'sum',
            'max_score': 'sum'
        }).reset_index()
        
        student_scores['score_rate'] = student_scores['score'] / student_scores['max_score']
        student_scores['score_rate'] = student_scores['score_rate'].fillna(0)
        
        return student_scores[['student_id', 'score_rate']]
    
    def _generate_dimension_pivot_table(self, mappings: List[DimensionMapping],
                                       score_data: pd.DataFrame) -> Dict[str, Any]:
        """生成维度数据透视表"""
        try:
            # 创建映射关系DataFrame
            mapping_df = pd.DataFrame([
                {
                    'question_id': m.question_id,
                    'dimension_type': m.dimension_type,
                    'dimension_value': m.dimension_value,
                    'hierarchy_level': m.hierarchy_level
                } for m in mappings
            ])
            
            # 合并映射和得分数据
            pivot_data = pd.merge(score_data, mapping_df, on='question_id')
            
            # 创建透视表
            pivot_table = pivot_data.groupby([
                'dimension_type', 'dimension_value', 'hierarchy_level'
            ]).agg({
                'score': ['count', 'mean', 'std'],
                'max_score': 'mean',
                'student_id': 'nunique'
            }).round(3)
            
            # 扁平化列名
            pivot_table.columns = ['_'.join(col).strip() for col in pivot_table.columns.values]
            pivot_table = pivot_table.reset_index()
            
            # 计算得分率
            pivot_table['score_rate'] = pivot_table['score_mean'] / pivot_table['max_score_mean']
            pivot_table['score_rate'] = pivot_table['score_rate'].fillna(0).round(3)
            
            return {
                'pivot_table': pivot_table.to_dict('records'),
                'summary': {
                    'total_combinations': len(pivot_table),
                    'dimension_types': len(pivot_table['dimension_type'].unique()),
                    'avg_score_rate': float(pivot_table['score_rate'].mean())
                }
            }
            
        except Exception as e:
            logger.error(f"生成透视表失败: {e}")
            return {'error': str(e)}
    
    def _calculate_hierarchy_analysis(self, dimensions_by_value: Dict[Tuple[str, int], List[DimensionMapping]],
                                    score_data: pd.DataFrame) -> Dict[str, Any]:
        """计算层次化分析"""
        try:
            hierarchy_stats = {}
            
            # 按层级分组
            levels = set(level for _, level in dimensions_by_value.keys())
            
            for level in sorted(levels):
                level_dims = {k: v for k, v in dimensions_by_value.items() if k[1] == level}
                level_questions = [q for dims in level_dims.values() for m in dims for q in [m.question_id]]
                
                if not level_questions:
                    continue
                
                level_data = score_data[score_data['question_id'].isin(level_questions)]
                
                if level_data.empty:
                    continue
                
                # 计算层级统计
                level_student_scores = level_data.groupby('student_id').agg({
                    'score': 'sum',
                    'max_score': 'sum'
                })
                
                hierarchy_stats[f'level_{level}'] = {
                    'dimension_count': len(level_dims),
                    'question_count': len(level_questions),
                    'student_count': len(level_student_scores),
                    'avg_score_rate': float((level_student_scores['score'] / level_student_scores['max_score']).mean()),
                    'std_score_rate': float((level_student_scores['score'] / level_student_scores['max_score']).std())
                }
            
            return hierarchy_stats
            
        except Exception as e:
            logger.error(f"层次化分析失败: {e}")
            return {'error': str(e)}
    
    def _generate_summary_stats(self, dimension_results: Dict[str, Any]) -> Dict[str, Any]:
        """生成汇总统计"""
        try:
            total_dimensions = sum(len(type_data['dimensions']) for type_data in dimension_results.values())
            
            # 计算平均指标
            all_score_rates = []
            all_difficulties = []
            all_discriminations = []
            
            for type_data in dimension_results.values():
                for dim_stats in type_data['dimensions'].values():
                    if isinstance(dim_stats, DimensionStats):
                        all_score_rates.append(dim_stats.score_rate)
                        all_difficulties.append(dim_stats.difficulty_coefficient)
                        all_discriminations.append(dim_stats.discrimination_index)
            
            return {
                'total_dimension_types': len(dimension_results),
                'total_dimensions': total_dimensions,
                'avg_score_rate': float(np.mean(all_score_rates)) if all_score_rates else 0,
                'avg_difficulty': float(np.mean(all_difficulties)) if all_difficulties else 0,
                'avg_discrimination': float(np.mean(all_discriminations)) if all_discriminations else 0,
                'score_rate_std': float(np.std(all_score_rates)) if len(all_score_rates) > 1 else 0
            }
            
        except Exception as e:
            logger.error(f"生成汇总统计失败: {e}")
            return {'error': str(e)}
    
    def _create_empty_dimension_stats(self, dimension_value: str, hierarchy_level: int) -> DimensionStats:
        """创建空维度统计"""
        return DimensionStats(
            dimension_id=f"{dimension_value}_L{hierarchy_level}",
            dimension_name=dimension_value,
            dimension_type="unknown",
            hierarchy_level=hierarchy_level,
            total_score=0,
            total_questions=0,
            student_count=0,
            avg_score=0,
            score_rate=0,
            std_dev=0,
            min_score=0,
            max_score=0,
            difficulty_coefficient=0,
            discrimination_index=0,
            grade_distribution={},
            percentiles={}
        )
    
    def _is_primary_grade(self, grade_level: str) -> bool:
        """判断是否为小学年级"""
        primary_grades = ['1st_grade', '2nd_grade', '3rd_grade', 
                         '4th_grade', '5th_grade', '6th_grade']
        return grade_level in primary_grades
    
    def _create_anomaly_detector(self):
        """创建异常检测器"""
        # 这里可以根据需要实现异常检测逻辑
        return None


class DimensionStatisticsStrategy(StatisticalStrategy):
    """维度统计策略"""
    
    def __init__(self):
        self.calculator = DimensionCalculator()
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """执行维度统计计算"""
        batch_code = config.get('batch_code')
        dimension_types = config.get('dimension_types')
        aggregation_level = config.get('aggregation_level', 'regional')
        
        if not batch_code:
            raise ValueError("配置中缺少batch_code")
        
        return self.calculator.calculate_dimension_statistics(
            batch_code, dimension_types, aggregation_level
        )
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        # 检查必需配置
        if not config.get('batch_code'):
            validation_result['is_valid'] = False
            validation_result['errors'].append("缺少必需配置: batch_code")
        
        # 检查维度类型配置
        dimension_types = config.get('dimension_types')
        if dimension_types and not isinstance(dimension_types, list):
            validation_result['warnings'].append("dimension_types应为列表格式")
        
        validation_result['stats']['batch_code'] = config.get('batch_code', '')
        validation_result['stats']['dimension_types'] = dimension_types or []
        
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        """获取算法信息"""
        return {
            'name': 'DimensionStatistics',
            'version': '1.0',
            'description': '多维度统计聚合分析',
            'features': [
                'multi_dimension_aggregation',
                'hierarchical_analysis', 
                'cross_dimension_correlation',
                'pivot_table_generation'
            ],
            'database_tables': [
                'question_dimension_mapping',
                'student_score_detail',
                'subject_question_config',
                'grade_aggregation_main'
            ]
        }


# 工厂函数
def create_dimension_calculator(db_session: Session = None) -> DimensionCalculator:
    """创建维度计算器实例"""
    data_provider = DimensionDataProvider(db_session)
    return DimensionCalculator(data_provider)


def create_dimension_statistics_strategy() -> DimensionStatisticsStrategy:
    """创建维度统计策略实例"""
    return DimensionStatisticsStrategy()