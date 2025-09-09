"""
问卷数据特殊处理服务
支持多种量表类型和选项分布计算
"""

import logging
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from ..schemas.simplified_aggregation_schema import (
    QuestionnaireOptionDistribution,
    QuestionnaireQuestionStats, 
    QuestionnaireDimensionStats,
    format_decimal
)

logger = logging.getLogger(__name__)


class ScaleType(Enum):
    """量表类型"""
    SCALE_4_POSITIVE = "4级量表-正向"    # 1→1, 2→2, 3→3, 4→4
    SCALE_4_NEGATIVE = "4级量表-反向"    # 1→4, 2→3, 3→2, 4→1
    SCALE_5_LIKERT = "5级李克特量表"     # 1→1, 2→2, 3→3, 4→4, 5→5
    SCALE_5_LIKERT_NEG = "5级李克特量表-反向"  # 1→5, 2→4, 3→3, 4→2, 5→1
    SCALE_10_SATISFACTION = "10分满意度量表"  # 1-10分


@dataclass
class QuestionnaireConfig:
    """问卷配置"""
    scale_type: ScaleType
    question_id: str
    question_name: str
    dimension_code: str
    dimension_name: str
    is_reverse: bool = False
    
    
class QuestionnaireProcessor:
    """问卷数据处理器"""
    
    # 选项标签映射
    OPTION_LABELS = {
        ScaleType.SCALE_4_POSITIVE: {
            1: "不同意", 2: "基本同意", 3: "同意", 4: "非常同意"
        },
        ScaleType.SCALE_4_NEGATIVE: {
            1: "不同意", 2: "基本同意", 3: "同意", 4: "非常同意"
        },
        ScaleType.SCALE_5_LIKERT: {
            1: "非常不满意", 2: "不满意", 3: "一般", 4: "满意", 5: "非常满意"
        },
        ScaleType.SCALE_5_LIKERT_NEG: {
            1: "非常不满意", 2: "不满意", 3: "一般", 4: "满意", 5: "非常满意"
        },
        ScaleType.SCALE_10_SATISFACTION: {
            i: f"{i}分" for i in range(1, 11)
        }
    }
    
    # 转换映射
    SCALE_TRANSFORMATIONS = {
        ScaleType.SCALE_4_POSITIVE: {1: 1, 2: 2, 3: 3, 4: 4},
        ScaleType.SCALE_4_NEGATIVE: {1: 4, 2: 3, 3: 2, 4: 1},
        ScaleType.SCALE_5_LIKERT: {1: 1, 2: 2, 3: 3, 4: 4, 5: 5},
        ScaleType.SCALE_5_LIKERT_NEG: {1: 5, 2: 4, 3: 3, 4: 2, 5: 1},
        ScaleType.SCALE_10_SATISFACTION: {i: i for i in range(1, 11)}
    }
    
    def __init__(self):
        """初始化处理器"""
        logger.info("初始化问卷数据处理器")
    
    def calculate_option_distributions(
        self,
        raw_scores: pd.Series,
        scale_type: ScaleType
    ) -> List[QuestionnaireOptionDistribution]:
        """
        计算选项分布
        
        Args:
            raw_scores: 原始分数序列
            scale_type: 量表类型
            
        Returns:
            选项分布列表
        """
        try:
            total_responses = len(raw_scores)
            if total_responses == 0:
                return []
            
            # 获取选项标签
            option_labels = self.OPTION_LABELS.get(scale_type, {})
            
            # 统计各选项频次
            value_counts = raw_scores.value_counts().sort_index()
            
            distributions = []
            for option_value, count in value_counts.items():
                # 获取选项标签，如果没有则使用数值
                option_label = option_labels.get(option_value, str(option_value))
                percentage = format_decimal((count / total_responses) * 100, 2)
                
                distributions.append(QuestionnaireOptionDistribution(
                    option_label=option_label,
                    count=int(count),
                    percentage=percentage
                ))
            
            return distributions
            
        except Exception as e:
            logger.error(f"计算选项分布时发生错误: {str(e)}")
            return []
    
    def calculate_dimension_distributions(
        self,
        dimension_data: pd.DataFrame,
        dimension_code: str,
        scale_type: ScaleType
    ) -> List[QuestionnaireOptionDistribution]:
        """
        计算维度级选项占比
        
        Args:
            dimension_data: 维度所有题目的数据
            dimension_code: 维度代码
            scale_type: 量表类型
            
        Returns:
            维度级选项分布
        """
        try:
            if dimension_data.empty:
                return []
            
            # 合并所有题目的原始分数
            all_scores = []
            score_columns = [col for col in dimension_data.columns 
                           if col.startswith('raw_score') or col == 'raw_score']
            
            for col in score_columns:
                if col in dimension_data.columns:
                    all_scores.extend(dimension_data[col].dropna().tolist())
            
            if not all_scores:
                logger.warning(f"维度 {dimension_code} 没有找到有效的原始分数数据")
                return []
            
            # 转换为Series并计算分布
            scores_series = pd.Series(all_scores)
            return self.calculate_option_distributions(scores_series, scale_type)
            
        except Exception as e:
            logger.error(f"计算维度 {dimension_code} 选项分布时发生错误: {str(e)}")
            return []
    
    def calculate_question_distributions(
        self,
        question_data: pd.DataFrame,
        question_config: QuestionnaireConfig
    ) -> QuestionnaireQuestionStats:
        """
        计算题目级选项占比
        
        Args:
            question_data: 题目数据
            question_config: 题目配置
            
        Returns:
            题目统计信息
        """
        try:
            # 查找原始分数列
            raw_score_col = None
            possible_cols = ['raw_score', f'raw_score_{question_config.question_id}']
            for col in possible_cols:
                if col in question_data.columns:
                    raw_score_col = col
                    break
            
            if raw_score_col is None:
                logger.warning(f"题目 {question_config.question_id} 没有找到原始分数列")
                return QuestionnaireQuestionStats(
                    question_id=question_config.question_id,
                    question_name=question_config.question_name,
                    option_distributions=[]
                )
            
            raw_scores = question_data[raw_score_col].dropna()
            distributions = self.calculate_option_distributions(
                raw_scores, question_config.scale_type
            )
            
            return QuestionnaireQuestionStats(
                question_id=question_config.question_id,
                question_name=question_config.question_name,
                option_distributions=distributions
            )
            
        except Exception as e:
            logger.error(f"计算题目 {question_config.question_id} 统计时发生错误: {str(e)}")
            return QuestionnaireQuestionStats(
                question_id=question_config.question_id,
                question_name=question_config.question_name,
                option_distributions=[]
            )
    
    def transform_scores(
        self,
        raw_scores: pd.Series,
        scale_type: ScaleType
    ) -> pd.Series:
        """
        转换原始分数到标准分数
        
        Args:
            raw_scores: 原始分数
            scale_type: 量表类型
            
        Returns:
            转换后的分数
        """
        try:
            transformation = self.SCALE_TRANSFORMATIONS.get(scale_type, {})
            return raw_scores.map(transformation).fillna(raw_scores)
        except Exception as e:
            logger.error(f"转换分数时发生错误: {str(e)}")
            return raw_scores
    
    def get_max_score(self, scale_type: ScaleType) -> float:
        """获取量表满分"""
        max_scores = {
            ScaleType.SCALE_4_POSITIVE: 4.0,
            ScaleType.SCALE_4_NEGATIVE: 4.0,
            ScaleType.SCALE_5_LIKERT: 5.0,
            ScaleType.SCALE_5_LIKERT_NEG: 5.0,
            ScaleType.SCALE_10_SATISFACTION: 10.0
        }
        return max_scores.get(scale_type, 5.0)  # 默认5分制
    
    def process_questionnaire_data(
        self,
        data: pd.DataFrame,
        questionnaire_configs: List[QuestionnaireConfig],
        batch_code: str
    ) -> List[QuestionnaireDimensionStats]:
        """
        处理完整问卷数据
        
        Args:
            data: 问卷数据
            questionnaire_configs: 问卷配置列表
            batch_code: 批次代码
            
        Returns:
            问卷维度统计列表
        """
        logger.info(f"开始处理批次 {batch_code} 的问卷数据")
        
        try:
            if data.empty or not questionnaire_configs:
                logger.warning(f"批次 {batch_code} 没有问卷数据或配置")
                return []
            
            # 按维度分组配置
            dimensions = {}
            for config in questionnaire_configs:
                dim_code = config.dimension_code
                if dim_code not in dimensions:
                    dimensions[dim_code] = {
                        'name': config.dimension_name,
                        'configs': [],
                        'scale_type': config.scale_type
                    }
                dimensions[dim_code]['configs'].append(config)
            
            dimension_stats = []
            
            for dim_code, dim_info in dimensions.items():
                try:
                    logger.info(f"处理维度: {dim_code} - {dim_info['name']}")
                    
                    # 获取该维度的所有题目数据
                    dim_configs = dim_info['configs']
                    scale_type = dim_info['scale_type']
                    
                    # 收集维度下所有题目的分数
                    dim_scores = []
                    question_stats = []
                    
                    for config in dim_configs:
                        # 查找题目数据列
                        question_data = data[data['question_id'] == config.question_id]
                        
                        if not question_data.empty:
                            # 计算题目级统计
                            q_stats = self.calculate_question_distributions(
                                question_data, config
                            )
                            question_stats.append(q_stats)
                            
                            # 收集分数用于维度级计算
                            raw_score_col = 'raw_score'
                            if raw_score_col in question_data.columns:
                                scores = question_data[raw_score_col].dropna()
                                # 进行量表转换
                                transformed_scores = self.transform_scores(scores, scale_type)
                                dim_scores.extend(transformed_scores.tolist())
                    
                    if not dim_scores:
                        logger.warning(f"维度 {dim_code} 没有找到有效分数数据")
                        continue
                    
                    # 计算维度指标
                    dim_scores_series = pd.Series(dim_scores)
                    avg_score = format_decimal(dim_scores_series.mean())
                    max_score = self.get_max_score(scale_type)
                    score_rate = format_decimal((avg_score / max_score) * 100, 2)
                    student_count = len(data['student_id'].unique()) if 'student_id' in data.columns else len(data)
                    
                    # 计算维度级选项分布
                    raw_scores_for_dist = []
                    for config in dim_configs:
                        question_data = data[data['question_id'] == config.question_id]
                        if not question_data.empty and 'raw_score' in question_data.columns:
                            raw_scores_for_dist.extend(
                                question_data['raw_score'].dropna().tolist()
                            )
                    
                    dimension_distributions = []
                    if raw_scores_for_dist:
                        raw_scores_series = pd.Series(raw_scores_for_dist)
                        dimension_distributions = self.calculate_option_distributions(
                            raw_scores_series, scale_type
                        )
                    
                    # 创建维度统计
                    dim_stat = QuestionnaireDimensionStats(
                        dimension_code=dim_code,
                        dimension_name=dim_info['name'],
                        avg_score=avg_score,
                        score_rate=score_rate,
                        rank=None,  # 排名在上层服务中计算
                        dimension_option_distributions=dimension_distributions,
                        questions=question_stats,
                        student_count=student_count
                    )
                    
                    dimension_stats.append(dim_stat)
                    
                except Exception as e:
                    logger.error(f"处理维度 {dim_code} 时发生错误: {str(e)}")
                    continue
            
            logger.info(f"成功处理 {len(dimension_stats)} 个问卷维度")
            return dimension_stats
            
        except Exception as e:
            logger.error(f"处理问卷数据时发生错误: {str(e)}")
            return []
    
    def detect_scale_type(self, data: pd.DataFrame, question_id: str) -> ScaleType:
        """
        自动检测量表类型
        
        Args:
            data: 数据
            question_id: 题目ID
            
        Returns:
            检测到的量表类型
        """
        try:
            question_data = data[data['question_id'] == question_id]
            if question_data.empty or 'raw_score' not in question_data.columns:
                return ScaleType.SCALE_5_LIKERT  # 默认值
            
            unique_values = sorted(question_data['raw_score'].dropna().unique())
            max_value = max(unique_values) if unique_values else 5
            
            if max_value <= 4:
                return ScaleType.SCALE_4_POSITIVE
            elif max_value <= 5:
                return ScaleType.SCALE_5_LIKERT
            elif max_value <= 10:
                return ScaleType.SCALE_10_SATISFACTION
            else:
                return ScaleType.SCALE_5_LIKERT
                
        except Exception as e:
            logger.warning(f"检测量表类型失败: {str(e)}")
            return ScaleType.SCALE_5_LIKERT