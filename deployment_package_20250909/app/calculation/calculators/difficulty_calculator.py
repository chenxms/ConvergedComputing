# 难度系数计算器
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any, List, Optional, Union
from ..engine import StatisticalStrategy

logger = logging.getLogger(__name__)


class DifficultyCalculator(StatisticalStrategy):
    """难度系数计算器
    
    难度系数定义：平均分 / 满分
    等级划分：
    - 容易 (Easy): > 0.7
    - 中等 (Medium): 0.3 - 0.7  
    - 困难 (Hard): < 0.3
    """
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """计算难度系数
        
        Args:
            data: 包含学生分数的DataFrame，必须有'score'列
            config: 配置参数，包括：
                - max_score: 满分(默认100)
                - question_id: 题目ID(可选)
                - subject_id: 科目ID(可选)
        
        Returns:
            Dict包含：
            - difficulty_coefficient: 难度系数值
            - average_score: 平均分  
            - max_score: 满分
            - difficulty_level: 难度等级
            - interpretation: 结果解释
            - sample_size: 样本数量
        """
        if 'score' not in data.columns:
            raise ValueError("数据中缺少'score'列")
            
        scores = data['score'].astype(float).dropna()
        
        if len(scores) == 0:
            raise ValueError("没有有效的分数数据")
        
        max_score = config.get('max_score', 100)
        if max_score <= 0:
            raise ValueError("满分必须大于0")
        
        # 计算平均分
        average_score = float(scores.mean())
        
        # 计算难度系数
        difficulty_coefficient = average_score / max_score
        
        # 难度等级判定
        difficulty_level = self._classify_difficulty_level(difficulty_coefficient)
        
        # 结果解释
        interpretation = self._interpret_difficulty(difficulty_coefficient, difficulty_level)
        
        # 计算题目级别统计
        question_stats = self._calculate_question_stats(scores, max_score)
        
        result = {
            'difficulty_coefficient': float(difficulty_coefficient),
            'average_score': float(average_score),
            'max_score': float(max_score),
            'difficulty_level': difficulty_level,
            'interpretation': interpretation,
            'sample_size': int(len(scores)),
            'question_stats': question_stats
        }
        
        # 添加可选的题目和科目信息
        if 'question_id' in config:
            result['question_id'] = config['question_id']
        if 'subject_id' in config:
            result['subject_id'] = config['subject_id']
            
        return result
    
    def calculate_batch_difficulty(self, data: pd.DataFrame, 
                                  config: Dict[str, Any]) -> Dict[str, Any]:
        """批量计算多题目难度系数
        
        Args:
            data: 包含学生分数的DataFrame，必须有'score', 'question_id'列
            config: 配置参数
        
        Returns:
            Dict包含每个题目的难度系数结果
        """
        if 'question_id' not in data.columns:
            raise ValueError("批量计算需要'question_id'列")
            
        results = {}
        grouped = data.groupby('question_id')
        
        for question_id, group in grouped:
            try:
                question_config = config.copy()
                question_config['question_id'] = question_id
                
                result = self.calculate(group, question_config)
                results[str(question_id)] = result
                
            except Exception as e:
                logger.error(f"计算题目 {question_id} 难度系数失败: {e}")
                results[str(question_id)] = {
                    'error': str(e),
                    'difficulty_coefficient': None
                }
        
        # 整体统计
        valid_results = [r for r in results.values() if 'error' not in r]
        if valid_results:
            overall_stats = self._calculate_batch_summary(valid_results)
            results['_summary'] = overall_stats
        
        return results
    
    def _classify_difficulty_level(self, coefficient: float) -> str:
        """根据难度系数分类难度等级"""
        if coefficient > 0.7:
            return "easy"
        elif coefficient >= 0.3:
            return "medium" 
        else:
            return "hard"
    
    def _interpret_difficulty(self, coefficient: float, level: str) -> Dict[str, str]:
        """解释难度系数结果"""
        interpretations = {
            "easy": {
                "zh": f"题目较容易，得分率{coefficient:.1%}",
                "en": f"Easy question with {coefficient:.1%} score rate",
                "suggestion": "可考虑增加题目难度或作为基础题保留"
            },
            "medium": {
                "zh": f"题目难度适中，得分率{coefficient:.1%}",
                "en": f"Medium difficulty with {coefficient:.1%} score rate", 
                "suggestion": "题目难度合适，有良好的区分效果"
            },
            "hard": {
                "zh": f"题目较困难，得分率{coefficient:.1%}",
                "en": f"Hard question with {coefficient:.1%} score rate",
                "suggestion": "题目较难，可考虑降低难度或作为挑战题"
            }
        }
        
        return interpretations.get(level, {
            "zh": f"难度系数{coefficient:.3f}",
            "en": f"Difficulty coefficient {coefficient:.3f}",
            "suggestion": "需要进一步分析"
        })
    
    def _calculate_question_stats(self, scores: pd.Series, max_score: float) -> Dict[str, Any]:
        """计算题目详细统计信息"""
        # 处理单个样本的情况，标准差和方差为0
        std_dev = float(scores.std(ddof=1)) if len(scores) > 1 else 0.0
        variance = float(scores.var(ddof=1)) if len(scores) > 1 else 0.0
        
        return {
            'min_score': float(scores.min()),
            'max_score_achieved': float(scores.max()),
            'median_score': float(scores.median()),
            'std_dev': std_dev,
            'score_variance': variance,
            'perfect_score_rate': float((scores == max_score).sum() / len(scores)),
            'zero_score_rate': float((scores == 0).sum() / len(scores)),
            'score_distribution': self._get_score_distribution(scores, max_score)
        }
    
    def _get_score_distribution(self, scores: pd.Series, max_score: float) -> Dict[str, float]:
        """获取分数分布"""
        bins = np.array([0, 0.2, 0.4, 0.6, 0.8, 1.0]) * max_score
        labels = ['0-20%', '20-40%', '40-60%', '60-80%', '80-100%']
        
        # 使用pandas cut进行分组
        score_groups = pd.cut(scores, bins=bins, labels=labels, include_lowest=True)
        distribution = score_groups.value_counts(normalize=True).sort_index()
        
        return {label: float(distribution.get(label, 0)) for label in labels}
    
    def _calculate_batch_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算批量结果的汇总统计"""
        coefficients = [r['difficulty_coefficient'] for r in results]
        levels = [r['difficulty_level'] for r in results]
        
        # 难度系数统计
        coeff_stats = {
            'mean_difficulty': float(np.mean(coefficients)),
            'median_difficulty': float(np.median(coefficients)),
            'std_difficulty': float(np.std(coefficients, ddof=1)),
            'min_difficulty': float(np.min(coefficients)),
            'max_difficulty': float(np.max(coefficients))
        }
        
        # 难度分布统计
        level_counts = pd.Series(levels).value_counts()
        total_questions = len(results)
        
        level_distribution = {
            'easy_count': int(level_counts.get('easy', 0)),
            'medium_count': int(level_counts.get('medium', 0)), 
            'hard_count': int(level_counts.get('hard', 0)),
            'easy_rate': float(level_counts.get('easy', 0) / total_questions),
            'medium_rate': float(level_counts.get('medium', 0) / total_questions),
            'hard_rate': float(level_counts.get('hard', 0) / total_questions),
            'total_questions': total_questions
        }
        
        return {
            'difficulty_statistics': coeff_stats,
            'difficulty_distribution': level_distribution,
            'quality_assessment': self._assess_batch_quality(level_distribution)
        }
    
    def _assess_batch_quality(self, distribution: Dict[str, Any]) -> Dict[str, str]:
        """评估试卷难度分布质量"""
        easy_rate = distribution['easy_rate']
        medium_rate = distribution['medium_rate'] 
        hard_rate = distribution['hard_rate']
        
        # 理想分布：易题30-40%，中等题40-50%，难题10-30%
        if 0.3 <= easy_rate <= 0.4 and 0.4 <= medium_rate <= 0.5 and 0.1 <= hard_rate <= 0.3:
            quality = "excellent"
            suggestion = "题目难度分布非常合理"
        elif 0.2 <= easy_rate <= 0.5 and 0.3 <= medium_rate <= 0.6 and 0.05 <= hard_rate <= 0.4:
            quality = "good"
            suggestion = "题目难度分布较为合理"
        else:
            quality = "needs_improvement"
            if easy_rate > 0.6:
                suggestion = "题目过于简单，建议增加中等和困难题"
            elif hard_rate > 0.5:
                suggestion = "题目过于困难，建议增加简单和中等题"
            else:
                suggestion = "题目难度分布需要调整以更好地区分学生能力"
        
        return {
            'quality_level': quality,
            'suggestion': suggestion,
            'ideal_distribution': "易题30-40%, 中等题40-50%, 难题10-30%"
        }
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        # 基础验证
        if data.empty:
            validation_result['is_valid'] = False
            validation_result['errors'].append("数据集为空")
            return validation_result
        
        if 'score' not in data.columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append("缺少必需字段: score")
            return validation_result
        
        # 分数数据验证
        scores = pd.to_numeric(data['score'], errors='coerce')
        null_count = scores.isna().sum()
        valid_count = len(scores) - null_count
        
        if valid_count == 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("所有分数数据均无效")
        elif null_count > 0:
            validation_result['warnings'].append(f"发现{null_count}个无效分数值")
        
        # 满分验证
        max_score = config.get('max_score', 100)
        if max_score <= 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("满分配置无效，必须大于0")
        
        # 分数范围验证  
        valid_scores = scores.dropna()
        if len(valid_scores) > 0:
            out_of_range = valid_scores[(valid_scores < 0) | (valid_scores > max_score)]
            if len(out_of_range) > 0:
                validation_result['warnings'].append(
                    f"发现{len(out_of_range)}个超出范围[0, {max_score}]的分数"
                )
        
        # 样本大小检查
        if valid_count < 5:
            validation_result['warnings'].append(f"样本数量过少({valid_count})，计算结果可能不稳定")
        
        validation_result['stats'] = {
            'total_records': len(data),
            'valid_scores': int(valid_count),
            'null_scores': int(null_count),
            'data_completeness': float(valid_count / len(data)),
            'max_score': float(max_score)
        }
        
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        """获取算法信息"""
        return {
            'name': 'DifficultyCoefficient',
            'version': '1.0',
            'description': '难度系数计算器',
            'formula': '难度系数 = 平均分 / 满分',
            'classification': 'Easy(>0.7), Medium(0.3-0.7), Hard(<0.3)',
            'standard': 'Educational Statistics',
            'use_case': '评估题目难度，优化试卷结构'
        }


# 便捷函数
def calculate_difficulty_coefficient(scores: Union[pd.Series, List[float]], 
                                   max_score: float = 100) -> float:
    """计算难度系数的简化函数
    
    Args:
        scores: 分数数据
        max_score: 满分
    
    Returns:
        难度系数值
    """
    if isinstance(scores, list):
        scores = pd.Series(scores)
    
    if scores.empty:
        return 0.0
        
    return float(scores.mean() / max_score)


def classify_difficulty_level(coefficient: float) -> str:
    """分类难度等级"""
    if coefficient > 0.7:
        return "easy"
    elif coefficient >= 0.3:
        return "medium"
    else:
        return "hard"


def batch_calculate_difficulty(data: pd.DataFrame, 
                              score_column: str = 'score',
                              question_column: str = 'question_id',
                              max_score: float = 100) -> pd.DataFrame:
    """批量计算题目难度系数
    
    Args:
        data: 包含分数和题目信息的DataFrame
        score_column: 分数列名
        question_column: 题目ID列名  
        max_score: 满分
    
    Returns:
        包含每个题目难度系数的DataFrame
    """
    if score_column not in data.columns or question_column not in data.columns:
        raise ValueError(f"缺少必要列: {score_column}, {question_column}")
    
    results = []
    
    for question_id, group in data.groupby(question_column):
        scores = group[score_column].astype(float).dropna()
        
        if len(scores) > 0:
            coefficient = calculate_difficulty_coefficient(scores, max_score)
            level = classify_difficulty_level(coefficient)
            
            results.append({
                'question_id': question_id,
                'difficulty_coefficient': coefficient,
                'difficulty_level': level,
                'average_score': scores.mean(),
                'sample_size': len(scores)
            })
    
    return pd.DataFrame(results)