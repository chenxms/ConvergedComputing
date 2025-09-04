# 区分度计算器
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from ..engine import StatisticalStrategy

logger = logging.getLogger(__name__)


class DiscriminationCalculator(StatisticalStrategy):
    """区分度计算器
    
    区分度定义：(前27%学生平均分 - 后27%学生平均分) / 满分
    等级划分：
    - 优秀 (Excellent): ≥ 0.4
    - 良好 (Good): 0.3 - 0.4
    - 一般 (Acceptable): 0.2 - 0.3  
    - 差 (Poor): < 0.2
    """
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """计算区分度
        
        Args:
            data: 包含学生分数的DataFrame，必须有'score'列
            config: 配置参数，包括：
                - max_score: 满分(默认100)
                - question_id: 题目ID(可选)
                - subject_id: 科目ID(可选)
                - group_percentage: 分组百分比(默认0.27)
        
        Returns:
            Dict包含：
            - discrimination_index: 区分度指数
            - high_group_mean: 高分组平均分
            - low_group_mean: 低分组平均分  
            - high_group_size: 高分组人数
            - low_group_size: 低分组人数
            - discrimination_level: 区分度等级
            - interpretation: 结果解释
            - group_details: 分组详细信息
        """
        if 'score' not in data.columns:
            raise ValueError("数据中缺少'score'列")
            
        scores = data['score'].astype(float).dropna()
        
        if len(scores) == 0:
            raise ValueError("没有有效的分数数据")
        
        max_score = config.get('max_score', 100)
        if max_score <= 0:
            raise ValueError("满分必须大于0")
        
        group_percentage = config.get('group_percentage', 0.27)
        if not 0.1 <= group_percentage <= 0.5:
            raise ValueError("分组百分比必须在0.1-0.5之间")
        
        # 样本大小检查
        if len(scores) < 10:
            logger.warning(f"样本数量过少({len(scores)})，区分度计算可能不准确")
        
        # 计算区分度
        discrimination_data = self._calculate_discrimination_groups(scores, max_score, group_percentage)
        
        # 区分度等级判定
        discrimination_level = self._classify_discrimination_level(discrimination_data['discrimination_index'])
        
        # 结果解释
        interpretation = self._interpret_discrimination(
            discrimination_data['discrimination_index'], 
            discrimination_level
        )
        
        # 组装结果
        result = {
            **discrimination_data,
            'discrimination_level': discrimination_level,
            'interpretation': interpretation,
            'max_score': float(max_score),
            'group_percentage': float(group_percentage),
            'total_sample_size': int(len(scores))
        }
        
        # 添加可选的题目和科目信息
        if 'question_id' in config:
            result['question_id'] = config['question_id']
        if 'subject_id' in config:
            result['subject_id'] = config['subject_id']
            
        return result
    
    def calculate_batch_discrimination(self, data: pd.DataFrame, 
                                     config: Dict[str, Any]) -> Dict[str, Any]:
        """批量计算多题目区分度
        
        Args:
            data: 包含学生分数的DataFrame，必须有'score', 'question_id'列
            config: 配置参数
        
        Returns:
            Dict包含每个题目的区分度结果
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
                logger.error(f"计算题目 {question_id} 区分度失败: {e}")
                results[str(question_id)] = {
                    'error': str(e),
                    'discrimination_index': None
                }
        
        # 整体统计
        valid_results = [r for r in results.values() if 'error' not in r]
        if valid_results:
            overall_stats = self._calculate_batch_summary(valid_results)
            results['_summary'] = overall_stats
        
        return results
    
    def calculate_exam_level_discrimination(self, data: pd.DataFrame, 
                                         config: Dict[str, Any]) -> Dict[str, Any]:
        """计算考试级别的区分度
        
        基于学生总分进行前后27%分组，然后计算各题目的区分度
        """
        if 'student_id' not in data.columns or 'question_id' not in data.columns:
            raise ValueError("考试级别计算需要'student_id'和'question_id'列")
        
        # 计算每个学生的总分
        student_totals = data.groupby('student_id')['score'].sum().reset_index()
        student_totals.columns = ['student_id', 'total_score']
        
        # 按总分排序并分组
        student_totals_sorted = student_totals.sort_values('total_score', ascending=False)
        n_students = len(student_totals_sorted)
        
        group_size = max(1, int(n_students * config.get('group_percentage', 0.27)))
        
        high_group_students = set(student_totals_sorted.iloc[:group_size]['student_id'])
        low_group_students = set(student_totals_sorted.iloc[-group_size:]['student_id'])
        
        # 为每个题目计算区分度
        results = {}
        max_score = config.get('max_score', 100)
        
        for question_id, question_data in data.groupby('question_id'):
            try:
                high_group_scores = question_data[
                    question_data['student_id'].isin(high_group_students)
                ]['score'].astype(float).dropna()
                
                low_group_scores = question_data[
                    question_data['student_id'].isin(low_group_students)
                ]['score'].astype(float).dropna()
                
                if len(high_group_scores) > 0 and len(low_group_scores) > 0:
                    high_mean = high_group_scores.mean()
                    low_mean = low_group_scores.mean()
                    discrimination_index = (high_mean - low_mean) / max_score
                    
                    results[str(question_id)] = {
                        'question_id': question_id,
                        'discrimination_index': float(discrimination_index),
                        'high_group_mean': float(high_mean),
                        'low_group_mean': float(low_mean),
                        'high_group_size': len(high_group_scores),
                        'low_group_size': len(low_group_scores),
                        'discrimination_level': self._classify_discrimination_level(discrimination_index),
                        'calculation_method': 'exam_level'
                    }
                else:
                    results[str(question_id)] = {
                        'error': '高分组或低分组数据不足',
                        'discrimination_index': None
                    }
                    
            except Exception as e:
                logger.error(f"计算题目 {question_id} 考试级区分度失败: {e}")
                results[str(question_id)] = {
                    'error': str(e),
                    'discrimination_index': None
                }
        
        # 添加分组信息
        results['_grouping_info'] = {
            'total_students': n_students,
            'high_group_size': group_size,
            'low_group_size': group_size,
            'high_group_students': list(high_group_students),
            'low_group_students': list(low_group_students),
            'method': 'exam_level_grouping'
        }
        
        return results
    
    def _calculate_discrimination_groups(self, scores: pd.Series, 
                                       max_score: float, 
                                       group_percentage: float) -> Dict[str, Any]:
        """计算区分度分组数据"""
        # 按分数降序排序
        scores_sorted = scores.sort_values(ascending=False).reset_index(drop=True)
        n = len(scores_sorted)
        
        # 计算分组大小
        group_size = max(1, int(n * group_percentage))
        
        # 分组
        high_group = scores_sorted.iloc[:group_size]
        low_group = scores_sorted.iloc[-group_size:]
        
        # 计算平均分
        high_group_mean = high_group.mean()
        low_group_mean = low_group.mean()
        
        # 计算区分度
        discrimination_index = (high_group_mean - low_group_mean) / max_score
        
        # 分组详细信息
        group_details = self._analyze_group_details(high_group, low_group, max_score)
        
        return {
            'discrimination_index': float(discrimination_index),
            'high_group_mean': float(high_group_mean),
            'low_group_mean': float(low_group_mean),
            'high_group_size': int(len(high_group)),
            'low_group_size': int(len(low_group)),
            'group_details': group_details
        }
    
    def _analyze_group_details(self, high_group: pd.Series, 
                              low_group: pd.Series, 
                              max_score: float) -> Dict[str, Any]:
        """分析分组详细信息"""
        return {
            'high_group_stats': {
                'min': float(high_group.min()),
                'max': float(high_group.max()),
                'median': float(high_group.median()),
                'std': float(high_group.std(ddof=1) if len(high_group) > 1 else 0),
                'score_rate': float(high_group.mean() / max_score)
            },
            'low_group_stats': {
                'min': float(low_group.min()),
                'max': float(low_group.max()),
                'median': float(low_group.median()),
                'std': float(low_group.std(ddof=1) if len(low_group) > 1 else 0),
                'score_rate': float(low_group.mean() / max_score)
            },
            'group_overlap': self._check_group_overlap(high_group, low_group),
            'score_gap': float(high_group.mean() - low_group.mean())
        }
    
    def _check_group_overlap(self, high_group: pd.Series, low_group: pd.Series) -> Dict[str, Any]:
        """检查分组重叠情况"""
        high_min = high_group.min()
        low_max = low_group.max()
        
        overlap = low_max >= high_min
        overlap_range = max(0, low_max - high_min) if overlap else 0
        
        return {
            'has_overlap': bool(overlap),
            'overlap_range': float(overlap_range),
            'high_group_min': float(high_min),
            'low_group_max': float(low_max),
            'separation_quality': 'poor' if overlap else 'good'
        }
    
    def _classify_discrimination_level(self, index: float) -> str:
        """根据区分度指数分类等级"""
        if index >= 0.4:
            return "excellent"
        elif index >= 0.3:
            return "good"
        elif index >= 0.2:
            return "acceptable"
        else:
            return "poor"
    
    def _interpret_discrimination(self, index: float, level: str) -> Dict[str, str]:
        """解释区分度结果"""
        interpretations = {
            "excellent": {
                "zh": f"区分度优秀({index:.3f})，能很好地区分不同能力的学生",
                "en": f"Excellent discrimination ({index:.3f}), well differentiates students",
                "suggestion": "题目质量很好，保持当前设计"
            },
            "good": {
                "zh": f"区分度良好({index:.3f})，有较好的区分效果",
                "en": f"Good discrimination ({index:.3f}), decent differentiation",
                "suggestion": "题目质量较好，可微调以提升区分度"
            },
            "acceptable": {
                "zh": f"区分度一般({index:.3f})，区分效果有限",
                "en": f"Acceptable discrimination ({index:.3f}), limited differentiation",
                "suggestion": "建议优化题目设计，提高区分度"
            },
            "poor": {
                "zh": f"区分度较差({index:.3f})，难以区分学生能力差异",
                "en": f"Poor discrimination ({index:.3f}), fails to differentiate students",
                "suggestion": "题目需要重新设计，提高区分能力"
            }
        }
        
        return interpretations.get(level, {
            "zh": f"区分度指数{index:.3f}",
            "en": f"Discrimination index {index:.3f}",
            "suggestion": "需要进一步分析"
        })
    
    def _calculate_batch_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算批量结果的汇总统计"""
        indices = [r['discrimination_index'] for r in results]
        levels = [r['discrimination_level'] for r in results]
        
        # 区分度统计
        discrimination_stats = {
            'mean_discrimination': float(np.mean(indices)),
            'median_discrimination': float(np.median(indices)),
            'std_discrimination': float(np.std(indices, ddof=1)),
            'min_discrimination': float(np.min(indices)),
            'max_discrimination': float(np.max(indices))
        }
        
        # 等级分布统计
        level_counts = pd.Series(levels).value_counts()
        total_questions = len(results)
        
        level_distribution = {
            'excellent_count': int(level_counts.get('excellent', 0)),
            'good_count': int(level_counts.get('good', 0)),
            'acceptable_count': int(level_counts.get('acceptable', 0)),
            'poor_count': int(level_counts.get('poor', 0)),
            'excellent_rate': float(level_counts.get('excellent', 0) / total_questions),
            'good_rate': float(level_counts.get('good', 0) / total_questions),
            'acceptable_rate': float(level_counts.get('acceptable', 0) / total_questions),
            'poor_rate': float(level_counts.get('poor', 0) / total_questions),
            'total_questions': total_questions
        }
        
        return {
            'discrimination_statistics': discrimination_stats,
            'level_distribution': level_distribution,
            'quality_assessment': self._assess_batch_discrimination_quality(level_distribution)
        }
    
    def _assess_batch_discrimination_quality(self, distribution: Dict[str, Any]) -> Dict[str, str]:
        """评估试卷区分度质量"""
        excellent_rate = distribution['excellent_rate']
        good_rate = distribution['good_rate']
        poor_rate = distribution['poor_rate']
        
        high_quality_rate = excellent_rate + good_rate
        
        if high_quality_rate >= 0.7 and poor_rate <= 0.2:
            quality = "excellent"
            suggestion = "试卷整体区分度很好，能有效区分不同能力学生"
        elif high_quality_rate >= 0.5 and poor_rate <= 0.3:
            quality = "good"
            suggestion = "试卷区分度较好，可进一步优化部分题目"
        elif high_quality_rate >= 0.3 and poor_rate <= 0.5:
            quality = "acceptable"
            suggestion = "试卷区分度一般，建议重新设计部分题目"
        else:
            quality = "poor"
            suggestion = "试卷区分度较差，需要全面优化题目设计"
        
        return {
            'quality_level': quality,
            'suggestion': suggestion,
            'high_quality_rate': float(high_quality_rate),
            'recommendation': "建议优秀+良好题目占比≥70%，差题目占比≤20%"
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
        
        # 分组百分比验证
        group_percentage = config.get('group_percentage', 0.27)
        if not 0.1 <= group_percentage <= 0.5:
            validation_result['is_valid'] = False
            validation_result['errors'].append("分组百分比必须在0.1-0.5之间")
        
        # 样本大小检查
        if valid_count < 10:
            validation_result['warnings'].append(
                f"样本数量过少({valid_count})，区分度计算可能不准确，建议至少30个样本"
            )
        elif valid_count < 30:
            validation_result['warnings'].append(
                f"样本数量较少({valid_count})，建议至少30个样本以获得稳定结果"
            )
        
        # 分数分布检查
        valid_scores = scores.dropna()
        if len(valid_scores) > 0:
            score_range = valid_scores.max() - valid_scores.min()
            if score_range < max_score * 0.1:
                validation_result['warnings'].append("分数分布过于集中，可能影响区分度计算")
        
        validation_result['stats'] = {
            'total_records': len(data),
            'valid_scores': int(valid_count),
            'null_scores': int(null_count),
            'data_completeness': float(valid_count / len(data)),
            'max_score': float(max_score),
            'group_percentage': float(group_percentage)
        }
        
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        """获取算法信息"""
        return {
            'name': 'Discrimination',
            'version': '1.0',
            'description': '区分度计算器（前27%后27%分组法）',
            'formula': '区分度 = (高分组平均分 - 低分组平均分) / 满分',
            'classification': 'Excellent(≥0.4), Good(0.3-0.4), Acceptable(0.2-0.3), Poor(<0.2)',
            'standard': 'Educational Statistics 27% Rule',
            'use_case': '评估题目区分不同能力学生的效果'
        }


# 便捷函数
def calculate_discrimination_index(scores: Union[pd.Series, List[float]], 
                                 max_score: float = 100,
                                 group_percentage: float = 0.27) -> float:
    """计算区分度的简化函数
    
    Args:
        scores: 分数数据
        max_score: 满分
        group_percentage: 分组百分比
    
    Returns:
        区分度指数
    """
    if isinstance(scores, list):
        scores = pd.Series(scores)
    
    if len(scores) < 4:  # 至少需要4个样本才能分组
        return 0.0
    
    scores_sorted = scores.sort_values(ascending=False)
    n = len(scores_sorted)
    
    group_size = max(1, int(n * group_percentage))
    
    high_group = scores_sorted.iloc[:group_size]
    low_group = scores_sorted.iloc[-group_size:]
    
    return float((high_group.mean() - low_group.mean()) / max_score)


def classify_discrimination_level(index: float) -> str:
    """分类区分度等级"""
    if index >= 0.4:
        return "excellent"
    elif index >= 0.3:
        return "good"
    elif index >= 0.2:
        return "acceptable"
    else:
        return "poor"


def analyze_discrimination_distribution(discrimination_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """分析区分度分布情况"""
    if not discrimination_results:
        return {'error': 'No results to analyze'}
    
    indices = [r.get('discrimination_index', 0) for r in discrimination_results if r.get('discrimination_index') is not None]
    levels = [r.get('discrimination_level', 'unknown') for r in discrimination_results]
    
    if not indices:
        return {'error': 'No valid discrimination indices found'}
    
    level_counts = pd.Series(levels).value_counts()
    total = len(discrimination_results)
    
    return {
        'statistics': {
            'mean': float(np.mean(indices)),
            'median': float(np.median(indices)),
            'std': float(np.std(indices, ddof=1)) if len(indices) > 1 else 0.0,
            'min': float(np.min(indices)),
            'max': float(np.max(indices))
        },
        'distribution': {
            'excellent': {'count': level_counts.get('excellent', 0), 'rate': level_counts.get('excellent', 0) / total},
            'good': {'count': level_counts.get('good', 0), 'rate': level_counts.get('good', 0) / total},
            'acceptable': {'count': level_counts.get('acceptable', 0), 'rate': level_counts.get('acceptable', 0) / total},
            'poor': {'count': level_counts.get('poor', 0), 'rate': level_counts.get('poor', 0) / total}
        },
        'total_questions': total
    }