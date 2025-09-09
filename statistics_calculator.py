#!/usr/bin/env python3
"""
教育统计计算模块
实现区分度、难度系数、分位数等核心算法
"""
import statistics
import numpy as np
from typing import List, Tuple, Dict, Any
import json

class EducationalStatisticsCalculator:
    """教育统计指标计算器"""
    
    @staticmethod
    def calculate_basic_stats(scores: List[float], max_scores: List[float] = None) -> Dict[str, Any]:
        """
        计算基础统计指标
        
        Args:
            scores: 分数列表
            max_scores: 满分列表（用于计算得分率）
            
        Returns:
            包含各项统计指标的字典
        """
        if not scores:
            return {
                'mean': 0,
                'std_dev': 0,
                'score_rate': 0,
                'percentiles': {'p10': 0, 'p50': 0, 'p90': 0},
                'student_count': 0
            }
        
        # 基础统计
        mean_score = statistics.mean(scores)
        std_dev = statistics.stdev(scores) if len(scores) > 1 else 0
        
        # 得分率计算
        if max_scores and len(max_scores) == len(scores):
            score_rates = [score / max_score if max_score > 0 else 0 
                          for score, max_score in zip(scores, max_scores)]
            avg_score_rate = statistics.mean(score_rates)
        else:
            avg_score_rate = 0
        
        # 分位数计算
        percentiles = EducationalStatisticsCalculator.calculate_percentiles(scores)
        
        return {
            'mean': round(mean_score, 2),
            'std_dev': round(std_dev, 2),
            'score_rate': round(avg_score_rate, 4),
            'percentiles': percentiles,
            'student_count': len(scores)
        }
    
    @staticmethod
    def calculate_percentiles(scores: List[float]) -> Dict[str, float]:
        """计算分位数 P10, P50, P90"""
        if not scores:
            return {'p10': 0, 'p50': 0, 'p90': 0}
        
        return {
            'p10': round(np.percentile(scores, 10), 2),
            'p50': round(np.percentile(scores, 50), 2),  # 中位数
            'p90': round(np.percentile(scores, 90), 2)
        }
    
    @staticmethod
    def calculate_discrimination(scores: List[float], max_scores: List[float] = None) -> float:
        """
        计算区分度
        
        区分度 = (高分组平均分 - 低分组平均分) / 满分
        高分组：前27%，低分组：后27%
        
        Args:
            scores: 分数列表
            max_scores: 满分列表，如果提供则用于标准化
            
        Returns:
            区分度值 (0-1之间，越大区分度越好)
        """
        if len(scores) < 10:  # 样本太少无法计算区分度
            return 0
        
        # 按分数排序，获取索引
        score_with_index = [(score, i) for i, score in enumerate(scores)]
        score_with_index.sort(key=lambda x: x[0], reverse=True)
        
        n = len(scores)
        high_group_size = max(1, int(n * 0.27))  # 前27%为高分组
        low_group_size = max(1, int(n * 0.27))   # 后27%为低分组
        
        # 高分组（前27%）
        high_group_scores = [score for score, _ in score_with_index[:high_group_size]]
        # 低分组（后27%） 
        low_group_scores = [score for score, _ in score_with_index[-low_group_size:]]
        
        high_group_mean = statistics.mean(high_group_scores)
        low_group_mean = statistics.mean(low_group_scores)
        
        # 计算区分度
        if max_scores:
            # 使用对应的满分进行标准化
            high_indices = [i for _, i in score_with_index[:high_group_size]]
            low_indices = [i for _, i in score_with_index[-low_group_size:]]
            
            high_max_scores = [max_scores[i] for i in high_indices if i < len(max_scores)]
            low_max_scores = [max_scores[i] for i in low_indices if i < len(max_scores)]
            
            if high_max_scores and low_max_scores:
                avg_max_score = statistics.mean(high_max_scores + low_max_scores)
                discrimination = (high_group_mean - low_group_mean) / avg_max_score if avg_max_score > 0 else 0
            else:
                # 如果没有满分信息，使用最高分作为参考
                max_score = max(scores)
                discrimination = (high_group_mean - low_group_mean) / max_score if max_score > 0 else 0
        else:
            # 使用最高分作为参考
            max_score = max(scores)
            discrimination = (high_group_mean - low_group_mean) / max_score if max_score > 0 else 0
        
        return round(max(0, min(1, discrimination)), 4)  # 限制在0-1之间
    
    @staticmethod
    def calculate_difficulty(scores: List[float], max_scores: List[float]) -> float:
        """
        计算难度系数
        
        难度系数 = 平均得分率 = 平均分 / 满分
        难度系数越大表示题目越简单
        
        Args:
            scores: 分数列表
            max_scores: 满分列表
            
        Returns:
            难度系数 (0-1之间，越大越简单)
        """
        if not scores or not max_scores or len(scores) != len(max_scores):
            return 0
        
        score_rates = []
        for score, max_score in zip(scores, max_scores):
            if max_score > 0:
                score_rates.append(score / max_score)
        
        if not score_rates:
            return 0
        
        difficulty = statistics.mean(score_rates)
        return round(max(0, min(1, difficulty)), 4)  # 限制在0-1之间
    
    @staticmethod
    def calculate_dimension_stats_from_json(dimension_scores_list: List[str], 
                                          dimension_max_scores_list: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        从JSON字符串计算维度统计指标
        
        Args:
            dimension_scores_list: 维度分数JSON字符串列表
            dimension_max_scores_list: 维度满分JSON字符串列表
            
        Returns:
            按维度代码组织的统计指标字典
        """
        dimension_data = {}
        
        # 解析所有学生的维度数据
        for scores_json, max_scores_json in zip(dimension_scores_list, dimension_max_scores_list):
            try:
                scores_dict = json.loads(scores_json) if scores_json else {}
                max_scores_dict = json.loads(max_scores_json) if max_scores_json else {}
                
                for dim_code, score_data in scores_dict.items():
                    if dim_code not in dimension_data:
                        dimension_data[dim_code] = {
                            'scores': [],
                            'max_scores': []
                        }
                    
                    # 处理两种格式：简单格式（数字）和复杂格式（包含name和score的对象）
                    if isinstance(score_data, dict):
                        # 复杂格式：{"name": "识别问题", "score": 1.0}
                        score = float(score_data.get('score', 0))
                    else:
                        # 简单格式：直接是数字
                        score = float(score_data)
                    
                    dimension_data[dim_code]['scores'].append(score)
                    
                    # 处理满分数据
                    max_score_data = max_scores_dict.get(dim_code, 0)
                    if isinstance(max_score_data, dict):
                        max_score = float(max_score_data.get('score', 0))
                    else:
                        max_score = float(max_score_data)
                    dimension_data[dim_code]['max_scores'].append(max_score)
                    
            except (json.JSONDecodeError, ValueError, TypeError):
                continue
        
        # 计算每个维度的统计指标
        dimension_stats = {}
        for dim_code, data in dimension_data.items():
            scores = data['scores']
            max_scores = data['max_scores']
            
            if scores:
                basic_stats = EducationalStatisticsCalculator.calculate_basic_stats(scores, max_scores)
                discrimination = EducationalStatisticsCalculator.calculate_discrimination(scores, max_scores)
                difficulty = EducationalStatisticsCalculator.calculate_difficulty(scores, max_scores)
                
                dimension_stats[dim_code] = {
                    **basic_stats,
                    'discrimination': discrimination,
                    'difficulty': difficulty
                }
        
        return dimension_stats
    
    @staticmethod
    def calculate_option_distribution(option_data: List[Tuple[str, int]]) -> Dict[str, Dict[str, Any]]:
        """
        计算选项分布统计
        
        Args:
            option_data: (选项标签, 数量) 元组列表
            
        Returns:
            选项分布统计字典
        """
        if not option_data:
            return {}
        
        total_count = sum(count for _, count in option_data)
        if total_count == 0:
            return {}
        
        distribution = {}
        for option_label, count in option_data:
            percentage = (count / total_count) * 100
            distribution[option_label] = {
                'count': count,
                'percentage': round(percentage, 2)
            }
        
        return distribution
    
    @staticmethod
    def validate_scores_data(scores: List[float], max_scores: List[float] = None) -> Tuple[bool, str]:
        """
        验证分数数据的有效性
        
        Returns:
            (是否有效, 错误信息)
        """
        if not scores:
            return False, "分数列表为空"
        
        if any(score < 0 for score in scores):
            return False, "存在负分数"
        
        if max_scores:
            if len(scores) != len(max_scores):
                return False, "分数和满分列表长度不一致"
            
            if any(max_score <= 0 for max_score in max_scores):
                return False, "存在非正数满分值"
            
            invalid_scores = [i for i, (score, max_score) in enumerate(zip(scores, max_scores)) 
                            if score > max_score]
            if invalid_scores:
                return False, f"存在超过满分的分数，索引: {invalid_scores[:5]}"
        
        return True, "数据有效"

def test_statistics_calculator():
    """测试统计计算器功能"""
    print("=== 测试教育统计计算器 ===\n")
    
    # 测试数据
    scores = [85, 92, 78, 95, 67, 89, 76, 88, 91, 73, 82, 90, 77, 86, 94]
    max_scores = [100] * len(scores)
    
    calc = EducationalStatisticsCalculator()
    
    # 基础统计
    basic_stats = calc.calculate_basic_stats(scores, max_scores)
    print("1. 基础统计:")
    print(f"   平均分: {basic_stats['mean']}")
    print(f"   标准差: {basic_stats['std_dev']}")
    print(f"   得分率: {basic_stats['score_rate']:.2%}")
    print(f"   学生数: {basic_stats['student_count']}")
    print(f"   分位数: P10={basic_stats['percentiles']['p10']}, P50={basic_stats['percentiles']['p50']}, P90={basic_stats['percentiles']['p90']}")
    
    # 区分度
    discrimination = calc.calculate_discrimination(scores, max_scores)
    print(f"\n2. 区分度: {discrimination}")
    
    # 难度系数
    difficulty = calc.calculate_difficulty(scores, max_scores)
    print(f"\n3. 难度系数: {difficulty}")
    
    # 选项分布测试
    option_data = [("非常满意", 6), ("满意", 4), ("不满意", 3), ("非常不满意", 2)]
    distribution = calc.calculate_option_distribution(option_data)
    print(f"\n4. 选项分布:")
    for option, data in distribution.items():
        print(f"   {option}: {data['count']}人 ({data['percentage']}%)")

if __name__ == "__main__":
    test_statistics_calculator()