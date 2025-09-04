# 教育统计公式和策略实现
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any, List, Optional
from .engine import StatisticalStrategy

logger = logging.getLogger(__name__)


class BasicStatisticsStrategy(StatisticalStrategy):
    """基础统计指标计算策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """计算基础统计指标"""
        if 'score' not in data.columns:
            raise ValueError("数据中缺少'score'列")
            
        scores = data['score'].astype(float).dropna()
        
        if len(scores) == 0:
            raise ValueError("没有有效的分数数据")
        
        # 使用向量化计算提高性能
        result = {
            'count': int(len(scores)),
            'sum': float(scores.sum()),
            'mean': float(scores.mean()),
            'median': float(scores.median()),
            'std': float(scores.std(ddof=1)),  # 样本标准差
            'variance': float(scores.var(ddof=1)),  # 样本方差
            'min': float(scores.min()),
            'max': float(scores.max()),
            'range': float(scores.max() - scores.min()),
            'skewness': float(scores.skew()),
            'kurtosis': float(scores.kurtosis())
        }
        
        # 处理众数
        mode_values = scores.mode()
        if not mode_values.empty:
            result['mode'] = float(mode_values.iloc[0])
        else:
            result['mode'] = None
        
        return result
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        if data.empty:
            validation_result['is_valid'] = False
            validation_result['errors'].append("数据集为空")
            return validation_result
        
        if 'score' not in data.columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append("缺少必需字段: score")
            return validation_result
        
        # 检查数据类型和范围
        scores = pd.to_numeric(data['score'], errors='coerce')
        null_count = scores.isna().sum()
        
        if null_count == len(data):
            validation_result['is_valid'] = False
            validation_result['errors'].append("所有分数数据均无效")
        elif null_count > 0:
            validation_result['warnings'].append(f"发现{null_count}个无效分数值")
        
        validation_result['stats']['total_records'] = len(data)
        validation_result['stats']['valid_scores'] = len(data) - null_count
        
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        return {
            'name': 'BasicStatistics',
            'version': '1.0',
            'description': '基础统计指标计算',
            'std_formula': 'sample_std_ddof_1',
            'variance_formula': 'sample_variance_ddof_1'
        }


class EducationalPercentileStrategy(StatisticalStrategy):
    """教育行业百分位数计算策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """使用教育统计标准的百分位数算法"""
        if 'score' not in data.columns:
            raise ValueError("数据中缺少'score'列")
            
        scores = data['score'].astype(float).dropna().sort_values()
        n = len(scores)
        
        if n == 0:
            raise ValueError("没有有效的分数数据")
        
        percentiles = config.get('percentiles', [10, 25, 50, 75, 90])
        result = {}
        
        for p in percentiles:
            # 教育统计标准：使用floor算法
            rank = int(np.floor(n * p / 100.0))
            # 确保索引不超出范围
            rank = max(0, min(rank, n - 1))
            result[f'P{p}'] = float(scores.iloc[rank])
        
        # 四分位距
        if 'P75' in result and 'P25' in result:
            result['IQR'] = result['P75'] - result['P25']
        
        return result
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        if data.empty or 'score' not in data.columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append("数据集为空或缺少score列")
            return validation_result
        
        scores = pd.to_numeric(data['score'], errors='coerce')
        valid_count = scores.notna().sum()
        
        if valid_count == 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("没有有效的分数数据")
        
        validation_result['stats']['valid_scores'] = int(valid_count)
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        return {
            'name': 'EducationalPercentile',
            'version': '1.0',
            'description': '教育统计百分位数计算',
            'algorithm': 'floor(n * p / 100)',
            'standard': 'Chinese Educational Statistics'
        }


class EducationalMetricsStrategy(StatisticalStrategy):
    """教育指标计算策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """计算得分率和等级分布"""
        if 'score' not in data.columns:
            raise ValueError("数据中缺少'score'列")
            
        scores = data['score'].astype(float).dropna()
        max_score = config.get('max_score', 100)
        grade_level = config.get('grade_level', '1st_grade')
        
        if len(scores) == 0:
            raise ValueError("没有有效的分数数据")
        
        result = {}
        total_count = len(scores)
        
        # 得分率计算
        result['average_score_rate'] = float(scores.mean() / max_score)
        
        # 计算各等级分布
        if self._is_primary_grade(grade_level):
            # 小学标准：优秀≥90, 良好80-89, 及格60-79, 不及格<60
            excellent_count = (scores >= max_score * 0.90).sum()
            good_count = ((scores >= max_score * 0.80) & (scores < max_score * 0.90)).sum()
            pass_count = ((scores >= max_score * 0.60) & (scores < max_score * 0.80)).sum()
            fail_count = (scores < max_score * 0.60).sum()
            
            result['grade_distribution'] = {
                'excellent_rate': float(excellent_count / total_count),
                'good_rate': float(good_count / total_count),
                'pass_rate': float(pass_count / total_count),
                'fail_rate': float(fail_count / total_count),
                'excellent_count': int(excellent_count),
                'good_count': int(good_count),
                'pass_count': int(pass_count),
                'fail_count': int(fail_count)
            }
        else:
            # 初中标准：A≥85, B70-84, C60-69, D<60
            a_count = (scores >= max_score * 0.85).sum()
            b_count = ((scores >= max_score * 0.70) & (scores < max_score * 0.85)).sum()
            c_count = ((scores >= max_score * 0.60) & (scores < max_score * 0.70)).sum()
            d_count = (scores < max_score * 0.60).sum()
            
            result['grade_distribution'] = {
                'a_rate': float(a_count / total_count),
                'b_rate': float(b_count / total_count),
                'c_rate': float(c_count / total_count),
                'd_rate': float(d_count / total_count),
                'a_count': int(a_count),
                'b_count': int(b_count),
                'c_count': int(c_count),
                'd_count': int(d_count)
            }
        
        # 及格率和优秀率（通用）
        result['pass_rate'] = float((scores >= max_score * 0.60).sum() / total_count)
        result['excellent_rate'] = float((scores >= max_score * 0.85).sum() / total_count)
        
        # 难度系数计算
        result['difficulty_coefficient'] = result['average_score_rate']
        
        return result
    
    def _is_primary_grade(self, grade_level: str) -> bool:
        """判断是否为小学年级"""
        primary_grades = ['1st_grade', '2nd_grade', '3rd_grade', 
                         '4th_grade', '5th_grade', '6th_grade']
        return grade_level in primary_grades
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        if data.empty or 'score' not in data.columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append("数据集为空或缺少score列")
            return validation_result
        
        scores = pd.to_numeric(data['score'], errors='coerce')
        valid_count = scores.notna().sum()
        max_score = config.get('max_score', 100)
        
        if valid_count == 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("没有有效的分数数据")
        
        # 检查满分配置
        if max_score <= 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("满分配置无效")
        
        validation_result['stats']['valid_scores'] = int(valid_count)
        validation_result['stats']['max_score'] = max_score
        
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        return {
            'name': 'EducationalMetrics',
            'version': '1.0',
            'description': '教育指标和等级分布计算',
            'grading_standard': 'primary_vs_middle_school'
        }


class DiscriminationStrategy(StatisticalStrategy):
    """区分度计算策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """计算区分度（前27%和后27%分组）"""
        if 'score' not in data.columns:
            raise ValueError("数据中缺少'score'列")
            
        scores = data['score'].astype(float).dropna().sort_values(ascending=False)
        n = len(scores)
        
        if n == 0:
            raise ValueError("没有有效的分数数据")
        
        if n < 10:  # 数据量太少无法计算区分度
            logger.warning(f"数据量过少({n})，区分度计算可能不准确")
        
        # 教育统计标准：前27%和后27%
        high_group_size = max(1, int(n * 0.27))
        low_group_size = max(1, int(n * 0.27))
        
        high_group = scores.iloc[:high_group_size]
        low_group = scores.iloc[-low_group_size:]
        
        high_mean = high_group.mean()
        low_mean = low_group.mean()
        max_score = config.get('max_score', 100)
        
        # 区分度 = (高分组平均分 - 低分组平均分) / 满分
        discrimination = (high_mean - low_mean) / max_score
        
        result = {
            'discrimination_index': float(discrimination),
            'high_group_mean': float(high_mean),
            'low_group_mean': float(low_mean),
            'high_group_size': high_group_size,
            'low_group_size': low_group_size,
            'interpretation': self._interpret_discrimination(discrimination)
        }
        
        return result
    
    def _interpret_discrimination(self, index: float) -> str:
        """解释区分度结果"""
        if index >= 0.4:
            return "excellent"
        elif index >= 0.3:
            return "good"
        elif index >= 0.2:
            return "acceptable"
        else:
            return "poor"
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        if data.empty or 'score' not in data.columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append("数据集为空或缺少score列")
            return validation_result
        
        scores = pd.to_numeric(data['score'], errors='coerce')
        valid_count = scores.notna().sum()
        max_score = config.get('max_score', 100)
        
        if valid_count == 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("没有有效的分数数据")
        elif valid_count < 10:
            validation_result['warnings'].append(f"数据量过少({valid_count})，区分度计算可能不准确")
        
        if max_score <= 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("满分配置无效")
        
        validation_result['stats']['valid_scores'] = int(valid_count)
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        return {
            'name': 'Discrimination',
            'version': '1.0',
            'description': '区分度计算（前27%后27%分组）',
            'formula': '(高分组平均分 - 低分组平均分) / 满分',
            'standard': 'Educational Statistics 27% Rule'
        }


class AnomalyDetector:
    """异常数据检测器"""
    
    def detect_outliers(self, data: pd.Series, method: str = 'iqr') -> Dict[str, Any]:
        """检测异常值"""
        if method == 'iqr':
            return self._detect_iqr_outliers(data)
        elif method == 'zscore':
            return self._detect_zscore_outliers(data)
        else:
            raise ValueError(f"不支持的异常检测方法: {method}")
    
    def _detect_iqr_outliers(self, data: pd.Series) -> Dict[str, Any]:
        """基于IQR的异常检测"""
        Q1 = data.quantile(0.25)
        Q3 = data.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = data[(data < lower_bound) | (data > upper_bound)]
        
        return {
            'method': 'IQR',
            'outlier_count': len(outliers),
            'outlier_percentage': len(outliers) / len(data),
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'outlier_indices': outliers.index.tolist()
        }
    
    def _detect_zscore_outliers(self, data: pd.Series, threshold: float = 3.0) -> Dict[str, Any]:
        """基于Z-score的异常检测"""
        z_scores = np.abs((data - data.mean()) / data.std())
        outliers = data[z_scores > threshold]
        
        return {
            'method': 'Z-Score',
            'threshold': threshold,
            'outlier_count': len(outliers),
            'outlier_percentage': len(outliers) / len(data),
            'outlier_indices': outliers.index.tolist()
        }


class VectorizedCalculator:
    """向量化计算器"""
    
    @staticmethod
    def calculate_basic_stats_vectorized(scores: pd.Series) -> Dict[str, float]:
        """向量化基础统计计算"""
        scores_array = scores.values
        
        result = {
            'count': len(scores_array),
            'sum': float(np.sum(scores_array)),
            'mean': float(np.mean(scores_array)),
            'std': float(np.std(scores_array, ddof=1)),
            'var': float(np.var(scores_array, ddof=1)),
            'min': float(np.min(scores_array)),
            'max': float(np.max(scores_array)),
            'median': float(np.median(scores_array))
        }
        
        return result
    
    @staticmethod
    def calculate_grade_distribution_vectorized(scores: pd.Series, 
                                              max_score: float,
                                              grade_level: str) -> Dict[str, Any]:
        """向量化等级分布计算"""
        scores_array = scores.values
        
        if grade_level in ['1st_grade', '2nd_grade', '3rd_grade', 
                          '4th_grade', '5th_grade', '6th_grade']:
            # 小学标准 - 使用NumPy布尔索引
            excellent_mask = scores_array >= (max_score * 0.90)
            good_mask = (scores_array >= (max_score * 0.80)) & (scores_array < (max_score * 0.90))
            pass_mask = (scores_array >= (max_score * 0.60)) & (scores_array < (max_score * 0.80))
            fail_mask = scores_array < (max_score * 0.60)
            
            return {
                'excellent_count': int(np.sum(excellent_mask)),
                'good_count': int(np.sum(good_mask)),
                'pass_count': int(np.sum(pass_mask)),
                'fail_count': int(np.sum(fail_mask)),
                'excellent_rate': float(np.mean(excellent_mask)),
                'good_rate': float(np.mean(good_mask)),
                'pass_rate': float(np.mean(pass_mask)),
                'fail_rate': float(np.mean(fail_mask))
            }
        else:
            # 初中标准
            a_mask = scores_array >= (max_score * 0.85)
            b_mask = (scores_array >= (max_score * 0.70)) & (scores_array < (max_score * 0.85))
            c_mask = (scores_array >= (max_score * 0.60)) & (scores_array < (max_score * 0.70))
            d_mask = scores_array < (max_score * 0.60)
            
            return {
                'a_count': int(np.sum(a_mask)),
                'b_count': int(np.sum(b_mask)),
                'c_count': int(np.sum(c_mask)),
                'd_count': int(np.sum(d_mask)),
                'a_rate': float(np.mean(a_mask)),
                'b_rate': float(np.mean(b_mask)),
                'c_rate': float(np.mean(c_mask)),
                'd_rate': float(np.mean(d_mask))
            }


# 传统函数式接口（保持向后兼容）
def calculate_average(scores: pd.Series) -> float:
    """计算平均分"""
    return float(scores.mean()) if not scores.empty else 0.0


def calculate_standard_deviation(scores: pd.Series) -> float:
    """计算标准差"""
    return float(scores.std(ddof=1)) if len(scores) > 1 else 0.0


def calculate_pass_rate(scores: pd.Series, pass_score: float = 60, max_score: float = 100) -> float:
    """计算及格率"""
    if scores.empty:
        return 0.0
    pass_threshold = max_score * (pass_score / 100.0)
    return float((scores >= pass_threshold).sum() / len(scores))


def calculate_excellent_rate(scores: pd.Series, excellent_score: float = 90, max_score: float = 100) -> float:
    """计算优秀率"""
    if scores.empty:
        return 0.0
    excellent_threshold = max_score * (excellent_score / 100.0)
    return float((scores >= excellent_threshold).sum() / len(scores))


def calculate_percentile(scores: pd.Series, percentile: float) -> float:
    """计算百分位数（教育统计标准）"""
    if scores.empty:
        return 0.0
    scores_sorted = scores.sort_values()
    n = len(scores_sorted)
    rank = int(np.floor(n * percentile / 100.0))
    rank = max(0, min(rank, n - 1))
    return float(scores_sorted.iloc[rank])


def calculate_difficulty_coefficient(scores: pd.Series, max_score: float = 100) -> float:
    """计算难度系数"""
    if scores.empty:
        return 0.0
    return float(scores.mean() / max_score)


def calculate_discrimination_index(scores: pd.Series, max_score: float = 100) -> float:
    """计算区分度"""
    if len(scores) < 10:
        return 0.0
    
    scores_sorted = scores.sort_values(ascending=False)
    n = len(scores_sorted)
    
    high_group_size = max(1, int(n * 0.27))
    low_group_size = max(1, int(n * 0.27))
    
    high_group_mean = scores_sorted.iloc[:high_group_size].mean()
    low_group_mean = scores_sorted.iloc[-low_group_size:].mean()
    
    return float((high_group_mean - low_group_mean) / max_score)