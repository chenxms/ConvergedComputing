# 精确的百分位数计算器
import numpy as np
import pandas as pd
import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from dataclasses import dataclass
from enum import Enum
from ..engine import StatisticalStrategy

logger = logging.getLogger(__name__)


class InterpolationMethod(Enum):
    """插值方法枚举"""
    NEAREST = "nearest"  # 最近邻
    LINEAR = "linear"    # 线性插值
    LOWER = "lower"      # 取较小值
    HIGHER = "higher"    # 取较大值
    MIDPOINT = "midpoint"  # 中点


@dataclass
class PercentileResult:
    """百分位数计算结果"""
    percentile: float
    value: float
    rank: int
    interpolated: bool
    method: InterpolationMethod


class PercentileCalculator:
    """专业的百分位数计算器
    
    支持多种插值方法和边界条件处理
    采用教育统计标准算法：floor(学生总数 × 百分位)
    """
    
    def __init__(self, interpolation_method: InterpolationMethod = InterpolationMethod.NEAREST):
        self.interpolation_method = interpolation_method
        
    def calculate_percentile(self, 
                           data: Union[pd.Series, np.ndarray, List[float]], 
                           percentile: float,
                           method: Optional[InterpolationMethod] = None) -> PercentileResult:
        """计算单个百分位数
        
        Args:
            data: 数据序列
            percentile: 百分位数 (0-100)
            method: 插值方法，如果为None则使用初始化时的方法
            
        Returns:
            PercentileResult: 百分位数计算结果
            
        Raises:
            ValueError: 数据无效或百分位数超出范围
        """
        # 参数验证
        if not (0 <= percentile <= 100):
            raise ValueError(f"百分位数必须在0-100之间，当前值: {percentile}")
            
        # 数据预处理
        scores = self._preprocess_data(data)
        if len(scores) == 0:
            raise ValueError("没有有效的数据进行百分位数计算")
            
        # 使用指定方法或默认方法
        method = method or self.interpolation_method
        
        # 边界条件处理
        if len(scores) == 1:
            return PercentileResult(
                percentile=percentile,
                value=scores[0],
                rank=0,
                interpolated=False,
                method=method
            )
            
        # 排序数据
        scores_sorted = np.sort(scores)
        n = len(scores_sorted)
        
        # 计算精确位置
        position = (percentile / 100.0) * (n - 1) if method == InterpolationMethod.LINEAR else np.floor(n * percentile / 100.0)
        
        return self._calculate_with_method(scores_sorted, position, percentile, method)
    
    def calculate_multiple_percentiles(self, 
                                     data: Union[pd.Series, np.ndarray, List[float]], 
                                     percentiles: List[float],
                                     method: Optional[InterpolationMethod] = None) -> Dict[str, PercentileResult]:
        """批量计算多个百分位数
        
        Args:
            data: 数据序列
            percentiles: 百分位数列表
            method: 插值方法
            
        Returns:
            Dict[str, PercentileResult]: 以P{percentile}为键的结果字典
        """
        # 验证输入
        for p in percentiles:
            if not (0 <= p <= 100):
                raise ValueError(f"百分位数必须在0-100之间，当前值: {p}")
                
        # 预处理数据
        scores = self._preprocess_data(data)
        if len(scores) == 0:
            raise ValueError("没有有效的数据进行百分位数计算")
            
        method = method or self.interpolation_method
        results = {}
        
        # 边界条件：单值数据
        if len(scores) == 1:
            for p in percentiles:
                results[f'P{int(p)}'] = PercentileResult(
                    percentile=p,
                    value=scores[0],
                    rank=0,
                    interpolated=False,
                    method=method
                )
            return results
            
        # 排序一次，重复使用
        scores_sorted = np.sort(scores)
        n = len(scores_sorted)
        
        # 批量计算
        for p in percentiles:
            position = (p / 100.0) * (n - 1) if method == InterpolationMethod.LINEAR else np.floor(n * p / 100.0)
            result = self._calculate_with_method(scores_sorted, position, p, method)
            results[f'P{int(p)}'] = result
            
        return results
    
    def calculate_standard_percentiles(self, 
                                     data: Union[pd.Series, np.ndarray, List[float]],
                                     method: Optional[InterpolationMethod] = None) -> Dict[str, PercentileResult]:
        """计算标准百分位数 (P5, P10, P25, P50, P75, P90, P95)"""
        standard_percentiles = [5, 10, 25, 50, 75, 90, 95]
        return self.calculate_multiple_percentiles(data, standard_percentiles, method)
    
    def calculate_quartiles(self, 
                          data: Union[pd.Series, np.ndarray, List[float]],
                          method: Optional[InterpolationMethod] = None) -> Dict[str, Any]:
        """计算四分位数及相关指标"""
        quartile_results = self.calculate_multiple_percentiles(data, [25, 50, 75], method)
        
        q1 = quartile_results['P25'].value
        q2 = quartile_results['P50'].value  # 中位数
        q3 = quartile_results['P75'].value
        
        return {
            'Q1': q1,
            'Q2': q2,  # 中位数
            'Q3': q3,
            'IQR': q3 - q1,  # 四分位距
            'quartile_results': quartile_results
        }
    
    def detect_percentile_outliers(self, 
                                 data: Union[pd.Series, np.ndarray, List[float]],
                                 lower_percentile: float = 1,
                                 upper_percentile: float = 99) -> Dict[str, Any]:
        """基于百分位数的异常值检测"""
        scores = self._preprocess_data(data)
        if len(scores) < 3:
            return {
                'outlier_count': 0,
                'outlier_percentage': 0.0,
                'lower_bound': None,
                'upper_bound': None,
                'outlier_indices': []
            }
            
        percentile_results = self.calculate_multiple_percentiles(
            scores, [lower_percentile, upper_percentile]
        )
        
        lower_bound = percentile_results[f'P{int(lower_percentile)}'].value
        upper_bound = percentile_results[f'P{int(upper_percentile)}'].value
        
        # 检测异常值
        outlier_mask = (scores < lower_bound) | (scores > upper_bound)
        outlier_indices = np.where(outlier_mask)[0].tolist()
        
        return {
            'method': 'percentile',
            'lower_percentile': lower_percentile,
            'upper_percentile': upper_percentile,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'outlier_count': len(outlier_indices),
            'outlier_percentage': len(outlier_indices) / len(scores),
            'outlier_indices': outlier_indices
        }
    
    def _preprocess_data(self, data: Union[pd.Series, np.ndarray, List[float]]) -> np.ndarray:
        """数据预处理：转换格式、移除无效值"""
        if isinstance(data, pd.Series):
            scores = data.astype(float).dropna().values
        elif isinstance(data, list):
            scores = np.array([x for x in data if x is not None and not np.isnan(float(x))])
        elif isinstance(data, np.ndarray):
            scores = data[~np.isnan(data)]
        else:
            raise ValueError(f"不支持的数据类型: {type(data)}")
            
        # 移除无限值
        scores = scores[np.isfinite(scores)]
        
        return scores
    
    def _calculate_with_method(self, 
                             scores_sorted: np.ndarray, 
                             position: float, 
                             percentile: float,
                             method: InterpolationMethod) -> PercentileResult:
        """根据指定方法计算百分位数值"""
        n = len(scores_sorted)
        
        if method == InterpolationMethod.LINEAR:
            # 线性插值方法
            if position == int(position):
                # 精确索引
                rank = int(position)
                rank = max(0, min(rank, n - 1))
                return PercentileResult(
                    percentile=percentile,
                    value=float(scores_sorted[rank]),
                    rank=rank,
                    interpolated=False,
                    method=method
                )
            else:
                # 需要插值
                lower_rank = int(np.floor(position))
                upper_rank = int(np.ceil(position))
                lower_rank = max(0, min(lower_rank, n - 1))
                upper_rank = max(0, min(upper_rank, n - 1))
                
                if lower_rank == upper_rank:
                    value = float(scores_sorted[lower_rank])
                else:
                    # 线性插值
                    weight = position - lower_rank
                    value = float(scores_sorted[lower_rank] * (1 - weight) + 
                                scores_sorted[upper_rank] * weight)
                
                return PercentileResult(
                    percentile=percentile,
                    value=value,
                    rank=int(position),
                    interpolated=True,
                    method=method
                )
        
        elif method == InterpolationMethod.NEAREST:
            # 教育统计标准：floor算法，最近邻
            rank = int(np.floor(position))
            rank = max(0, min(rank, n - 1))
            return PercentileResult(
                percentile=percentile,
                value=float(scores_sorted[rank]),
                rank=rank,
                interpolated=False,
                method=method
            )
            
        elif method == InterpolationMethod.LOWER:
            # 取较小值
            rank = int(np.floor(position))
            rank = max(0, min(rank, n - 1))
            return PercentileResult(
                percentile=percentile,
                value=float(scores_sorted[rank]),
                rank=rank,
                interpolated=False,
                method=method
            )
            
        elif method == InterpolationMethod.HIGHER:
            # 取较大值
            rank = int(np.ceil(position))
            rank = max(0, min(rank, n - 1))
            return PercentileResult(
                percentile=percentile,
                value=float(scores_sorted[rank]),
                rank=rank,
                interpolated=False,
                method=method
            )
            
        elif method == InterpolationMethod.MIDPOINT:
            # 中点方法
            lower_rank = int(np.floor(position))
            upper_rank = int(np.ceil(position))
            lower_rank = max(0, min(lower_rank, n - 1))
            upper_rank = max(0, min(upper_rank, n - 1))
            
            if lower_rank == upper_rank:
                value = float(scores_sorted[lower_rank])
                interpolated = False
            else:
                value = float((scores_sorted[lower_rank] + scores_sorted[upper_rank]) / 2)
                interpolated = True
                
            return PercentileResult(
                percentile=percentile,
                value=value,
                rank=int(position),
                interpolated=interpolated,
                method=method
            )
        
        else:
            raise ValueError(f"不支持的插值方法: {method}")


class AdvancedPercentileStrategy(StatisticalStrategy):
    """高级百分位数计算策略
    
    与基础计算引擎集成，提供完整的百分位数计算服务
    """
    
    def __init__(self, interpolation_method: InterpolationMethod = InterpolationMethod.NEAREST):
        self.calculator = PercentileCalculator(interpolation_method)
        
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """执行百分位数计算"""
        if 'score' not in data.columns:
            raise ValueError("数据中缺少'score'列")
            
        scores = data['score'].astype(float).dropna()
        
        if len(scores) == 0:
            raise ValueError("没有有效的分数数据")
        
        # 获取配置参数
        percentiles = config.get('percentiles', [5, 10, 25, 50, 75, 90, 95])
        method_name = config.get('interpolation_method', 'nearest')
        include_quartiles = config.get('include_quartiles', True)
        detect_outliers = config.get('detect_outliers', False)
        
        # 转换方法名称
        try:
            method = InterpolationMethod(method_name)
        except ValueError:
            method = InterpolationMethod.NEAREST
            logger.warning(f"不支持的插值方法 '{method_name}'，使用默认方法 'nearest'")
        
        result = {}
        
        # 计算指定百分位数
        percentile_results = self.calculator.calculate_multiple_percentiles(
            scores, percentiles, method
        )
        
        # 转换为数值格式
        for key, percentile_result in percentile_results.items():
            result[key] = percentile_result.value
            result[f'{key}_rank'] = percentile_result.rank
            result[f'{key}_interpolated'] = percentile_result.interpolated
        
        # 计算四分位数指标
        if include_quartiles:
            quartile_data = self.calculator.calculate_quartiles(scores, method)
            result.update({
                'Q1': quartile_data['Q1'],
                'Q2': quartile_data['Q2'],  # 中位数
                'Q3': quartile_data['Q3'],
                'IQR': quartile_data['IQR']
            })
        
        # 异常值检测
        if detect_outliers:
            outlier_percentiles = config.get('outlier_percentiles', [1, 99])
            if len(outlier_percentiles) == 2:
                outlier_data = self.calculator.detect_percentile_outliers(
                    scores, outlier_percentiles[0], outlier_percentiles[1]
                )
                result['outlier_detection'] = outlier_data
        
        # 添加统计摘要
        result['percentile_count'] = len(percentiles)
        result['data_size'] = len(scores)
        result['interpolation_method'] = method.value
        
        return result
    
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        # 基础检查
        if data.empty:
            validation_result['is_valid'] = False
            validation_result['errors'].append("数据集为空")
            return validation_result
        
        if 'score' not in data.columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append("缺少必需字段: score")
            return validation_result
        
        # 数据有效性检查
        scores = pd.to_numeric(data['score'], errors='coerce')
        valid_count = scores.notna().sum()
        
        if valid_count == 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("没有有效的分数数据")
        elif valid_count < 3:
            validation_result['warnings'].append(f"数据量过少({valid_count})，百分位数计算可能不准确")
        
        # 百分位数配置检查
        percentiles = config.get('percentiles', [])
        if percentiles:
            invalid_percentiles = [p for p in percentiles if not (0 <= p <= 100)]
            if invalid_percentiles:
                validation_result['is_valid'] = False
                validation_result['errors'].append(f"无效的百分位数: {invalid_percentiles}")
        
        # 插值方法检查
        method_name = config.get('interpolation_method', 'nearest')
        try:
            InterpolationMethod(method_name)
        except ValueError:
            validation_result['warnings'].append(f"不支持的插值方法 '{method_name}'，将使用默认方法")
        
        validation_result['stats']['total_records'] = len(data)
        validation_result['stats']['valid_scores'] = int(valid_count)
        validation_result['stats']['data_completeness'] = valid_count / len(data)
        
        return validation_result
    
    def get_algorithm_info(self) -> Dict[str, str]:
        """获取算法信息"""
        return {
            'name': 'AdvancedPercentile',
            'version': '1.0',
            'description': '高级百分位数计算器',
            'default_algorithm': 'floor(n * p / 100)',
            'supported_methods': list(InterpolationMethod),
            'standard': 'Educational Statistics with Multiple Interpolation Methods',
            'features': 'quartiles, outlier_detection, batch_calculation'
        }


# 便捷函数接口
def calculate_percentile(data: Union[pd.Series, np.ndarray, List[float]], 
                       percentile: float,
                       method: str = 'nearest') -> float:
    """计算单个百分位数的便捷函数
    
    Args:
        data: 数据序列
        percentile: 百分位数 (0-100)
        method: 插值方法名称
        
    Returns:
        float: 百分位数值
    """
    calculator = PercentileCalculator(InterpolationMethod(method))
    result = calculator.calculate_percentile(data, percentile)
    return result.value


def calculate_standard_percentiles(data: Union[pd.Series, np.ndarray, List[float]], 
                                 method: str = 'nearest') -> Dict[str, float]:
    """计算标准百分位数的便捷函数"""
    calculator = PercentileCalculator(InterpolationMethod(method))
    results = calculator.calculate_standard_percentiles(data)
    return {key: result.value for key, result in results.items()}


def calculate_quartiles_simple(data: Union[pd.Series, np.ndarray, List[float]], 
                             method: str = 'nearest') -> Dict[str, float]:
    """计算四分位数的便捷函数"""
    calculator = PercentileCalculator(InterpolationMethod(method))
    quartile_data = calculator.calculate_quartiles(data)
    return {
        'Q1': quartile_data['Q1'],
        'Q2': quartile_data['Q2'],
        'Q3': quartile_data['Q3'],
        'IQR': quartile_data['IQR']
    }


# 与现有代码的兼容性函数
def calculate_percentile_educational_standard(scores: pd.Series, percentile: float) -> float:
    """使用教育统计标准计算百分位数（向后兼容）
    
    这个函数保持与原有formulas.py中calculate_percentile函数的兼容性
    """
    return calculate_percentile(scores, percentile, method='nearest')