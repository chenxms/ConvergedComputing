# 数据精度处理工具
import pandas as pd
import numpy as np
from typing import Dict, Any, Union, List, Optional
from decimal import Decimal, ROUND_HALF_UP
import logging

logger = logging.getLogger(__name__)


def format_decimal(value: Union[float, int, str, None], decimal_places: int = 2) -> Optional[float]:
    """
    格式化数值到指定小数位数
    
    Args:
        value: 需要格式化的数值
        decimal_places: 小数位数，默认2位
        
    Returns:
        格式化后的浮点数，异常情况返回None
    """
    try:
        if value is None or pd.isna(value):
            return None
        
        if isinstance(value, str):
            # 处理字符串类型
            if value.strip() == '' or value.lower() in ['null', 'none', 'nan']:
                return None
            value = float(value)
        
        if not isinstance(value, (int, float, Decimal)):
            return None
        
        # 处理无穷大和NaN
        if np.isinf(value) or np.isnan(value):
            return None
        
        # 使用Decimal进行精确计算，避免浮点数精度问题
        decimal_value = Decimal(str(value))
        rounded_decimal = decimal_value.quantize(
            Decimal('0.' + '0' * decimal_places),
            rounding=ROUND_HALF_UP
        )
        
        return float(rounded_decimal)
    
    except (ValueError, TypeError, Exception) as e:
        logger.warning(f"数值格式化失败: {value}, 错误: {str(e)}")
        return None


def format_percentage(value: Union[float, int, None], decimal_places: int = 2) -> Optional[float]:
    """
    格式化百分比数值（0-1之间的小数）
    
    Args:
        value: 需要格式化的百分比值
        decimal_places: 小数位数，默认2位
        
    Returns:
        格式化后的百分比值
    """
    formatted = format_decimal(value, decimal_places)
    if formatted is None:
        return None
    
    # 确保百分比值在合理范围内
    if formatted < 0:
        logger.warning(f"百分比值小于0: {formatted}")
        return 0.0
    elif formatted > 1:
        logger.warning(f"百分比值大于1: {formatted}")
        return 1.0
    
    return formatted


def batch_format_dict(data: Dict[str, Any], decimal_places: int = 2, 
                      exclude_keys: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    批量格式化字典中的数值
    
    Args:
        data: 需要格式化的字典
        decimal_places: 小数位数，默认2位
        exclude_keys: 排除的键列表，这些键的值不会被格式化
        
    Returns:
        格式化后的字典
    """
    if not isinstance(data, dict):
        return data
    
    exclude_keys = exclude_keys or []
    result = {}
    
    # 特殊处理的百分比字段
    percentage_keys = [
        'rate', 'percentage', 'ratio', '_rate', 'excellent_rate', 'good_rate',
        'pass_rate', 'fail_rate', 'a_rate', 'b_rate', 'c_rate', 'd_rate',
        'average_score_rate', 'difficulty_coefficient'
    ]
    
    for key, value in data.items():
        if key in exclude_keys:
            result[key] = value
            continue
        
        if isinstance(value, dict):
            # 递归处理嵌套字典
            result[key] = batch_format_dict(value, decimal_places, exclude_keys)
        elif isinstance(value, list):
            # 处理列表
            result[key] = batch_format_list(value, decimal_places, exclude_keys)
        elif any(pk in key.lower() for pk in percentage_keys):
            # 百分比字段处理
            result[key] = format_percentage(value, decimal_places)
        else:
            # 普通数值字段处理
            formatted_value = format_decimal(value, decimal_places)
            result[key] = formatted_value if formatted_value is not None else value
    
    return result


def batch_format_list(data: List[Any], decimal_places: int = 2, 
                      exclude_keys: Optional[List[str]] = None) -> List[Any]:
    """
    批量格式化列表中的数值
    
    Args:
        data: 需要格式化的列表
        decimal_places: 小数位数，默认2位
        exclude_keys: 排除的键列表（用于列表中的字典）
        
    Returns:
        格式化后的列表
    """
    if not isinstance(data, list):
        return data
    
    result = []
    for item in data:
        if isinstance(item, dict):
            result.append(batch_format_dict(item, decimal_places, exclude_keys))
        elif isinstance(item, list):
            result.append(batch_format_list(item, decimal_places, exclude_keys))
        else:
            formatted_item = format_decimal(item, decimal_places)
            result.append(formatted_item if formatted_item is not None else item)
    
    return result


def batch_format_dataframe(df: pd.DataFrame, decimal_places: int = 2,
                          numeric_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    批量格式化DataFrame中的数值列
    
    Args:
        df: 需要格式化的DataFrame
        decimal_places: 小数位数，默认2位
        numeric_columns: 指定要格式化的数值列，None表示自动检测
        
    Returns:
        格式化后的DataFrame
    """
    if df.empty:
        return df
    
    df_copy = df.copy()
    
    # 自动检测数值列
    if numeric_columns is None:
        numeric_columns = df_copy.select_dtypes(include=[np.number]).columns.tolist()
    
    for col in numeric_columns:
        if col in df_copy.columns:
            # 使用vectorized函数提高性能
            df_copy[col] = df_copy[col].apply(
                lambda x: format_decimal(x, decimal_places)
            )
    
    return df_copy


def create_statistics_summary(data: Dict[str, Any], decimal_places: int = 2) -> Dict[str, Any]:
    """
    创建格式化的统计摘要
    
    Args:
        data: 原始统计数据
        decimal_places: 小数位数，默认2位
        
    Returns:
        格式化的统计摘要
    """
    try:
        summary = {
            'total_students': data.get('count', 0),
            'avg_score': format_decimal(data.get('mean'), decimal_places),
            'median_score': format_decimal(data.get('median'), decimal_places),
            'std_deviation': format_decimal(data.get('std'), decimal_places),
            'min_score': format_decimal(data.get('min'), decimal_places),
            'max_score': format_decimal(data.get('max'), decimal_places),
            'difficulty_coefficient': format_percentage(
                data.get('difficulty_coefficient'), decimal_places
            ),
            'discrimination_index': format_decimal(
                data.get('discrimination_index'), decimal_places
            )
        }
        
        # 添加百分位数
        percentiles = ['P10', 'P50', 'P90']
        for p in percentiles:
            if p in data:
                summary[p.lower()] = format_decimal(data[p], decimal_places)
        
        # 添加等级分布
        if 'grade_distribution' in data:
            summary['grade_distribution'] = batch_format_dict(
                data['grade_distribution'], decimal_places
            )
        
        return summary
    
    except Exception as e:
        logger.error(f"创建统计摘要失败: {str(e)}")
        return {}


def validate_numeric_ranges(data: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    验证数值范围的合理性
    
    Args:
        data: 需要验证的数据字典
        
    Returns:
        验证结果，包含warnings和errors列表
    """
    warnings = []
    errors = []
    
    # 检查百分比值
    percentage_fields = [
        'excellent_rate', 'good_rate', 'pass_rate', 'fail_rate',
        'a_rate', 'b_rate', 'c_rate', 'd_rate',
        'average_score_rate', 'difficulty_coefficient'
    ]
    
    for field in percentage_fields:
        if field in data and data[field] is not None:
            value = data[field]
            if not isinstance(value, (int, float)):
                continue
            if value < 0 or value > 1:
                warnings.append(f"{field}值超出合理范围[0,1]: {value}")
    
    # 检查分数范围
    score_fields = ['avg_score', 'median_score', 'min_score', 'max_score', 'p10', 'p50', 'p90']
    for field in score_fields:
        if field in data and data[field] is not None:
            value = data[field]
            if not isinstance(value, (int, float)):
                continue
            if value < 0:
                warnings.append(f"{field}为负值: {value}")
    
    # 检查标准差
    if 'std_deviation' in data and data['std_deviation'] is not None:
        if data['std_deviation'] < 0:
            errors.append(f"标准差不能为负: {data['std_deviation']}")
    
    # 检查区分度
    if 'discrimination_index' in data and data['discrimination_index'] is not None:
        value = data['discrimination_index']
        if not isinstance(value, (int, float)):
            pass
        elif value < -1 or value > 1:
            warnings.append(f"区分度超出合理范围[-1,1]: {value}")
    
    return {'warnings': warnings, 'errors': errors}


def safe_divide(numerator: Union[float, int], denominator: Union[float, int], 
                default_value: float = 0.0, decimal_places: int = 2) -> Optional[float]:
    """
    安全的除法运算，处理除零等异常情况
    
    Args:
        numerator: 分子
        denominator: 分母
        default_value: 除零时的默认值
        decimal_places: 小数位数
        
    Returns:
        计算结果或默认值
    """
    try:
        if denominator == 0 or pd.isna(denominator):
            return default_value
        
        if pd.isna(numerator):
            return None
        
        result = numerator / denominator
        return format_decimal(result, decimal_places)
    
    except Exception as e:
        logger.warning(f"安全除法计算失败: {numerator}/{denominator}, 错误: {str(e)}")
        return default_value


# 常用精度处理常量
DEFAULT_DECIMAL_PLACES = 2
PERCENTAGE_DECIMAL_PLACES = 2
SCIENTIFIC_DECIMAL_PLACES = 4

# 精度处理配置
PRECISION_CONFIG = {
    'statistics': {
        'score_fields': ['avg_score', 'median_score', 'min_score', 'max_score', 'std_deviation'],
        'percentage_fields': ['excellent_rate', 'pass_rate', 'difficulty_coefficient'],
        'percentile_fields': ['p10', 'p25', 'p50', 'p75', 'p90'],
        'decimal_places': DEFAULT_DECIMAL_PLACES
    },
    'performance': {
        'calculation_duration': 3,  # 计算耗时保留3位小数
        'memory_usage': 2,
        'success_rate': 4
    }
}