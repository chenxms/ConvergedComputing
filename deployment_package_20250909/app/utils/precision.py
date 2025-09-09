#!/usr/bin/env python3
"""
精度处理工具模块 v1.2
统一数据精度处理，支持汇聚模块修复方案v1.2要求
"""
from typing import Dict, Any, Union, List, Optional
from decimal import Decimal, ROUND_HALF_UP
import logging
import json

logger = logging.getLogger(__name__)


def round2(value: Union[float, int, None]) -> Optional[float]:
    """
    两位小数精度处理
    
    Args:
        value: 需要处理的数值
        
    Returns:
        处理后的两位小数浮点数，None保持None
    """
    if value is None:
        return None
    
    try:
        if isinstance(value, str):
            if value.strip() == '' or value.lower() in ['null', 'none', 'nan']:
                return None
            value = float(value)
        
        if not isinstance(value, (int, float, Decimal)):
            return None
            
        # 使用Decimal进行精确计算
        decimal_value = Decimal(str(value))
        rounded_decimal = decimal_value.quantize(
            Decimal('0.01'),  # 两位小数
            rounding=ROUND_HALF_UP
        )
        
        return float(rounded_decimal)
    except (ValueError, TypeError, Exception) as e:
        logger.warning(f"数值精度处理失败: {value}, 错误: {str(e)}")
        return None


def to_pct(value: Union[float, int, None]) -> Optional[float]:
    """
    百分比字段处理：将0-1之间的小数转换为0-100的数值并保留两位小数
    
    Args:
        value: 0-1之间的百分比值
        
    Returns:
        0-100之间的两位小数数值
    """
    if value is None:
        return None
        
    try:
        if isinstance(value, str):
            if value.strip() == '' or value.lower() in ['null', 'none', 'nan']:
                return None
            value = float(value)
        
        if not isinstance(value, (int, float, Decimal)):
            return None
            
        # 转换为百分比
        pct_value = value * 100
        
        # 确保在合理范围内
        if pct_value < 0:
            logger.warning(f"百分比值小于0: {pct_value}")
            pct_value = 0.0
        elif pct_value > 100:
            logger.warning(f"百分比值大于100: {pct_value}")
            pct_value = 100.0
        
        return round2(pct_value)
    except Exception as e:
        logger.warning(f"百分比转换失败: {value}, 错误: {str(e)}")
        return None


def round2_json(data: Union[Dict, List, Any]) -> Union[Dict, List, Any]:
    """
    递归处理JSON数据中的数值精度
    
    Args:
        data: 需要处理的数据（字典、列表或其他类型）
        
    Returns:
        精度处理后的数据
    """
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            # 检查是否为百分比字段
            if key.endswith('_pct') or key.endswith('_rate') or key.endswith('_percentage'):
                # 百分比字段特殊处理
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    # 如果值在0-1之间，转换为百分比；否则直接保留两位小数
                    if 0 <= value <= 1:
                        result[key] = to_pct(value)
                    else:
                        result[key] = round2(value)
                else:
                    result[key] = round2_json(value)
            else:
                # 普通字段递归处理
                result[key] = round2_json(value)
        return result
    elif isinstance(data, list):
        return [round2_json(item) for item in data]
    elif isinstance(data, (int, float)) and not isinstance(data, bool):
        return round2(data)
    else:
        return data


def format_statistics_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    格式化统计数据，应用精度规则
    
    Args:
        data: 原始统计数据
        
    Returns:
        格式化后的数据
    """
    if not isinstance(data, dict):
        return data
    
    # 定义百分比字段模式
    percentage_patterns = [
        '_rate', '_pct', '_percentage', 'difficulty', 'discrimination',
        'excellent_rate', 'good_rate', 'pass_rate', 'fail_rate',
        'a_rate', 'b_rate', 'c_rate', 'd_rate'
    ]
    
    formatted_data = {}
    
    for key, value in data.items():
        if isinstance(value, dict):
            # 递归处理嵌套字典
            formatted_data[key] = format_statistics_data(value)
        elif isinstance(value, list):
            # 处理列表
            formatted_data[key] = [format_statistics_data(item) if isinstance(item, dict) else round2_json(item) for item in value]
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            # 检查是否为百分比字段
            is_percentage = any(pattern in key.lower() for pattern in percentage_patterns)
            
            if is_percentage:
                # 百分比字段：如果是0-1之间转为百分比，否则保持两位小数
                if 0 <= value <= 1:
                    formatted_data[key] = to_pct(value)
                else:
                    formatted_data[key] = round2(value)
            else:
                # 普通数值字段：两位小数
                formatted_data[key] = round2(value)
        else:
            # 其他类型保持不变
            formatted_data[key] = value
    
    return formatted_data


def apply_precision_to_aggregation_result(result: Dict[str, Any], schema_version: str = "v1.2") -> Dict[str, Any]:
    """
    对汇聚结果应用精度处理
    
    Args:
        result: 汇聚结果数据
        schema_version: 数据格式版本
        
    Returns:
        精度处理后的结果
    """
    if not isinstance(result, dict):
        return result
    
    # 添加schema版本标识
    processed_result = result.copy()
    processed_result['schema_version'] = schema_version
    
    # 对整个结果应用精度处理
    processed_result = format_statistics_data(processed_result)
    
    return processed_result


def validate_precision_requirements(data: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    验证精度要求是否满足
    
    Args:
        data: 需要验证的数据
        
    Returns:
        验证结果，包含warnings和errors
    """
    warnings = []
    errors = []
    
    def check_precision(value, key_path):
        if isinstance(value, float):
            # 检查小数位数
            str_value = str(value)
            if '.' in str_value:
                decimal_places = len(str_value.split('.')[1])
                if decimal_places > 2:
                    warnings.append(f"{key_path}: 小数位数超过2位: {value}")
        
        # 检查百分比字段范围
        if any(pattern in key_path.lower() for pattern in ['_pct', '_rate', '_percentage']):
            if isinstance(value, (int, float)):
                if value < 0 or value > 100:
                    warnings.append(f"{key_path}: 百分比值超出[0,100]范围: {value}")
    
    def traverse_data(data, path=""):
        if isinstance(data, dict):
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key
                if isinstance(value, (dict, list)):
                    traverse_data(value, current_path)
                else:
                    check_precision(value, current_path)
        elif isinstance(data, list):
            for i, item in enumerate(data):
                current_path = f"{path}[{i}]"
                if isinstance(item, (dict, list)):
                    traverse_data(item, current_path)
                else:
                    check_precision(item, current_path)
    
    traverse_data(data)
    
    return {'warnings': warnings, 'errors': errors}
