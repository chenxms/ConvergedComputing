# 核心统计计算引擎
import pandas as pd
import numpy as np
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Callable, Union
from dataclasses import dataclass
import time
import gc
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

# Optional dependency for memory monitoring
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)


class StatisticalStrategy(ABC):
    """统计计算策略抽象基类"""
    
    @abstractmethod
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """执行统计计算"""
        pass
    
    @abstractmethod
    def validate_input(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据"""
        pass
    
    @abstractmethod
    def get_algorithm_info(self) -> Dict[str, str]:
        """获取算法信息"""
        pass


@dataclass
class CalculationMetrics:
    """计算指标"""
    operation_name: str
    data_size: int
    execution_time: float
    memory_usage: float
    success: bool
    error_message: Optional[str] = None


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self):
        self.metrics: List[CalculationMetrics] = []
    
    def record_calculation(self, operation: str, data_size: int, 
                         execution_time: float, memory_usage: float,
                         success: bool, error: Optional[str] = None):
        """记录计算指标"""
        metric = CalculationMetrics(
            operation_name=operation,
            data_size=data_size,
            execution_time=execution_time,
            memory_usage=memory_usage,
            success=success,
            error_message=error
        )
        self.metrics.append(metric)
        
        # 性能告警
        if execution_time > self._get_performance_threshold(data_size):
            logger.warning(f"计算性能告警: {operation} 耗时 {execution_time:.2f}s")
    
    def _get_performance_threshold(self, data_size: int) -> float:
        """获取性能阈值"""
        if data_size <= 10000:
            return 5.0  # 1万以下数据5秒内
        elif data_size <= 50000:
            return 15.0  # 5万以下数据15秒内
        else:
            return 30.0  # 更大数据集30秒内
    
    def get_stats(self) -> Dict[str, Any]:
        """获取性能统计"""
        if not self.metrics:
            return {}
        
        successful_metrics = [m for m in self.metrics if m.success]
        failed_metrics = [m for m in self.metrics if not m.success]
        
        return {
            'total_operations': len(self.metrics),
            'successful_operations': len(successful_metrics),
            'failed_operations': len(failed_metrics),
            'success_rate': len(successful_metrics) / len(self.metrics),
            'avg_execution_time': np.mean([m.execution_time for m in successful_metrics]) if successful_metrics else 0,
            'avg_memory_usage': np.mean([m.memory_usage for m in successful_metrics]) if successful_metrics else 0,
            'total_data_processed': sum(m.data_size for m in successful_metrics)
        }


class DataValidator:
    """数据验证器"""
    
    def validate_input_data(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证输入数据完整性"""
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
        
        # 必需字段检查
        required_columns = config.get('required_columns', ['score'])
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            validation_result['is_valid'] = False
            validation_result['errors'].append(f"缺少必需字段: {missing_columns}")
        
        # 数据类型检查
        if 'score' in data.columns:
            score_series = pd.to_numeric(data['score'], errors='coerce')
            null_count = score_series.isna().sum()
            
            if null_count > 0:
                validation_result['warnings'].append(f"发现{null_count}个无效分数值")
                
            # 分数范围检查
            max_score = config.get('max_score', 100)
            invalid_scores = score_series[(score_series < 0) | (score_series > max_score)]
            if len(invalid_scores) > 0:
                validation_result['warnings'].append(f"发现{len(invalid_scores)}个超出范围的分数")
        
        # 数据量检查
        data_count = len(data)
        validation_result['stats']['total_records'] = data_count
        validation_result['stats']['valid_scores'] = len(data) - null_count if 'score' in data.columns else data_count
        validation_result['stats']['data_completeness'] = (data_count - null_count) / data_count if 'score' in data.columns else 1.0
        
        return validation_result
    
    def validate_calculation_result(self, result: Dict[str, Any], algorithm_info: Dict[str, str]) -> bool:
        """验证计算结果合理性"""
        # 基础合理性检查
        if 'mean' in result:
            if not isinstance(result['mean'], (int, float)) or np.isnan(result['mean']):
                return False
                
        if 'std' in result:
            if result['std'] < 0:
                return False
        
        # 百分比检查
        percentage_keys = [k for k in result.keys() if k.endswith('_rate')]
        for key in percentage_keys:
            if not (0 <= result[key] <= 1):
                return False
        
        return True


class MemoryManager:
    """内存管理器"""
    
    def __init__(self, max_memory_usage: float = 0.8):
        self.max_memory_usage = max_memory_usage
        
    def get_memory_usage(self) -> float:
        """获取当前内存使用率"""
        if PSUTIL_AVAILABLE:
            return psutil.virtual_memory().percent / 100.0
        else:
            # 如果没有psutil，返回一个默认值
            return 0.5
    
    def optimize_dataframe_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化DataFrame内存使用"""
        # 数值类型下采样
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')
            
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')
        
        # 字符串类型优化
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() / len(df) < 0.5:  # 重复率高的转为category
                df[col] = df[col].astype('category')
        
        return df
    
    def should_trigger_gc(self) -> bool:
        """判断是否需要触发垃圾回收"""
        return self.get_memory_usage() > self.max_memory_usage


class ChunkProcessor:
    """分块处理器"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_size = chunk_size
        self.memory_threshold = 0.8
        self.memory_manager = MemoryManager()
    
    def process_large_dataset(self, data: pd.DataFrame, 
                            calculation_func: Callable,
                            merge_func: Callable = None,
                            **kwargs) -> Dict[str, Any]:
        """处理大数据集"""
        n_rows = len(data)
        
        if n_rows <= self.chunk_size:
            return calculation_func(data, **kwargs)
        
        # 分块处理
        results = []
        for i in range(0, n_rows, self.chunk_size):
            chunk = data.iloc[i:i + self.chunk_size]
            chunk_result = calculation_func(chunk, **kwargs)
            results.append(chunk_result)
            
            # 内存管理
            if self.memory_manager.should_trigger_gc():
                gc.collect()
        
        # 合并结果
        if merge_func:
            return merge_func(results, data)
        else:
            return self._merge_chunk_results(results, data)
    
    def _merge_chunk_results(self, results: List[Dict[str, Any]], original_data: pd.DataFrame) -> Dict[str, Any]:
        """合并分块计算结果"""
        if not results:
            return {}
        
        merged = {}
        
        # 对于可以简单求和的指标
        sum_keys = ['count', 'sum', 'excellent_count', 'good_count', 'pass_count', 'fail_count']
        for key in sum_keys:
            if key in results[0]:
                merged[key] = sum(r.get(key, 0) for r in results)
        
        # 对于需要重新计算的指标
        if 'sum' in merged and 'count' in merged and merged['count'] > 0:
            merged['mean'] = merged['sum'] / merged['count']
        
        # 对于需要从原始数据重新计算的指标（如中位数、百分位数）
        if 'score' in original_data.columns:
            # 确保score字段为数值类型，防止Categorical类型导致median计算失败
            scores = pd.to_numeric(original_data['score'], errors='coerce').dropna()
            merged['median'] = float(scores.median())
            merged['std'] = float(scores.std(ddof=1))
            merged['var'] = float(scores.var(ddof=1))
            merged['min'] = float(scores.min())
            merged['max'] = float(scores.max())
            
            # 计算百分位数
            for p in [10, 25, 50, 75, 90]:
                rank = int(np.floor(len(scores) * p / 100.0))
                rank = max(0, min(rank, len(scores) - 1))
                merged[f'P{p}'] = float(scores.sort_values().iloc[rank])
        
        return merged


class ParallelCalculationEngine:
    """并行计算引擎"""
    
    def __init__(self, max_workers: Optional[int] = None):
        self.max_workers = max_workers or mp.cpu_count()
    
    def parallel_calculation(self, data_chunks: List[pd.DataFrame],
                           calculation_func: Callable,
                           **kwargs) -> List[Dict[str, Any]]:
        """并行执行计算任务"""
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交任务
            futures = {
                executor.submit(calculation_func, chunk, **kwargs): i 
                for i, chunk in enumerate(data_chunks)
            }
            
            results = [None] * len(data_chunks)
            
            # 收集结果
            for future in as_completed(futures):
                chunk_index = futures[future]
                try:
                    results[chunk_index] = future.result()
                except Exception as exc:
                    logger.error(f'分块{chunk_index}计算失败: {exc}')
                    results[chunk_index] = None
        
        return [r for r in results if r is not None]
    
    def split_data_for_parallel(self, data: pd.DataFrame, 
                              min_chunk_size: int = 1000) -> List[pd.DataFrame]:
        """将数据分割为适合并行处理的块"""
        n_rows = len(data)
        
        # 计算最优分块数量
        optimal_chunks = min(self.max_workers, n_rows // min_chunk_size)
        if optimal_chunks == 0:
            optimal_chunks = 1
            
        chunk_size = n_rows // optimal_chunks
        
        chunks = []
        for i in range(0, n_rows, chunk_size):
            chunk = data.iloc[i:i + chunk_size]
            if not chunk.empty:
                chunks.append(chunk)
        
        return chunks


class CalculationEngine:
    """统计计算引擎核心"""
    
    def __init__(self):
        self.strategies: Dict[str, StatisticalStrategy] = {}
        self.validator = DataValidator()
        self.memory_manager = MemoryManager()
        self.chunk_processor = ChunkProcessor()
        self.parallel_engine = ParallelCalculationEngine()
        self.performance_monitor = PerformanceMonitor()
        
    def register_strategy(self, name: str, strategy: StatisticalStrategy):
        """注册计算策略"""
        self.strategies[name] = strategy
        logger.info(f"已注册计算策略: {name}")
        
    def calculate(self, strategy_name: str, data: pd.DataFrame, 
                 config: Dict[str, Any]) -> Dict[str, Any]:
        """执行计算"""
        start_time = time.time()
        memory_before = self.memory_manager.get_memory_usage()
        
        try:
            if strategy_name not in self.strategies:
                raise ValueError(f"未知的计算策略: {strategy_name}")
                
            strategy = self.strategies[strategy_name]
            
            # 数据验证
            validation_result = strategy.validate_input(data, config)
            if not validation_result['is_valid']:
                raise ValueError(f"数据验证失败: {validation_result['errors']}")
            
            # 内存优化
            data = self.memory_manager.optimize_dataframe_memory(data)
            
            # 选择处理方式
            # 对于需要全局数据计算的策略，禁用分块处理
            no_chunk_strategies = ['educational_metrics', 'discrimination', 'percentiles']
            
            if len(data) > self.chunk_processor.chunk_size and strategy_name not in no_chunk_strategies:
                # 大数据集分块处理
                result = self.chunk_processor.process_large_dataset(
                    data, 
                    strategy.calculate, 
                    config=config
                )
            else:
                # 直接计算
                result = strategy.calculate(data, config)
            
            # 结果验证
            algorithm_info = strategy.get_algorithm_info()
            if not self.validator.validate_calculation_result(result, algorithm_info):
                logger.warning(f"计算结果验证失败: {strategy_name}")
            
            # 添加元信息
            result['_meta'] = {
                'algorithm_info': algorithm_info,
                'data_size': len(data),
                'calculation_time': time.time() - start_time,
                'validation_warnings': validation_result.get('warnings', [])
            }
            
            # 记录性能指标
            execution_time = time.time() - start_time
            memory_after = self.memory_manager.get_memory_usage()
            self.performance_monitor.record_calculation(
                strategy_name, len(data), execution_time, 
                memory_after - memory_before, True
            )
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            memory_after = self.memory_manager.get_memory_usage()
            self.performance_monitor.record_calculation(
                strategy_name, len(data), execution_time,
                memory_after - memory_before, False, str(e)
            )
            raise
    
    def calculate_basic_statistics(self, data: pd.DataFrame, config: Dict[str, Any] = None) -> dict:
        """计算基础统计信息"""
        config = config or {}
        return self.calculate('basic_statistics', data, config)
    
    def calculate_advanced_statistics(self, data: pd.DataFrame, config: Dict[str, Any] = None) -> dict:
        """计算高级统计信息"""
        config = config or {}
        
        # 执行多个计算策略并合并结果
        results = {}
        
        # 基础统计
        basic_stats = self.calculate('basic_statistics', data, config)
        results.update(basic_stats)
        
        # 教育指标
        if 'educational_metrics' in self.strategies:
            educational_metrics = self.calculate('educational_metrics', data, config)
            results.update(educational_metrics)
        
        # 百分位数
        if 'percentiles' in self.strategies:
            percentiles = self.calculate('percentiles', data, config)
            results.update(percentiles)
        
        # 区分度
        if 'discrimination' in self.strategies:
            discrimination = self.calculate('discrimination', data, config)
            results.update(discrimination)
        
        return results
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计"""
        return self.performance_monitor.get_stats()
    
    def reset_performance_stats(self):
        """重置性能统计"""
        self.performance_monitor = PerformanceMonitor()
    
    def get_available_strategies(self) -> List[str]:
        """获取所有可用的计算策略列表
        
        Returns:
            List[str]: 已注册策略名称列表
        """
        return list(self.strategies.keys())
    
    def get_strategy_info(self, strategy_name: str) -> Dict[str, Any]:
        """获取特定策略的元数据信息
        
        Args:
            strategy_name (str): 策略名称
            
        Returns:
            Dict[str, Any]: 策略元数据信息，包含name、description、version、features等字段
            
        Raises:
            ValueError: 当策略不存在时
        """
        if strategy_name not in self.strategies:
            raise ValueError(f"策略 '{strategy_name}' 不存在")
        
        strategy = self.strategies[strategy_name]
        algorithm_info = strategy.get_algorithm_info()
        
        # 从strategy_registry获取更完整的信息
        try:
            from .calculators.strategy_registry import get_strategy_info
            return get_strategy_info(strategy_name)
        except (ImportError, ValueError):
            # 如果registry不可用，返回基础信息
            return {
                'name': strategy_name,
                'description': algorithm_info.get('description', '无描述'),
                'version': algorithm_info.get('version', '1.0.0'),
                'features': algorithm_info.get('features', []),
                'algorithm_info': algorithm_info
            }
    
    def get_registered_strategies(self) -> List[str]:
        """获取已注册的策略列表"""
        return list(self.strategies.keys())


# 全局计算引擎实例
_calculation_engine = None


def get_calculation_engine() -> CalculationEngine:
    """获取全局计算引擎实例"""
    global _calculation_engine
    if _calculation_engine is None:
        _calculation_engine = CalculationEngine()
        # 在这里可以注册默认策略
        logger.info("已初始化全局计算引擎")
    return _calculation_engine