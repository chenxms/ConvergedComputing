# 基础统计计算引擎 - 技术分析文档

## 概述

基础统计计算引擎是教育统计分析系统的核心组件，负责实现教育行业特有的统计算法和大数据量计算优化。该引擎需要支持10万学生数据在30分钟内完成计算，同时保证教育统计算法的精确性。

## 1. 架构设计

### 1.1 整体架构

采用分层策略模式架构，支持可插拔的算法实现：

```
├── CalculationEngine (核心引擎)
│   ├── StrategyFactory (策略工厂)
│   ├── DataValidator (数据验证器)
│   ├── PreprocessingPipeline (数据预处理管道)
│   └── ResultSerializer (结果序列化器)
├── Strategies (计算策略层)
│   ├── BasicStatisticsStrategy (基础统计策略)
│   ├── EducationalMetricsStrategy (教育指标策略)
│   └── PercentileStrategy (百分位数策略)
├── Optimizers (性能优化层)
│   ├── ChunkProcessor (分块处理器)
│   ├── VectorizedCalculator (向量化计算器)
│   └── MemoryManager (内存管理器)
└── Validators (验证层)
    ├── AlgorithmValidator (算法验证器)
    └── AnomalyDetector (异常检测器)
```

### 1.2 策略模式实现

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

class StatisticalStrategy(ABC):
    """统计计算策略抽象基类"""
    
    @abstractmethod
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """执行统计计算"""
        pass
    
    @abstractmethod
    def validate_input(self, data: pd.DataFrame) -> bool:
        """验证输入数据"""
        pass
    
    @abstractmethod
    def get_algorithm_info(self) -> Dict[str, str]:
        """获取算法信息"""
        pass

class CalculationEngine:
    """统计计算引擎核心"""
    
    def __init__(self):
        self.strategies: Dict[str, StatisticalStrategy] = {}
        self.validators: List[Callable] = []
        self.preprocessors: List[Callable] = []
        
    def register_strategy(self, name: str, strategy: StatisticalStrategy):
        """注册计算策略"""
        self.strategies[name] = strategy
        
    def calculate(self, strategy_name: str, data: pd.DataFrame, 
                 config: Dict[str, Any]) -> Dict[str, Any]:
        """执行计算"""
        if strategy_name not in self.strategies:
            raise ValueError(f"未知的计算策略: {strategy_name}")
            
        strategy = self.strategies[strategy_name]
        
        # 数据预处理
        processed_data = self._preprocess(data)
        
        # 执行计算
        result = strategy.calculate(processed_data, config)
        
        # 结果验证
        self._validate_result(result, strategy.get_algorithm_info())
        
        return result
```

## 2. 教育统计算法实现

### 2.1 基础统计指标

```python
class BasicStatisticsStrategy(StatisticalStrategy):
    """基础统计指标计算策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """计算基础统计指标"""
        scores = data['score'].astype(float)
        
        # 使用向量化计算提高性能
        result = {
            'count': int(scores.count()),
            'mean': float(scores.mean()),
            'median': float(scores.median()),
            'mode': float(scores.mode().iloc[0] if not scores.mode().empty else np.nan),
            'std': float(scores.std(ddof=1)),  # 样本标准差
            'variance': float(scores.var(ddof=1)),  # 样本方差
            'min': float(scores.min()),
            'max': float(scores.max()),
            'range': float(scores.max() - scores.min()),
            'skewness': float(scores.skew()),
            'kurtosis': float(scores.kurtosis())
        }
        
        return result
    
    def validate_input(self, data: pd.DataFrame) -> bool:
        """验证输入数据"""
        required_columns = ['score']
        if not all(col in data.columns for col in required_columns):
            return False
        
        # 检查数据类型和范围
        if data['score'].dtype not in ['int64', 'float64']:
            return False
            
        return True
    
    def get_algorithm_info(self) -> Dict[str, str]:
        return {
            'name': 'BasicStatistics',
            'version': '1.0',
            'description': '基础统计指标计算',
            'std_formula': 'sample_std_ddof_1'
        }
```

### 2.2 教育专用百分位数算法

```python
class EducationalPercentileStrategy(StatisticalStrategy):
    """教育行业百分位数计算策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """使用教育统计标准的百分位数算法"""
        scores = data['score'].astype(float).sort_values()
        n = len(scores)
        
        percentiles = config.get('percentiles', [10, 25, 50, 75, 90])
        result = {}
        
        for p in percentiles:
            # 教育统计标准：使用floor算法
            rank = np.floor(n * p / 100.0).astype(int)
            # 确保索引不超出范围
            rank = max(0, min(rank, n - 1))
            result[f'P{p}'] = float(scores.iloc[rank])
        
        # 四分位距
        if 'P75' in result and 'P25' in result:
            result['IQR'] = result['P75'] - result['P25']
        
        return result
    
    def validate_input(self, data: pd.DataFrame) -> bool:
        """验证输入数据"""
        if data.empty or 'score' not in data.columns:
            return False
        return not data['score'].isna().all()
    
    def get_algorithm_info(self) -> Dict[str, str]:
        return {
            'name': 'EducationalPercentile',
            'version': '1.0',
            'description': '教育统计百分位数计算',
            'algorithm': 'floor(n * p / 100)',
            'standard': 'Chinese Educational Statistics'
        }
```

### 2.3 得分率和等级分布

```python
class EducationalMetricsStrategy(StatisticalStrategy):
    """教育指标计算策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """计算得分率和等级分布"""
        scores = data['score'].astype(float)
        max_score = config.get('max_score', 100)
        grade_level = config.get('grade_level', '1st_grade')
        
        result = {}
        
        # 得分率计算
        result['average_score_rate'] = float(scores.mean() / max_score)
        
        # 获取年级阈值
        thresholds = self._get_grade_thresholds(grade_level)
        
        # 计算各等级分布
        total_count = len(scores)
        
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
        
        return result
    
    def _is_primary_grade(self, grade_level: str) -> bool:
        """判断是否为小学年级"""
        primary_grades = ['1st_grade', '2nd_grade', '3rd_grade', 
                         '4th_grade', '5th_grade', '6th_grade']
        return grade_level in primary_grades
    
    def _get_grade_thresholds(self, grade_level: str) -> Dict[str, float]:
        """获取年级阈值配置"""
        if self._is_primary_grade(grade_level):
            return {'excellent': 0.90, 'good': 0.80, 'pass': 0.60}
        else:
            return {'a': 0.85, 'b': 0.70, 'c': 0.60}
```

### 2.4 区分度计算

```python
class DiscriminationStrategy(StatisticalStrategy):
    """区分度计算策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """计算区分度（前27%和后27%分组）"""
        scores = data['score'].astype(float).sort_values(ascending=False)
        n = len(scores)
        
        # 教育统计标准：前27%和后27%
        high_group_size = int(n * 0.27)
        low_group_size = int(n * 0.27)
        
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
```

## 3. 大数据量处理优化

### 3.1 分块处理策略

```python
class ChunkProcessor:
    """分块处理器"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_size = chunk_size
        self.memory_threshold = 0.8  # 内存使用阈值
    
    def process_large_dataset(self, data: pd.DataFrame, 
                            calculation_func: Callable,
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
            if self._check_memory_usage():
                import gc
                gc.collect()
        
        # 合并结果
        return self._merge_chunk_results(results)
    
    def _merge_chunk_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
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
        if 'sum' in merged and 'count' in merged:
            merged['mean'] = merged['sum'] / merged['count']
        
        # 对于需要从原始数据重新计算的指标（如中位数、百分位数）
        # 这些需要特殊处理，可能需要保存中间结果
        
        return merged
```

### 3.2 向量化计算优化

```python
class VectorizedCalculator:
    """向量化计算器"""
    
    @staticmethod
    def calculate_basic_stats_vectorized(scores: pd.Series) -> Dict[str, float]:
        """向量化基础统计计算"""
        # 使用NumPy向量化操作，避免Python循环
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
        total_count = len(scores_array)
        
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
```

### 3.3 内存管理

```python
class MemoryManager:
    """内存管理器"""
    
    def __init__(self, max_memory_usage: float = 0.8):
        self.max_memory_usage = max_memory_usage
        
    def get_memory_usage(self) -> float:
        """获取当前内存使用率"""
        import psutil
        return psutil.virtual_memory().percent / 100.0
    
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
```

## 4. 数据验证和异常处理

### 4.1 数据完整性验证

```python
class DataValidator:
    """数据验证器"""
    
    def validate_input_data(self, data: pd.DataFrame, 
                          config: Dict[str, Any]) -> Dict[str, Any]:
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
        validation_result['stats']['valid_scores'] = len(data) - null_count
        validation_result['stats']['data_completeness'] = (data_count - null_count) / data_count
        
        return validation_result
    
    def validate_calculation_result(self, result: Dict[str, Any], 
                                  algorithm_info: Dict[str, str]) -> bool:
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
```

### 4.2 异常数据检测

```python
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
```

## 5. 并行化处理方案

### 5.1 多进程并行计算

```python
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Callable

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
```

### 5.2 异步计算支持

```python
import asyncio
from typing import AsyncGenerator

class AsyncCalculationEngine:
    """异步计算引擎"""
    
    async def async_batch_calculation(self, 
                                    batches: List[str],
                                    calculation_func: Callable) -> AsyncGenerator[Dict[str, Any], None]:
        """异步批量计算"""
        semaphore = asyncio.Semaphore(4)  # 限制并发数
        
        async def process_batch(batch_code: str) -> Dict[str, Any]:
            async with semaphore:
                # 模拟异步数据库查询和计算
                data = await self.fetch_batch_data(batch_code)
                result = await asyncio.to_thread(calculation_func, data)
                return {'batch_code': batch_code, 'result': result}
        
        tasks = [process_batch(batch) for batch in batches]
        
        for task in asyncio.as_completed(tasks):
            yield await task
    
    async def fetch_batch_data(self, batch_code: str) -> pd.DataFrame:
        """异步获取批次数据"""
        # 这里应该是异步数据库查询
        await asyncio.sleep(0.1)  # 模拟I/O延迟
        return pd.DataFrame()  # 返回实际数据
```

## 6. 性能基准和优化目标

### 6.1 性能指标

- **计算速度**: 10万学生数据 < 30分钟
- **内存使用**: 峰值内存 < 4GB
- **CPU利用率**: 多核并行利用率 > 70%
- **准确性**: 与Excel手工计算结果误差 < 0.001%

### 6.2 优化策略

1. **算法优化**
   - 使用NumPy向量化操作替代Python循环
   - 预排序数据以优化百分位数计算
   - 使用适当的数据类型减少内存占用

2. **数据处理优化**
   - 分块处理避免内存溢出
   - 流式处理大数据集
   - 智能缓存中间结果

3. **并行化优化**
   - 学校级并行计算
   - 科目维度并行处理
   - 异步I/O减少等待时间

## 7. 集成方案

### 7.1 与数据访问层集成

```python
from app.database.repositories import StatisticalRepository
from app.database.models import StatisticalAggregation, AggregationLevel

class IntegratedCalculationService:
    """集成计算服务"""
    
    def __init__(self, db_session: Session):
        self.repository = StatisticalRepository(db_session)
        self.engine = CalculationEngine()
        self._register_strategies()
    
    async def calculate_batch_statistics(self, batch_code: str) -> Dict[str, Any]:
        """计算批次统计数据"""
        # 1. 获取数据
        data = await self.repository.get_batch_student_scores(batch_code)
        
        # 2. 数据验证
        validation_result = self.engine.validate_input_data(data)
        if not validation_result['is_valid']:
            raise ValueError(f"数据验证失败: {validation_result['errors']}")
        
        # 3. 执行计算
        results = {}
        
        # 基础统计
        basic_stats = self.engine.calculate('basic_statistics', data, {})
        results.update(basic_stats)
        
        # 教育指标
        educational_metrics = self.engine.calculate('educational_metrics', data, {
            'max_score': await self.repository.get_subject_max_score(batch_code),
            'grade_level': await self.repository.get_grade_level(batch_code)
        })
        results.update(educational_metrics)
        
        # 百分位数
        percentiles = self.engine.calculate('percentiles', data, {})
        results.update(percentiles)
        
        # 4. 保存结果
        await self.repository.save_statistical_result(
            batch_code=batch_code,
            aggregation_level=AggregationLevel.REGIONAL,
            statistics_data=results
        )
        
        return results
```

## 8. 测试策略

### 8.1 单元测试

```python
import pytest
import numpy as np

class TestEducationalPercentileStrategy:
    """教育百分位数策略测试"""
    
    def setup_method(self):
        self.strategy = EducationalPercentileStrategy()
        
    def test_percentile_calculation_accuracy(self):
        """测试百分位数计算精度"""
        # 构造已知结果的测试数据
        scores = pd.DataFrame({'score': [60, 70, 80, 85, 90, 95]})
        
        result = self.strategy.calculate(scores, {'percentiles': [50]})
        
        # 验证结果与Excel计算一致
        expected_p50 = 82.5  # Excel计算结果
        assert abs(result['P50'] - expected_p50) < 0.01
        
    def test_edge_cases(self):
        """测试边界情况"""
        # 单个数据点
        single_score = pd.DataFrame({'score': [85]})
        result = self.strategy.calculate(single_score, {'percentiles': [50]})
        assert result['P50'] == 85
        
        # 相同分数
        same_scores = pd.DataFrame({'score': [80] * 100})
        result = self.strategy.calculate(same_scores, {'percentiles': [25, 50, 75]})
        assert result['P25'] == result['P50'] == result['P75'] == 80
```

### 8.2 性能测试

```python
import time
import pytest

class TestCalculationPerformance:
    """计算性能测试"""
    
    def test_large_dataset_performance(self):
        """测试大数据集性能"""
        # 生成10万条测试数据
        np.random.seed(42)
        scores = np.random.normal(75, 15, 100000)
        scores = np.clip(scores, 0, 100)  # 限制在合理范围内
        
        data = pd.DataFrame({'score': scores})
        
        engine = CalculationEngine()
        
        start_time = time.time()
        result = engine.calculate('basic_statistics', data, {})
        calculation_time = time.time() - start_time
        
        # 性能要求：10万数据基础统计 < 5秒
        assert calculation_time < 5.0
        
        # 验证结果合理性
        assert 70 < result['mean'] < 80
        assert 10 < result['std'] < 20
```

## 9. 部署和监控

### 9.1 配置管理

```python
from pydantic import BaseSettings

class CalculationSettings(BaseSettings):
    """计算引擎配置"""
    
    # 性能配置
    max_chunk_size: int = 10000
    max_parallel_workers: int = 4
    memory_threshold: float = 0.8
    
    # 算法配置
    percentile_algorithm: str = "floor"  # floor, ceiling, linear
    outlier_detection_method: str = "iqr"  # iqr, zscore
    
    # 缓存配置
    enable_result_cache: bool = True
    cache_ttl: int = 3600
    
    class Config:
        env_prefix = "CALCULATION_"
```

### 9.2 性能监控

```python
import time
from dataclasses import dataclass
from typing import Dict, List

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
        # 基于数据量的性能期望
        if data_size <= 10000:
            return 5.0  # 1万以下数据5秒内
        elif data_size <= 50000:
            return 15.0  # 5万以下数据15秒内
        else:
            return 30.0  # 更大数据集30秒内
```

## 10. 实现优先级

### Phase 1: 核心算法实现 (Week 1-2)
1. 基础统计策略实现
2. 教育百分位数算法
3. 等级分布计算
4. 单元测试编写

### Phase 2: 性能优化 (Week 3)
1. 分块处理实现
2. 向量化计算优化
3. 内存管理机制
4. 性能基准测试

### Phase 3: 集成和验证 (Week 4)
1. 数据访问层集成
2. 异常处理完善
3. 端到端测试
4. 性能调优

这个分析文档为基础统计计算引擎的实现提供了全面的技术方案，特别关注了教育统计的特殊要求和大数据量处理的性能优化需求。