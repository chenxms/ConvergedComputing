# 策略注册表
import logging
from typing import Dict, Type, List, Optional
from ..engine import StatisticalStrategy, CalculationEngine, get_calculation_engine
from ..formulas import (
    BasicStatisticsStrategy,
    EducationalPercentileStrategy, 
    EducationalMetricsStrategy,
    DiscriminationStrategy
)

logger = logging.getLogger(__name__)


class CalculationStrategyRegistry:
    """计算策略注册表"""
    
    def __init__(self):
        self._strategies: Dict[str, Type[StatisticalStrategy]] = {}
        self._descriptions: Dict[str, str] = {}
    
    def register(self, name: str, strategy_class: Type[StatisticalStrategy], description: str = ""):
        """注册计算策略"""
        if not issubclass(strategy_class, StatisticalStrategy):
            raise ValueError(f"策略类 {strategy_class.__name__} 必须继承 StatisticalStrategy")
        
        self._strategies[name] = strategy_class
        self._descriptions[name] = description or strategy_class.__doc__ or "无描述"
        logger.info(f"已注册计算策略: {name} ({strategy_class.__name__})")
    
    def get_strategy(self, name: str) -> Type[StatisticalStrategy]:
        """获取策略类"""
        if name not in self._strategies:
            raise ValueError(f"未找到策略: {name}")
        return self._strategies[name]
    
    def create_strategy(self, name: str) -> StatisticalStrategy:
        """创建策略实例"""
        strategy_class = self.get_strategy(name)
        return strategy_class()
    
    def list_strategies(self) -> List[Dict[str, str]]:
        """列出所有已注册的策略"""
        return [
            {
                'name': name,
                'class_name': strategy_class.__name__,
                'description': self._descriptions[name]
            }
            for name, strategy_class in self._strategies.items()
        ]
    
    def is_registered(self, name: str) -> bool:
        """检查策略是否已注册"""
        return name in self._strategies
    
    def unregister(self, name: str) -> bool:
        """注销策略"""
        if name in self._strategies:
            del self._strategies[name]
            del self._descriptions[name]
            logger.info(f"已注销计算策略: {name}")
            return True
        return False
    
    def register_to_engine(self, engine: CalculationEngine):
        """将所有策略注册到计算引擎"""
        for name in self._strategies:
            strategy_instance = self.create_strategy(name)
            engine.register_strategy(name, strategy_instance)
            logger.debug(f"策略 {name} 已注册到计算引擎")


# 全局策略注册表
_registry = CalculationStrategyRegistry()


def get_registry() -> CalculationStrategyRegistry:
    """获取全局策略注册表"""
    return _registry


def register_strategy(name: str, strategy_class: Type[StatisticalStrategy], description: str = ""):
    """注册策略到全局注册表"""
    _registry.register(name, strategy_class, description)


def register_default_strategies():
    """注册默认的计算策略"""
    
    # 基础统计策略
    register_strategy(
        'basic_statistics',
        BasicStatisticsStrategy,
        '计算基础统计指标：平均分、中位数、标准差、方差等'
    )
    
    # 教育百分位数策略
    register_strategy(
        'percentiles',
        EducationalPercentileStrategy,
        '使用教育统计标准计算百分位数(P10, P25, P50, P75, P90)'
    )
    
    # 教育指标策略
    register_strategy(
        'educational_metrics',
        EducationalMetricsStrategy,
        '计算教育专用指标：得分率、等级分布、及格率、优秀率、难度系数'
    )
    
    # 区分度策略
    register_strategy(
        'discrimination',
        DiscriminationStrategy,
        '计算区分度指标(前27%后27%分组法)'
    )
    
    logger.info("默认计算策略注册完成")
    
    # 自动注册到全局计算引擎
    engine = get_calculation_engine()
    _registry.register_to_engine(engine)
    
    logger.info(f"已将 {len(_registry.list_strategies())} 个策略注册到计算引擎")


def initialize_calculation_system():
    """初始化计算系统"""
    logger.info("正在初始化计算系统...")
    
    # 注册默认策略
    register_default_strategies()
    
    # 获取计算引擎实例
    engine = get_calculation_engine()
    
    # 输出系统状态
    registered_strategies = engine.get_registered_strategies()
    logger.info(f"计算系统初始化完成，共注册 {len(registered_strategies)} 个策略: {registered_strategies}")
    
    return engine


def get_strategy_info(name: str) -> Dict[str, any]:
    """获取策略详细信息"""
    if not _registry.is_registered(name):
        raise ValueError(f"策略 {name} 未注册")
    
    strategy_instance = _registry.create_strategy(name)
    algorithm_info = strategy_instance.get_algorithm_info()
    
    return {
        'name': name,
        'description': _registry._descriptions[name],
        'class_name': _registry._strategies[name].__name__,
        'algorithm_info': algorithm_info
    }


def list_all_strategies() -> List[Dict[str, any]]:
    """列出所有策略的详细信息"""
    strategies = []
    for strategy_info in _registry.list_strategies():
        try:
            detailed_info = get_strategy_info(strategy_info['name'])
            strategies.append(detailed_info)
        except Exception as e:
            logger.error(f"获取策略 {strategy_info['name']} 信息失败: {e}")
            strategies.append(strategy_info)  # 使用基础信息
    
    return strategies