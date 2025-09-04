# 问卷数据处理系统设计分析

## 系统概述

问卷数据处理是教育统计分析系统中的专门模块，主要处理问卷类（调查问卷、心理测量等）数据的智能转换、计分和统计分析。该系统需要支持正向/反向量表转换、5级李克特量表处理、选项频率统计、维度汇总计算等核心功能。

## 1. 系统架构设计

### 1.1 整体架构
基于现有FastAPI + SQLAlchemy 2.0架构，采用策略模式扩展计算引擎，实现问卷数据的专门处理。

```
app/
├── calculation/
│   ├── survey_engine.py          # 问卷计算引擎
│   ├── survey_strategies.py      # 问卷计算策略
│   └── scale_config.py          # 量表配置管理
├── services/
│   └── survey_service.py        # 问卷业务服务
├── schemas/
│   ├── survey_schemas.py        # 问卷数据模型
│   └── scale_schemas.py         # 量表配置模型
└── database/
    └── survey_models.py         # 问卷数据库模型
```

### 1.2 核心组件设计

#### 1.2.1 问卷计算引擎 (SurveyCalculationEngine)
- 继承现有CalculationEngine，专门处理问卷数据
- 支持多种问卷计算策略的注册和调用
- 集成数据质量检查和异常处理

#### 1.2.2 量表配置管理 (ScaleConfigManager)
- 管理正向/反向量表配置
- 支持李克特量表定义和扩展
- 提供问卷维度结构配置

#### 1.2.3 数据质量控制 (SurveyQualityController)
- 作答时间检查
- 连续选项检测
- 完成率验证
- 异常响应识别

## 2. 量表转换算法设计

### 2.1 量表配置结构

```python
# 量表类型配置
SCALE_TYPES = {
    'forward': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5},    # 正向量表
    'reverse': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}     # 反向量表
}

# 李克特量表标签
LIKERT_LABELS = {
    1: '完全不同意',
    2: '不同意',
    3: '中性',
    4: '同意', 
    5: '完全同意'
}

# 问卷维度配置
SURVEY_DIMENSIONS = {
    'curiosity': {
        'name': '好奇心',
        'forward_questions': ['Q1', 'Q3', 'Q5'],
        'reverse_questions': ['Q2', 'Q4'],
        'weight': 1.0
    },
    'observation': {
        'name': '观察能力', 
        'forward_questions': ['Q6', 'Q8'],
        'reverse_questions': ['Q7'],
        'weight': 1.2
    }
}
```

### 2.2 量表转换策略

#### 2.2.1 ScaleTransformationStrategy
- 基于题目配置进行正向/反向计分转换
- 支持批量处理和向量化计算
- 提供转换结果验证

#### 2.2.2 LikertScaleStrategy
- 专门处理5级李克特量表
- 支持标签映射和分值转换
- 提供量表一致性检查

### 2.3 转换算法实现

```python
class ScaleTransformationStrategy(StatisticalStrategy):
    """量表转换计算策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        # 获取问卷配置
        dimensions = config.get('dimensions', {})
        scale_config = config.get('scale_config', SCALE_TYPES)
        
        results = {}
        transformed_data = data.copy()
        
        # 按维度处理题目
        for dimension_name, dimension_config in dimensions.items():
            # 正向题目转换
            forward_questions = dimension_config.get('forward_questions', [])
            for question in forward_questions:
                if question in data.columns:
                    transformed_data[f'{question}_transformed'] = data[question].map(
                        scale_config['forward']
                    )
            
            # 反向题目转换
            reverse_questions = dimension_config.get('reverse_questions', [])
            for question in reverse_questions:
                if question in data.columns:
                    transformed_data[f'{question}_transformed'] = data[question].map(
                        scale_config['reverse']
                    )
            
            # 维度得分计算
            dimension_score = self._calculate_dimension_score(
                transformed_data, dimension_config
            )
            results[dimension_name] = dimension_score
        
        return results
```

## 3. 统计计算集成方案

### 3.1 复用现有算法

#### 3.1.1 基础统计算法复用
- 利用BasicStatisticsStrategy计算维度得分的描述统计
- 复用EducationalPercentileStrategy进行分布分析
- 集成VectorizedCalculator提升处理性能

#### 3.1.2 高级分析功能集成
- 维度间相关性分析（基于已完成的相关分析模块）
- 问卷得分等级分布（基于等级分布算法）
- 异常检测（集成现有异常检测器）

### 3.2 问卷专用统计策略

#### 3.2.1 FrequencyAnalysisStrategy
```python
class FrequencyAnalysisStrategy(StatisticalStrategy):
    """选项频率分析策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        questions = config.get('questions', [])
        results = {}
        
        for question in questions:
            if question in data.columns:
                # 使用向量化计算选项频率
                value_counts = data[question].value_counts(dropna=False)
                total_responses = len(data[question])
                
                # 计算频率和百分比
                frequency_data = {
                    'frequencies': value_counts.to_dict(),
                    'percentages': (value_counts / total_responses).to_dict(),
                    'total_responses': total_responses,
                    'missing_count': data[question].isna().sum()
                }
                
                results[question] = frequency_data
        
        return results
```

#### 3.2.2 DimensionAggregationStrategy
```python
class DimensionAggregationStrategy(StatisticalStrategy):
    """维度汇总计算策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        dimensions = config.get('dimensions', {})
        results = {}
        
        for dimension_name, dimension_config in dimensions.items():
            # 获取维度相关题目的转换后得分
            dimension_questions = (
                dimension_config.get('forward_questions', []) + 
                dimension_config.get('reverse_questions', [])
            )
            
            # 计算维度得分
            dimension_scores = []
            for question in dimension_questions:
                transformed_col = f'{question}_transformed'
                if transformed_col in data.columns:
                    dimension_scores.append(data[transformed_col])
            
            if dimension_scores:
                # 使用现有基础统计算法
                dimension_df = pd.DataFrame(dimension_scores).T
                dimension_df['dimension_score'] = dimension_df.mean(axis=1, skipna=True)
                
                # 计算维度统计指标
                basic_stats = self._calculate_basic_stats(dimension_df['dimension_score'])
                results[dimension_name] = basic_stats
        
        return results
```

## 4. 数据质量控制设计

### 4.1 质量检查规则

```python
QUALITY_RULES = {
    'response_time_min': 30,         # 最少作答时间(秒)
    'response_time_max': 1800,       # 最长作答时间(秒)
    'straight_line_max': 10,         # 连续相同选项最大数量
    'completion_rate_min': 0.8,      # 最低完成率
    'variance_threshold': 0.1        # 方差阈值(检测无变化响应)
}
```

### 4.2 质量控制策略

#### 4.2.1 SurveyQualityStrategy
```python
class SurveyQualityStrategy(StatisticalStrategy):
    """问卷数据质量检查策略"""
    
    def calculate(self, data: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        quality_rules = config.get('quality_rules', QUALITY_RULES)
        results = {
            'total_responses': len(data),
            'quality_flags': {},
            'quality_summary': {}
        }
        
        # 完成率检查
        completion_rates = data.notna().mean(axis=1)
        low_completion = completion_rates < quality_rules['completion_rate_min']
        results['quality_flags']['low_completion'] = low_completion.sum()
        
        # 连续相同选项检查
        straight_line_count = self._detect_straight_line_responses(
            data, quality_rules['straight_line_max']
        )
        results['quality_flags']['straight_line'] = straight_line_count
        
        # 响应时间检查（如果有时间数据）
        if 'response_time' in data.columns:
            time_issues = self._check_response_time(
                data['response_time'], quality_rules
            )
            results['quality_flags'].update(time_issues)
        
        # 无变化响应检查
        no_variance = self._detect_no_variance_responses(
            data, quality_rules['variance_threshold']
        )
        results['quality_flags']['no_variance'] = no_variance
        
        return results
```

## 5. 与现有系统集成

### 5.1 计算引擎集成

#### 5.1.1 策略注册
在现有CalculationEngine中注册问卷相关策略：

```python
# 在get_calculation_engine()中注册问卷策略
engine = CalculationEngine()
engine.register_strategy('scale_transformation', ScaleTransformationStrategy())
engine.register_strategy('frequency_analysis', FrequencyAnalysisStrategy())
engine.register_strategy('dimension_aggregation', DimensionAggregationStrategy())
engine.register_strategy('survey_quality', SurveyQualityStrategy())
```

#### 5.1.2 服务层集成
创建SurveyService整合各种计算策略：

```python
class SurveyService:
    """问卷数据处理服务"""
    
    def __init__(self):
        self.calculation_engine = get_calculation_engine()
        self.scale_config_manager = ScaleConfigManager()
    
    async def process_survey_data(
        self, 
        data: pd.DataFrame, 
        survey_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """处理问卷数据的完整管道"""
        
        # 1. 数据质量检查
        quality_result = self.calculation_engine.calculate(
            'survey_quality', data, survey_config
        )
        
        # 2. 量表转换
        transformation_result = self.calculation_engine.calculate(
            'scale_transformation', data, survey_config
        )
        
        # 3. 频率分析
        frequency_result = self.calculation_engine.calculate(
            'frequency_analysis', data, survey_config
        )
        
        # 4. 维度汇总
        dimension_result = self.calculation_engine.calculate(
            'dimension_aggregation', data, survey_config
        )
        
        # 5. 整合结果
        return {
            'quality_check': quality_result,
            'scale_transformation': transformation_result,
            'frequency_analysis': frequency_result,
            'dimension_aggregation': dimension_result,
            'processing_metadata': {
                'total_responses': len(data),
                'processing_time': time.time(),
                'config_version': survey_config.get('version', '1.0')
            }
        }
```

### 5.2 数据库集成

#### 5.2.1 扩展现有模型
基于StatisticalMetadata存储问卷配置：

```python
# 问卷配置数据示例
survey_metadata = {
    'metadata_type': MetadataType.SURVEY_CONFIG,
    'metadata_key': 'student_curiosity_survey',
    'metadata_value': {
        'dimensions': SURVEY_DIMENSIONS,
        'scale_config': SCALE_TYPES,
        'quality_rules': QUALITY_RULES
    },
    'subject_type': SubjectType.SURVEY,
    'description': '学生好奇心调查问卷配置'
}
```

#### 5.2.2 结果存储格式
在StatisticalAggregation中存储问卷统计结果：

```python
# 问卷结果JSON结构
survey_statistics = {
    'survey_type': 'student_curiosity',
    'total_responses': 1250,
    'quality_summary': {
        'valid_responses': 1180,
        'quality_issues': 70,
        'completion_rate': 0.94
    },
    'dimension_scores': {
        'curiosity': {
            'mean': 3.85,
            'std': 0.73,
            'distribution': {1: 45, 2: 89, 3: 234, 4: 456, 5: 356}
        },
        'observation': {
            'mean': 3.92,
            'std': 0.68,
            'distribution': {1: 32, 2: 78, 3: 201, 4: 489, 5: 380}
        }
    },
    'option_frequencies': {
        'Q1': {'1': 0.05, '2': 0.12, '3': 0.25, '4': 0.35, '5': 0.23},
        'Q2': {'1': 0.08, '2': 0.18, '3': 0.28, '4': 0.28, '5': 0.18}
    }
}
```

## 6. 性能优化策略

### 6.1 向量化计算
- 使用Pandas向量化操作处理量表转换
- NumPy数组操作优化频率统计
- 批量处理降低内存占用

### 6.2 缓存策略
- 量表配置缓存减少重复加载
- 计算结果缓存避免重复计算
- 分块处理大数据集

### 6.3 并行处理
- 维度计算并行化
- 多问卷同时处理
- 利用现有ParallelCalculationEngine

## 7. 扩展性考虑

### 7.1 量表类型扩展
- 支持7级、9级量表
- 自定义量表配置
- 多语言量表标签

### 7.2 分析功能扩展
- 问卷信效度分析
- 因子分析集成
- 聚类分析支持

### 7.3 数据源扩展
- 在线问卷平台集成
- 纸质问卷数据导入
- 实时数据流处理

## 8. 测试验证策略

### 8.1 单元测试
- 量表转换准确性验证
- 频率统计计算验证
- 维度汇总正确性测试
- 数据质量检查测试

### 8.2 集成测试
- 端到端问卷处理流程
- 与现有算法集成测试
- 性能基准测试

### 8.3 业务验证
- 教育专家评审计分规则
- 实际问卷数据对比验证
- 用户体验测试

## 9. 部署和监控

### 9.1 部署配置
- 问卷配置版本管理
- 环境变量配置
- 容器化部署支持

### 9.2 监控指标
- 处理性能监控
- 数据质量监控
- 错误率监控
- 资源使用监控

## 10. 风险评估和缓解

### 10.1 技术风险
- **数据一致性风险**: 通过严格的验证和测试确保量表转换准确性
- **性能风险**: 采用向量化计算和分块处理应对大数据集
- **兼容性风险**: 保持与现有系统API兼容性

### 10.2 业务风险
- **计分标准变化**: 配置化管理支持快速调整
- **数据质量问题**: 多层次质量控制和异常处理
- **用户需求变更**: 模块化设计支持功能扩展

## 11. 实施计划

### Phase 1: 核心功能实现 (8小时)
- 量表配置管理器
- 基础转换策略
- 频率统计策略

### Phase 2: 高级功能 (8小时)
- 维度汇总策略
- 数据质量控制
- 服务层集成

### Phase 3: 优化和测试 (8小时)
- 性能优化
- 全面测试
- 文档完善

## 12. 成功标准

### 12.1 功能标准
- [x] 正确实现正向/反向量表转换
- [x] 准确计算选项频率分布
- [x] 正确汇总维度得分
- [x] 有效识别数据质量问题

### 12.2 性能标准
- 10万问卷响应处理时间 < 2分钟
- 内存使用 < 2GB
- API响应时间 < 1秒

### 12.3 质量标准
- 测试覆盖率 > 90%
- 计算准确率 = 100%
- 代码符合现有规范

通过以上设计，问卷数据处理系统将无缝集成到现有教育统计分析系统中，提供专业、高效、可扩展的问卷数据处理能力，为教育评估和心理测量提供强有力的技术支持。