---
name: data-calculation
status: in_analysis
created: 2025-09-04T18:28:55Z
updated: 2025-09-04T19:20:00Z
progress: 15%
prd: .claude/prds/data-calculation.md
github: [Will be updated when synced to GitHub]
---

# Epic: data-calculation

## 概览

实施学业发展质量监测数据统计分析与汇聚计算服务。基于现有FastAPI+SQLAlchemy架构，构建高性能的教育统计计算引擎，支持区域级和学校级的多维度统计分析，将原始答题数据转化为结构化的教育洞察。

**技术核心**：使用Pandas+NumPy实现教育统计算法，通过JSON字段持久化复杂统计结果，提供RESTful API支持前端报告系统和雷达图展示。

## 架构决策

### 核心技术选择
- **计算引擎**：Pandas+NumPy - 提供高性能的统计计算和数据处理能力
- **数据存储**：MySQL JSON字段 - 灵活存储复杂的统计结果结构
- **任务处理**：FastAPI异步处理 - 支持长时间运行的统计汇聚任务
- **数据访问**：SQLAlchemy 2.0 Repository模式 - 保持与现有架构一致性

### 设计模式
- **服务层模式**：`CalculationService` 负责业务逻辑编排
- **策略模式**：不同科目类型（考试类、人机交互类、问卷类）采用不同计算策略
- **工厂模式**：统计计算器工厂，根据需求创建相应的计算器实例
- **模板模式**：区域级和学校级使用统一计算模板，仅数据分组不同

### 数据流架构
```
原始数据层 (student_score_detail) 
    ↓
数据验证层 (满分配置检查、数据完整性验证)
    ↓
计算引擎层 (Pandas统计计算、百分位数算法)
    ↓ 
结果序列化层 (JSON结构构建、前端格式适配)
    ↓
持久化层 (MySQL JSON字段存储)
    ↓
API服务层 (RESTful接口、状态管理)
```

## 技术实施方案

### 后端服务扩展

**核心计算模块**：
```python
# 统计计算引擎
app/calculation/
├── engine.py              # 核心计算引擎
├── calculators/
│   ├── regional_calculator.py    # 区域级统计计算
│   ├── school_calculator.py      # 学校级统计计算
│   └── percentile_calculator.py  # 百分位数计算器
├── formulas.py            # 教育统计公式实现
└── validators.py          # 数据验证器

# 业务服务层
app/services/
├── calculation_service.py # 计算任务编排服务
├── batch_service.py      # 批次管理服务
└── task_manager.py       # 异步任务管理

# API接口层
app/api/
├── calculation_api.py    # 计算管理API
├── reporting_api.py      # 统计结果查询API
└── schemas/
    ├── calculation_schemas.py  # 计算相关数据模型
    └── response_schemas.py     # API响应模型
```

**数据库模型扩展**：
```python
# 新增/扩展表模型
class RegionalStatisticsSummary(Base):
    batch_code = Column(String, primary_key=True) 
    statistics_data = Column(JSON)  # 区域级统计结果
    created_at = Column(DateTime, default=datetime.utcnow)

class SchoolStatisticsSummary(Base):
    batch_code = Column(String, primary_key=True)
    school_id = Column(String, primary_key=True)
    statistics_data = Column(JSON)  # 学校级统计结果
    created_at = Column(DateTime, default=datetime.utcnow)

class StatisticsTaskStatus(Base):
    task_id = Column(String, primary_key=True)
    batch_code = Column(String, nullable=False)
    status = Column(Enum('pending', 'processing', 'completed', 'failed'))
    progress = Column(Float, default=0.0)
    error_message = Column(Text)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
```

### 基础设施要求

**性能优化**：
- 数据库连接池配置优化，支持长时间计算任务
- Pandas DataFrame内存管理，支持10万条记录处理
- 分批处理策略，避免大数据集内存溢出

**监控和日志**：
- 计算任务执行日志，包含详细的统计指标
- 性能监控，跟踪计算时间和内存使用
- 错误报告，精确定位计算异常位置

## 实施策略

### 开发阶段划分

**阶段1：基础架构 (2周)**
- 数据库模型创建和迁移
- 核心计算引擎框架搭建
- 基础数据访问层实现

**阶段2：核心算法 (2.5周)**
- 教育统计公式实现（难度、区分度、百分位数）
- 维度计算逻辑和数据聚合
- 等级分布和得分率计算

**阶段3：业务集成 (1.5周)**
- 问卷类数据特殊处理（正向/反向量表）
- JSON数据结构设计和序列化
- 批次任务管理和状态跟踪

**阶段4：API和验证 (1.5周)**
- RESTful API端点实现
- 计算准确性验证和测试
- 性能优化和错误处理

### 风险缓解策略
- **数据一致性风险**：实施严格的数据验证层，确保max_score等关键配置完整
- **性能瓶颈风险**：采用分页计算和内存优化策略
- **计算精度风险**：建立全面的单元测试，特别关注边界条件

### 测试方法
- **单元测试**：每个统计公式的独立验证
- **集成测试**：端到端的批次计算流程测试  
- **性能测试**：10万学生数据的负载测试
- **准确性验证**：与Excel手工计算结果对比

## 任务分解预览

基于复杂度分析，将实施分解为以下10个核心任务：

- [ ] **任务1：数据库模型和迁移** - 创建统计相关表结构，支持JSON数据存储
- [ ] **任务2：数据访问层构建** - Repository模式实现，统一数据查询接口
- [ ] **任务3：基础统计计算引擎** - 平均分、得分率、标准差等基础指标计算
- [ ] **任务4：百分位数计算器** - P10/P50/P90精确算法实现，处理边界条件
- [ ] **任务5：区分度和难度计算** - 前27%/后27%分组算法和教育统计公式
- [ ] **任务6：维度统计处理** - 基于question_dimension_mapping的多维度聚合
- [ ] **任务7：等级分布计算** - 小学/初中不同阈值的等级划分逻辑
- [ ] **任务8：问卷数据处理** - 正向/反向量表转换和问卷类统计算法
- [ ] **任务9：任务管理API** - 批次管理、状态查询、手动触发等管理接口
- [ ] **任务10：JSON数据结构** - 统计结果序列化和前端雷达图数据格式

## 依赖关系

### 数据依赖
- **关键表数据完整性**：student_score_detail、subject_question_config、question_dimension_mapping
- **批次年级信息**：grade_aggregation_main.grade_level字段（1-6th_grade=小学，7-9th_grade=初中）
- **配置数据准确性**：学段等级划分标准、科目类型定义、正向/反向量表配置

### 技术依赖  
- **现有架构组件**：FastAPI框架、SQLAlchemy 2.0、MySQL 8.4.6数据库
- **新增依赖包**：pandas>=1.5.0、numpy>=1.24.0、scipy>=1.10.0（统计计算）

### 业务依赖
- **数据质量保证**：数据导入团队确保原始数据完整性和格式正确性
- **业务规则确认**：教育统计计算规则和公式的最终确认
- **接口适配**：前端报告系统对新增JSON数据格式的支持和适配

### 关键路径依赖
```
任务1(数据库) → 任务2(数据访问) → 任务3(基础计算) → 
(任务4,5,6并行执行) → 任务7,8(业务逻辑) → 任务9(API) → 任务10(数据格式)
```

## 成功标准（技术）

### 性能基准
- **计算性能**：单批次10万学生数据汇聚完成时间 < 30分钟
- **API响应**：统计结果查询API响应时间95%分位数 < 500毫秒
- **并发处理**：支持3个批次并发计算无性能下降
- **内存使用**：计算过程峰值内存使用 < 4GB

### 质量要求
- **计算精度**：所有统计指标计算结果与手工验证误差 < 0.01%
- **数据一致性**：同一批次多次计算结果完全一致
- **错误处理**：异常情况下的graceful degradation和详细错误信息
- **代码质量**：单元测试覆盖率 > 90%，核心算法100%覆盖

### 功能验收
- **完整性验证**：所有FR1-FR6功能需求100%实现
- **格式规范**：JSON数据结构符合前端消费格式要求
- **API兼容**：所有管理和查询API端点功能验证通过
- **业务准确**：教育统计指标计算符合教育学专业标准

## 预估工作量

### 整体时间线
- **总开发周期**：7.5周（约2个月）
- **核心开发**：6周
- **测试验证**：1周  
- **部署调优**：0.5周

### 资源需求
- **主力开发**：2名后端工程师全职投入
  - 1名负责计算引擎和算法实现
  - 1名负责API开发和数据库设计
- **支持资源**：1名测试工程师兼职支持，业务专家定期咨询

### 关键里程碑
- **Week 2**：数据库设计完成，基础架构就绪
- **Week 4.5**：核心统计算法全部实现并通过单元测试
- **Week 6**：业务逻辑完成，JSON数据格式确定
- **Week 7.5**：API开发完成，系统集成测试通过

**关键风险**：百分位数算法的边界条件处理和大数据量的内存优化可能需要额外1-2周调优时间。