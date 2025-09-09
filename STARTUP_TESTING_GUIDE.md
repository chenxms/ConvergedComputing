# 🚀 Data-Calculation 项目启动与测试指南

## 📋 测试清单概览

### ✅ 环境准备阶段
- [ ] Python环境和依赖包安装
- [ ] 数据库连接配置和验证
- [ ] Redis缓存服务检查（可选）
- [ ] 环境变量配置

### ✅ 基础组件测试
- [ ] 数据库模型和迁移验证
- [ ] Repository层功能测试
- [ ] 缓存机制测试

### ✅ 核心算法测试
- [ ] 统计计算引擎初始化
- [ ] 各个计算器策略验证
- [ ] 问卷处理系统测试

### ✅ API接口测试
- [ ] FastAPI服务启动
- [ ] 任务管理API测试
- [ ] 统计报告API测试
- [ ] JSON序列化验证

### ✅ 集成测试
- [ ] 端到端数据流测试
- [ ] 性能基准测试
- [ ] 错误处理验证

---

## 🔧 阶段一：环境准备与基础验证

### 1.1 检查Python环境和依赖
```bash
# 检查Python版本（要求3.11+）
python --version

# 检查Poetry是否安装
poetry --version

# 安装项目依赖
poetry install

# 激活虚拟环境
poetry shell
```

### 1.2 验证数据库连接
```bash
# 测试数据库连接
python scripts/test_database_connection.py
```

**预期输出**：
```
✅ 数据库连接成功
✅ 数据库版本: MySQL 8.4.6
✅ 权限检查通过
```

### 1.3 运行数据库迁移
```bash
# 检查迁移状态
alembic current

# 运行迁移到最新版本
alembic upgrade head

# 验证表结构创建
python scripts/verify_database_schema.py
```

**预期结果**：
- statistical_aggregations 表创建成功
- statistical_metadata 表创建成功  
- statistical_history 表创建成功
- 索引和约束正确创建

---

## 🧮 阶段二：核心算法测试

### 2.1 统计计算引擎初始化测试
```bash
# 运行引擎初始化测试
python tests/test_engine_initialization.py
```

**验证点**：
- ✅ 计算引擎正确初始化
- ✅ 6个策略正确注册（basic_statistics, percentiles, educational_metrics, discrimination, grade_distribution, dimension_statistics）
- ✅ 策略元数据完整

### 2.2 各计算器功能测试
```bash
# 测试基础统计计算
python tests/test_calculation_engine.py::TestBasicStatistics

# 测试百分位数计算器
python tests/test_percentile_calculator.py

# 测试区分度和难度计算
python tests/test_difficulty_calculator.py
python tests/test_discrimination_calculator.py

# 测试等级分布计算
python tests/test_grade_calculator.py

# 测试维度统计处理
python tests/test_dimension_calculator.py

# 测试问卷数据处理
python tests/test_survey_calculator.py
```

### 2.3 计算准确性验证
```bash
# 运行算法精度验证
python scripts/validate_calculation_accuracy.py
```

**验证指标**：
- 与Excel手工计算误差 < 0.001%
- 边界条件处理正确
- 异常数据检测有效

---

## 🌐 阶段三：API服务测试

### 3.1 启动FastAPI服务
```bash
# 启动开发服务器
poetry run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# 或使用脚本启动
poetry run python app/main.py
```

**验证服务启动**：
- 访问 http://localhost:8000/docs 查看API文档
- 检查健康检查端点：http://localhost:8000/health

### 3.2 API接口功能测试

#### 3.2.1 任务管理API测试
```bash
# 测试批次管理
curl -X POST http://localhost:8000/api/v1/statistics/batches \
  -H "Content-Type: application/json" \
  -d '{"batch_code": "TEST_2025_001", "description": "测试批次"}'

# 查询批次列表
curl -X GET http://localhost:8000/api/v1/statistics/batches

# 启动计算任务
curl -X POST http://localhost:8000/api/v1/statistics/tasks/TEST_2025_001/start
```

#### 3.2.2 统计报告API测试
```bash
# 获取区域统计报告（需要有数据）
curl -X GET http://localhost:8000/api/v1/reports/regional/TEST_2025_001

# 获取学校统计报告
curl -X GET http://localhost:8000/api/v1/reports/school/TEST_2025_001/SCHOOL_001

# 获取雷达图数据
curl -X GET http://localhost:8000/api/v1/reports/radar-chart/TEST_2025_001
```

### 3.3 运行完整API测试套件
```bash
# 运行所有API测试
python tests/test_calculation_api.py
python tests/test_reporting_api.py

# 或使用pytest
pytest tests/test_*api*.py -v
```

---

## 📊 阶段四：数据处理集成测试

### 4.1 准备测试数据

**创建测试数据脚本**：
```bash
python scripts/create_test_data.py
```

这将创建：
- 1000个测试学生记录
- 多个科目的答题数据
- 问卷调查数据
- 维度映射数据

### 4.2 端到端数据流测试
```bash
# 运行完整数据处理流程
python scripts/end_to_end_test.py
```

**测试流程**：
1. 原始数据导入 → 数据库存储
2. 统计计算执行 → 各种指标计算
3. 结果序列化 → JSON格式输出
4. API查询验证 → 前端数据获取

### 4.3 性能基准测试
```bash
# 运行性能测试
python tests/test_performance_benchmarks.py

# 大数据量测试（10万条记录）
python scripts/large_dataset_test.py
```

**性能目标验证**：
- ✅ 10万学生数据处理 < 30分钟
- ✅ API响应95%分位数 < 500ms  
- ✅ 内存使用峰值 < 4GB
- ✅ 缓存命中率 > 80%

---

## 🐛 错误排查指南

### 常见问题和解决方案

#### 🔴 数据库连接问题
```bash
# 错误：Unable to connect to database
# 解决方案：
1. 检查数据库服务是否运行
2. 验证连接参数（IP、端口、用户名、密码）
3. 检查防火墙设置
4. 测试网络连通性：telnet 117.72.14.166 23506
```

#### 🔴 迁移失败
```bash
# 错误：Migration failed
# 解决方案：
1. 检查数据库权限：CREATE, ALTER, DROP
2. 查看迁移日志：alembic history
3. 手动重置迁移：alembic downgrade base && alembic upgrade head
```

#### 🔴 计算结果异常
```bash
# 排查步骤：
1. 检查输入数据完整性
python scripts/validate_input_data.py

2. 验证配置参数
python scripts/check_calculation_config.py

3. 运行单步调试
python -m pdb scripts/debug_calculation.py
```

#### 🔴 API响应错误
```bash
# 排查步骤：
1. 查看服务器日志
tail -f logs/app.log

2. 检查数据库查询
python scripts/debug_database_queries.py

3. 验证JSON序列化
python scripts/validate_json_serialization.py
```

#### 🔴 性能问题
```bash
# 性能分析工具：
1. 数据库查询性能
python scripts/profile_database_queries.py

2. 内存使用分析
python -m memory_profiler scripts/memory_analysis.py

3. API响应时间分析
python scripts/api_performance_test.py
```

---

## ✅ 测试验收标准

### 功能测试通过标准
- [ ] 所有单元测试通过（90%+覆盖率）
- [ ] API接口响应正确
- [ ] 计算结果精度达标（<0.001%误差）
- [ ] JSON输出符合规范

### 性能测试通过标准
- [ ] 10万学生数据处理时间 < 30分钟
- [ ] API平均响应时间 < 200ms
- [ ] 95%分位响应时间 < 500ms
- [ ] 并发100用户无错误

### 稳定性测试通过标准
- [ ] 连续运行24小时无崩溃
- [ ] 内存泄露检测通过
- [ ] 错误恢复机制有效
- [ ] 日志记录完整

---

## 📞 技术支持

### 日志查看位置
```bash
# 应用日志
tail -f logs/application.log

# 数据库查询日志
tail -f logs/database.log  

# 错误日志
tail -f logs/error.log
```

### 监控检查
```bash
# 系统状态检查
curl http://localhost:8000/api/v1/statistics/system/status

# 数据库连接检查  
python scripts/health_check.py

# 缓存状态检查
python scripts/cache_status.py
```

### 问题报告格式
请提供以下信息：
1. 错误现象描述
2. 复现步骤
3. 错误日志截图
4. 系统环境信息
5. 测试数据样本

---

**📋 使用此指南逐步测试，确保每个环节都达到预期效果！**