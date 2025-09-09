# 🚀 快速测试指南 (Docker环境)

## 🎯 适用场景
您的环境已就绪：
- ✅ Docker容器运行中
- ✅ FastAPI服务 http://localhost:8000 可访问
- ✅ 数据库连接正常 (117.72.14.166:23506)

## ⚡ 快速验证 (5分钟)

### 1. 统计计算引擎测试
```bash
# 测试核心算法是否正常
python scripts/test_engine_initialization.py
```

**预期结果**：
- ✅ 6个计算策略注册成功
- ✅ 基础统计、百分位数、等级分布计算正确
- ✅ 问卷处理功能正常

### 2. API接口功能测试
```bash
# 测试所有API端点
python scripts/test_api_endpoints.py
```

**预期结果**：
- ✅ 健康检查端点正常
- ✅ 批次管理API工作正常
- ✅ 报告API结构正确
- ✅ API响应时间 < 500ms

### 3. 端到端业务流程测试
```bash
# 完整业务流程验证
python scripts/end_to_end_test.py
```

**预期结果**：
- ✅ 数据输入 → 统计计算 → JSON输出 全流程正常
- ✅ 所有核心功能验证通过
- ✅ 性能指标满足要求

---

## 🔧 如果测试失败

### 常见问题快速排查

#### 🔴 API连接失败
```bash
# 检查Docker容器状态
docker-compose ps

# 查看应用日志
docker-compose logs -f web

# 重启服务
docker-compose restart
```

#### 🔴 计算引擎异常
```bash
# 检查依赖包
docker-compose exec web pip list | grep -E "(pandas|numpy|sqlalchemy)"

# 查看详细错误
docker-compose exec web python scripts/test_engine_initialization.py
```

#### 🔴 数据库连接问题
```bash
# 测试数据库连接
docker-compose exec web python scripts/test_database_connection.py

# 检查网络连通性
docker-compose exec web ping 117.72.14.166
```

---

## 🧪 深度测试 (可选)

### 创建测试数据
```bash
# 生成完整测试数据集
python scripts/create_test_data.py

# 验证数据质量
ls -la *.json
```

### 性能压力测试
```bash
# 大数据量计算测试
python -c "
from app.calculation import initialize_calculation_system
import time
engine = initialize_calculation_system()
data = list(range(10000))  # 1万条数据
start = time.time()
result = engine.calculate('basic_statistics', data, {'data_type': 'scores'})
print(f'10K数据计算耗时: {(time.time()-start)*1000:.2f}ms')
"
```

### API负载测试
```bash
# 安装测试工具 (如果需要)
pip install locust

# 简单并发测试
python -c "
import requests
import threading
import time

def test_api():
    try:
        r = requests.get('http://localhost:8000/health', timeout=5)
        return r.status_code == 200
    except:
        return False

# 10个并发请求
results = []
threads = []
for i in range(10):
    t = threading.Thread(target=lambda: results.append(test_api()))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print(f'并发测试结果: {sum(results)}/10 成功')
"
```

---

## 📊 验收标准

### ✅ 必须通过的测试
- [ ] 统计计算引擎 - 所有策略正常工作
- [ ] API接口 - 所有端点响应正确  
- [ ] 端到端流程 - 完整业务流程无错误
- [ ] JSON序列化 - 数据格式符合规范

### ✅ 性能要求
- [ ] API平均响应时间 < 200ms
- [ ] 1万条数据计算 < 5秒
- [ ] 10并发请求全部成功

### ✅ 功能完整性
- [ ] 基础统计：平均分、标准差、百分位数
- [ ] 教育指标：难度系数、区分度、等级分布
- [ ] 问卷处理：量表转换、维度汇总
- [ ] 数据管理：批次CRUD、状态查询

---

## 🎉 测试通过后

### 生产环境准备清单
- [ ] 配置生产数据库连接
- [ ] 设置环境变量和安全配置
- [ ] 配置日志和监控系统
- [ ] 准备真实业务数据进行测试
- [ ] 建立备份和恢复策略

### 开发环境扩展
- [ ] 集成IDE调试配置
- [ ] 配置代码质量检查工具
- [ ] 设置持续集成流水线
- [ ] 编写更多业务场景测试用例

---

## 🆘 技术支持

### 日志查看
```bash
# 应用运行日志
docker-compose logs web

# 实时监控日志
docker-compose logs -f web

# 数据库相关日志
docker-compose logs db
```

### 进入容器调试
```bash
# 进入应用容器
docker-compose exec web bash

# 运行交互式Python
docker-compose exec web python

# 查看容器资源使用
docker stats
```

### 问题反馈
如遇到问题，请提供：
1. 错误截图和日志
2. `docker-compose ps` 输出
3. `docker-compose logs` 相关日志
4. 具体的操作步骤

---

**🎯 目标：5-10分钟内完成核心功能验证，确保系统运行正常！**