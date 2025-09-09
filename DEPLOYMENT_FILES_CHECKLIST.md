# 部署文件清单 - 运维交付核心文件

## 一、必须交付的核心程序文件

### 1. 应用程序目录 (`app/`) - 完整目录
这是核心业务代码，必须完整拷贝：

```
app/
├── __init__.py
├── main.py                    # 主API服务入口
├── subjects_api_main.py       # 科目API服务入口
│
├── api/                       # API接口层
│   ├── __init__.py
│   ├── management_api.py     # 管理接口
│   ├── reporting_api.py      # 报告接口
│   └── subjects_v12_api.py   # 科目汇聚接口
│
├── services/                  # 业务服务层
│   ├── __init__.py
│   ├── batch_service.py      # 批次服务
│   ├── calculation_service.py # 计算服务
│   ├── task_manager.py       # 任务管理
│   ├── ranking_service.py    # 排名服务
│   ├── subjects_builder.py   # 科目构建
│   ├── questionnaire_processor.py # 问卷处理
│   ├── simplified_aggregation_service.py # 简化汇聚
│   └── serialization/        # 序列化模块
│       ├── __init__.py
│       ├── data_integrator.py
│       └── statistics_json_serializer.py
│
├── calculation/               # 计算引擎
│   ├── __init__.py
│   ├── engine.py             # 核心引擎
│   ├── formulas.py           # 统计公式
│   ├── simplified_calculator.py # 简化计算器
│   └── calculators/          # 专项计算器
│       ├── __init__.py
│       ├── grade_calculator.py
│       └── survey_calculator.py
│
├── database/                  # 数据库层
│   ├── __init__.py
│   ├── connection.py         # 连接管理
│   ├── models.py             # 数据模型
│   └── repositories.py       # 数据仓库
│
├── schemas/                   # 数据架构
│   ├── __init__.py
│   ├── request_schemas.py    # 请求模型
│   ├── json_schemas.py       # JSON模型
│   └── simplified_aggregation_schema.py
│
├── repositories/              # 额外仓库（如果存在）
│   └── ...
│
└── utils/                     # 工具函数（如果存在）
    └── ...
```

### 2. 脚本目录 (`scripts/`) - 部分文件
只需要这几个关键脚本：

```
scripts/
├── rewrite_subjects_v12.py       # 批次物化脚本（重要）
├── acceptance_quick_check.py     # 验收检查脚本
└── complete_questionnaire_labels.py # 问卷标签脚本
```

### 3. 文档目录 (`docs/`) - 完整目录
包含数据规范和接口文档：

```
docs/
├── 数据库表结构详细说明文档.md
├── API_v1.2_汇聚接口规范.md
├── 学业发展质量监测统计分析服务_技术文档.md
├── 数据汇聚实现与API契约.md
└── ...
```

## 二、Docker相关文件（必须）

```
Dockerfile                # Docker镜像定义
docker-compose.yml        # 服务编排配置
.dockerignore            # Docker忽略规则（如果有）
```

## 三、配置和部署文件（必须）

```
.env.example             # 环境变量模板（重要）
deploy.sh                # 自动部署脚本
nginx.conf.example       # Nginx配置示例
package.sh               # 打包脚本
```

## 四、部署文档（必须）

```
DEPLOYMENT_GUIDE.md      # 详细部署指南
DEPLOYMENT_CHECKLIST.md  # 部署检查清单
DEPLOYMENT_FILES_CHECKLIST.md # 本文件
```

## 五、依赖定义文件（至少一个）

```
requirements.txt         # Python依赖列表（必须）
pyproject.toml          # Poetry配置（可选）
poetry.lock             # Poetry锁文件（可选）
```

## 六、不需要交付的文件

以下文件**不要**包含在部署包中：

```
❌ __pycache__/         # Python缓存
❌ *.pyc               # 编译的Python文件
❌ .git/               # Git版本控制
❌ .env                # 实际环境变量（包含密码）
❌ *.log               # 日志文件
❌ test_*.py           # 测试脚本
❌ *_test.py           # 测试脚本
❌ *.sql               # SQL脚本（除非必要）
❌ *.json              # 测试数据文件
❌ .vscode/            # IDE配置
❌ .idea/              # IDE配置
❌ node_modules/       # 如果有前端代码
```

## 七、快速打包命令

### 方法1：使用打包脚本（推荐）

```bash
# 在项目根目录执行
chmod +x package.sh
./package.sh

# 将生成: converged-computing_[时间戳].tar.gz
```

### 方法2：手动打包

```bash
# 创建临时目录
mkdir deployment_package

# 复制必要文件
cp -r app deployment_package/
cp -r scripts deployment_package/
cp -r docs deployment_package/
cp Dockerfile docker-compose.yml deployment_package/
cp .env.example deploy.sh nginx.conf.example deployment_package/
cp requirements.txt deployment_package/
cp DEPLOYMENT_*.md deployment_package/

# 清理缓存
find deployment_package -type d -name "__pycache__" -exec rm -rf {} +
find deployment_package -type f -name "*.pyc" -delete

# 打包
tar -czf converged-computing.tar.gz deployment_package/
```

### 方法3：使用Git打包（如果使用Git）

```bash
# 打包当前分支的干净代码
git archive --format=tar.gz -o converged-computing.tar.gz HEAD
```

## 八、交付前检查清单

在交付给运维前，请确认：

- [ ] 所有Python源代码文件完整（app/目录）
- [ ] 必要的脚本文件包含（scripts/目录）
- [ ] Docker配置文件存在
- [ ] 环境变量模板（.env.example）已更新
- [ ] 部署文档完整且最新
- [ ] requirements.txt包含所有依赖
- [ ] 没有包含敏感信息（密码、密钥等）
- [ ] 没有包含测试数据和临时文件
- [ ] 打包文件可以正常解压

## 九、预估文件大小

正常情况下，打包后的文件大小约为：
- 核心代码：~500KB - 2MB
- 包含文档：~1MB - 5MB
- 完整打包：~2MB - 10MB

如果超过10MB，请检查是否包含了不必要的文件。

## 十、交付方式建议

1. **文件传输**：
   - 使用企业内部文件共享系统
   - 通过加密的压缩包传输
   - 使用安全的FTP/SFTP服务器

2. **版本控制**：
   - 如果有内部Git服务器，可以直接给运维Git仓库访问权限
   - 使用Git tag标记版本

3. **容器镜像**：
   - 可以预先构建Docker镜像并推送到私有镜像仓库
   - 运维直接拉取镜像部署

## 联系信息

如果运维在部署过程中遇到文件缺失或其他问题，请及时联系开发团队补充。