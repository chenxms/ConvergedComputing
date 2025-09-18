# Repository Guidelines

## 项目结构与模块组织
- `app/` 为 FastAPI 单体核心，`api/` 暴露管理与报告路由，`main.py` 负责装配依赖并注册中间件。
- `app/calculation/` 维护统计引擎与公式实现，`app/database/` 提供 SQLAlchemy 模型与仓储，`schemas/` 定义 Pydantic 请求与响应模型，保证输入输出一致。
- `app/services/` 聚合业务流程，配合 `utils/` 与 `repositories/` 处理缓存、校验和数据访问扩展，脚本侧引用可避免重复实现。
- `scripts/` 收录批量清洗、诊断和回填脚本，命名遵循 `动词_目标.py` 便于检索；`tests/` 存放 pytest 用例，覆盖单元、集成与数据契约场景。
- 数据迁移集中在 `alembic/`（含 `alembic.ini`），配置与数据治理样例位于 `config/data_governance.json` 及 `ci/` 工具脚本，请按环境复制后再修改敏感项。

## 构建、测试与开发命令
- `docker-compose up -d` 启动应用与 MySQL 依赖，`docker-compose logs -f app` 可实时查看接口输出，结束时使用 `docker-compose down` 释放资源。
- `make sync` 依次执行镜像构建、容器自检与同步核对，是提交前的推荐流程；`make verify` 用于批次清洗验证，`make clean` 可清除缓存镜像。
- `poetry install` 安装依赖，`poetry run uvicorn app.main:app --reload --port 8000` 支持本地热重载；脚本调试可用 `poetry run python scripts/simple_batch_aggregation.py` 复现场景。
- `poetry run pytest` 执行全部测试，可配合 `-k` 或 `-m slow` 筛选场景；`poetry run black app/`、`isort app/`、`flake8 app/`、`mypy app/` 统一代码质量。

## 代码风格与命名约定
- 采用 Python 4 空格缩进与类型注解，业务函数保持动宾结构命名，如 `calculate_percentile`、`sync_batch_status`，并在复杂流程前添加简短约束注释。
- 模型与 Schema 使用单数名词，数据访问层遵循 `*Repository` 后缀；异步或批处理入口以 `*_task` 结尾，脚本文件采用蛇形命名并在顶部注明用法。
- 提交前运行 Black、Isort、Flake8 与 Mypy，保持 import 顺序、类型校验与静态分析一致；格式化会覆盖整目录，建议先提交工作进度再执行以便回滚。

## 测试指引
- 核心单元测试位于 `tests/`，新增模块需提供对应 `test_*.py`；根目录现有的 `test_aggregation_*.py` 和 `test_api_*.py` 可作为真实数据回归模板。
- 覆盖率以关键计算路径为优先，涉及批次或区域汇总时需在 `test_statistical_metrics_integration.py` 或相邻文件添加集成场景并附带模拟数据。
- 使用远程数据库动作需打桩或改用本地 Docker MySQL，CI 默认在无网络环境执行，推荐利用 fixtures 生成内存数据或 CSV 样例。

## 提交与 PR 规范
- Git 历史使用 `feat|fix|chore: 摘要` 形式，若对应需求请追加 `Issue #编号` 并保持现在时语态；必要时补充中英文摘要便于外部审阅。
- PR 描述需包含：变化摘要、验证步骤（命令或截图）、影响范围与风险评估；涉及接口变动请提供 Swagger 链接或响应样例 JSON。
- 合并前确认 Makefile 与 Poetry 检查全部通过，在描述中勾选完成的测试、文档同步与回滚方案，避免压缩或日志残留在提交中。

## 架构与调试提示
- 请求流自 `api` 进入 `services`，随后调用 `calculation` 与 `database` 模块；必要时可在 `middleware/` 中扩展日志以定位性能瓶颈。
- 数据质量排查可先运行 `scripts/` 下的诊断脚本，再在 `tests` 内编写最小复现；若遇到学科接口异常，可参考 `app/subjects_api_main.py` 提供的只读路由。
- 监控与安全基线请通过环境变量控制，敏感凭据存放于未提交的 `.env` 文件或 CI 密钥库，务必核对仓库根目录的大文件与打包产物是否列入 `.gitignore`。
