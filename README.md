# 学业发展质量监测统计分析服务

一个基于FastAPI + SQLAlchemy 2.0 + MySQL的教育统计分析服务，专门处理学业发展质量监测数据。

## 项目简介

该服务采用**面向服务的单体 (Service-Oriented Monolith)** 架构，提供学生答题数据的多维度统计分析功能，包括：

- 教育统计指标计算：难度系数、区分度、百分位数、等级分布
- 区域级和学校级数据汇聚
- 批次任务管理和进度跟踪
- 标准化JSON数据输出用于前端报告系统

## 技术栈

- **语言**: Python 3.11+
- **Web框架**: FastAPI
- **数据处理**: Pandas, NumPy, SciPy
- **数据库ORM**: SQLAlchemy 2.0
- **依赖管理**: Poetry
- **本地环境**: Docker & Docker Compose

## 快速开始

### 前提条件

确保您已安装以下软件：
- [Docker Desktop](https://www.docker.com/products/docker-desktop)

### 本地开发环境

1. **克隆项目并进入目录**
   ```bash
   git clone <repository-url>
   cd ConvergedComputing
   ```

2. **启动开发环境**
   ```bash
   docker-compose up -d
   ```
   
   这将启动：
   - FastAPI应用服务（端口：8000）
   - MySQL 8.4.6 数据库（端口：3307，避免与本地MySQL冲突）

3. **访问服务**
   - API文档：http://localhost:8000/docs
   - ReDoc文档：http://localhost:8000/redoc
   - 健康检查：http://localhost:8000/health

4. **开发调试**
   - 直接修改 `/app` 目录下的Python代码
   - FastAPI服务会自动热重载
   - 使用Postman等工具进行API测试

5. **停止环境**
   ```bash
   docker-compose down
   ```

### 非Docker开发（可选）

如果你希望在本地直接运行：

1. **安装依赖**
   ```bash
   poetry install
   ```

2. **启动服务**
   ```bash
   poetry run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

3. **代码质量检查**
   ```bash
   # 格式化代码
   poetry run black app/
   poetry run isort app/
   
   # 代码检查
   poetry run flake8 app/
   poetry run mypy app/
   
   # 运行测试
   poetry run pytest
   ```

## 项目结构

```
/app
├── main.py                 # FastAPI应用主入口
├── /api                    # API路由层
│   ├── management_api.py   # 管理API路由
│   └── reporting_api.py    # 报告API路由
├── /services               # 业务逻辑层
│   ├── batch_service.py    # 批次管理服务
│   ├── task_service.py     # 任务管理服务
│   └── reporting_service.py # 报告生成服务
├── /calculation            # 统计计算引擎
│   ├── engine.py           # 核心计算引擎
│   └── formulas.py         # 教育统计公式实现
├── /database               # 数据访问层
│   ├── models.py           # SQLAlchemy模型定义
│   ├── repositories.py     # 数据仓库，封装CRUD操作
│   └── connection.py       # 数据库连接配置
└── /schemas                # Pydantic数据模型
    ├── request_schemas.py  # API请求体验证模型
    └── response_schemas.py # API响应体模型
```

## 核心API接口

### 管理API (`/api/v1/management`)
- 批次管理：创建、查询、更新、删除统计批次
- 任务管理：手动触发统计任务、查询任务状态和进度
- 系统监控：健康检查、系统状态

### 报告API (`/api/v1/reporting`)
- 区域级统计报告：平均分、难度、区分度、等级分布
- 学校级对标分析：排名、百分位数、标准差等
- 多维度统计数据：基于知识点、能力等维度的分析

## 教育统计特性

### 关键算法
- **百分位数计算**: 使用`floor(student_count × percentile)`算法
- **区分度计算**: 前27%/后27%分组的教育统计标准
- **难度系数**: 平均分与满分的比值
- **等级分布**: 基于年级的差异化阈值

### 年级分类
- **小学**(1-6th_grade): 优秀≥90, 良好80-89, 及格60-79, 不及格<60
- **初中**(7-9th_grade): A≥85, B70-84, C60-69, D<60

## 数据库配置

### 环境变量
```bash
DATABASE_URL=mysql+pymysql://user:password@db:3306/appraisal_test
```

### 核心表结构
- `student_score_detail`: 学生答题明细数据
- `subject_question_config`: 题目配置和满分信息
- `question_dimension_mapping`: 题目维度映射
- `grade_aggregation_main`: 年级汇总信息

## 开发规范

- 遵循PEP 8代码规范
- 使用类型注解（mypy检查）
- 单元测试覆盖率要求>90%
- 教育统计算法必须与标准软件验证

## 项目管理

此项目集成了Claude Code PM系统，支持：
- PRD驱动的开发流程：`/pm:prd-new`
- GitHub Issues任务管理：`/pm:issue-start <id>`
- 并行开发工作流：`/pm:epic-start`

### PM常用命令
```bash
/pm:status          # 查看项目状态
/pm:next            # 获取下一个优先任务
/pm:issue-start <id> # 开始特定任务
```

## 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

## 贡献指南

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 支持

如有问题，请通过GitHub Issues提出。