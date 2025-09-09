# 学业发展质量监测统计分析服务

这是一个基于 FastAPI + SQLAlchemy 2.0 + MySQL 的教育统计分析服务，专门处理学业发展质量监测数据。

## 项目概述

该项目采用**面向服务的单体 (Service-Oriented Monolith)** 架构，提供学生答题数据的多维度统计分析功能，包括教育统计指标计算、区域级和学校级数据汇聚、批次任务管理和标准化JSON数据输出。

## 核心特性

- **教育统计指标**: 难度系数、区分度、百分位数、等级分布
- **数据汇聚**: 支持区域级和学校级数据汇聚
- **任务管理**: 支持后台任务队列、进度跟踪和任务取消
- **标准化输出**: 生成标准化JSON数据用于前端报告系统

## 技术栈

- **语言**: Python 3.11+
- **Web框架**: FastAPI
- **数据处理**: Pandas, NumPy, SciPy
- **数据库ORM**: SQLAlchemy 2.0
- **依赖管理**: Poetry
- **本地环境**: Docker & Docker Compose

## 目录结构

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

## 快速开始

### 本地开发环境

1. **启动开发环境**
   ```bash
   docker-compose up -d
   ```
   
2. **访问服务**
   - API文档：http://localhost:8000/docs
   - 健康检查：http://localhost:8000/health

3. **非Docker开发**
   ```bash
   poetry install
   poetry run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

### 核心API接口

#### 管理API (`/api/v1/management`)
- 批次管理：创建、查询、更新、删除统计批次
- 任务管理：手动触发统计任务、查询任务状态和进度

#### 报告API (`/api/v1/reporting`)
- 区域级统计报告：平均分、难度、区分度、等级分布
- 学校级对标分析：排名、百分位数、标准差等

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

### 远程数据库连接
- **主机**: 117.72.14.166
- **端口**: 23506  
- **用户名**: root
- **数据库**: appraisal_test

### 环境变量
```bash
DATABASE_URL=mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test
```

## 开发规范

- 遵循PEP 8代码规范
- 使用类型注解（mypy检查）
- 单元测试覆盖率要求>90%
- 教育统计算法必须与标准软件验证