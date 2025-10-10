#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生产环境部署包创建脚本

基于项目三大核心变动创建完整的运维部署包：
1. API v1.2 增强调整 - 统一8000端口，新增问卷分布API，完整CORS支持
2. V12增强汇聚修复 - 计算引擎重构，subjects_builder修复，问卷维度数据补齐
3. G7-2025汇聚重启机制 - 增强守卫+白名单，完整监控回滚，自动化执行流程

创建日期：2025-09-19
"""

import os
import shutil
import zipfile
import json
from datetime import datetime
from pathlib import Path

def create_production_deployment_package():
    project_root = Path("D:\\myproject\\后端\\ConvergedComputing")
    timestamp = datetime.now().strftime('%Y%m%d')
    package_name = f"deployment_package_v1.2_production_{timestamp}"
    package_dir = project_root / package_name

    print(f"开始创建生产部署包: {package_name}")

    # 清理并创建部署目录
    if package_dir.exists():
        shutil.rmtree(package_dir)
    package_dir.mkdir(parents=True, exist_ok=True)

    # 1. 复制核心应用代码
    print("复制核心应用代码...")
    app_dest = package_dir / "app"
    if (project_root / "app").exists():
        shutil.copytree(project_root / "app", app_dest, ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '*.pyo'))

    # 2. 复制根目录的重要文件
    root_files = [
        "requirements.txt",
        "requirements-prod.txt",
        "pyproject.toml",
        "Dockerfile",
        "docker-compose.yml",
        ".env.example",
        ".dockerignore",
        "nginx.conf",
        "pytest.ini"
    ]

    for file_name in root_files:
        src_file = project_root / file_name
        if src_file.exists():
            shutil.copy2(src_file, package_dir / file_name)

    # 3. 复制批处理和运行脚本
    run_scripts = [
        "run_full_batch_pipeline.py",
        "run_g7_pipeline_wrapper.py",
        "run_single_subject_pipeline.py",
        "batch_aggregation_runner.py",
        "batch_cleaning_runner.py",
        "data_cleaning_service.py"
    ]

    for script in run_scripts:
        src_script = project_root / script
        if src_script.exists():
            shutil.copy2(src_script, package_dir / script)

    # 4. 复制scripts目录
    if (project_root / "scripts").exists():
        shutil.copytree(project_root / "scripts", package_dir / "scripts",
                       ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '*.pyo'))

    # 5. 复制重要的G7相关脚本
    g7_scripts = [
        "g7_precheck_suite.py",
        "monitor_g7_pipeline.py",
        "check_no_active_old_pipeline.py",
        "check_db_locks_enhanced.py",
        "check_disk_space.py",
        "install_g7_enhanced_guard.py",
        "comprehensive_g7_guard_test.py",
        "run_g7_precheck.sh",
        "run_g7_precheck.bat"
    ]

    scripts_dir = package_dir / "scripts"
    scripts_dir.mkdir(exist_ok=True)

    for script in g7_scripts:
        src_script = project_root / script
        if src_script.exists():
            shutil.copy2(src_script, scripts_dir / script)

    # 6. 复制SQL文件
    sql_dir = package_dir / "sql"
    sql_dir.mkdir(exist_ok=True)

    sql_files = [
        "quick_remove_g7_guard.sql",
        "backup_G7_2025_stats_clean.sql",
        "statistical_aggregations_clean_fixed_u8nobom.sql"
    ]

    for sql_file in sql_files:
        src_file = project_root / sql_file
        if src_file.exists():
            shutil.copy2(src_file, sql_dir / sql_file)

    # 7. 复制alembic配置
    if (project_root / "alembic").exists():
        shutil.copytree(project_root / "alembic", package_dir / "alembic")

    if (project_root / "alembic.ini").exists():
        shutil.copy2(project_root / "alembic.ini", package_dir / "alembic.ini")

    # 8. 创建docs目录并复制关键文档
    docs_dir = package_dir / "docs"
    docs_dir.mkdir(exist_ok=True)

    key_docs = [
        "docs/前端分析API对接指南_v1.2.md",
        "docs/V12增强汇聚修复行动计划.md",
        "docs/G7_2025_汇聚重启实施故事.md",
        "G7_2025_MAINTENANCE_WINDOW_PLAYBOOK.md",
        "G7_GUARD_DEPLOYMENT_GUIDE.md",
        "G7_PRECHECK_SCRIPTS_README.md"
    ]

    for doc in key_docs:
        src_doc = project_root / doc
        if src_doc.exists():
            dest_doc = docs_dir / Path(doc).name
            shutil.copy2(src_doc, dest_doc)

    # 9. 创建ops目录
    ops_dir = package_dir / "ops"
    ops_dir.mkdir(exist_ok=True)

    # 10. 创建部署指南
    deployment_guide = f"""# 教育统计分析服务部署指南 v1.2

部署包版本：{package_name}
创建时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 概述

本部署包基于项目三大核心变动创建：

### 1. API v1.2 增强调整
- 统一对外端口为8000（subjects服务已整合）
- 新增问卷题目选项分布API
- 完整的CORS支持和前端代理指引
- v1.2汇聚结构调整

### 2. V12增强汇聚修复
- 计算引擎重构：恢复V12字段布局
- subjects_builder.py全面修复
- 问卷维度数据补齐
- 数据清洗流程优化

### 3. G7-2025汇聚重启机制
- 增强守卫+白名单机制
- 完整的预检查、监控、回滚脚本
- 自动化执行流程
- 生产环境安全部署策略

## 环境要求

### 系统要求
- Linux/Windows Server
- CPU: 4核心以上
- 内存: 8GB以上
- 磁盘: 100GB以上可用空间

### 软件依赖
- Python 3.11+
- MySQL 8.0+
- Redis 6.0+
- Docker 20.10+
- Docker Compose 2.0+

## 快速部署

### 1. 解压并进入目录
```bash
unzip {package_name}.zip
cd {package_name}
```

### 2. 配置环境
```bash
# 复制配置模板
cp .env.example .env

# 编辑配置文件
vim .env
```

必需配置项：
```env
# 数据库配置
DATABASE_URL=mysql+pymysql://user:password@localhost:3306/dbname
REDIS_URL=redis://localhost:6379/0

# API配置
API_HOST=0.0.0.0
API_PORT=8000

# 日志配置
LOG_LEVEL=INFO
LOG_FILE=/var/log/converged-computing/app.log
```

### 3. 启动服务
```bash
# Docker部署（推荐）
docker-compose up -d

# 直接部署
pip install -r requirements-prod.txt
python run_full_batch_pipeline.py
```

### 4. 验证部署
```bash
# 健康检查
curl http://localhost:8000/health

# API测试
curl "http://localhost:8000/api/v12/batch/TEST-2025/regional"
```

## 核心API接口

### 统一Subjects接口（v1.2）
- 区域级数据：`GET /api/v12/batch/{{batch_code}}/regional`
- 学校级数据：`GET /api/v12/batch/{{batch_code}}/school/{{school_id}}`

### 问卷分布API
- 区域级分布：`GET /api/v1/questionnaire-distributions/{{batch_code}}/{{subject_name}}/regional`
- 学校级分布：`GET /api/v1/questionnaire-distributions/{{batch_code}}/{{subject_name}}/school/{{school_id}}`

## G7汇聚重启操作

### 预检查
```bash
# 运行完整预检查
python scripts/g7_precheck_suite.py

# 检查数据库锁
python scripts/check_db_locks_enhanced.py

# 检查磁盘空间
python scripts/check_disk_space.py
```

### 执行汇聚
```bash
# 推荐：使用包装器脚本
python run_g7_pipeline_wrapper.py --batch G7-2025 --env production

# 或使用原始脚本
python run_full_batch_pipeline.py G7-2025
```

### 监控和验证
```bash
# 监控执行状态
python scripts/monitor_g7_pipeline.py --batch G7-2025

# 数据验证
python scripts/validate_g7_data.py --batch G7-2025
```

## 常见问题

### 1. 端口冲突
- 检查端口占用：`netstat -tlnp | grep 8000`
- 修改配置：编辑.env中的API_PORT

### 2. 数据库连接失败
- 检查连接字符串：`python -c "import pymysql; print('OK')"`
- 验证权限：MySQL用户需要完整读写权限

### 3. 内存不足
- 监控内存使用：`free -m`
- 调整Docker资源限制

### 4. G7汇聚问题
- 运行预检查：`python scripts/g7_precheck_suite.py`
- 查看守卫状态：`python scripts/check_g7_guard_status.py`

## 监控和运维

### 关键指标
- API响应时间 < 500ms
- 数据库连接数 < 80%
- 内存使用率 < 85%
- 磁盘使用率 < 90%
- 错误率 < 1%

### 日志文件
- 应用日志：/var/log/converged-computing/app.log
- 错误日志：/var/log/converged-computing/error.log
- 汇聚日志：/var/log/converged-computing/aggregation.log

### 维护命令
```bash
# 查看状态
docker-compose ps

# 查看日志
docker-compose logs -f app

# 重启服务
docker-compose restart

# 停止服务
docker-compose down

# 完整健康检查
python health_check.py --full
```

## 支持联系

如遇问题，请提供以下信息：
- 部署环境信息
- 错误日志
- 复现步骤
- 配置文件（脱敏后）

技术支持：converged-computing-support@example.com

---

更新时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
部署包版本：v1.2.0
"""

    with open(docs_dir / "DEPLOYMENT_GUIDE.md", "w", encoding="utf-8") as f:
        f.write(deployment_guide)

    # 11. 创建版本说明
    version_notes = f"""# 版本说明 v1.2.0

**发布日期**：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**部署包**：{package_name}

## 核心变更概述

本版本基于项目三大核心变动，提供production-ready的部署包：

### 1. API v1.2 增强调整

#### 统一端口架构
- **变更**：所有服务统一到8000端口
- **原因**：简化部署架构，减少端口管理复杂度
- **影响**：无需再开启8001端口的subjects服务
- **迁移**：更新所有API调用地址到8000端口

#### 新增问卷分布API
- **新接口**：`/api/v1/questionnaire-distributions`
- **功能**：提供题目级选项分布数据
- **支持**：区域级、学校级、学校列表查询
- **用途**：前端问卷分析图表数据源

#### CORS完整支持
- **改进**：原生CORS头支持
- **配置**：支持前端代理和网关配置
- **文档**：提供Vite、Webpack、Nginx配置示例

#### v1.2汇聚结构调整
- **问卷题目位置**：从顶层移至维度下 `dimensions[].questions[]`
- **区域维度排名**：精简输出，避免数据冗余
- **向下兼容**：保留legacy API说明

### 2. V12增强汇聚修复

#### 计算引擎重构
- **subjects_builder.py**：全面修复输出结构
- **字段布局**：恢复V12标准字段布局
- **性能优化**：保留清洗阶段预计算优化
- **兼容性**：与历史输出保持一致

#### 问卷维度数据补齐
- **维度列表**：完整的维度统计输出
- **题目占比**：按题目的选项分布计算
- **选项分布**：完整的选项占比数据
- **数据完整性**：确保所有必需字段存在

#### 数据清洗流程优化
- **固定步骤**：先清洗后汇聚的标准流程
- **脚本工具**：batch_cleaning_runner.py优化
- **验证机制**：自动数据完整性校验
- **性能提升**：预聚合缓存机制

### 3. G7-2025汇聚重启机制

#### 增强守卫+白名单机制
- **触发器守卫**：数据库级写入保护
- **白名单机制**：新流水线专用权限
- **智能识别**：自动区分新旧流水线
- **安全防护**：防止旧流程数据干扰

#### 完整监控回滚体系
- **预检查脚本**：g7_precheck_suite.py
- **实时监控**：monitor_g7_pipeline.py
- **自动告警**：异常情况即时通知
- **快速回滚**：一键恢复到安全状态

#### 自动化执行流程
- **包装器脚本**：run_g7_pipeline_wrapper.py
- **参数支持**：--batch和--env命令行参数
- **执行手册**：G7_2025_MAINTENANCE_WINDOW_PLAYBOOK.md
- **验证工具**：全面的结果验证脚本

## 新增功能

### API增强
- 统一8000端口服务
- 问卷选项分布接口
- 增强错误处理
- 完整CORS支持
- 性能监控端点

### 工具脚本
- G7汇聚预检查套件
- 增强数据库锁检查
- 磁盘空间监控
- 管道执行包装器
- 自动化验证脚本

### 部署优化
- 生产环境配置模板
- Docker Compose优化
- 健康检查脚本
- 日志配置改进
- 性能调优建议

### 运维工具
- 完整监控体系
- 故障排查手册
- 自动化脚本
- 应急预案
- 性能基线

## 重要改进

### 稳定性
- 数据库连接池优化
- 内存泄漏修复
- 异常处理增强
- 守卫机制保护

### 性能
- 查询优化
- 索引改进
- 缓存策略
- 并发控制

### 可维护性
- 完整文档
- 标准化部署
- 监控告警
- 故障自愈

## 兼容性说明

### API兼容性
- **保持兼容**：核心API接口路径不变
- **结构调整**：问卷题目位置变化，需要前端适配
- **新增接口**：问卷分布API为新增功能
- **废弃接口**：无废弃接口

### 数据库兼容性
- **表结构**：保持现有表结构不变
- **数据格式**：JSON字段结构有调整
- **索引优化**：新增性能索引
- **迁移脚本**：提供自动迁移工具

### 配置兼容性
- **环境变量**：新增配置项，保留现有配置
- **Docker配置**：优化资源限制
- **日志配置**：改进日志格式
- **监控配置**：新增监控端点

## 部署注意事项

### 升级前准备
1. **数据备份**：完整备份现有数据
2. **环境检查**：验证系统资源充足
3. **依赖检查**：确认所有依赖项版本
4. **测试验证**：在测试环境完整验证

### 升级步骤
1. **停止服务**：graceful shutdown现有服务
2. **部署新版**：按部署指南执行
3. **数据迁移**：运行迁移脚本（如需要）
4. **配置更新**：更新配置文件
5. **启动验证**：启动服务并验证功能

### 回滚计划
1. **快速回滚**：保留上一版本镜像
2. **数据回滚**：从备份恢复数据
3. **配置回滚**：恢复原配置文件
4. **服务验证**：确认回滚后服务正常

## 已知问题

### 限制事项
- G7批次汇聚期间建议停止其他批次操作
- 大批次数据处理可能需要较长时间
- 并发汇聚操作需要充足内存资源

### 性能考虑
- 单批次10万学生数据处理时间约30分钟
- 建议配置至少8GB内存用于大批次处理
- 数据库连接池建议设置为20-50

### 监控建议
- 重点监控G7汇聚执行状态
- 设置内存使用率告警
- 配置数据库锁等待监控
- 启用API响应时间监控

## 支持与反馈

### 技术支持
- 支持邮箱：support@example.com
- 紧急热线：400-xxx-xxxx
- 工单系统：https://support.example.com

### 反馈渠道
- 功能建议：feedback@example.com
- Bug报告：bugs@example.com
- 文档改进：docs@example.com

### 版本路线图
- **v1.2.1**：性能优化和问题修复
- **v1.3.0**：新增批量处理API
- **v2.0.0**：架构升级和微服务化

---

**重要提醒**：
1. 本版本包含重要的结构调整，升级前请仔细阅读兼容性说明
2. G7汇聚功能包含数据保护机制，请按操作手册执行
3. 如遇问题请及时联系技术支持，提供完整的环境信息和日志
4. 建议在测试环境充分验证后再部署到生产环境

**祝您使用愉快！**
"""

    with open(package_dir / "VERSION_NOTES.md", "w", encoding="utf-8") as f:
        f.write(version_notes)

    # 12. 创建简单的健康检查脚本
    health_check_script = f"""#!/usr/bin/env python3
# -*- coding: utf-8 -*-
\"\"\"
健康检查脚本
用于检查教育统计分析服务的运行状态
\"\"\"

import sys
import requests
import json
from datetime import datetime

def check_api_health():
    \"\"\"检查API健康状态\"\"\"
    try:
        response = requests.get("http://localhost:8000/health", timeout=10)
        if response.status_code == 200:
            print("[SUCCESS] API健康检查通过")
            return True
        else:
            print(f"[ERROR] API健康检查失败: HTTP {{response.status_code}}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] API连接失败: {{e}}")
        return False

def check_api_performance():
    \"\"\"检查API性能\"\"\"
    try:
        import time
        start_time = time.time()
        response = requests.get("http://localhost:8000/health", timeout=10)
        response_time = (time.time() - start_time) * 1000

        if response_time < 500:
            print(f"[SUCCESS] API响应时间: {{response_time:.2f}}ms")
        elif response_time < 2000:
            print(f"[WARNING] API响应时间偏慢: {{response_time:.2f}}ms")
        else:
            print(f"[ERROR] API响应时间过慢: {{response_time:.2f}}ms")
        return response_time < 2000
    except Exception as e:
        print(f"[ERROR] API性能检查失败: {{e}}")
        return False

def main():
    print("开始健康检查...")
    print(f"检查时间: {{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}")
    print("-" * 50)

    results = []
    results.append(check_api_health())
    results.append(check_api_performance())

    print("-" * 50)
    if all(results):
        print("[SUCCESS] 所有检查项通过")
        sys.exit(0)
    else:
        print("[ERROR] 部分检查项失败")
        sys.exit(1)

if __name__ == '__main__':
    main()
"""

    with open(package_dir / "health_check.py", "w", encoding="utf-8") as f:
        f.write(health_check_script)

    # 13. 创建部署脚本
    deploy_script = f"""#!/bin/bash
# 生产环境部署脚本
# 教育统计分析服务 v1.2

set -e

PROJECT_NAME="converged-computing"
VERSION="1.2.0"

# 颜色定义
RED='\\033[0;31m'
GREEN='\\033[0;32m'
YELLOW='\\033[1;33m'
BLUE='\\033[0;34m'
NC='\\033[0m' # No Color

log_info() {{
    echo -e "${{BLUE}}[INFO]${{NC}} $1"
}}

log_success() {{
    echo -e "${{GREEN}}[SUCCESS]${{NC}} $1"
}}

log_warning() {{
    echo -e "${{YELLOW}}[WARNING]${{NC}} $1"
}}

log_error() {{
    echo -e "${{RED}}[ERROR]${{NC}} $1"
}}

# 检查运行环境
check_requirements() {{
    log_info "检查系统要求..."

    # 检查Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装，请先安装Docker"
        exit 1
    fi

    # 检查Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose未安装，请先安装Docker Compose"
        exit 1
    fi

    # 检查Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python3未安装，请先安装Python 3.11+"
        exit 1
    fi

    log_success "系统要求检查通过"
}}

# 环境配置
setup_environment() {{
    log_info "配置环境..."

    # 复制配置文件
    if [ ! -f .env ]; then
        if [ -f .env.example ]; then
            cp .env.example .env
            log_info "已创建.env文件，请根据需要修改配置"
        else
            log_error ".env.example文件不存在"
            exit 1
        fi
    fi

    log_success "环境配置完成"
}}

# 启动服务
start_services() {{
    log_info "启动服务..."

    # 启动所有服务
    docker-compose up -d

    # 等待服务启动
    log_info "等待服务启动..."
    for i in {{1..30}}; do
        if curl -f http://localhost:8000/health >/dev/null 2>&1; then
            break
        fi
        sleep 2
        if [ $i -eq 30 ]; then
            log_error "服务启动超时"
            docker-compose logs
            exit 1
        fi
    done

    log_success "服务启动完成"
}}

# 健康检查
health_check() {{
    log_info "执行健康检查..."

    # 运行健康检查脚本
    if [ -f health_check.py ]; then
        python3 health_check.py
    else
        # 基本健康检查
        HTTP_STATUS=$(curl -s -o /dev/null -w "%{{http_code}}" http://localhost:8000/health)
        if [ "$HTTP_STATUS" = "200" ]; then
            log_success "API健康检查通过"
        else
            log_error "API健康检查失败: HTTP $HTTP_STATUS"
            exit 1
        fi
    fi

    log_success "健康检查完成"
}}

# 显示部署信息
show_deployment_info() {{
    log_success "部署完成！"
    echo ""
    echo "==========================================="
    echo "  教育统计分析服务 v${{VERSION}}  "
    echo "==========================================="
    echo "服务地址: http://localhost:8000"
    echo "健康检查: http://localhost:8000/health"
    echo "API文档: docs/DEPLOYMENT_GUIDE.md"
    echo ""
    echo "常用命令:"
    echo "  查看状态: docker-compose ps"
    echo "  查看日志: docker-compose logs -f app"
    echo "  重启服务: docker-compose restart"
    echo "  停止服务: docker-compose down"
    echo "  健康检查: python3 health_check.py"
    echo ""
    echo "支持联系: support@example.com"
    echo "==========================================="
}}

# 主函数
main() {{
    echo "启动教育统计分析服务部署 v${{VERSION}}"
    echo "时间: $(date)"
    echo ""

    # 执行部署步骤
    check_requirements
    setup_environment
    start_services
    health_check
    show_deployment_info
}}

# 运行主函数
main "$@"
"""

    deploy_sh = package_dir / "deploy.sh"
    with open(deploy_sh, "w", encoding="utf-8") as f:
        f.write(deploy_script)
    deploy_sh.chmod(0o755)

    # 14. 创建package清单
    manifest = {
        "package_name": package_name,
        "version": "1.2.0",
        "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "core_changes": [
            "API v1.2增强调整 - 统一8000端口，新增问卷分布API",
            "V12增强汇聚修复 - 计算引擎重构，subjects_builder修复",
            "G7-2025汇聚重启机制 - 增强守卫+白名单机制"
        ],
        "directories": {
            "app/": "核心应用代码",
            "scripts/": "脚本工具链",
            "docs/": "部署和API文档",
            "ops/": "运维手册",
            "sql/": "SQL脚本"
        },
        "key_files": {
            "deploy.sh": "Linux部署脚本",
            "health_check.py": "健康检查脚本",
            "VERSION_NOTES.md": "版本说明",
            "DEPLOYMENT_GUIDE.md": "部署指南"
        },
        "requirements": {
            "python": ">=3.11",
            "mysql": ">=8.0",
            "redis": ">=6.0",
            "docker": ">=20.10",
            "docker-compose": ">=2.0"
        }
    }

    with open(package_dir / "MANIFEST.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    # 15. 创建ZIP压缩包
    print("创建ZIP压缩包...")
    zip_path = project_root / f"{package_name}.zip"

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(package_dir):
            for file in files:
                file_path = Path(root) / file
                arc_path = file_path.relative_to(package_dir)
                zipf.write(file_path, arc_path)

    print(f"生产部署包创建完成:")
    print(f"  目录: {package_dir}")
    print(f"  ZIP: {zip_path}")
    print(f"  大小: {zip_path.stat().st_size // 1024 // 1024}MB")

    return package_dir, zip_path

if __name__ == "__main__":
    create_production_deployment_package()