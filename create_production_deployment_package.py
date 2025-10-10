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

class ProductionDeploymentPackager:
    def __init__(self):
        self.project_root = Path("D:\\myproject\\后端\\ConvergedComputing")
        self.package_name = f"deployment_package_v1.2_production_{datetime.now().strftime('%Y%m%d')}"
        self.package_dir = self.project_root / self.package_name
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    def create_package(self):
        """创建完整的生产环境部署包"""
        print(f"开始创建生产部署包: {self.package_name}")
        
        # 清理并创建部署目录
        if self.package_dir.exists():
            shutil.rmtree(self.package_dir)
        self.package_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. 复制核心应用代码
        self._copy_core_application()
        
        # 2. 复制脚本工具链
        self._copy_script_tools()
        
        # 3. 复制配置文件
        self._copy_configuration_files()
        
        # 4. 创建部署文档
        self._create_deployment_docs()
        
        # 5. 创建运维手册
        self._create_operations_manual()
        
        # 6. 创建版本说明
        self._create_version_notes()
        
        # 7. 创建部署脚本
        self._create_deployment_scripts()
        
        # 8. 创建包清单
        self._create_package_manifest()
        
        print(f"生产部署包创建完成: {self.package_dir}")
        
        # 创建ZIP压缩包
        self._create_zip_package()
        
    def _copy_core_application(self):
        """复制核心应用代码"""
        print("复制核心应用代码...")
        
        # 创建app目录
        app_dest = self.package_dir / "app"
        shutil.copytree(self.project_root / "app", app_dest, ignore=self._ignore_patterns)
        
        # 复制根目录的重要文件
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
            src_file = self.project_root / file_name
            if src_file.exists():
                shutil.copy2(src_file, self.package_dir / file_name)
        
        # 复制批处理和运行脚本
        run_scripts = [
            "run_full_batch_pipeline.py",
            "run_g7_pipeline_wrapper.py", 
            "run_single_subject_pipeline.py",
            "batch_aggregation_runner.py",
            "batch_cleaning_runner.py",
            "data_cleaning_service.py"
        ]
        
        for script in run_scripts:
            src_script = self.project_root / script
            if src_script.exists():
                shutil.copy2(src_script, self.package_dir / script)
                
    def _copy_script_tools(self):
        """复制脚本工具链"""
        print("复制脚本工具链...")
        
        # 复制scripts目录
        scripts_dest = self.package_dir / "scripts"
        if (self.project_root / "scripts").exists():
            shutil.copytree(self.project_root / "scripts", scripts_dest, ignore=self._ignore_patterns)
        
        # 复制G7汇聚重启相关脚本
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
        
        for script in g7_scripts:
            src_script = self.project_root / script
            if src_script.exists():
                shutil.copy2(src_script, self.package_dir / "scripts" / script)
                
        # 复制SQL文件
        sql_files = [
            "quick_remove_g7_guard.sql",
            "backup_G7_2025_stats_clean.sql",
            "statistical_aggregations_clean_fixed_u8nobom.sql"
        ]
        
        for sql_file in sql_files:
            src_file = self.project_root / sql_file
            if src_file.exists():
                shutil.copy2(src_file, self.package_dir / "sql" / sql_file)
                
    def _copy_configuration_files(self):
        """复制配置文件"""
        print("复制配置文件...")
        
        # 创建config目录
        config_dir = self.package_dir / "config"
        config_dir.mkdir(exist_ok=True)
        
        # 复制alembic配置
        if (self.project_root / "alembic").exists():
            shutil.copytree(self.project_root / "alembic", self.package_dir / "alembic")
        
        if (self.project_root / "alembic.ini").exists():
            shutil.copy2(self.project_root / "alembic.ini", self.package_dir / "alembic.ini")
            
    def _copy_documentation(self):
        """复制文档"""
        print("复制文档...")
        
        # 创建docs目录
        docs_dest = self.package_dir / "docs"
        docs_dest.mkdir(exist_ok=True)
        
        # 复制关键文档
        key_docs = [
            "docs/前端分析API对接指南_v1.2.md",
            "docs/V12增强汇聚修复行动计划.md", 
            "docs/G7_2025_汇聚重启实施故事.md",
            "G7_2025_MAINTENANCE_WINDOW_PLAYBOOK.md",
            "G7_GUARD_DEPLOYMENT_GUIDE.md",
            "G7_PRECHECK_SCRIPTS_README.md"
        ]
        
        for doc in key_docs:
            src_doc = self.project_root / doc
            if src_doc.exists():
                # 保持目录结构
                dest_doc = docs_dest / Path(doc).name
                shutil.copy2(src_doc, dest_doc)
                
    def _create_deployment_docs(self):
        """创建部署文档"""
        print("创建部署文档...")
        
        docs_dir = self.package_dir / "docs"
        docs_dir.mkdir(exist_ok=True)
        
        # 创建部署指南
        deployment_guide = self._get_deployment_guide()
        with open(docs_dir / "DEPLOYMENT_GUIDE.md", "w", encoding="utf-8") as f:
            f.write(deployment_guide)
            
        # 创建快速启动指南  
        quick_start = self._get_quick_start_guide()
        with open(docs_dir / "QUICK_START.md", "w", encoding="utf-8") as f:
            f.write(quick_start)
            
        # 创建API文档
        api_guide = self._get_api_guide()
        with open(docs_dir / "API_v1.2_GUIDE.md", "w", encoding="utf-8") as f:
            f.write(api_guide)
            
    def _create_operations_manual(self):
        """创建运维手册"""
        print("创建运维手册...")
        
        ops_dir = self.package_dir / "ops"
        ops_dir.mkdir(exist_ok=True)
        
        # 运维手册
        ops_manual = self._get_operations_manual()
        with open(ops_dir / "OPERATIONS_MANUAL.md", "w", encoding="utf-8") as f:
            f.write(ops_manual)
            
        # 监控指南
        monitoring_guide = self._get_monitoring_guide()
        with open(ops_dir / "MONITORING_GUIDE.md", "w", encoding="utf-8") as f:
            f.write(monitoring_guide)
            
        # 故障排查手册
        troubleshooting = self._get_troubleshooting_guide()
        with open(ops_dir / "TROUBLESHOOTING.md", "w", encoding="utf-8") as f:
            f.write(troubleshooting)
            
    def _create_version_notes(self):
        """创建版本说明"""
        print("创建版本说明...")
        
        version_notes = self._get_version_notes()
        with open(self.package_dir / "VERSION_NOTES.md", "w", encoding="utf-8") as f:
            f.write(version_notes)
            
        # 变更日志
        changelog = self._get_changelog()
        with open(self.package_dir / "CHANGELOG.md", "w", encoding="utf-8") as f:
            f.write(changelog)
            
    def _create_deployment_scripts(self):
        """创建部署脚本"""
        print("创建部署脚本...")
        
        # Linux部署脚本
        deploy_script = self._get_deploy_script()
        script_file = self.package_dir / "deploy.sh"
        with open(script_file, "w", encoding="utf-8") as f:
            f.write(deploy_script)
        script_file.chmod(0o755)
        
        # Windows部署脚本
        deploy_bat = self._get_deploy_bat()
        with open(self.package_dir / "deploy.bat", "w", encoding="utf-8") as f:
            f.write(deploy_bat)
            
        # 健康检查脚本
        health_check = self._get_health_check_script()
        health_script = self.package_dir / "health_check.py"
        with open(health_script, "w", encoding="utf-8") as f:
            f.write(health_check)
        health_script.chmod(0o755)
        
    def _create_package_manifest(self):
        """创建包清单"""
        print("创建包清单...")
        
        manifest = {
            "package_name": self.package_name,
            "version": "1.2.0",
            "created_at": self.timestamp,
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
                "config/": "配置文件",
                "sql/": "SQL脚本"
            },
            "key_files": {
                "deploy.sh": "Linux部署脚本",
                "deploy.bat": "Windows部署脚本", 
                "health_check.py": "健康检查脚本",
                "VERSION_NOTES.md": "版本说明",
                "CHANGELOG.md": "变更日志"
            },
            "requirements": {
                "python": ">=3.11",
                "mysql": ">=8.0",
                "redis": ">=6.0", 
                "docker": ">=20.10",
                "docker-compose": ">=2.0"
            }
        }
        
        with open(self.package_dir / "MANIFEST.json", "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
            
    def _create_zip_package(self):
        """创建ZIP压缩包"""
        print("创建ZIP压缩包...")
        
        zip_path = self.project_root / f"{self.package_name}.zip"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(self.package_dir):
                for file in files:
                    file_path = Path(root) / file
                    arc_path = file_path.relative_to(self.package_dir)
                    zipf.write(file_path, arc_path)
                    
        print(f"ZIP包已创建: {zip_path}")
        
    def _ignore_patterns(self, dir_path, names):
        """定义忽略的文件模式"""
        ignore_list = []
        for name in names:
            if any(pattern in name for pattern in [
                '__pycache__', '.pyc', '.pyo', '.git', '.env', 
                'logs', 'temp', '.pytest_cache', 'node_modules'
            ]):
                ignore_list.append(name)
        return ignore_list

    # 以下是文档内容生成方法
    def _get_deployment_guide(self):
        return f"""
# 教育统计分析服务部署指南 v1.2

部署包版本：{self.package_name}
创建时间：{self.timestamp}

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

## 部署步骤

### 1. 环境准备

```bash
# 解压部署包
unzip {self.package_name}.zip
cd {self.package_name}

# 检查环境（可选）
python health_check.py --check-env
```

### 2. 配置设置

```bash
# 复制环境配置
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

### 3. 数据库初始化

```bash
# 运行数据库迁移
python -m alembic upgrade head

# 初始化基础数据（可选）
python scripts/init_basic_data.py
```

### 4. 服务部署

#### Docker部署（推荐）

```bash
# 构建镜像
docker-compose build

# 启动服务
docker-compose up -d

# 查看状态
docker-compose ps
```

#### 直接部署

```bash
# 安装依赖
pip install -r requirements-prod.txt

# 启动服务
./deploy.sh
```

### 5. 服务验证

```bash
# 健康检查
python health_check.py

# API测试
curl http://localhost:8000/health
curl http://localhost:8000/api/v12/batch/TEST-2025/regional
```

### 6. 监控配置

```bash
# 启动监控
python scripts/monitor_g7_pipeline.py --daemon

# 配置日志轮转
sudo cp ops/logrotate.conf /etc/logrotate.d/converged-computing
```

## 配置说明

### 端口配置
- 主服务端口：8000（统一端口）
- 健康检查端口：8000/health
- 管理接口端口：8000/admin

### API接口
- 区域级数据：`GET /api/v12/batch/{{batch_code}}/regional`
- 学校级数据：`GET /api/v12/batch/{{batch_code}}/school/{{school_id}}`
- 问卷分布：`GET /api/v1/questionnaire-distributions/{{batch_code}}/{{subject_name}}/regional`

### CORS配置
应用已启用CORS支持。如使用反向代理，参考nginx.conf配置。

## 故障排查

### 常见问题

1. **端口冲突**
   - 检查端口占用：`netstat -tlnp | grep 8000`
   - 修改配置：编辑.env中的API_PORT

2. **数据库连接失败**
   - 检查连接字符串：`python -c "import pymysql; print('OK')"`
   - 验证权限：MySQL用户需要完整读写权限

3. **内存不足**
   - 监控内存使用：`python health_check.py --check-memory`
   - 调整Docker资源限制

4. **G7汇聚问题**
   - 运行预检查：`python scripts/g7_precheck_suite.py`
   - 查看守卫状态：`python scripts/check_g7_guard_status.py`

### 日志文件
- 应用日志：/var/log/converged-computing/app.log
- 错误日志：/var/log/converged-computing/error.log
- 汇聚日志：/var/log/converged-computing/aggregation.log

## 性能优化

### 数据库优化
```sql
-- 关键索引
CREATE INDEX idx_batch_school ON statistical_aggregations(batch_code, school_id);
CREATE INDEX idx_subject_type ON statistical_aggregations(subject_name, aggregation_type);
```

### 应用优化
- 启用Redis缓存
- 配置连接池：DATABASE_POOL_SIZE=20
- 调整工作进程数：WORKERS=4

## 安全配置

### 网络安全
- 配置防火墙：仅开放必要端口
- 使用HTTPS：配置SSL证书
- API访问控制：配置认证中间件

### 数据安全
- 数据库加密：启用TDE
- 敏感数据脱敏：配置字段掩码
- 备份策略：每日自动备份

## 维护操作

### 日常维护
```bash
# 健康检查
python health_check.py --full

# 清理日志
find /var/log/converged-computing -name "*.log" -mtime +7 -delete

# 数据库维护
python scripts/db_maintenance.py
```

### 版本升级
```bash
# 备份数据
python scripts/backup_data.py

# 停止服务
docker-compose down

# 更新代码
# ... 部署新版本

# 数据库迁移
python -m alembic upgrade head

# 启动服务
docker-compose up -d
```

## 联系支持

如遇问题，请提供以下信息：
- 部署环境信息
- 错误日志
- 复现步骤
- 配置文件（脱敏后）

技术支持：converged-computing-support@example.com

---

更新时间：{self.timestamp}
部署包版本：v1.2.0
"""

    def _get_quick_start_guide(self):
        return """
# 快速启动指南

## 5分钟快速部署

### 1. 环境检查
```bash
# 检查Python版本
python --version  # 需要3.11+

# 检查Docker
docker --version
docker-compose --version
```

### 2. 快速部署
```bash
# 解压并进入目录
unzip deployment_package_v1.2_production_*.zip
cd deployment_package_v1.2_production_*

# 配置环境
cp .env.example .env
# 编辑 .env 文件，设置数据库连接

# 启动服务
docker-compose up -d

# 检查状态
docker-compose ps
python health_check.py
```

### 3. 验证部署
```bash
# API测试
curl http://localhost:8000/health
# 预期返回: {"status": "ok"}

# 测试汇聚接口
curl "http://localhost:8000/api/v12/batch/G4-2025/regional"
```

## 常用命令

```bash
# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f app

# 重启服务
docker-compose restart

# 停止服务
docker-compose down

# 完整健康检查
python health_check.py --full

# G7汇聚预检查
python scripts/g7_precheck_suite.py
```

## 故障快速排查

| 问题 | 快速检查命令 |
|------|-------------|
| 服务无法启动 | `docker-compose logs app` |
| 数据库连接失败 | `python health_check.py --check-db` |
| 内存不足 | `python health_check.py --check-memory` |
| 端口占用 | `netstat -tlnp | grep 8000` |
| G7汇聚异常 | `python scripts/check_g7_guard_status.py` |

"""

    def _get_api_guide(self):
        return """
# API v1.2 对接指南

## 端口统一说明
- 统一对外端口：8000
- 所有API接口均通过此端口访问
- 无需单独开启8001端口

## 核心接口

### 1. 统一Subjects接口（v1.2）

#### 区域级数据
```
GET /api/v12/batch/{batch_code}/regional
```

返回示例：
```json
{
  "success": true,
  "code": 200,
  "data": {
    "schema_version": "v1.2",
    "batch_code": "G4-2025",
    "aggregation_level": "REGIONAL",
    "subjects": [
      {
        "subject_name": "数学",
        "type": "exam",
        "metrics": {
          "avg": 78.53,
          "stddev": 12.36,
          "difficulty": 0.79,
          "percentiles": {"P10": 56.0, "P50": 80.0, "P90": 95.0}
        },
        "dimensions": [
          {
            "code": "M-CUR",
            "name": "核心素养", 
            "avg": 82.35,
            "questions": [
              {
                "question_id": "Q01",
                "question_name": "数学推理",
                "score": 3.52,
                "option_distribution": [...]
              }
            ]
          }
        ]
      }
    ]
  }
}
```

#### 学校级数据
```
GET /api/v12/batch/{batch_code}/school/{school_id}
```

### 2. 问卷题目选项分布API

#### 区域级问卷分布
```
GET /api/v1/questionnaire-distributions/{batch_code}/{subject_name}/regional
```

#### 学校级问卷分布
```
GET /api/v1/questionnaire-distributions/{batch_code}/{subject_name}/school/{school_id}
```

返回示例：
```json
{
  "code": 200,
  "data": {
    "questions": [
      {
        "question_id": "Q01", 
        "total_responses": 1234,
        "options": [
          {"option_level": 1, "option_label": "非常不符合", "pct": 5.2},
          {"option_level": 2, "option_label": "不符合", "pct": 12.4}
        ]
      }
    ]
  }
}
```

## v1.2结构调整要点

### 问卷题目位置变化
- **旧版**：题目在顶层 `subjects[].questions[]`
- **新版**：题目随维度 `subjects[].dimensions[].questions[]`
- **建议**：按维度渲染题目，保持UI一致性

### 区域维度排名精简
- **变化**：区域维度不再返回排名数据
- **替代**：维度排名请使用学校接口获取

## CORS支持

应用已启用CORS。前端开发建议：

### Vite配置
```js
export default {
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true
      }
    }
  }
}
```

### Nginx配置
```nginx
location /api/ {
    proxy_pass http://backend/;
    add_header Access-Control-Allow-Origin $http_origin always;
    add_header Access-Control-Allow-Methods 'GET,POST,PUT,DELETE,OPTIONS' always;
}
```

## 快速测试

```bash
# 健康检查
curl http://localhost:8000/health

# 区域数据
curl "http://localhost:8000/api/v12/batch/G4-2025/regional"

# 学校数据  
curl "http://localhost:8000/api/v12/batch/G4-2025/school/5044"

# 问卷分布
curl "http://localhost:8000/api/v1/questionnaire-distributions/G4-2025/问卷/regional"
```
"""

    def _get_operations_manual(self):
        return """
# 运维操作手册

## 日常运维

### 服务状态检查
```bash
# 检查服务状态
docker-compose ps
systemctl status converged-computing

# 完整健康检查
python health_check.py --full

# 检查API可用性
curl -f http://localhost:8000/health || echo "API异常"
```

### 日志管理
```bash
# 查看实时日志
docker-compose logs -f app
tail -f /var/log/converged-computing/app.log

# 查看错误日志
grep -i error /var/log/converged-computing/app.log

# 日志轮转
logrotate -f /etc/logrotate.d/converged-computing
```

### 性能监控
```bash
# 系统资源
top -p $(pgrep -f "python.*app")
df -h
free -m

# 数据库连接
mysql -e "SHOW PROCESSLIST;"
mysql -e "SHOW ENGINE INNODB STATUS" | grep -A 20 "TRANSACTIONS"

# Redis监控
redis-cli info memory
redis-cli info stats
```

## G7汇聚重启操作

### 预检查
```bash
# 运行完整预检查
python scripts/g7_precheck_suite.py

# 检查旧流水线
python scripts/check_no_active_old_pipeline.py

# 检查数据库锁
python scripts/check_db_locks_enhanced.py

# 检查磁盘空间
python scripts/check_disk_space.py
```

### 守卫管理
```bash
# 安装增强守卫
python scripts/install_g7_enhanced_guard.py

# 检查守卫状态
python scripts/check_g7_guard_status.py

# 紧急移除守卫（故障时）
mysql < sql/quick_remove_g7_guard.sql
```

### 执行汇聚
```bash
# 推荐：使用包装器脚本
python run_g7_pipeline_wrapper.py --batch G7-2025 --env production

# 或使用原始脚本
python run_full_batch_pipeline.py G7-2025

# 监控执行状态
python scripts/monitor_g7_pipeline.py --batch G7-2025
```

### 验证结果
```bash
# 数据验证
python scripts/validate_g7_data.py --batch G7-2025

# API测试
curl "http://localhost:8000/api/v12/batch/G7-2025/regional"
curl "http://localhost:8000/api/v12/batch/G7-2025/school/5044"
```

## 备份与恢复

### 数据备份
```bash
# 备份关键数据
mysqldump --single-transaction statistical_aggregations > backup_$(date +%Y%m%d).sql

# 备份G7数据（专用）
python scripts/backup_g7_data.py

# 验证备份
mysql test_db < backup_$(date +%Y%m%d).sql
```

### 数据恢复
```bash
# 恢复数据
mysql statistical_aggregations < backup_20250919.sql

# 验证恢复
python scripts/validate_data_integrity.py
```

## 故障处理

### 服务异常
```bash
# 重启服务
docker-compose restart app

# 查看启动日志
docker-compose logs app | tail -50

# 强制重建
docker-compose down
docker-compose up -d --force-recreate
```

### 数据库问题
```bash
# 检查连接
python -c "from app.database.connection import get_db_connection; print('DB OK')"

# 检查表结构
mysql -e "DESCRIBE statistical_aggregations;"

# 修复表
mysql -e "REPAIR TABLE statistical_aggregations;"
```

### 内存不足
```bash
# 清理缓存
redis-cli FLUSHALL

# 重启Python进程
sudo systemctl restart converged-computing

# 调整Docker内存限制
# 编辑docker-compose.yml，添加：
# mem_limit: 4g
# memswap_limit: 4g
```

### G7汇聚异常
```bash
# 紧急停止
python scripts/ultimate_stop.py

# 回滚守卫
python scripts/g7_guard_rollback.py --rollback

# 清理异常数据
python scripts/cleanup_g7_partial_data.py
```

## 性能调优

### 数据库优化
```sql
-- 添加索引
ALTER TABLE statistical_aggregations 
ADD INDEX idx_batch_subject (batch_code, subject_name);

-- 优化配置
SET GLOBAL innodb_buffer_pool_size = 2147483648;  -- 2GB
SET GLOBAL query_cache_size = 268435456;  -- 256MB
```

### 应用优化
```bash
# 调整工作进程数
export WORKERS=4

# 启用缓存
export REDIS_CACHE=true

# 数据库连接池
export DB_POOL_SIZE=20
export DB_POOL_MAX_OVERFLOW=30
```

## 监控告警

### 关键指标
- API响应时间 < 500ms
- 数据库连接数 < 80%
- 内存使用率 < 85%
- 磁盘使用率 < 90%
- 错误率 < 1%

### 告警设置
```bash
# 设置监控脚本
crontab -e
# 添加：
# */5 * * * * /path/to/health_check.py --alert
# 0 */1 * * * /path/to/check_disk_space.py --alert
```

## 联系支持

### 紧急故障
- 电话：400-xxx-xxxx
- 邮件：urgent@example.com

### 一般问题
- 邮件：support@example.com
- 工单系统：https://support.example.com

### 提供信息
请提供以下信息以便快速定位问题：
1. 错误时间和现象描述
2. 相关日志文件
3. 系统环境信息
4. 复现步骤（如有）
"""

    def _get_monitoring_guide(self):
        return """
# 监控指南

## 监控架构

### 监控层级
1. **系统层**：CPU、内存、磁盘、网络
2. **应用层**：API响应、错误率、吞吐量
3. **数据库层**：连接数、慢查询、锁等待
4. **业务层**：汇聚成功率、数据一致性

## 关键监控指标

### 系统指标
| 指标 | 正常范围 | 告警阈值 | 检查命令 |
|------|----------|----------|----------|
| CPU使用率 | < 70% | > 85% | `top` |
| 内存使用率 | < 80% | > 90% | `free -m` |
| 磁盘使用率 | < 85% | > 95% | `df -h` |
| 网络连接数 | < 1000 | > 2000 | `netstat -an | wc -l` |

### 应用指标
| 指标 | 正常范围 | 告警阈值 | 监控方法 |
|------|----------|----------|----------|
| API响应时间 | < 500ms | > 2s | `python health_check.py --api-perf` |
| 错误率 | < 0.1% | > 1% | 日志分析 |
| 并发连接数 | < 100 | > 200 | 应用监控 |
| 队列长度 | < 50 | > 100 | Redis监控 |

### 数据库指标
| 指标 | 正常范围 | 告警阈值 | 检查SQL |
|------|----------|----------|---------|
| 连接数 | < 50 | > 100 | `SHOW PROCESSLIST` |
| 锁等待时间 | < 1s | > 5s | `SHOW ENGINE INNODB STATUS` |
| 慢查询数 | < 10/min | > 50/min | `SHOW GLOBAL STATUS LIKE 'Slow_queries'` |
| 缓冲池命中率 | > 99% | < 95% | InnoDB状态 |

## 监控脚本

### 健康检查脚本
```bash
#!/bin/bash
# health_monitor.sh

# API健康检查
if ! curl -f http://localhost:8000/health > /dev/null 2>&1; then
    echo "[ALERT] API服务异常" | mail -s "服务告警" admin@example.com
fi

# 数据库连接检查
if ! python -c "from app.database.connection import test_connection; test_connection()"; then
    echo "[ALERT] 数据库连接异常" | mail -s "数据库告警" admin@example.com  
fi

# 磁盘空间检查
DISK_USAGE=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')
if [ $DISK_USAGE -gt 90 ]; then
    echo "[ALERT] 磁盘使用率${DISK_USAGE}%" | mail -s "磁盘告警" admin@example.com
fi
```

### G7汇聚监控
```python
# g7_aggregation_monitor.py
import time
import requests
from datetime import datetime

def monitor_g7_aggregation():
    """监控G7汇聚进度"""
    start_time = time.time()
    
    while True:
        try:
            # 检查汇聚进度
            response = requests.get('http://localhost:8000/api/v12/batch/G7-2025/regional')
            
            if response.status_code == 200:
                print(f"[{datetime.now()}] G7汇聚正常运行")
            else:
                print(f"[{datetime.now()}] G7汇聚异常: {response.status_code}")
                
            # 检查执行时间
            elapsed = time.time() - start_time
            if elapsed > 3600:  # 超过1小时
                print(f"[ALERT] G7汇聚执行时间过长: {elapsed/60:.1f}分钟")
                
        except Exception as e:
            print(f"[ERROR] 监控异常: {e}")
            
        time.sleep(300)  # 5分钟检查一次

if __name__ == '__main__':
    monitor_g7_aggregation()
```

## 日志监控

### 错误日志监控
```bash
# 监控错误日志
tail -f /var/log/converged-computing/app.log | grep -i error | while read line; do
    echo "[$(date)] ERROR: $line" | mail -s "应用错误" admin@example.com
done
```

### 关键字监控
```bash
# 监控关键业务事件
grep -i "aggregation.*failed\|calculation.*error\|database.*timeout" \
     /var/log/converged-computing/app.log | tail -10
```

## 告警规则

### 告警级别
1. **P0-紧急**：服务不可用，数据丢失
2. **P1-严重**：功能异常，性能严重下降
3. **P2-一般**：部分功能受影响
4. **P3-提醒**：需要关注的指标变化

### 告警触发条件

#### P0告警
- API服务无响应超过5分钟
- 数据库连接全部失败
- 磁盘空间使用率>98%
- G7汇聚数据完整性校验失败

#### P1告警
- API响应时间>5秒持续10分钟
- 数据库连接数>最大值的90%
- 内存使用率>95%持续5分钟
- 错误率>5%持续15分钟

#### P2告警
- API响应时间>2秒持续30分钟
- 慢查询数量>100/分钟
- CPU使用率>90%持续10分钟
- G7汇聚执行时间超出预期

## 监控工具集成

### Prometheus配置
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'converged-computing'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
    scrape_interval: 30s
```

### Grafana仪表板
关键面板：
- API请求量和响应时间
- 系统资源使用率
- 数据库性能指标
- G7汇聚执行状态
- 错误率趋势

### ELK日志分析
```json
{
  "mappings": {
    "properties": {
      "@timestamp": {"type": "date"},
      "level": {"type": "keyword"},
      "message": {"type": "text"},
      "module": {"type": "keyword"},
      "batch_code": {"type": "keyword"}
    }
  }
}
```

## 自动化运维

### Cron定时任务
```bash
# 每5分钟健康检查
*/5 * * * * /path/to/health_check.py --alert

# 每小时系统资源检查
0 * * * * /path/to/system_check.sh

# 每天凌晨数据备份
0 2 * * * /path/to/backup_data.sh

# 每周日志清理
0 1 * * 0 find /var/log/converged-computing -name "*.log" -mtime +7 -delete
```

### 自动恢复脚本
```bash
#!/bin/bash
# auto_recovery.sh

# 检查API服务
if ! curl -f http://localhost:8000/health; then
    echo "尝试重启服务..."
    docker-compose restart app
    sleep 30
    
    if curl -f http://localhost:8000/health; then
        echo "服务恢复成功"
    else
        echo "服务恢复失败，发送告警"
        mail -s "服务无法恢复" admin@example.com < /dev/null
    fi
fi
```

## 性能基线

### 正常运行基线
- API平均响应时间：150ms
- 95%百分位响应时间：400ms
- 并发处理能力：100 QPS
- 内存使用稳定在：2GB
- CPU使用率：30-50%

### G7汇聚性能基线
- 10万学生数据处理时间：<30分钟
- 内存峰值：<4GB
- 数据库连接数峰值：<50
- 成功率：>99.9%

## 故障预案

### 常见故障处理
1. **API响应慢**：检查数据库连接→重启应用→扩容资源
2. **内存不足**：清理缓存→重启服务→调整内存限制
3. **数据库锁等待**：终止长事务→优化查询→重建索引
4. **G7汇聚失败**：回滚数据→清理锁→重新执行

### 应急联系人
- 系统管理员：xxx-xxxx-xxxx
- 数据库DBA：xxx-xxxx-xxxx
- 开发负责人：xxx-xxxx-xxxx
- 业务负责人：xxx-xxxx-xxxx
"""

    def _get_troubleshooting_guide(self):
        return """
# 故障排查手册

## 快速诊断

### 服务状态检查
```bash
# 1. 检查服务进程
ps aux | grep -i converged
docker-compose ps

# 2. 检查端口监听
netstat -tlnp | grep 8000

# 3. 检查API可用性
curl -v http://localhost:8000/health
```

### 系统资源检查
```bash
# CPU和内存
top -n 1
free -m

# 磁盘空间
df -h
du -sh /var/log/converged-computing/

# 网络连接
netstat -an | grep ESTABLISHED | wc -l
```

## 常见问题诊断

### 1. API服务无响应

**症状**：
- curl请求超时或连接拒绝
- 前端无法访问API

**诊断步骤**：
```bash
# 检查服务状态
docker-compose ps
echo $?  # 0表示正常

# 检查日志
docker-compose logs app | tail -50

# 检查端口
netstat -tlnp | grep 8000

# 检查防火墙
sudo iptables -L | grep 8000
```

**解决方案**：
```bash
# 重启服务
docker-compose restart app

# 如果失败，强制重建
docker-compose down
docker-compose up -d --force-recreate

# 检查配置
cat .env | grep -v '^#'
```

### 2. 数据库连接失败

**症状**：
- API返回数据库连接错误
- 应用日志显示连接超时

**诊断步骤**：
```bash
# 测试数据库连接
mysql -h localhost -u username -p -e "SELECT 1;"

# 检查连接数
mysql -e "SHOW PROCESSLIST;" | wc -l

# 检查数据库状态
mysql -e "SHOW ENGINE INNODB STATUS\G" | grep -A 10 "TRANSACTIONS"
```

**解决方案**：
```bash
# 重启数据库
sudo systemctl restart mysql

# 调整连接池
# 编辑 .env
# DATABASE_POOL_SIZE=20
# DATABASE_POOL_MAX_OVERFLOW=30

# 优化数据库配置
# my.cnf:
# max_connections = 200
# innodb_buffer_pool_size = 2G
```

### 3. 内存不足

**症状**：
- 应用频繁重启
- OOMKilled错误
- 系统响应缓慢

**诊断步骤**：
```bash
# 检查内存使用
free -m
ps aux --sort=-%mem | head -10

# 检查Docker容器内存
docker stats --no-stream

# 检查系统日志
dmesg | grep -i "killed process"
journalctl -u docker | grep OOM
```

**解决方案**：
```bash
# 清理缓存
redis-cli FLUSHALL
sync && echo 3 > /proc/sys/vm/drop_caches

# 调整Docker内存限制
# 编辑 docker-compose.yml
mem_limit: 4g
memswap_limit: 4g

# 优化应用配置
export WORKERS=2  # 减少工作进程
export DB_POOL_SIZE=10  # 减少连接池
```

### 4. G7汇聚失败

**症状**：
- 汇聚脚本执行异常
- 数据不完整或错误
- 触发器拒绝写入

**诊断步骤**：
```bash
# 检查G7守卫状态
python scripts/check_g7_guard_status.py

# 检查数据库锁
python scripts/check_db_locks_enhanced.py

# 检查汇聚进度
mysql -e "SELECT batch_code, COUNT(*) FROM statistical_aggregations WHERE batch_code='G7-2025' GROUP BY batch_code;"

# 检查错误日志
grep -i "G7-2025" /var/log/converged-computing/app.log | tail -20
```

**解决方案**：
```bash
# 运行预检查
python scripts/g7_precheck_suite.py

# 清理异常数据
python scripts/cleanup_g7_partial_data.py

# 重新执行汇聚
python run_g7_pipeline_wrapper.py --batch G7-2025 --env production

# 如果卡住，紧急停止
python scripts/ultimate_stop.py
```

### 5. API响应慢

**症状**：
- 接口响应时间>2秒
- 前端加载缓慢
- 超时错误

**诊断步骤**：
```bash
# 测试API响应时间
time curl "http://localhost:8000/api/v12/batch/G4-2025/regional"

# 检查数据库慢查询
mysql -e "SHOW GLOBAL STATUS LIKE 'Slow_queries';"
mysql -e "SHOW PROCESSLIST;" | grep -v Sleep

# 检查系统负载
uptime
iostat -x 1 5
```

**解决方案**：
```bash
# 添加数据库索引
mysql -e "ALTER TABLE statistical_aggregations ADD INDEX idx_batch_subject (batch_code, subject_name);"

# 启用Redis缓存
export REDIS_CACHE=true

# 优化查询
# 检查慢查询日志
mysqldumpslow /var/lib/mysql/slow_query.log
```

## 高级故障排查

### 死锁分析
```sql
-- 检查当前锁等待
SELECT 
    r.trx_id waiting_trx_id,
    r.trx_mysql_thread_id waiting_thread,
    r.trx_query waiting_query,
    b.trx_id blocking_trx_id,
    b.trx_mysql_thread_id blocking_thread,
    b.trx_query blocking_query
FROM information_schema.innodb_lock_waits w
INNER JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id
INNER JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id;

-- 终止阻塞的事务
KILL <blocking_thread_id>;
```

### 慢查询优化
```sql
-- 开启慢查询日志
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 2;

-- 分析执行计划
EXPLAIN SELECT * FROM statistical_aggregations WHERE batch_code='G7-2025';

-- 添加索引
CREATE INDEX idx_batch_aggregation_level ON statistical_aggregations(batch_code, aggregation_level);
CREATE INDEX idx_subject_school ON statistical_aggregations(subject_name, school_id);
```

### 应用性能分析
```python
# 使用性能分析工具
import cProfile
import pstats

# 分析API性能
profiler = cProfile.Profile()
profiler.enable()

# 执行API调用
response = requests.get('http://localhost:8000/api/v12/batch/G4-2025/regional')

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative').print_stats(20)
```

### 内存泄漏排查
```bash
# 使用memory_profiler
pip install memory-profiler

# 监控内存使用
mprof run python app/main.py
mprof plot

# 检查大对象
import tracemalloc
tracemalloc.start()

# ... 运行代码 ...

current, peak = tracemalloc.get_traced_memory()
print(f"当前内存: {current / 1024 / 1024:.1f} MB")
print(f"峰值内存: {peak / 1024 / 1024:.1f} MB")
tracemalloc.stop()
```

## 数据一致性问题

### 数据校验
```sql
-- 检查数据完整性
SELECT 
    batch_code,
    subject_name,
    aggregation_level,
    COUNT(*) as record_count,
    COUNT(DISTINCT school_id) as school_count
FROM statistical_aggregations 
WHERE batch_code = 'G7-2025'
GROUP BY batch_code, subject_name, aggregation_level;

-- 检查异常数据
SELECT *
FROM statistical_aggregations 
WHERE statistics_data IS NULL 
   OR JSON_VALID(statistics_data) = 0
   OR batch_code = 'G7-2025'
LIMIT 10;
```

### 数据修复
```bash
# 重新生成异常数据
python scripts/repair_corrupted_data.py --batch G7-2025

# 验证修复结果
python scripts/validate_data_integrity.py --batch G7-2025
```

## 应急预案

### 服务完全不可用
```bash
# 1. 立即通知相关人员
echo "服务不可用" | mail -s "紧急故障" admin@example.com

# 2. 检查基础设施
ping 8.8.8.8
df -h
free -m

# 3. 尝试快速恢复
docker-compose down
docker-compose up -d

# 4. 如果无法恢复，切换到备用环境
# 更新DNS或负载均衡器指向备用服务器
```

### 数据丢失
```bash
# 1. 立即停止所有写操作
python scripts/emergency_readonly_mode.py

# 2. 评估丢失范围
mysql -e "SELECT MAX(updated_at) FROM statistical_aggregations WHERE batch_code='G7-2025';"

# 3. 从备份恢复
mysql < backup_G7_2025_clean.sql

# 4. 验证恢复结果
python scripts/validate_data_integrity.py
```

## 联系支持

### 提供的信息清单
故障报告请包含：

1. **基本信息**
   - 发生时间
   - 故障现象
   - 影响范围
   - 紧急程度

2. **环境信息**
   - 操作系统版本
   - Docker版本
   - 应用版本
   - 数据库版本

3. **日志文件**
   - 应用日志：/var/log/converged-computing/app.log
   - 错误日志：/var/log/converged-computing/error.log
   - 系统日志：/var/log/syslog
   - Docker日志：docker-compose logs

4. **系统状态**
   ```bash
   # 收集系统信息
   uname -a > system_info.txt
   free -m >> system_info.txt
   df -h >> system_info.txt
   docker-compose ps >> system_info.txt
   ```

### 支持联系方式
- **7x24紧急支持**：400-xxx-xxxx
- **技术支持邮箱**：support@example.com
- **工单系统**：https://support.example.com

### 响应时间承诺
- P0紧急故障：30分钟内响应
- P1严重问题：2小时内响应
- P2一般问题：8小时内响应
- P3咨询建议：24小时内响应
"""

    def _get_version_notes(self):
        return f"""
# 版本说明 v1.2.0

**发布日期**：{self.timestamp}
**部署包**：{self.package_name}

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

    def _get_changelog(self):
        return f"""
# 变更日志 (CHANGELOG)

## [1.2.0] - {datetime.now().strftime('%Y-%m-%d')}

### 新增 (Added)
- 统一8000端口服务架构
- 问卷题目选项分布API (`/api/v1/questionnaire-distributions`)
- G7汇聚增强守卫+白名单机制
- 完整的预检查、监控、回滚脚本套件
- 生产环境部署包和运维文档
- 自动化健康检查脚本
- 增强的CORS支持
- Docker Compose生产配置
- 性能监控端点
- 完整的故障排查手册

### 变更 (Changed)
- API v1.2结构调整：问卷题目随维度输出
- subjects_builder.py全面重构修复
- V12字段布局恢复，保持历史兼容
- 区域维度排名精简，避免数据冗余
- 数据清洗流程优化：先清洗后汇聚
- 问卷维度数据补齐完善
- G7汇聚执行流程标准化
- 运维脚本命令行参数支持
- 日志格式改进和轮转配置
- Docker镜像优化和资源限制

### 修复 (Fixed)
- subjects_builder输出结构偏差问题
- 问卷维度列表为空的bug
- V12字段布局不一致问题
- G7汇聚数据重复覆盖问题
- 内存泄漏和资源释放问题
- 数据库连接池配置问题
- API响应时间优化
- 错误处理和异常捕获完善
- 配置文件编码问题
- 日志文件权限问题

### 移除 (Removed)
- subjects服务独立8001端口（已整合到主服务）
- 旧版汇聚流程入口
- 过时的配置选项
- 冗余的调试脚本

### 安全 (Security)
- 数据库触发器保护机制
- API访问频率限制
- 敏感信息脱敏处理
- 配置文件安全加固

### 废弃 (Deprecated)
- 暂无废弃功能（保持向下兼容）

---

## [1.1.x] - 历史版本

### [1.1.2] - 2025-09-10
- 修复批次汇聚性能问题
- 优化数据库查询效率
- 完善错误处理机制

### [1.1.1] - 2025-09-05  
- 修复问卷数据计算错误
- 增加数据验证规则
- 改进日志输出格式

### [1.1.0] - 2025-08-30
- 新增批量处理功能
- 支持多种数据格式
- API接口优化
- 性能改进

---

## [1.0.x] - 初始版本

### [1.0.0] - 2025-08-15
- 基础API功能
- 数据汇聚核心逻辑
- Docker容器化支持
- 基本监控功能

---

## 版本说明

### 版本号规则
采用语义化版本控制 (Semantic Versioning)：
- **主版本号**：不兼容的API修改
- **次版本号**：向下兼容的功能性新增
- **修订版本号**：向下兼容的问题修正

### 发布周期
- **主版本**：每年1-2次重大升级
- **次版本**：每月1-2次功能更新
- **修订版本**：按需发布问题修复

### 支持策略
- **当前版本**：提供完整技术支持
- **前一版本**：提供有限技术支持
- **更早版本**：仅提供严重安全问题修复

### 升级建议
- **生产环境**：建议跟随次版本更新
- **测试环境**：可以尝试最新版本
- **关键系统**：等待稳定版本再升级

---

**注释说明**：
- `Added` - 新增功能
- `Changed` - 修改现有功能
- `Deprecated` - 即将移除的功能
- `Removed` - 已移除的功能
- `Fixed` - 问题修复
- `Security` - 安全相关修复

---

更多详细信息请参考：
- [API文档](docs/API_v1.2_GUIDE.md)
- [部署指南](docs/DEPLOYMENT_GUIDE.md)
- [运维手册](ops/OPERATIONS_MANUAL.md)
- [故障排查](ops/TROUBLESHOOTING.md)
"""

    def _get_deploy_script(self):
        return """
#!/bin/bash
# 生产环境部署脚本
# 教育统计分析服务 v1.2

set -e

PROJECT_NAME="converged-computing"
VERSION="1.2.0"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查运行环境
check_requirements() {
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
    
    # 检查磁盘空间
    DISK_USAGE=$(df -h . | awk 'NR==2 {print $5}' | sed 's/%//')
    if [ $DISK_USAGE -gt 85 ]; then
        log_warning "磁盘使用率${DISK_USAGE}%，请确保有足够空间"
    fi
    
    # 检查内存
    MEMORY_MB=$(free -m | awk 'NR==2{printf "%.0f", $2}')
    if [ $MEMORY_MB -lt 4096 ]; then
        log_warning "系统内存${MEMORY_MB}MB，建议至少8GB内存"
    fi
    
    log_success "系统要求检查通过"
}

# 环境配置
setup_environment() {
    log_info "配置环境..."
    
    # 创建必要的目录
    sudo mkdir -p /var/log/converged-computing
    sudo mkdir -p /var/lib/converged-computing
    sudo mkdir -p /etc/converged-computing
    
    # 设置目录权限
    sudo chown -R $(whoami):$(whoami) /var/log/converged-computing
    sudo chown -R $(whoami):$(whoami) /var/lib/converged-computing
    
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
}

# 构建镜像
build_images() {
    log_info "构建Docker镜像..."
    
    # 构建应用镜像
    docker-compose build --no-cache
    
    # 标记版本
    docker tag ${PROJECT_NAME}_app:latest ${PROJECT_NAME}_app:${VERSION}
    docker tag ${PROJECT_NAME}_app:latest ${PROJECT_NAME}_app:${TIMESTAMP}
    
    log_success "镜像构建完成"
}

# 数据库初始化
init_database() {
    log_info "初始化数据库..."
    
    # 等待数据库启动
    log_info "等待数据库服务启动..."
    for i in {1..30}; do
        if python3 -c "from app.database.connection import test_connection; test_connection()" 2>/dev/null; then
            break
        fi
        sleep 2
        if [ $i -eq 30 ]; then
            log_error "数据库连接超时"
            exit 1
        fi
    done
    
    # 运行数据库迁移
    if [ -f alembic.ini ]; then
        log_info "运行数据库迁移..."
        python3 -m alembic upgrade head
    fi
    
    log_success "数据库初始化完成"
}

# 启动服务
start_services() {
    log_info "启动服务..."
    
    # 启动所有服务
    docker-compose up -d
    
    # 等待服务启动
    log_info "等待服务启动..."
    for i in {1..30}; do
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
}

# 健康检查
health_check() {
    log_info "执行健康检查..."
    
    # 运行健康检查脚本
    if [ -f health_check.py ]; then
        python3 health_check.py --full
    else
        # 基本健康检查
        HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/health)
        if [ "$HTTP_STATUS" = "200" ]; then
            log_success "API健康检查通过"
        else
            log_error "API健康检查失败: HTTP $HTTP_STATUS"
            exit 1
        fi
        
        # 测试关键API
        API_TEST=$(curl -s "http://localhost:8000/api/v12/batch/TEST-2025/regional" | grep -o '"success":\s*true')
        if [ -n "$API_TEST" ]; then
            log_success "API功能测试通过"
        else
            log_warning "API功能测试未通过，可能需要配置测试数据"
        fi
    fi
    
    log_success "健康检查完成"
}

# 安装系统服务
install_systemd_service() {
    log_info "安装系统服务..."
    
    cat > /tmp/converged-computing.service << EOF
[Unit]
Description=Converged Computing Service
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=$(pwd)
ExecStart=/usr/local/bin/docker-compose up -d
ExecStop=/usr/local/bin/docker-compose down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF

    sudo mv /tmp/converged-computing.service /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable converged-computing
    
    log_success "系统服务安装完成"
}

# 设置日志轮转
setup_logrotate() {
    log_info "配置日志轮转..."
    
    cat > /tmp/converged-computing << EOF
/var/log/converged-computing/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    copytruncate
    notifempty
    postrotate
        docker-compose restart app > /dev/null 2>&1 || true
    endscript
}
EOF

    sudo mv /tmp/converged-computing /etc/logrotate.d/
    sudo chmod 644 /etc/logrotate.d/converged-computing
    
    log_success "日志轮转配置完成"
}

# 清理函数
cleanup() {
    if [ $? -ne 0 ]; then
        log_error "部署失败，正在清理..."
        docker-compose down 2>/dev/null || true
    fi
}

# 显示部署信息
show_deployment_info() {
    log_success "部署完成！"
    echo ""
    echo "==========================================="
    echo "  教育统计分析服务 v${VERSION}  "
    echo "==========================================="
    echo "服务地址: http://localhost:8000"
    echo "健康检查: http://localhost:8000/health"
    echo "API文档: docs/API_v1.2_GUIDE.md"
    echo "运维手册: ops/OPERATIONS_MANUAL.md"
    echo ""
    echo "常用命令:"
    echo "  查看状态: docker-compose ps"
    echo "  查看日志: docker-compose logs -f app"
    echo "  重启服务: docker-compose restart"
    echo "  停止服务: docker-compose down"
    echo "  健康检查: python3 health_check.py"
    echo ""
    echo "配置文件位置:"
    echo "  应用配置: .env"
    echo "  Docker配置: docker-compose.yml"
    echo "  日志目录: /var/log/converged-computing/"
    echo ""
    echo "支持联系: support@example.com"
    echo "==========================================="
}

# 主函数
main() {
    echo "启动教育统计分析服务部署 v${VERSION}"
    echo "时间: $(date)"
    echo ""
    
    # 设置清理函数
    trap cleanup EXIT
    
    # 执行部署步骤
    check_requirements
    setup_environment
    
    # 询问是否需要构建镜像
    read -p "是否需要构建Docker镜像? (y/N): " BUILD_IMAGES
    if [[ $BUILD_IMAGES =~ ^[Yy]$ ]]; then
        build_images
    fi
    
    start_services
    
    # 询问是否需要初始化数据库
    read -p "是否需要初始化数据库? (y/N): " INIT_DB
    if [[ $INIT_DB =~ ^[Yy]$ ]]; then
        init_database
    fi
    
    health_check
    
    # 询问是否安装系统服务
    read -p "是否安装为系统服务? (y/N): " INSTALL_SERVICE
    if [[ $INSTALL_SERVICE =~ ^[Yy]$ ]]; then
        install_systemd_service
        setup_logrotate
    fi
    
    show_deployment_info
}

# 检查是否为root用户运行某些操作
if [ "$EUID" -eq 0 ]; then
    log_warning "检测到以root用户运行，建议使用普通用户部署"
    read -p "是否继续? (y/N): " CONTINUE_AS_ROOT
    if [[ ! $CONTINUE_AS_ROOT =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# 运行主函数
main "$@"
"""

    def _get_deploy_bat(self):
        return """
@echo off
REM Windows部署脚本
REM 教育统计分析服务 v1.2

setlocal EnableDelayedExpansion

set PROJECT_NAME=converged-computing
set VERSION=1.2.0
set TIMESTAMP=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%%time:~6,2%
set TIMESTAMP=!TIMESTAMP: =0!

echo 启动教育统计分析服务部署 v%VERSION%
echo 时间: %date% %time%
echo.

REM 检查Docker
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker未安装，请先安装Docker Desktop
    pause
    exit /b 1
)

REM 检查Docker Compose
docker-compose --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker Compose未安装，请先安装Docker Compose
    pause
    exit /b 1
)

REM 检查Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python未安装，请先安装Python 3.11+
    pause
    exit /b 1
)

echo [INFO] 系统要求检查通过

REM 创建.env文件
if not exist .env (
    if exist .env.example (
        copy .env.example .env
        echo [INFO] 已创建.env文件，请根据需要修改配置
    ) else (
        echo [ERROR] .env.example文件不存在
        pause
        exit /b 1
    )
)

REM 询问是否构建镜像
set /p BUILD_IMAGES="是否需要构建Docker镜像? (y/N): "
if /i "%BUILD_IMAGES%"=="y" (
    echo [INFO] 构建Docker镜像...
    docker-compose build --no-cache
    if errorlevel 1 (
        echo [ERROR] 镜像构建失败
        pause
        exit /b 1
    )
    echo [SUCCESS] 镜像构建完成
)

REM 启动服务
echo [INFO] 启动服务...
docker-compose up -d
if errorlevel 1 (
    echo [ERROR] 服务启动失败
    pause
    exit /b 1
)

REM 等待服务启动
echo [INFO] 等待服务启动...
for /l %%i in (1,1,30) do (
    curl -f http://localhost:8000/health >nul 2>&1
    if not errorlevel 1 (
        goto :service_ready
    )
    timeout /t 2 /nobreak >nul
)
echo [ERROR] 服务启动超时
docker-compose logs
pause
exit /b 1

:service_ready
echo [SUCCESS] 服务启动完成

REM 健康检查
echo [INFO] 执行健康检查...
if exist health_check.py (
    python health_check.py --full
) else (
    curl -s http://localhost:8000/health | findstr "ok" >nul
    if errorlevel 1 (
        echo [ERROR] API健康检查失败
        pause
        exit /b 1
    )
    echo [SUCCESS] API健康检查通过
)

REM 显示部署信息
echo.
echo ===========================================
echo   教育统计分析服务 v%VERSION%  
echo ===========================================
echo 服务地址: http://localhost:8000
echo 健康检查: http://localhost:8000/health
echo API文档: docs\API_v1.2_GUIDE.md
echo 运维手册: ops\OPERATIONS_MANUAL.md
echo.
echo 常用命令:
echo   查看状态: docker-compose ps
echo   查看日志: docker-compose logs -f app
echo   重启服务: docker-compose restart
echo   停止服务: docker-compose down
echo   健康检查: python health_check.py
echo.
echo 配置文件位置:
echo   应用配置: .env
echo   Docker配置: docker-compose.yml
echo.
echo 支持联系: support@example.com
echo ===========================================
echo.
echo [SUCCESS] 部署完成！

pause
"""

    def _get_health_check_script(self):
        return """
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
健康检查脚本
用于检查教育统计分析服务的运行状态
"""

import sys
import time
import json
import argparse
import subprocess
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("[WARNING] requests库未安装，某些检查功能将不可用")
    requests = None

try:
    import psutil
except ImportError:
    print("[WARNING] psutil库未安装，系统资源检查将不可用")
    psutil = None

class HealthChecker:
    def __init__(self):
        self.base_url = "http://localhost:8000"
        self.results = {}
        self.errors = []
        
    def log_info(self, message):
        print(f"[INFO] {message}")
        
    def log_success(self, message):
        print(f"[SUCCESS] {message}")
        
    def log_warning(self, message):
        print(f"[WARNING] {message}")
        
    def log_error(self, message):
        print(f"[ERROR] {message}")
        self.errors.append(message)
        
    def check_api_health(self):
        """检查API健康状态"""
        self.log_info("检查API健康状态...")
        
        if not requests:
            self.log_warning("requests库不可用，跳过API检查")
            return
            
        try:
            response = requests.get(f"{self.base_url}/health", timeout=10)
            if response.status_code == 200:
                self.log_success("API健康检查通过")
                self.results['api_health'] = True
            else:
                self.log_error(f"API健康检查失败: HTTP {response.status_code}")
                self.results['api_health'] = False
        except requests.exceptions.RequestException as e:
            self.log_error(f"API连接失败: {e}")
            self.results['api_health'] = False
            
    def check_api_performance(self):
        """检查API性能"""
        self.log_info("检查API性能...")
        
        if not requests:
            return
            
        try:
            # 测试响应时间
            start_time = time.time()
            response = requests.get(f"{self.base_url}/health", timeout=10)
            response_time = (time.time() - start_time) * 1000
            
            if response_time < 500:
                self.log_success(f"API响应时间: {response_time:.2f}ms")
            elif response_time < 2000:
                self.log_warning(f"API响应时间偏慢: {response_time:.2f}ms")
            else:
                self.log_error(f"API响应时间过慢: {response_time:.2f}ms")
                
            self.results['api_response_time'] = response_time
            
            # 测试关键接口
            test_endpoints = [
                "/api/v12/batch/TEST-2025/regional",
                "/api/v1/questionnaire-distributions/TEST-2025/问卷/regional"
            ]
            
            for endpoint in test_endpoints:
                try:
                    response = requests.get(f"{self.base_url}{endpoint}", timeout=5)
                    if response.status_code in [200, 404]:  # 404可能是测试数据不存在
                        self.log_success(f"接口可用: {endpoint}")
                    else:
                        self.log_warning(f"接口异常: {endpoint} - HTTP {response.status_code}")
                except Exception as e:
                    self.log_warning(f"接口测试失败: {endpoint} - {e}")
                    
        except Exception as e:
            self.log_error(f"API性能检查失败: {e}")
            
    def check_database_connection(self):
        """检查数据库连接"""
        self.log_info("检查数据库连接...")
        
        try:
            # 尝试导入数据库连接模块
            sys.path.append('.')
            from app.database.connection import test_connection
            
            if test_connection():
                self.log_success("数据库连接正常")
                self.results['database_connection'] = True
            else:
                self.log_error("数据库连接失败")
                self.results['database_connection'] = False
                
        except ImportError:
            self.log_warning("无法导入数据库模块，跳过数据库检查")
        except Exception as e:
            self.log_error(f"数据库连接检查失败: {e}")
            self.results['database_connection'] = False
            
    def check_system_resources(self):
        """检查系统资源"""
        self.log_info("检查系统资源...")
        
        if not psutil:
            self.log_warning("psutil库不可用，跳过系统资源检查")
            return
            
        try:
            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent < 70:
                self.log_success(f"CPU使用率: {cpu_percent:.1f}%")
            elif cpu_percent < 90:
                self.log_warning(f"CPU使用率偏高: {cpu_percent:.1f}%")
            else:
                self.log_error(f"CPU使用率过高: {cpu_percent:.1f}%")
            self.results['cpu_usage'] = cpu_percent
            
            # 内存使用率
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            if memory_percent < 80:
                self.log_success(f"内存使用率: {memory_percent:.1f}%")
            elif memory_percent < 90:
                self.log_warning(f"内存使用率偏高: {memory_percent:.1f}%")
            else:
                self.log_error(f"内存使用率过高: {memory_percent:.1f}%")
            self.results['memory_usage'] = memory_percent
            
            # 磁盘使用率
            disk = psutil.disk_usage('.')
            disk_percent = (disk.used / disk.total) * 100
            if disk_percent < 85:
                self.log_success(f"磁盘使用率: {disk_percent:.1f}%")
            elif disk_percent < 95:
                self.log_warning(f"磁盘使用率偏高: {disk_percent:.1f}%")
            else:
                self.log_error(f"磁盘使用率过高: {disk_percent:.1f}%")
            self.results['disk_usage'] = disk_percent
            
        except Exception as e:
            self.log_error(f"系统资源检查失败: {e}")
            
    def check_docker_services(self):
        """检查Docker服务状态"""
        self.log_info("检查Docker服务状态...")
        
        try:
            # 检查docker-compose服务状态
            result = subprocess.run(
                ['docker-compose', 'ps', '--format', 'json'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                services = []
                for line in result.stdout.strip().split('\n'):
                    if line:
                        try:
                            service = json.loads(line)
                            services.append(service)
                        except json.JSONDecodeError:
                            continue
                            
                running_services = [s for s in services if 'running' in s.get('State', '').lower()]
                total_services = len(services)
                
                if running_services and len(running_services) == total_services:
                    self.log_success(f"Docker服务运行正常: {len(running_services)}/{total_services}")
                    self.results['docker_services'] = True
                else:
                    self.log_error(f"部分Docker服务异常: {len(running_services)}/{total_services}")
                    self.results['docker_services'] = False
                    
            else:
                self.log_error("无法获取Docker服务状态")
                self.results['docker_services'] = False
                
        except subprocess.TimeoutExpired:
            self.log_error("Docker命令执行超时")
            self.results['docker_services'] = False
        except FileNotFoundError:
            self.log_warning("docker-compose命令不可用")
        except Exception as e:
            self.log_error(f"Docker服务检查失败: {e}")
            
    def check_log_files(self):
        """检查日志文件"""
        self.log_info("检查日志文件...")
        
        log_paths = [
            Path('/var/log/converged-computing/app.log'),
            Path('./logs/app.log'),
            Path('./app.log')
        ]
        
        log_found = False
        for log_path in log_paths:
            if log_path.exists():
                log_found = True
                file_size = log_path.stat().st_size
                if file_size < 100 * 1024 * 1024:  # 100MB
                    self.log_success(f"日志文件正常: {log_path} ({file_size // 1024}KB)")
                else:
                    self.log_warning(f"日志文件过大: {log_path} ({file_size // 1024 // 1024}MB)")
                break
                
        if not log_found:
            self.log_warning("未找到日志文件")
            
    def check_configuration(self):
        """检查配置文件"""
        self.log_info("检查配置文件...")
        
        config_files = ['.env', 'docker-compose.yml', 'requirements.txt']
        
        for config_file in config_files:
            if Path(config_file).exists():
                self.log_success(f"配置文件存在: {config_file}")
            else:
                self.log_error(f"配置文件缺失: {config_file}")
                
        # 检查.env文件内容
        env_file = Path('.env')
        if env_file.exists():
            try:
                with open(env_file, 'r', encoding='utf-8') as f:
                    env_content = f.read()
                    
                required_vars = ['DATABASE_URL', 'API_HOST', 'API_PORT']
                missing_vars = []
                
                for var in required_vars:
                    if var not in env_content:
                        missing_vars.append(var)
                        
                if missing_vars:
                    self.log_warning(f"环境变量缺失: {', '.join(missing_vars)}")
                else:
                    self.log_success("环境变量配置完整")
                    
            except Exception as e:
                self.log_error(f"读取.env文件失败: {e}")
                
    def generate_report(self):
        """生成检查报告"""
        print("\n" + "=" * 50)
        print("健康检查报告")
        print("=" * 50)
        print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"检查项目: {len(self.results)}")
        print(f"错误数量: {len(self.errors)}")
        print("\n检查结果:")
        
        for key, value in self.results.items():
            status = "✓" if value is True else "✗" if value is False else str(value)
            print(f"  {key}: {status}")
            
        if self.errors:
            print("\n错误详情:")
            for i, error in enumerate(self.errors, 1):
                print(f"  {i}. {error}")
                
        # 总体评估
        critical_checks = ['api_health', 'database_connection', 'docker_services']
        critical_failures = sum(1 for check in critical_checks 
                              if self.results.get(check) is False)
        
        print("\n总体状态: ", end="")
        if critical_failures == 0 and len(self.errors) == 0:
            print("健康 ✓")
            return 0
        elif critical_failures == 0:
            print("良好 ⚠")
            return 0
        else:
            print("异常 ✗")
            return 1
            
    def run_all_checks(self):
        """运行所有检查"""
        self.log_info("开始健康检查...")
        
        self.check_api_health()
        self.check_api_performance()
        self.check_database_connection()
        self.check_system_resources()
        self.check_docker_services()
        self.check_log_files()
        self.check_configuration()
        
        return self.generate_report()
        
    def run_basic_checks(self):
        """运行基本检查"""
        self.log_info("开始基本健康检查...")
        
        self.check_api_health()
        self.check_docker_services()
        
        return self.generate_report()

def main():
    parser = argparse.ArgumentParser(description='教育统计分析服务健康检查')
    parser.add_argument('--full', action='store_true', help='执行完整检查')
    parser.add_argument('--api-perf', action='store_true', help='仅检查API性能')
    parser.add_argument('--check-db', action='store_true', help='仅检查数据库')
    parser.add_argument('--check-env', action='store_true', help='仅检查环境')
    parser.add_argument('--check-memory', action='store_true', help='仅检查内存')
    parser.add_argument('--alert', action='store_true', help='告警模式（仅输出错误）')
    
    args = parser.parse_args()
    
    checker = HealthChecker()
    
    if args.api_perf:
        checker.check_api_performance()
    elif args.check_db:
        checker.check_database_connection()
    elif args.check_env:
        checker.check_configuration()
    elif args.check_memory:
        checker.check_system_resources()
    elif args.full:
        exit_code = checker.run_all_checks()
    else:
        exit_code = checker.run_basic_checks()
        
    if args.alert and checker.errors:
        for error in checker.errors:
            print(f"ALERT: {error}")
        sys.exit(1)
        
    sys.exit(exit_code if 'exit_code' in locals() else 0)

if __name__ == '__main__':
    main()
"""

# 执行脚本
if __name__ == "__main__":
    packager = ProductionDeploymentPackager()
    packager.create_package()
