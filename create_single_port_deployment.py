#!/usr/bin/env python3
"""
创建单端口部署包脚本 - 只使用8000端口，包含CORS修复
"""
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

def create_single_port_deployment():
    """创建单端口部署包"""
    # 生成版本号
    version = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"deployment_single_port_{version}"
    archive_name = f"converged-computing-single-port_{version}.zip"

    print("="*50)
    print("教育统计分析服务 - 单端口部署包生成器")
    print("="*50)
    print(f"版本: {version}")
    print(f"服务端口: 8000（单一服务）")
    print(f"输出目录: {output_dir}")
    print()

    # 删除旧目录（如果存在）
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    # 创建输出目录
    print("[1/8] 创建部署目录...")
    os.makedirs(output_dir, exist_ok=True)

    # 复制app目录
    print("[2/8] 复制应用代码...")
    if os.path.exists("app"):
        shutil.copytree("app", os.path.join(output_dir, "app"))
        # 确保middleware目录存在
        middleware_dir = os.path.join(output_dir, "app", "middleware")
        os.makedirs(middleware_dir, exist_ok=True)
        # 创建__init__.py
        init_file = os.path.join(middleware_dir, "__init__.py")
        if not os.path.exists(init_file):
            Path(init_file).touch()

    # 清理Python缓存
    print("[3/8] 清理缓存文件...")
    for root, dirs, files in os.walk(os.path.join(output_dir, "app")):
        # 删除__pycache__目录
        if "__pycache__" in dirs:
            shutil.rmtree(os.path.join(root, "__pycache__"))
        # 删除.pyc文件
        for file in files:
            if file.endswith(".pyc"):
                os.remove(os.path.join(root, file))

    # 复制scripts目录
    print("[4/8] 复制脚本文件...")
    scripts_dir = os.path.join(output_dir, "scripts")
    os.makedirs(scripts_dir, exist_ok=True)

    scripts_to_copy = [
        "rewrite_subjects_v12.py",
        "acceptance_quick_check.py",
        "clean_g4_statistical_data.py",
        "rebuild_g4_aggregation.py",
        "test_database_connection.py"
    ]

    for script in scripts_to_copy:
        src = os.path.join("scripts", script)
        if os.path.exists(src):
            shutil.copy2(src, scripts_dir)

    # 复制服务文件
    if os.path.exists("data_cleaning_service.py"):
        shutil.copy2("data_cleaning_service.py", output_dir)

    # 复制文档
    print("[5/8] 复制文档...")
    if os.path.exists("docs"):
        shutil.copytree("docs", os.path.join(output_dir, "docs"))

    # 复制Docker文件
    print("[6/8] 复制Docker配置...")

    # 复制Dockerfile
    if os.path.exists("Dockerfile"):
        shutil.copy2("Dockerfile", output_dir)

    # 使用单端口的docker-compose配置
    if os.path.exists("docker-compose.single-port.yml"):
        shutil.copy2("docker-compose.single-port.yml",
                    os.path.join(output_dir, "docker-compose.yml"))

    # 复制部署文件
    deployment_files = [
        ".env.example",
        ".dockerignore",
        "CORS_FIX_DEPLOYMENT.md",
        "requirements.txt",
        "requirements-prod.txt"
    ]

    for file in deployment_files:
        if os.path.exists(file):
            shutil.copy2(file, output_dir)

    # 创建必要的目录
    print("[7/8] 创建运行时目录...")
    for dir_name in ["logs", "temp", "reports", "config"]:
        os.makedirs(os.path.join(output_dir, dir_name), exist_ok=True)

    # 创建.env.production
    env_content = """# 生产环境配置 - 单端口版本
# 请根据实际情况修改

# 数据库配置
DATABASE_URL=mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4

# 应用配置
APP_ENV=production
LOG_LEVEL=INFO
DEBUG=false

# 服务端口（单一服务）
PORT=8000

# CORS配置（重要）
CORS_ORIGINS=["http://localhost:8080", "http://117.72.14.166:8080", "*"]
CORS_ALLOW_CREDENTIALS=true
CORS_ALLOW_METHODS=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]
CORS_ALLOW_HEADERS=["*"]
CORS_MAX_AGE=3600

# 性能配置
WORKERS=4
MAX_CONNECTIONS=100
POOL_SIZE=20
POOL_RECYCLE=3600

# 批处理配置
BATCH_SIZE=10
BATCH_TIMEOUT=300
"""

    with open(os.path.join(output_dir, ".env.production"), "w", encoding="utf-8") as f:
        f.write(env_content)

    # 创建部署脚本
    deploy_script = """#!/bin/bash
# 单端口快速部署脚本

echo "====================================="
echo "教育统计分析服务 - 单端口部署"
echo "====================================="

# 1. 停止旧服务
echo "停止旧服务..."
docker-compose down 2>/dev/null || true

# 2. 配置环境变量
if [ ! -f .env ]; then
    echo "创建环境配置文件..."
    cp .env.production .env
    echo "请编辑 .env 文件配置数据库连接信息"
    read -p "配置完成后按回车继续..."
fi

# 3. 构建Docker镜像
echo "构建Docker镜像..."
docker-compose build

# 4. 启动服务
echo "启动服务..."
docker-compose up -d

# 5. 等待服务启动
echo "等待服务启动..."
sleep 10

# 6. 健康检查
echo "执行健康检查..."
if curl -f http://localhost:8000/health &>/dev/null; then
    echo "✓ 服务启动成功"
else
    echo "✗ 服务启动失败，请检查日志"
    docker-compose logs --tail=50
    exit 1
fi

# 7. 测试CORS配置
echo "测试CORS配置..."
curl -X OPTIONS http://localhost:8000/api/v12/batch/G7-2025/regional \\
    -H "Origin: http://localhost:8080" \\
    -H "Access-Control-Request-Method: GET" \\
    -v 2>&1 | grep -i "access-control" || echo "请手动验证CORS"

echo ""
echo "====================================="
echo "部署完成！"
echo "服务地址: http://localhost:8000"
echo "API文档: http://localhost:8000/docs"
echo "====================================="
"""

    with open(os.path.join(output_dir, "deploy.sh"), "w", encoding="utf-8") as f:
        f.write(deploy_script)

    # 创建部署说明
    readme_content = f"""# 教育统计分析服务 - 单端口部署包

## 📌 版本信息
- **版本号**: {version}
- **服务端口**: 8000（单一服务）
- **特性**: 包含完整CORS修复，所有API通过一个服务提供

## 🎯 架构说明

### 单端口架构
- **一个服务处理所有API**
- **端口**: 8000
- **包含的API**:
  - `/api/v1/management/*` - 管理API
  - `/api/v1/reporting/*` - 报告API
  - `/api/v1/statistics/*` - 统计API
  - `/api/v12/*` - v1.2版本API（包含问题的接口）

## ✅ CORS问题已修复

1. **增强的CORS中间件** (`app/middleware/cors_config.py`)
2. **支持OPTIONS预检请求**
3. **正确处理所有CORS响应头**

## 🚀 快速部署

### 方法1: 使用部署脚本
```bash
chmod +x deploy.sh
./deploy.sh
```

### 方法2: 手动部署
```bash
# 1. 配置环境变量
cp .env.production .env
vi .env  # 修改数据库连接

# 2. 构建并启动
docker-compose build
docker-compose up -d

# 3. 验证服务
curl http://localhost:8000/health
```

## 📝 前端调用指南

### 正确的API端点（统一使用8000端口）:

```javascript
const API_BASE = 'http://117.72.14.166:8000';

const api = {{
  // 管理API
  management: `${{API_BASE}}/api/v1/management`,

  // 报告API
  reporting: `${{API_BASE}}/api/v1/reporting`,

  // 统计API
  statistics: `${{API_BASE}}/api/v1/statistics`,

  // v1.2 API（问题接口）
  v12: {{
    batch: `${{API_BASE}}/api/v12/batch`,
    regional: `${{API_BASE}}/api/v12/batch/G7-2025/regional`,
    school: `${{API_BASE}}/api/v12/batch/G7-2025/school`
  }}
}};
```

### 前端开发环境代理配置:

```javascript
// vue.config.js 或 vite.config.js
module.exports = {{
  devServer: {{
    proxy: {{
      '/api': {{
        target: 'http://117.72.14.166:8000',
        changeOrigin: true
      }}
    }}
  }}
}}
```

## 🔍 验证CORS修复

### 测试OPTIONS请求:
```bash
curl -X OPTIONS http://117.72.14.166:8000/api/v12/batch/G7-2025/regional \\
  -H "Origin: http://localhost:8080" \\
  -H "Access-Control-Request-Method: GET" \\
  -H "Access-Control-Request-Headers: Content-Type" \\
  -v
```

### 应该看到的响应头:
```
< Access-Control-Allow-Origin: *
< Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS, PATCH
< Access-Control-Allow-Headers: *
< Access-Control-Allow-Credentials: true
```

## 📊 服务监控

```bash
# 查看日志
docker-compose logs -f

# 查看容器状态
docker ps

# 重启服务
docker-compose restart
```

## ⚠️ 注意事项

1. **端口变更**: 如果之前使用8010/8011端口，现在统一使用8000
2. **单一服务**: 不再需要subjects独立服务
3. **性能**: 所有请求由一个服务处理，已配置4个workers

## 🆘 故障排查

### CORS问题
1. 确认服务正在运行: `docker ps`
2. 检查响应头: `curl -I`命令查看headers
3. 查看日志: `docker-compose logs`

### 连接问题
1. 检查防火墙: 确保8000端口开放
2. 测试连接: `telnet 117.72.14.166 8000`

## 📁 文件结构
```
{output_dir}/
├── app/                     # 应用代码
│   ├── main.py             # 主入口（含所有路由）
│   ├── middleware/         # 中间件
│   │   └── cors_config.py  # CORS配置
│   └── api/                # 所有API路由
├── docker-compose.yml      # 单服务Docker配置
├── Dockerfile             # 镜像定义
├── .env.production        # 环境配置模板
└── deploy.sh              # 部署脚本
```

## 更新历史
- {datetime.now().strftime("%Y-%m-%d %H:%M")} - 创建单端口部署包，修复CORS问题
"""

    with open(os.path.join(output_dir, "README.md"), "w", encoding="utf-8") as f:
        f.write(readme_content)

    # 创建压缩包
    print("[8/8] 创建压缩包...")
    with zipfile.ZipFile(archive_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, os.path.dirname(output_dir))
                zipf.write(file_path, arcname)

    # 统计信息
    file_count = sum([len(files) for _, _, files in os.walk(output_dir)])
    archive_size = os.path.getsize(archive_name) / (1024 * 1024)  # MB

    print()
    print("="*50)
    print("打包完成！")
    print("="*50)
    print(f"部署包: {archive_name}")
    print(f"输出目录: {output_dir}")
    print(f"文件数量: {file_count}")
    print(f"包大小: {archive_size:.2f} MB")
    print("="*50)
    print("特性:")
    print("- 单端口服务（8000）")
    print("- 包含CORS修复")
    print("- 所有API统一入口")
    print("- 简化部署流程")
    print("="*50)

    return archive_name, version, output_dir

if __name__ == "__main__":
    try:
        archive_name, version, output_dir = create_single_port_deployment()
        print(f"\n部署包已准备就绪: {archive_name}")
        print("请将此文件发送给运维团队进行部署。")
    except Exception as e:
        print(f"\n打包失败: {e}")
        import traceback
        traceback.print_exc()