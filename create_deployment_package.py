#!/usr/bin/env python3
"""
创建部署包脚本 - 包含CORS修复
"""
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

def create_deployment_package():
    """创建部署包"""
    # 生成版本号
    version = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"deployment_package_cors_{version}"
    archive_name = f"converged-computing-cors-fixed_{version}.zip"

    print("="*50)
    print("教育统计分析服务 - 部署包生成器（CORS修复版）")
    print("="*50)
    print(f"版本: {version}")
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

    # 复制scripts目录（完整保留，避免缺失部署所需脚本）
    print("[4/8] 复制脚本文件...")
    if os.path.exists(os.path.join(output_dir, "scripts")):
        shutil.rmtree(os.path.join(output_dir, "scripts"))
    shutil.copytree("scripts", os.path.join(output_dir, "scripts"))

    # 复制服务/管道脚本
    root_scripts = [
        "data_cleaning_service.py",
        "run_single_subject_pipeline.py",
        "run_full_batch_pipeline.py",
        "fast_materialize_subjects_v12.py",
        "fast_materialize_all_batches_v12.py",
        "enhanced_questionnaire_clean.py",
        "fixed_questionnaire_clean.py",
        "quick_questionnaire_clean.py",
    ]

    for file in root_scripts:
        if os.path.exists(file):
            shutil.copy2(file, output_dir)

    # 复制Alembic迁移与配置
    print("[5/8] 复制数据库迁移与配置文件...")
    if os.path.exists("alembic.ini"):
        shutil.copy2("alembic.ini", os.path.join(output_dir, "alembic.ini"))
    if os.path.exists(os.path.join(output_dir, "alembic")):
        shutil.rmtree(os.path.join(output_dir, "alembic"))
    if os.path.exists("alembic"):
        shutil.copytree("alembic", os.path.join(output_dir, "alembic"))
    if os.path.exists("config"):
        if os.path.exists(os.path.join(output_dir, "config")):
            shutil.rmtree(os.path.join(output_dir, "config"))
        shutil.copytree("config", os.path.join(output_dir, "config"))

    # 复制文档
    print("[6/8] 复制文档...")
    if os.path.exists("docs"):
        shutil.copytree("docs", os.path.join(output_dir, "docs"))

    # 复制Docker文件
    print("[7/8] 复制Docker配置...")
    docker_files = [
        "Dockerfile",
        "docker-compose.yml",
        ".dockerignore"
    ]

    for file in docker_files:
        if os.path.exists(file):
            shutil.copy2(file, output_dir)

    # 复制部署文件
    deployment_files = [
        ".env.example",
        "deploy.sh",
        "DEPLOYMENT_GUIDE.md",
        "DEPLOYMENT_CHECKLIST.md",
        "CORS_FIX_DEPLOYMENT.md",
        "requirements.txt",
        "requirements-prod.txt"
    ]

    for file in deployment_files:
        if os.path.exists(file):
            shutil.copy2(file, output_dir)

    # 复制nginx配置
    if os.path.exists("nginx.conf"):
        shutil.copy2("nginx.conf", os.path.join(output_dir, "nginx.conf.example"))

    # 创建必要的目录
    print("[8/8] 创建运行时目录...")
    for dir_name in ["logs", "temp", "reports", "config"]:
        os.makedirs(os.path.join(output_dir, dir_name), exist_ok=True)

    # 创建.env.production
    env_content = """# 生产环境配置
# 请根据实际情况修改

# 数据库配置
DATABASE_URL=mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4

# 应用配置
APP_ENV=production
LOG_LEVEL=INFO
DEBUG=false

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

# 写入控制（如需阻断某批次写入，请在部署前填写实际批次代码，例如 DISABLE_WRITES_FOR_BATCHES=G4-2025）
DISABLE_WRITES_FOR_BATCHES=
"""

    with open(os.path.join(output_dir, ".env.production"), "w", encoding="utf-8") as f:
        f.write(env_content)

    # 创建部署说明
    readme_content = f"""# 教育统计分析服务 - 部署包（CORS修复版）

## 版本信息
- **版本号**: {version}
- **特性**: 包含CORS跨域问题修复
- **构建时间**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 重要更新
✅ **CORS问题已修复**
- 增强的CORS中间件支持
- OPTIONS预检请求正确处理
- 支持多源配置

## 快速部署指南

### 1. 解压部署包
```bash
unzip {archive_name}
cd {output_dir}
```

### 2. 配置环境变量
```bash
cp .env.production .env
# 编辑.env文件，修改数据库连接信息
```

### 3. Docker部署
```bash
# 构建镜像
docker-compose build

# 启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f
```

### 4. 验证服务
```bash
# 健康检查
curl http://localhost:8010/health

# 测试CORS（重要，替换 <BATCH_CODE> 为实际批次）
curl -X OPTIONS http://localhost:8010/api/v12/batch/<BATCH_CODE>/regional \\
  -H "Origin: http://localhost:8080" \\
  -H "Access-Control-Request-Method: GET" \\
  -H "Access-Control-Request-Headers: Content-Type" \\
  -v
```

## CORS配置说明

### 后端已配置
- `app/middleware/cors_config.py` - 增强的CORS中间件
- `app/main.py` - 已应用CORS配置

### Nginx配置（如需要）
参考 `nginx.conf.example` 文件配置Nginx反向代理

### 前端临时方案
如果仍有问题，前端可配置代理：
```javascript
// vue.config.js 或 vite.config.js
proxy: {{
  '/api': {{
    target: 'http://117.72.14.166:8010',
    changeOrigin: true
  }}
}}
```

## 文件结构
```
{output_dir}/
├── app/                     # 应用代码
│   ├── main.py             # 主入口（含CORS）
│   ├── middleware/         # 中间件
│   │   └── cors_config.py  # CORS配置
│   ├── api/                # API路由
│   ├── services/           # 业务逻辑
│   └── database/           # 数据层
├── scripts/                # 维护脚本
├── docs/                   # 文档
├── docker-compose.yml      # Docker编排
├── Dockerfile             # 镜像定义
├── .env.production        # 生产配置模板
├── nginx.conf.example     # Nginx示例
└── CORS_FIX_DEPLOYMENT.md # CORS详细说明
```

## 故障排查

### CORS问题
1. 确认服务正在运行: `docker ps`
2. 检查响应头: 查看是否包含 `Access-Control-Allow-Origin`
3. 检查Nginx配置（如使用）
4. 查看 `CORS_FIX_DEPLOYMENT.md`

### 其他问题
- 查看日志: `docker-compose logs --tail=100`
- 检查端口: 确保8010端口可访问
- 数据库连接: 验证.env中的DATABASE_URL

## 技术支持
如遇问题，请查阅：
1. `DEPLOYMENT_CHECKLIST.md` - 部署检查清单
2. `CORS_FIX_DEPLOYMENT.md` - CORS问题解决方案
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
    print("✅ 打包完成！")
    print("="*50)
    print(f"📦 部署包: {archive_name}")
    print(f"📁 输出目录: {output_dir}")
    print(f"📊 文件数量: {file_count}")
    print(f"💾 包大小: {archive_size:.2f} MB")
    print("="*50)
    print("特性:")
    print("✓ 包含CORS修复")
    print("✓ 支持OPTIONS预检请求")
    print("✓ Nginx配置示例")
    print("✓ 生产环境配置模板")
    print("✓ 完整部署文档")
    print("="*50)

    # 询问是否删除临时目录（在非交互环境默认保留）
    try:
        response = input("\n是否删除临时目录? (y/n): ")
    except EOFError:
        response = "n"
    if response.lower() == 'y':
        shutil.rmtree(output_dir)
        print("✅ 临时目录已删除")
    else:
        print(f"📁 临时目录保留: {output_dir}")

    print(f"\n🚀 部署包已准备就绪: {archive_name}")
    print("请将此文件发送给运维团队进行部署。")

    return archive_name, version

if __name__ == "__main__":
    try:
        archive_name, version = create_deployment_package()
    except Exception as e:
        print(f"\n❌ 打包失败: {e}")
        import traceback
        traceback.print_exc()
