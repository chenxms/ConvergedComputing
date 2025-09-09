#!/bin/bash

# ===================================
# 项目打包脚本 - 用于创建部署包
# ===================================

set -e

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# 打包信息
PACKAGE_NAME="converged-computing"
VERSION=$(date +%Y%m%d_%H%M%S)
OUTPUT_DIR="deployment_package_${VERSION}"
ARCHIVE_NAME="${PACKAGE_NAME}_${VERSION}.tar.gz"

echo -e "${GREEN}开始打包教育统计分析服务...${NC}"
echo "版本: ${VERSION}"

# 创建临时打包目录
echo -e "${GREEN}创建打包目录...${NC}"
mkdir -p ${OUTPUT_DIR}

# ===================================
# 复制核心程序文件
# ===================================

echo -e "${GREEN}复制核心程序文件...${NC}"

# 1. 复制整个app目录（核心应用代码）
cp -r app ${OUTPUT_DIR}/
# 删除Python缓存文件
find ${OUTPUT_DIR}/app -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find ${OUTPUT_DIR}/app -type f -name "*.pyc" -delete 2>/dev/null || true

# 2. 复制必要的脚本文件
mkdir -p ${OUTPUT_DIR}/scripts
cp scripts/rewrite_subjects_v12.py ${OUTPUT_DIR}/scripts/ 2>/dev/null || true
cp scripts/acceptance_quick_check.py ${OUTPUT_DIR}/scripts/ 2>/dev/null || true
cp scripts/complete_questionnaire_labels.py ${OUTPUT_DIR}/scripts/ 2>/dev/null || true

# 3. 复制文档目录（包含数据规范）
cp -r docs ${OUTPUT_DIR}/ 2>/dev/null || true

# ===================================
# 复制Docker相关文件
# ===================================

echo -e "${GREEN}复制Docker配置文件...${NC}"

cp Dockerfile ${OUTPUT_DIR}/
cp docker-compose.yml ${OUTPUT_DIR}/
cp .dockerignore ${OUTPUT_DIR}/ 2>/dev/null || true

# ===================================
# 复制部署相关文件
# ===================================

echo -e "${GREEN}复制部署文档和脚本...${NC}"

cp .env.example ${OUTPUT_DIR}/
cp deploy.sh ${OUTPUT_DIR}/
cp DEPLOYMENT_GUIDE.md ${OUTPUT_DIR}/
cp DEPLOYMENT_CHECKLIST.md ${OUTPUT_DIR}/
cp nginx.conf.example ${OUTPUT_DIR}/ 2>/dev/null || true

# ===================================
# 复制依赖定义文件
# ===================================

echo -e "${GREEN}复制依赖文件...${NC}"

# Python依赖
cp requirements.txt ${OUTPUT_DIR}/ 2>/dev/null || true
cp pyproject.toml ${OUTPUT_DIR}/ 2>/dev/null || true
cp poetry.lock ${OUTPUT_DIR}/ 2>/dev/null || true

# ===================================
# 创建必要的空目录
# ===================================

echo -e "${GREEN}创建必要的目录结构...${NC}"

mkdir -p ${OUTPUT_DIR}/logs
mkdir -p ${OUTPUT_DIR}/temp
mkdir -p ${OUTPUT_DIR}/reports

# ===================================
# 创建文件清单
# ===================================

echo -e "${GREEN}生成文件清单...${NC}"

cat > ${OUTPUT_DIR}/FILES_LIST.md << 'EOF'
# 部署包文件清单

## 一、核心程序文件

### 1. 应用主目录 (`app/`)
```
app/
├── __init__.py              # Python包初始化
├── main.py                  # FastAPI主应用入口
├── subjects_api_main.py     # 科目API服务入口
├── api/                     # API路由层
│   ├── management_api.py    # 任务管理API
│   ├── reporting_api.py     # 统计报告API
│   └── subjects_v12_api.py  # 科目汇聚API v1.2
├── services/                # 业务逻辑层
│   ├── task_manager.py      # 任务管理服务
│   ├── batch_service.py     # 批次管理服务
│   ├── calculation_service.py # 计算服务
│   ├── ranking_service.py   # 排名服务
│   ├── subjects_builder.py  # 科目构建器
│   ├── questionnaire_processor.py # 问卷处理器
│   └── serialization/       # 数据序列化
│       ├── data_integrator.py # 数据集成器
│       └── statistics_json_serializer.py # JSON序列化器
├── calculation/             # 统计计算引擎
│   ├── engine.py           # 核心计算引擎
│   ├── formulas.py         # 教育统计公式
│   └── calculators/        # 专项计算器
│       ├── grade_calculator.py  # 年级计算器
│       └── survey_calculator.py # 问卷计算器
├── database/               # 数据访问层
│   ├── connection.py       # 数据库连接管理
│   ├── models.py           # SQLAlchemy模型
│   └── repositories.py     # Repository模式实现
├── schemas/                # 数据模型定义
│   ├── request_schemas.py  # 请求数据模型
│   └── json_schemas.py     # JSON数据规范
├── repositories/           # 额外的Repository实现
└── utils/                  # 工具函数
```

### 2. 脚本目录 (`scripts/`)
- `rewrite_subjects_v12.py` - 批次数据物化脚本
- `acceptance_quick_check.py` - 验收快速检查脚本
- `complete_questionnaire_labels.py` - 问卷标签补全脚本

### 3. 文档目录 (`docs/`)
- 包含数据库表结构说明
- API接口规范文档
- 数据汇聚实现文档

## 二、Docker配置文件

- `Dockerfile` - Docker镜像构建配置
- `docker-compose.yml` - 服务编排配置
- `.dockerignore` - Docker忽略文件配置

## 三、部署配置文件

- `.env.example` - 环境变量配置模板
- `deploy.sh` - 自动化部署脚本
- `nginx.conf.example` - Nginx配置示例

## 四、部署文档

- `DEPLOYMENT_GUIDE.md` - 详细部署指南
- `DEPLOYMENT_CHECKLIST.md` - 部署检查清单
- `FILES_LIST.md` - 本文件清单

## 五、依赖文件

- `requirements.txt` - Python依赖列表
- `pyproject.toml` - Poetry项目配置（可选）
- `poetry.lock` - Poetry锁定文件（可选）

## 六、运行时目录（自动创建）

- `logs/` - 日志文件目录
- `temp/` - 临时文件目录  
- `reports/` - 报告输出目录

## 注意事项

1. **Python版本要求**: Python 3.11+
2. **数据库要求**: MySQL 8.0+
3. **编码要求**: 所有文件使用UTF-8编码
4. **敏感信息**: 不包含任何密码、密钥等敏感信息
EOF

# ===================================
# 创建部署说明
# ===================================

cat > ${OUTPUT_DIR}/README.md << 'EOF'
# 教育统计分析服务 - 部署包

## 快速开始

1. 解压部署包
```bash
tar -xzvf converged-computing_*.tar.gz
cd deployment_package_*
```

2. 配置环境变量
```bash
cp .env.example .env
vim .env  # 修改数据库连接等配置
```

3. 运行部署脚本
```bash
chmod +x deploy.sh
./deploy.sh --basic  # 基础部署
```

4. 验证服务
```bash
curl http://localhost:8010/health
```

## 详细说明

请查看 `DEPLOYMENT_GUIDE.md` 获取完整部署指南。

## 文件说明

请查看 `FILES_LIST.md` 了解所有文件的用途。

## 技术支持

如有问题，请参考 `DEPLOYMENT_CHECKLIST.md` 进行排查。
EOF

# ===================================
# 打包压缩
# ===================================

echo -e "${GREEN}创建压缩包...${NC}"
tar -czf ${ARCHIVE_NAME} ${OUTPUT_DIR}

# 计算包大小
SIZE=$(du -sh ${ARCHIVE_NAME} | cut -f1)

echo -e "${GREEN}打包完成！${NC}"
echo "====================================="
echo "包名称: ${ARCHIVE_NAME}"
echo "包大小: ${SIZE}"
echo "包含文件数: $(find ${OUTPUT_DIR} -type f | wc -l)"
echo "====================================="

# 可选：删除临时目录
read -p "是否删除临时打包目录? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -rf ${OUTPUT_DIR}
    echo -e "${GREEN}临时目录已删除${NC}"
else
    echo -e "${YELLOW}临时目录保留在: ${OUTPUT_DIR}${NC}"
fi

echo -e "${GREEN}部署包已准备就绪：${ARCHIVE_NAME}${NC}"
echo "请将此文件发送给运维同事进行部署。"