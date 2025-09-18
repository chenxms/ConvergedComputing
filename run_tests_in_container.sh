#!/bin/bash
set -e

echo "============================="
echo "容器内测试运行脚本"
echo "============================="

# 检查环境变量
echo "检查必要的环境变量..."
if [ -z "$DATABASE_URL" ]; then
    echo "警告: DATABASE_URL 未设置，使用默认测试数据库"
    export DATABASE_URL="mysql+pymysql://root:password@db:3306/test_db"
fi

echo "当前环境变量:"
echo "  PYTHONPATH=$PYTHONPATH"
echo "  DATABASE_URL=$DATABASE_URL"

# 检查依赖是否安装
echo ""
echo "检查测试依赖..."
python -c "import pytest; print('✓ pytest')"
python -c "import httpx; print('✓ httpx')"
python -c "import requests; print('✓ requests')"

# 运行代码质量检查
echo ""
echo "============================="
echo "代码质量检查"
echo "============================="

echo "运行 black 格式化检查..."
black --check app/ || echo "警告: 代码格式不符合 black 标准"

echo "运行 isort 导入排序检查..."
isort --check-only app/ || echo "警告: 导入排序不符合 isort 标准"

echo "运行 flake8 代码风格检查..."
flake8 app/ || echo "警告: 发现代码风格问题"

# 运行类型检查
echo "运行 mypy 类型检查..."
mypy app/ || echo "警告: 发现类型注解问题"

# 运行测试
echo ""
echo "============================="
echo "运行测试套件"
echo "============================="

echo "运行单元测试..."
pytest tests/ -v --tb=short --maxfail=5

echo ""
echo "============================="
echo "生成测试覆盖率报告"
echo "============================="

echo "运行带覆盖率的测试..."
python -m pytest tests/ --cov=app --cov-report=term-missing --cov-report=html

echo ""
echo "测试完成！"
echo "HTML 覆盖率报告生成在: htmlcov/index.html"