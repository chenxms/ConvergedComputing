# Docker开发辅助Makefile

.PHONY: help build test check sync verify clean

# 默认目标
help:
	@echo "🛠️  Docker同步开发工具"
	@echo ""
	@echo "可用命令:"
	@echo "  make build   - 重新构建Docker镜像"
	@echo "  make test    - 测试Docker容器功能"  
	@echo "  make check   - 检查文件同步状态"
	@echo "  make sync    - 完整同步流程 (build + test + check)"
	@echo "  make verify  - 验证批次清洗结果"
	@echo "  make clean   - 清理Docker缓存"
	@echo ""
	@echo "💡 推荐工作流程:"
	@echo "  1. 开发后: make sync"
	@echo "  2. 提交前: make test"
	@echo "  3. 问题排查: make check"

# 构建Docker镜像
build:
	@echo "🔧 重新构建Docker镜像..."
	docker-compose build

# 测试Docker容器
test:
	@echo "🧪 测试Docker容器功能..."
	docker-compose run --rm app python -c "print('✅ Docker容器运行正常')"
	docker-compose run --rm app python -c "import clean_batch; print('✅ clean_batch模块导入成功')"
	docker-compose run --rm app python -c "import aggregation_api; print('✅ aggregation_api模块导入成功')"

# 检查文件同步
check:
	@echo "🔍 检查文件同步状态..."
	docker-compose run --rm app ls -la *.py | head -10

# 完整同步流程  
sync: build test check
	@echo "✅ Docker同步完成！"

# 验证批次数据
verify:
	@echo "🔬 验证G4-2025批次数据..."
	docker-compose run --rm app python clean_batch.py G4-2025

# 清理Docker缓存
clean:
	@echo "🧹 清理Docker缓存..."
	docker system prune -f
	docker-compose down --rmi all --volumes --remove-orphans

# 快速开发测试
dev: sync verify
	@echo "🎉 开发环境就绪！"