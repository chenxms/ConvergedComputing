# Docker容器同步开发指南

## 📋 问题背景

在增量开发过程中，Docker容器可能与本地开发环境不同步，导致：
- 新开发的文件未包含在容器中
- 容器内无法访问最新代码
- 本地测试通过但容器内运行失败

## 🔧 解决方案

### **1. 自动化Dockerfile**
```dockerfile
# 使用通配符自动复制所有相关文件
COPY *.py ./
COPY *.sql ./
```

### **2. .dockerignore优化**
排除不必要文件，减少构建时间和镜像大小。

### **3. 开发工具脚本**
提供自动化检查和同步工具。

## 🚀 推荐工作流程

### **日常开发**
```bash
# 1. 开发新功能
# 2. 同步容器
python dev-tools.py all
# 或
make sync
```

### **提交前检查**
```bash
# 构建和测试
make build test
# 或
python dev-tools.py build
```

### **问题排查**
```bash
# 检查同步状态
make check
# 或  
python dev-tools.py check
```

## 📊 可用命令

### **Python开发工具**
```bash
python dev-tools.py build     # 构建并测试
python dev-tools.py check     # 检查同步状态
python dev-tools.py verify    # 验证批次数据
python dev-tools.py all       # 完整流程
```

### **Makefile命令**
```bash
make help      # 显示帮助
make build     # 构建镜像
make test      # 测试功能
make check     # 检查同步
make sync      # 完整同步
make verify    # 验证数据
make clean     # 清理缓存
make dev       # 开发就绪
```

## 🔍 同步检查列表

### **构建前检查**
- [ ] 新Python文件已创建
- [ ] 导入路径正确
- [ ] 依赖已添加到requirements.txt

### **构建后验证**
- [ ] Docker镜像构建成功
- [ ] 容器内文件数量与本地一致
- [ ] 关键模块可正常导入
- [ ] 批次验证通过

### **部署前确认**
- [ ] 所有测试通过
- [ ] 数据库连接正常
- [ ] API接口响应正确

## ⚡ 性能优化

### **减少构建时间**
1. 使用.dockerignore排除大文件
2. 合理分层，利用Docker缓存
3. 只复制必要文件

### **减少镜像大小**
1. 使用多阶段构建
2. 清理临时文件
3. 选择轻量基础镜像

## 🐛 常见问题

### **模块导入失败**
```bash
# 检查文件是否复制到容器
docker-compose run --rm app ls -la *.py

# 测试特定模块导入
docker-compose run --rm app python -c "import module_name"
```

### **数据库连接问题**
```bash
# 检查环境变量
docker-compose run --rm app env | grep DATABASE

# 测试数据库连接
docker-compose run --rm app python -c "from sqlalchemy import create_engine; print('DB连接正常')"
```

### **权限问题**
```bash
# 检查文件权限
docker-compose run --rm app ls -la

# 修复权限（如需要）
chmod +x *.py
```

## 🔄 自动化集成

### **Git Hooks**
在`.git/hooks/pre-commit`中添加：
```bash
#!/bin/bash
echo "检查Docker同步状态..."
python dev-tools.py check
if [ $? -ne 0 ]; then
    echo "❌ Docker同步检查失败，请运行: python dev-tools.py all"
    exit 1
fi
```

### **CI/CD集成**
```yaml
# .github/workflows/docker-sync-check.yml
name: Docker Sync Check
on: [push, pull_request]
jobs:
  check-sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Build and test Docker
        run: |
          python dev-tools.py build
          python dev-tools.py check
```

## 💡 最佳实践

1. **开发新功能时**：立即运行同步检查
2. **提交代码前**：确保Docker测试通过  
3. **代码审查时**：检查Dockerfile是否需要更新
4. **部署前**：进行完整的容器化测试

通过这套流程，可以完全避免增量开发与容器化不同步的问题！