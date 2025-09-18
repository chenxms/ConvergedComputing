# 生产环境部署清单

## 📦 交付物

- **部署包文件**: `converged-computing-production-v1.2.0.tar.gz` (304KB)
- **版本号**: v1.2.0
- **打包时间**: 2024-12-12

## 🚀 部署前准备

### 1. 服务器环境检查
- [ ] Ubuntu 20.04 LTS 或 CentOS 7+
- [ ] Docker 20.10.0+ 已安装
- [ ] Docker Compose 1.29.0+ 已安装
- [ ] 8GB+ 可用内存
- [ ] 50GB+ 可用磁盘空间
- [ ] 端口 8000, 80, 443 未被占用

### 2. 数据库准备
- [ ] MySQL 8.0+ 已安装并运行
- [ ] 创建专用数据库（如：appraisal_prod）
- [ ] 创建数据库用户并授权
- [ ] 测试数据库连接

## 📋 部署步骤

### 1. 上传并解压部署包
```bash
# 上传文件到服务器
scp converged-computing-production-v1.2.0.tar.gz user@server:/opt/

# 登录服务器并解压
ssh user@server
cd /opt
tar -xzf converged-computing-production-v1.2.0.tar.gz
cd converged-computing-production-v1.2.0
```

### 2. 配置环境变量
```bash
# 复制配置模板
cp config/.env.production.example config/.env.production

# 编辑配置文件
vim config/.env.production
```

**必须修改的配置项**：
- `DATABASE_URL`: MySQL连接字符串
- `API_KEY`: 设置强密码
- `REDIS_PASSWORD`: Redis密码
- `CORS_ORIGINS`: 允许的前端域名

### 3. 执行部署
```bash
# 运行一键部署脚本
bash deploy.sh
```

### 4. 验证部署
```bash
# 检查服务状态
docker ps

# 测试健康检查
curl http://localhost:8000/health

# 查看API文档
curl http://localhost:8000/docs
```

## ✅ 部署后验证

### 功能验证
- [ ] 健康检查接口正常响应
- [ ] API文档页面可访问
- [ ] 数据库连接正常
- [ ] Redis缓存正常工作

### 接口测试
```bash
# 获取批次列表
curl -H "X-API-Key: your-api-key" \
     http://localhost:8000/api/v1/management/batches

# 获取学校统计数据（示例）
curl -H "X-API-Key: your-api-key" \
     http://localhost:8000/api/v1/statistics/school/G4-2025/5068
```

## 🔧 常用运维命令

### 服务管理
```bash
# 查看日志
docker-compose -f docker/docker-compose.production.yml logs -f

# 重启服务
docker-compose -f docker/docker-compose.production.yml restart

# 停止服务
docker-compose -f docker/docker-compose.production.yml down
```

### 批处理任务
```bash
# 执行批处理
docker-compose -f docker/docker-compose.production.yml \
    run --rm -e BATCH_CODE=G4-2025 batch-processor
```

## ⚠️ 注意事项

1. **安全配置**
   - 修改所有默认密码
   - 限制数据库访问IP
   - 配置防火墙规则
   - 启用HTTPS（生产环境必须）

2. **性能优化**
   - 根据服务器配置调整WORKERS数量
   - 配置适当的数据库连接池大小
   - 启用Redis缓存

3. **监控告警**
   - 配置日志收集
   - 设置健康检查告警
   - 监控资源使用情况

4. **数据备份**
   - 定期备份MySQL数据库
   - 备份应用日志和报告

## 📞 技术支持联系方式

- 部署问题：检查 `docs/部署指南.md`
- API问题：查看 `docs/API接口文档.md`
- 紧急支持：support@convergedcomputing.com

---

**请确保所有步骤都已完成并验证通过后，再向前端团队提供API访问信息。**