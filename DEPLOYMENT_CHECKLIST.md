# 部署清单 - 运维交付材料

## 一、交付文件清单

### 必需文件
- [x] `Dockerfile` - Docker镜像构建文件
- [x] `docker-compose.yml` - Docker Compose编排文件
- [x] `.env.example` - 环境变量配置模板
- [x] `deploy.sh` - 自动化部署脚本
- [x] `DEPLOYMENT_GUIDE.md` - 详细部署指南

### 可选文件
- [x] `nginx.conf.example` - Nginx反向代理配置(生产环境)
- [ ] SSL证书文件 (需要单独提供)

## 二、环境要求确认

### 服务器配置
- [ ] CPU: 最低2核，推荐4核
- [ ] 内存: 最低4GB，推荐8GB
- [ ] 硬盘: 最低20GB，推荐50GB SSD
- [ ] 操作系统: Ubuntu 20.04+ / CentOS 7+ / Debian 10+

### 软件依赖
- [ ] Docker >= 20.10
- [ ] Docker Compose >= 2.0
- [ ] Git (可选，用于代码更新)

### 网络配置
- [ ] 开放端口 8010 (主API服务)
- [ ] 开放端口 8011 (科目API服务)
- [ ] 开放端口 6379 (Redis，可选)
- [ ] 开放端口 80/443 (Nginx，生产环境)

## 三、数据库准备

### MySQL数据库
- [ ] MySQL版本 >= 8.0
- [ ] 字符集: utf8mb4
- [ ] 排序规则: utf8mb4_unicode_ci
- [ ] 最大连接数 >= 200
- [ ] 创建专用数据库和用户

### 数据库权限
需要以下权限：
- SELECT, INSERT, UPDATE, DELETE
- CREATE, ALTER, DROP (用于表结构管理)
- INDEX (用于创建索引)
- EXECUTE (用于存储过程，如需要)

## 四、部署步骤概要

### 1. 基础部署流程
```bash
# 1. 上传项目文件到服务器
scp -r ./ConvergedComputing user@server:/opt/

# 2. 登录服务器
ssh user@server

# 3. 进入项目目录
cd /opt/ConvergedComputing

# 4. 配置环境变量
cp .env.example .env
vim .env  # 修改数据库连接等配置

# 5. 运行部署脚本
chmod +x deploy.sh
./deploy.sh --basic  # 基础部署
# 或
./deploy.sh --full   # 完整部署
```

### 2. 验证部署
```bash
# 检查服务状态
docker-compose ps

# 健康检查
curl http://localhost:8010/health

# 查看日志
docker-compose logs -f app
```

## 五、关键配置项

### 必须配置的环境变量
```env
DATABASE_URL=mysql+pymysql://用户:密码@主机:端口/数据库?charset=utf8mb4
APP_ENV=production
WORKERS=4  # 根据CPU核心数调整
```

### 建议优化的配置
```env
LOG_LEVEL=INFO
MAX_CONNECTIONS=100
POOL_SIZE=20
POOL_RECYCLE=3600
```

## 六、监控和维护

### 日志位置
- 容器内: `/app/logs/`
- 宿主机: `./logs/`

### 常用命令
```bash
# 查看服务状态
./deploy.sh --status

# 查看日志
./deploy.sh --logs

# 重启服务
./deploy.sh --restart

# 更新部署
./deploy.sh --update

# 停止服务
./deploy.sh --stop
```

### 性能监控
```bash
# 查看资源使用
docker stats

# 查看容器日志
docker-compose logs --tail=100 app
```

## 七、故障排查

### 常见问题
1. **端口被占用**
   ```bash
   netstat -tlnp | grep 8010
   # 修改docker-compose.yml中的端口映射
   ```

2. **数据库连接失败**
   - 检查DATABASE_URL配置
   - 确认数据库服务可访问
   - 检查防火墙规则

3. **内存不足**
   - 调整docker-compose.yml中的资源限制
   - 清理Docker缓存: `docker system prune -a`

## 八、安全建议

### 生产环境必做
- [ ] 修改所有默认密码
- [ ] 配置HTTPS (使用nginx.conf.example)
- [ ] 限制数据库访问IP
- [ ] 配置防火墙规则
- [ ] 定期备份数据
- [ ] 启用日志审计

### SSL证书配置
```bash
# 使用Let's Encrypt获取免费证书
apt-get install certbot
certbot certonly --standalone -d your-domain.com
```

## 九、备份策略

### 数据库备份
```bash
# 创建备份脚本
cat > backup.sh << 'EOF'
#!/bin/bash
BACKUP_DIR="/backup/mysql"
DATE=$(date +%Y%m%d_%H%M%S)
mysqldump -h [数据库主机] -u [用户] -p[密码] [数据库名] > $BACKUP_DIR/backup_$DATE.sql
# 保留最近7天的备份
find $BACKUP_DIR -name "backup_*.sql" -mtime +7 -delete
EOF

# 添加到crontab
crontab -e
# 每天凌晨2点备份
0 2 * * * /opt/backup.sh
```

### 日志备份
```bash
# 日志轮转配置
cat > /etc/logrotate.d/converged-computing << EOF
/opt/ConvergedComputing/logs/*.log {
    daily
    rotate 30
    compress
    delaycompress
    notifempty
    create 0644 appuser appuser
    sharedscripts
    postrotate
        docker-compose restart app
    endscript
}
EOF
```

## 十、联系方式

### 技术支持
- 项目文档: 见 `docs/` 目录
- API文档: 部署后访问 http://[服务器IP]:8010/docs

### 问题反馈模板
```
环境信息:
- 操作系统: 
- Docker版本: 
- 错误时间: 
- 错误描述: 

错误日志:
[粘贴相关日志]

重现步骤:
1. 
2. 
3. 
```

## 十一、快速验证清单

部署完成后，请验证以下项目：

- [ ] 主API服务可访问 (http://localhost:8010/health)
- [ ] API文档可查看 (http://localhost:8010/docs)
- [ ] 数据库连接正常
- [ ] 日志文件正常生成
- [ ] 容器资源使用正常
- [ ] 所有配置的服务都已启动

## 十二、附加说明

### 性能调优建议
1. 根据实际负载调整WORKERS数量
2. 优化数据库连接池大小
3. 启用Redis缓存提升性能
4. 使用Nginx进行负载均衡

### 扩展部署
如需横向扩展，可以：
1. 使用Docker Swarm或Kubernetes
2. 配置多个应用实例
3. 使用外部负载均衡器
4. 分离数据库到独立服务器

---

**注意**: 本清单为标准部署流程，具体环境可能需要调整。部署前请仔细阅读`DEPLOYMENT_GUIDE.md`完整文档。