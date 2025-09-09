# Docker部署指南 - 汇聚模块v1.2

生成日期：2025-09-09  
版本：v1.0  
适用范围：ConvergedComputing项目汇聚模块

---

## 一、快速开始

### 1.1 基本部署

```bash
# 1. 构建镜像
docker-compose build

# 2. 启动主服务
docker-compose up -d

# 3. 查看日志
docker-compose logs -f app

# 4. 健康检查
curl http://localhost:8000/health
```

### 1.2 批处理任务

```bash
# 运行批处理（使用profile）
docker-compose --profile batch up batch-processor

# 运行数据验证
docker-compose --profile validation up validator
```

---

## 二、环境配置

### 2.1 环境变量文件 (.env)

创建`.env`文件配置环境变量：

```env
# 数据库配置
DATABASE_URL=mysql+pymysql://user:password@host:port/database?charset=utf8mb4

# 应用配置
APP_ENV=production
LOG_LEVEL=INFO

# 性能配置
WORKERS=4
MAX_CONNECTIONS=100
POOL_SIZE=20
POOL_RECYCLE=3600

# 批处理配置
BATCH_SIZE=10
BATCH_TIMEOUT=300
```

### 2.2 必要的目录结构

```
ConvergedComputing/
├── docker-compose.yml
├── Dockerfile
├── .dockerignore
├── .env
├── logs/              # 日志输出目录
├── reports/           # 报告输出目录
├── temp/              # 临时文件目录
└── docs/              # 文档目录
```

---

## 三、服务说明

### 3.1 主应用服务 (app)

**功能**：提供API服务和汇聚功能

```yaml
服务名: app
端口: 8000
健康检查: /health
重启策略: unless-stopped
资源限制: CPU 2核, 内存 4GB
```

**访问接口**：
- 区域汇聚：`GET http://localhost:8000/api/v12/batch/{batch_code}/regional`
- 学校汇聚：`GET http://localhost:8000/api/v12/batch/{batch_code}/school/{school_code}`
- 批次物化：`POST http://localhost:8000/api/v12/batch/{batch_code}/materialize`

### 3.2 批处理服务 (batch-processor)

**功能**：执行批量数据处理任务

```bash
# 手动运行批处理
docker-compose run --rm batch-processor python process_g7_g8_v2.py G7-2025

# 重处理所有批次
docker-compose run --rm batch-processor python reprocess_all_batches_final.py
```

### 3.3 验证服务 (validator)

**功能**：数据质量验证和报告生成

```bash
# 运行验证报告
docker-compose --profile validation up validator

# 查看验证结果
cat reports/final_validation_report_*.json
```

### 3.4 可选服务

#### Redis缓存服务
```bash
# 启用Redis缓存
docker-compose --profile cache up -d redis
```

#### Nginx反向代理
```bash
# 启用Nginx（需要配置nginx.conf）
docker-compose --profile production up -d nginx
```

---

## 四、核心文件管理

### 4.1 已包含的核心文件

**汇聚引擎**：
- `enhanced_aggregation_engine_v2.py` - 优化版汇聚引擎（推荐）
- `final_aggregation_engine.py` - 完整功能版本
- `enhanced_aggregation_engine_optimized.py` - 性能优化版

**处理脚本**：
- `process_g7_g8_v2.py` - G7/G8批次处理
- `reprocess_all_batches_final.py` - 批量重处理
- `scripts/rewrite_subjects_v12.py` - 数据重写脚本
- `scripts/acceptance_quick_check.py` - 验收检查脚本

**工具文件**：
- `app/utils/precision.py` - 精度处理工具
- `app/services/subjects_builder.py` - 统一构建器
- `inspect_current_aggregation.py` - 数据检查
- `final_validation_report.py` - 验证报告

### 4.2 排除的文件（.dockerignore）

以下文件类型不会被包含到Docker镜像中：
- 测试文件：`test_*.py`, `*_test.py`
- 调试脚本：`debug_*.py`, `check_*.py`, `verify_*.py`
- 临时文件：`*.tmp`, `*.bak`, `_chunk.txt`
- 报告文件：`*_report_*.json`, `validation_results.json`
- 开发配置：`.git/`, `.vscode/`, `.idea/`

---

## 五、运维操作

### 5.1 日志管理

```bash
# 查看实时日志
docker-compose logs -f app

# 查看最近100行日志
docker-compose logs --tail=100 app

# 导出日志
docker-compose logs app > app_logs.txt

# 清理日志目录
rm -rf logs/*.log
```

### 5.2 数据备份

```bash
# 备份报告数据
tar -czf reports_backup_$(date +%Y%m%d).tar.gz reports/

# 备份验证结果
cp reports/final_validation_report_*.json backups/
```

### 5.3 性能监控

```bash
# 查看容器资源使用
docker stats converged-computing-app

# 查看容器详细信息
docker inspect converged-computing-app

# 进入容器调试
docker exec -it converged-computing-app bash
```

### 5.4 故障排查

```bash
# 检查容器状态
docker-compose ps

# 重启服务
docker-compose restart app

# 完全重建
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

---

## 六、批次处理指南

### 6.1 处理单个批次

```bash
# 在容器内执行
docker exec -it converged-computing-app python process_g7_g8_v2.py G4-2025
```

### 6.2 批量处理

```bash
# 使用批处理服务
docker-compose run --rm batch-processor
```

### 6.3 验证处理结果

```bash
# 运行验证
docker-compose run --rm app python final_validation_report.py

# 快速检查
docker-compose run --rm app python scripts/acceptance_quick_check.py
```

---

## 七、生产部署建议

### 7.1 安全配置

1. **使用环境变量管理敏感信息**
   ```bash
   # 不要在docker-compose.yml中硬编码密码
   DATABASE_URL=${DATABASE_URL}
   ```

2. **限制容器权限**
   - 使用非root用户运行（已配置）
   - 限制网络访问范围

3. **启用HTTPS**
   ```bash
   # 使用Nginx profile启用SSL
   docker-compose --profile production up -d nginx
   ```

### 7.2 性能优化

1. **调整资源限制**
   ```yaml
   deploy:
     resources:
       limits:
         cpus: '4'      # 根据服务器配置调整
         memory: 8G     # 根据数据量调整
   ```

2. **启用Redis缓存**
   ```bash
   docker-compose --profile cache up -d redis
   ```

3. **优化批处理参数**
   ```env
   BATCH_SIZE=20         # 增加批次大小
   BATCH_TIMEOUT=600     # 增加超时时间
   ```

### 7.3 监控和告警

1. **配置健康检查**
   - 默认每30秒检查一次
   - 连续3次失败后重启

2. **日志收集**
   ```bash
   # 使用Docker日志驱动
   docker-compose logs --follow --tail=100
   ```

3. **性能指标**
   - 监控CPU和内存使用率
   - 跟踪API响应时间
   - 记录批处理完成时间

---

## 八、常见问题

### Q1: 容器启动失败
```bash
# 检查端口占用
netstat -tulpn | grep 8000

# 查看详细错误
docker-compose logs app
```

### Q2: 数据库连接失败
```bash
# 验证数据库连接
docker-compose run --rm app python -c "from app.database.connection import get_db; print('Connected')"

# 检查环境变量
docker-compose config
```

### Q3: 批处理超时
```bash
# 增加超时设置
BATCH_TIMEOUT=600 docker-compose run --rm batch-processor
```

### Q4: 内存不足
```bash
# 调整内存限制
docker-compose down
# 修改docker-compose.yml中的memory限制
docker-compose up -d
```

---

## 九、版本升级

### 9.1 备份当前版本
```bash
# 备份镜像
docker save converged-computing-app:latest > app_backup.tar

# 备份数据
tar -czf data_backup.tar.gz reports/ logs/
```

### 9.2 升级步骤
```bash
# 1. 停止服务
docker-compose down

# 2. 更新代码
git pull origin main

# 3. 重建镜像
docker-compose build --no-cache

# 4. 启动新版本
docker-compose up -d

# 5. 验证升级
docker-compose run --rm app python scripts/acceptance_quick_check.py
```

### 9.3 回滚方案
```bash
# 恢复备份镜像
docker load < app_backup.tar

# 重启服务
docker-compose up -d
```

---

## 十、清理和维护

### 10.1 清理未使用的资源
```bash
# 清理未使用的镜像
docker image prune -a

# 清理未使用的容器
docker container prune

# 清理未使用的卷
docker volume prune

# 清理所有未使用的资源
docker system prune -a
```

### 10.2 定期维护任务
```bash
# 每周清理日志
0 0 * * 0 find /app/logs -name "*.log" -mtime +7 -delete

# 每月备份报告
0 0 1 * * tar -czf /backup/reports_$(date +\%Y\%m).tar.gz /app/reports/

# 每日健康检查
0 */6 * * * curl -f http://localhost:8000/health || docker-compose restart app
```

---

## 附录A：Docker命令速查

```bash
# 构建
docker-compose build [service]

# 启动
docker-compose up -d [service]

# 停止
docker-compose stop [service]

# 重启
docker-compose restart [service]

# 删除
docker-compose down

# 日志
docker-compose logs -f [service]

# 执行命令
docker-compose exec [service] [command]

# 运行一次性任务
docker-compose run --rm [service] [command]
```

## 附录B：环境变量参考

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| DATABASE_URL | - | 数据库连接字符串 |
| APP_ENV | production | 运行环境 |
| LOG_LEVEL | INFO | 日志级别 |
| WORKERS | 4 | 工作进程数 |
| BATCH_SIZE | 10 | 批处理大小 |
| BATCH_TIMEOUT | 300 | 批处理超时(秒) |

---

**文档维护**：开发团队  
**最后更新**：2025-09-09