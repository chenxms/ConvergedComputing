# 教育统计分析服务 - 部署包（CORS修复版）

## 版本信息
- **版本号**: 20250918_000749
- **特性**: 包含CORS跨域问题修复
- **构建时间**: 2025-09-18 00:07:57

## 重要更新
✅ **CORS问题已修复**
- 增强的CORS中间件支持
- OPTIONS预检请求正确处理
- 支持多源配置

## 快速部署指南

### 1. 解压部署包
```bash
unzip converged-computing-cors-fixed_20250918_000749.zip
cd deployment_package_cors_20250918_000749
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
curl -X OPTIONS http://localhost:8010/api/v12/batch/<BATCH_CODE>/regional \
  -H "Origin: http://localhost:8080" \
  -H "Access-Control-Request-Method: GET" \
  -H "Access-Control-Request-Headers: Content-Type" \
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
proxy: {
  '/api': {
    target: 'http://117.72.14.166:8010',
    changeOrigin: true
  }
}
```

## 文件结构
```
deployment_package_cors_20250918_000749/
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

## 补充说明
- 当前版本仅提供只读 API，所有写入/清洗操作已在代码层禁用。
- subjects v1.2 接口缺数据时返回 404，不会触发重建；报告接口返回 503 表示在线重建被禁止。
- 如需执行批处理脚本，请在运行命令时显式传入目标批次，例如：
  ```bash
  python batch_aggregation_runner.py G4-2025 G8-2025
  python batch_cleaning_runner.py --database-url <DB_URL> G4-2025
  ```
- 若需阻断写入批次，请在 `.env` 中配置 `DISABLE_WRITES_FOR_BATCHES=G4-2025,G8-2025` 等实际清单。
