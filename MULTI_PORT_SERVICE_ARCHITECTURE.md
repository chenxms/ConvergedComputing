# 多端口服务架构说明

## 🏗️ 服务架构

项目包含**两个独立的FastAPI服务**，运行在不同端口：

### 1. 主服务 (Main API Service)
- **容器端口**: 8000
- **主机端口**: 8010
- **入口文件**: `app/main.py`
- **容器名称**: `converged-computing-app`
- **API路径**:
  - `/api/v1/management/*` - 管理API
  - `/api/v1/reporting/*` - 报告API
  - `/api/v1/statistics/*` - 统计计算API
  - `/api/v12/*` - 也包含v1.2接口（与独立服务重复）

### 2. Subjects v1.2专用服务 (Subjects v1.2 Service)
- **容器端口**: 8001
- **主机端口**: 8011
- **入口文件**: `app/subjects_api_main.py`
- **容器名称**: `subjects-v12-api`
- **API路径**: `/api/v12/*` - 专门处理v1.2版本的subjects接口

## 📋 端口映射关系

```
外部访问端口 -> 容器内部端口
8010 -> 8000 (主服务)
8011 -> 8001 (v1.2服务)
```

## 🔍 为什么有两个服务？

1. **性能隔离**: v1.2接口计算密集，独立部署可避免影响主服务
2. **独立扩展**: 可以根据负载独立调整workers数量
3. **版本管理**: v1.2是新版本API，独立部署便于迭代

## ⚠️ CORS问题原因

前端访问 `http://117.72.14.166:8010/api/v12/batch/G7-2025/regional` 出现CORS错误，可能的原因：

1. **URL混淆**:
   - v1.2接口应该访问 **8011端口**，而不是8010
   - 正确URL: `http://117.72.14.166:8011/api/v12/batch/G7-2025/regional`

2. **两个服务都需要CORS配置**:
   - 主服务（8010）
   - v1.2服务（8011）

## ✅ CORS修复方案

### 已完成的修复：

1. **创建增强CORS中间件** (`app/middleware/cors_config.py`)
   - 支持OPTIONS预检请求
   - 自动处理所有CORS头

2. **更新两个服务入口**:
   - `app/main.py` - 主服务已应用新CORS配置
   - `app/subjects_api_main.py` - v1.2服务已应用新CORS配置

3. **Docker配置确认**:
   ```yaml
   services:
     app:
       ports:
         - "8010:8000"  # 主服务

     subjects:
       ports:
         - "8011:8001"  # v1.2服务
   ```

## 🚀 部署步骤

### 1. 重新构建镜像
```bash
docker-compose build
```

### 2. 重启所有服务
```bash
docker-compose down
docker-compose up -d
```

### 3. 验证两个服务的CORS

#### 测试主服务（8010端口）
```bash
curl -X OPTIONS http://117.72.14.166:8010/api/v1/management/batch \
  -H "Origin: http://localhost:8080" \
  -H "Access-Control-Request-Method: GET" \
  -v
```

#### 测试v1.2服务（8011端口）
```bash
curl -X OPTIONS http://117.72.14.166:8011/api/v12/batch/G7-2025/regional \
  -H "Origin: http://localhost:8080" \
  -H "Access-Control-Request-Method: GET" \
  -v
```

## 📝 前端调用指南

### 正确的API端点：

```javascript
// 主服务API（端口8010）
const mainApi = {
  management: 'http://117.72.14.166:8010/api/v1/management',
  reporting: 'http://117.72.14.166:8010/api/v1/reporting',
  statistics: 'http://117.72.14.166:8010/api/v1/statistics'
}

// v1.2专用服务API（端口8011）
const v12Api = {
  batch: 'http://117.72.14.166:8011/api/v12/batch',
  regional: 'http://117.72.14.166:8011/api/v12/batch/{batchCode}/regional',
  school: 'http://117.72.14.166:8011/api/v12/batch/{batchCode}/school'
}
```

### 前端代理配置（开发环境）：

```javascript
// vue.config.js 或 vite.config.js
module.exports = {
  devServer: {
    proxy: {
      '/api/v1': {
        target: 'http://117.72.14.166:8010',
        changeOrigin: true
      },
      '/api/v12': {
        target: 'http://117.72.14.166:8011',
        changeOrigin: true
      }
    }
  }
}
```

## 🔧 Nginx配置（如果使用）

如果使用Nginx反向代理，需要配置两个upstream：

```nginx
upstream main_api {
    server 127.0.0.1:8010;
}

upstream v12_api {
    server 127.0.0.1:8011;
}

server {
    listen 80;

    # 主服务路由
    location /api/v1/ {
        proxy_pass http://main_api;
        # CORS头配置...
    }

    # v1.2服务路由
    location /api/v12/ {
        proxy_pass http://v12_api;
        # CORS头配置...
    }
}
```

## 📊 服务状态检查

```bash
# 查看运行的容器
docker ps

# 应该看到两个容器：
# - converged-computing-app (8010:8000)
# - subjects-v12-api (8011:8001)

# 检查主服务健康状态
curl http://117.72.14.166:8010/health

# 检查v1.2服务健康状态
curl http://117.72.14.166:8011/health
```

## ⚡ 性能配置

- 主服务：4个workers（处理大部分请求）
- v1.2服务：2个workers（专门处理计算密集型任务）

## 🎯 总结

1. **两个端口**：8010（主服务）和 8011（v1.2服务）
2. **前端需要根据API类型访问正确的端口**
3. **两个服务都已配置CORS支持**
4. **部署包已包含所有必要的修复**