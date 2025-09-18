# API Key认证与404错误解决方案

## 🔍 问题诊断

### 当前问题：
1. **前端报404错误**：访问 `http://117.72.14.166:8000/api/v12/batch/G7-2025/regional`
2. **运维配置了API_KEY**：`JDCIWWDAODAJJFAAFAJFJjdsmdjf23232`
3. **后端原本没有API Key验证代码**

### 404错误的可能原因：

#### 原因1：Nginx/网关拦截（最可能）
运维可能在Nginx层面配置了API Key验证，没有API Key的请求被直接返回404

#### 原因2：服务未正确启动
Docker容器可能没有正确运行

## ✅ 解决方案

### 方案A：前端添加API Key Header（立即解决）

前端需要在所有请求中添加API Key：

```javascript
// 在axios请求拦截器中添加
axios.interceptors.request.use(config => {
  // 添加API Key到请求头
  config.headers['X-API-Key'] = 'JDCIWWDAODAJJFAAFAJFJjdsmdjf23232';
  return config;
});

// 或者在具体请求中添加
const response = await axios.get(
  'http://117.72.14.166:8000/api/v12/batch/G7-2025/regional',
  {
    headers: {
      'X-API-Key': 'JDCIWWDAODAJJFAAFAJFJjdsmdjf23232'
    }
  }
);
```

### 方案B：后端支持API Key（已实现）

已创建API Key认证中间件：
- 文件：`app/middleware/api_key_auth.py`
- 支持从Header或查询参数获取API Key
- 通过环境变量控制是否启用

#### 部署配置：

1. **启用API Key认证**
```bash
# .env文件
ENABLE_API_KEY_AUTH=true
API_KEY=JDCIWWDAODAJJFAAFAJFJjdsmdjf23232
```

2. **Docker部署**
```yaml
environment:
  - ENABLE_API_KEY_AUTH=true
  - API_KEY=JDCIWWDAODAJJFAAFAJFJjdsmdjf23232
```

## 📝 验证步骤

### 1. 先验证服务是否运行
```bash
# 不带API Key测试（应该返回401或403，而不是404）
curl http://117.72.14.166:8000/health

# 如果返回404，说明服务没运行或路径错误
```

### 2. 测试带API Key的请求
```bash
# 使用Header方式
curl -H "X-API-Key: JDCIWWDAODAJJFAAFAJFJjdsmdjf23232" \
     http://117.72.14.166:8000/api/v12/batch/G7-2025/regional

# 使用查询参数方式（备选）
curl http://117.72.14.166:8000/api/v12/batch/G7-2025/regional?api_key=JDCIWWDAODAJJFAAFAJFJjdsmdjf23232
```

### 3. 检查Nginx配置
如果运维在Nginx配置了API Key验证，可能的配置：
```nginx
server {
    location /api/ {
        # API Key验证
        if ($http_x_api_key != "JDCIWWDAODAJJFAAFAJFJjdsmdjf23232") {
            return 404;  # 或者401/403
        }

        proxy_pass http://127.0.0.1:8000;
    }
}
```

## 🎯 推荐解决步骤

### 立即解决（前端）：
1. **前端添加API Key Header**
2. **使用正确的请求格式**：
```javascript
// subjectsV12.js 中的请求
const config = {
  headers: {
    'X-API-Key': 'JDCIWWDAODAJJFAAFAJFJjdsmdjf23232',
    'Content-Type': 'application/json'
  }
};

const response = await axios.get(
  `${API_BASE}/api/v12/batch/${batchCode}/regional`,
  config
);
```

### 长期解决（后端）：
1. **部署新的后端包**（包含API Key认证）
2. **配置环境变量启用认证**
3. **统一认证机制**

## 🔧 调试建议

### 1. 确认服务状态
```bash
# SSH到服务器
docker ps | grep converged
docker logs converged-computing-app --tail 100
```

### 2. 测试不同端点
```bash
# 测试根路径
curl http://117.72.14.166:8000/

# 测试健康检查
curl http://117.72.14.166:8000/health

# 测试API文档
curl http://117.72.14.166:8000/docs
```

### 3. 查看Nginx日志
```bash
# 如果使用Nginx
tail -f /var/log/nginx/access.log
tail -f /var/log/nginx/error.log
```

## 📋 总结

**最可能的问题**：运维在Nginx/网关层配置了API Key验证，没有API Key的请求被拦截返回404

**最快解决方案**：前端在所有请求中添加 `X-API-Key` Header

**Header名称可能是**：
- `X-API-Key`（最常见）
- `Authorization`
- `API-Key`
- `x-api-key`

建议前端先尝试添加 `X-API-Key` Header，如果还是404，可以询问运维具体的Header名称。