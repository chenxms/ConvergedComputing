# CORS问题修复部署指南

## 问题描述
前端（http://localhost:8080）访问后端API（http://117.72.14.166:8010）时出现CORS错误。

## 解决方案

### 方案1：更新后端代码（推荐）

1. **部署新的后端代码**
   ```bash
   # 1. 更新代码
   git pull

   # 2. 重新构建Docker镜像
   docker-compose build

   # 3. 重启服务
   docker-compose down
   docker-compose up -d
   ```

2. **验证CORS配置**
   ```bash
   # 测试OPTIONS请求
   curl -X OPTIONS http://117.72.14.166:8010/api/v12/batch/<BATCH_CODE>/regional \
     -H "Origin: http://localhost:8080" \
     -H "Access-Control-Request-Method: GET" \
     -H "Access-Control-Request-Headers: Content-Type" \
     -v
   ```

### 方案2：配置Nginx（如果使用了Nginx）

1. **检查是否使用了Nginx**
   ```bash
   # 在服务器上执行
   ps aux | grep nginx
   netstat -tulpn | grep :80
   ```

2. **如果使用了Nginx，更新配置**
   - 将 `nginx.conf` 文件中的配置添加到服务器的Nginx配置中
   - 重启Nginx：
     ```bash
     nginx -t  # 测试配置
     nginx -s reload  # 重新加载配置
     ```

### 方案3：临时解决方案（仅用于开发测试）

1. **直接访问后端端口**
   如果没有使用Nginx，确保防火墙开放8010端口：
   ```bash
   # 检查防火墙规则
   iptables -L -n | grep 8010

   # 开放端口（如果需要）
   iptables -A INPUT -p tcp --dport 8010 -j ACCEPT
   ```

2. **前端使用代理**
   在前端项目的 `vue.config.js` 或 `vite.config.js` 中配置代理：
   ```javascript
   module.exports = {
     devServer: {
       proxy: {
         '/api': {
           target: 'http://117.72.14.166:8010',
           changeOrigin: true,
           ws: true,
           pathRewrite: {
             '^/api': '/api'
           }
         }
       }
     }
   }
   ```

## 验证步骤

1. **测试CORS头**
   ```bash
   # GET请求测试
   curl -H "Origin: http://localhost:8080" \
        -H "Access-Control-Request-Method: GET" \
        -H "Access-Control-Request-Headers: X-Requested-With" \
        -X GET http://117.72.14.166:8010/api/v12/batch/<BATCH_CODE>/regional \
        -v 2>&1 | grep -i "access-control"
   ```

2. **检查响应头**
   响应应包含以下头：
   - `Access-Control-Allow-Origin: *` 或 `Access-Control-Allow-Origin: http://localhost:8080`
   - `Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS, PATCH`
   - `Access-Control-Allow-Headers: *`
   - `Access-Control-Allow-Credentials: true`

## 问题排查

### 1. 确认服务运行状态
```bash
docker ps | grep converged-computing
curl http://117.72.14.166:8010/health
```

### 2. 查看服务日志
```bash
docker logs converged-computing-app --tail 100
```

### 3. 检查网络连通性
```bash
# 从前端机器测试
telnet 117.72.14.166 8010
ping 117.72.14.166
```

## 联系方式

如果问题仍未解决，请联系：
- 后端开发团队：检查CORS中间件配置
- 运维团队：检查Nginx/防火墙配置

## 注意事项

1. **生产环境安全性**
   - 不要在生产环境使用 `allow_origins=["*"]`
   - 应该配置具体的前端域名列表

2. **性能考虑**
   - OPTIONS预检请求可以设置 `max_age` 减少请求次数
   - 当前配置为3600秒（1小时）

3. **更新部署包**
   新的部署包已包含增强的CORS配置：
   - `app/middleware/cors_config.py` - 增强的CORS中间件
   - `app/main.py` - 已更新使用新的CORS配置