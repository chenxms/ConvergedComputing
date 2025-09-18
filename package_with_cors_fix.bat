@echo off
setlocal enabledelayedexpansion

REM ===================================
REM 项目打包脚本 - Windows版本（包含CORS修复）
REM ===================================

echo =====================================
echo 教育统计分析服务 - 打包工具（CORS修复版）
echo =====================================

REM 获取当前时间作为版本号
for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "VERSION=%dt:~0,8%_%dt:~8,6%"
set "OUTPUT_DIR=deployment_package_cors_%VERSION%"
set "ARCHIVE_NAME=converged-computing-cors-fixed_%VERSION%.zip"

echo 版本: %VERSION%
echo.

REM 创建打包目录
echo [1/7] 创建打包目录...
if exist "%OUTPUT_DIR%" rmdir /s /q "%OUTPUT_DIR%"
mkdir "%OUTPUT_DIR%"

REM ===================================
REM 复制核心程序文件
REM ===================================

echo [2/7] 复制核心程序文件...
xcopy /E /I /Q app "%OUTPUT_DIR%\app" > nul

REM 确保middleware目录存在
if not exist "%OUTPUT_DIR%\app\middleware" mkdir "%OUTPUT_DIR%\app\middleware"
if not exist "%OUTPUT_DIR%\app\middleware\__init__.py" echo. > "%OUTPUT_DIR%\app\middleware\__init__.py"

REM 删除Python缓存
echo [3/7] 清理Python缓存...
for /d /r "%OUTPUT_DIR%\app" %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d" 2>nul
del /s /q "%OUTPUT_DIR%\app\*.pyc" 2>nul

REM 复制脚本文件
echo [4/7] 复制脚本和服务文件...
if not exist "%OUTPUT_DIR%\scripts" mkdir "%OUTPUT_DIR%\scripts"
if exist "scripts\rewrite_subjects_v12.py" copy "scripts\rewrite_subjects_v12.py" "%OUTPUT_DIR%\scripts\" > nul
if exist "scripts\acceptance_quick_check.py" copy "scripts\acceptance_quick_check.py" "%OUTPUT_DIR%\scripts\" > nul
if exist "scripts\clean_g4_statistical_data.py" copy "scripts\clean_g4_statistical_data.py" "%OUTPUT_DIR%\scripts\" > nul
if exist "scripts\rebuild_g4_aggregation.py" copy "scripts\rebuild_g4_aggregation.py" "%OUTPUT_DIR%\scripts\" > nul
if exist "scripts\test_database_connection.py" copy "scripts\test_database_connection.py" "%OUTPUT_DIR%\scripts\" > nul

REM 复制服务文件
if exist "data_cleaning_service.py" copy "data_cleaning_service.py" "%OUTPUT_DIR%\" > nul

REM 复制文档
if exist "docs" xcopy /E /I /Q docs "%OUTPUT_DIR%\docs" > nul

REM ===================================
REM 复制Docker和部署文件
REM ===================================

echo [5/7] 复制Docker和部署配置...
copy Dockerfile "%OUTPUT_DIR%\" > nul
copy docker-compose.yml "%OUTPUT_DIR%\" > nul
if exist ".dockerignore" copy .dockerignore "%OUTPUT_DIR%\" > nul
if exist ".env.example" copy .env.example "%OUTPUT_DIR%\" > nul
if exist "deploy.sh" copy deploy.sh "%OUTPUT_DIR%\" > nul
if exist "DEPLOYMENT_GUIDE.md" copy DEPLOYMENT_GUIDE.md "%OUTPUT_DIR%\" > nul
if exist "DEPLOYMENT_CHECKLIST.md" copy DEPLOYMENT_CHECKLIST.md "%OUTPUT_DIR%\" > nul

REM 复制CORS相关文件
if exist "nginx.conf" copy nginx.conf "%OUTPUT_DIR%\nginx.conf.example" > nul
if exist "CORS_FIX_DEPLOYMENT.md" copy CORS_FIX_DEPLOYMENT.md "%OUTPUT_DIR%\" > nul

REM 复制依赖文件
if exist "requirements.txt" copy requirements.txt "%OUTPUT_DIR%\" > nul
if exist "requirements-prod.txt" copy requirements-prod.txt "%OUTPUT_DIR%\" > nul

REM ===================================
REM 创建必要目录
REM ===================================

echo [6/7] 创建运行时目录...
mkdir "%OUTPUT_DIR%\logs" 2>nul
mkdir "%OUTPUT_DIR%\temp" 2>nul
mkdir "%OUTPUT_DIR%\reports" 2>nul
mkdir "%OUTPUT_DIR%\config" 2>nul

REM ===================================
REM 创建配置文件
REM ===================================

echo [7/7] 生成配置文件...

REM 创建.env.production
(
echo # 生产环境配置
echo # 请根据实际情况修改
echo.
echo # 数据库配置
echo DATABASE_URL=mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4
echo.
echo # 应用配置
echo APP_ENV=production
echo LOG_LEVEL=INFO
echo DEBUG=false
echo.
echo # CORS配置
echo CORS_ORIGINS=["http://localhost:8080", "http://117.72.14.166:8080", "*"]
echo CORS_ALLOW_CREDENTIALS=true
echo CORS_ALLOW_METHODS=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]
echo CORS_ALLOW_HEADERS=["*"]
echo.
echo # 性能配置
echo WORKERS=4
echo MAX_CONNECTIONS=100
echo POOL_SIZE=20
echo POOL_RECYCLE=3600
echo.
echo # 批处理配置
echo BATCH_SIZE=10
echo BATCH_TIMEOUT=300
echo.
echo # 写入控制
echo DISABLE_WRITES_FOR_BATCHES=G7-2025
) > "%OUTPUT_DIR%\.env.production"

REM 创建README
(
echo # 教育统计分析服务 - 部署包（CORS修复版）
echo.
echo ## 版本信息
echo - **包含CORS修复**: 解决前端跨域访问问题
echo - **构建时间**: %VERSION%
echo.
echo ## 快速部署
echo.
echo ### Docker部署
echo ```bash
echo # 1. 配置环境变量
echo cp .env.production .env
echo vi .env  # 修改数据库连接
echo.
echo # 2. 构建并启动
echo docker-compose build
echo docker-compose up -d
echo.
echo # 3. 验证服务
echo curl http://localhost:8010/health
echo ```
echo.
echo ## CORS问题解决
echo.
echo 本版本已包含CORS修复：
echo 1. 增强的CORS中间件（`app/middleware/cors_config.py`）
echo 2. 支持OPTIONS预检请求
echo 3. Nginx配置示例（`nginx.conf.example`）
echo.
echo 详见 `CORS_FIX_DEPLOYMENT.md`
echo.
echo ## 验证CORS配置
echo.
echo ```bash
echo # 测试OPTIONS请求
echo curl -X OPTIONS http://localhost:8010/api/v12/batch/G7-2025/regional \
echo   -H "Origin: http://localhost:8080" \
echo   -H "Access-Control-Request-Method: GET" -v
echo ```
echo.
echo ## 目录结构
echo.
echo ```
echo deployment_package/
echo ├── app/                    # 核心应用代码
echo │   ├── main.py            # 主应用（已更新CORS）
echo │   ├── middleware/        # 中间件
echo │   │   └── cors_config.py # CORS配置
echo │   ├── api/               # API路由
echo │   ├── services/          # 业务逻辑
echo │   └── database/          # 数据访问
echo ├── scripts/               # 维护脚本
echo ├── docs/                  # 文档
echo ├── docker-compose.yml     # Docker编排
echo ├── Dockerfile            # Docker镜像定义
echo ├── .env.production       # 生产环境配置模板
echo ├── nginx.conf.example    # Nginx配置示例
echo └── CORS_FIX_DEPLOYMENT.md # CORS修复说明
echo ```
echo.
echo ## 故障排查
echo.
echo 1. **CORS仍有问题**
echo    - 检查防火墙是否开放8010端口
echo    - 检查是否有Nginx反向代理
echo    - 查看 `CORS_FIX_DEPLOYMENT.md`
echo.
echo 2. **服务无法启动**
echo    ```bash
echo    docker-compose logs --tail=100
echo    ```
echo.
echo 3. **数据库连接失败**
echo    - 检查 `.env` 文件中的DATABASE_URL
echo    - 确认数据库服务可访问
) > "%OUTPUT_DIR%\README.md"

REM ===================================
REM 创建压缩包
REM ===================================

echo.
echo 正在创建压缩包...

REM 检查是否有PowerShell可用于压缩
where powershell >nul 2>nul
if %errorlevel%==0 (
    powershell -Command "Compress-Archive -Path '%OUTPUT_DIR%' -DestinationPath '%ARCHIVE_NAME%' -Force"
    if exist "%ARCHIVE_NAME%" (
        echo 压缩包创建成功！
    ) else (
        echo 压缩失败，但文件已准备在目录: %OUTPUT_DIR%
    )
) else (
    echo PowerShell不可用，跳过压缩步骤
    echo 文件已准备在目录: %OUTPUT_DIR%
)

REM 统计文件
for /f %%A in ('dir /s /b "%OUTPUT_DIR%\*" ^| find /c /v ""') do set FILE_COUNT=%%A

echo.
echo =====================================
echo 打包完成！
echo =====================================
echo 输出目录: %OUTPUT_DIR%
echo 包含文件: %FILE_COUNT% 个
if exist "%ARCHIVE_NAME%" echo 压缩包: %ARCHIVE_NAME%
echo =====================================
echo 特性：
echo [√] 包含CORS修复
echo [√] 支持OPTIONS预检请求
echo [√] 包含Nginx配置示例
echo [√] 包含生产环境配置模板
echo =====================================
echo.

REM 询问是否删除临时目录
if exist "%ARCHIVE_NAME%" (
    set /p DELETE_TEMP="是否删除临时目录 %OUTPUT_DIR%? (Y/N): "
    if /i "!DELETE_TEMP!"=="Y" (
        rmdir /s /q "%OUTPUT_DIR%"
        echo 临时目录已删除
    ) else (
        echo 临时目录保留在: %OUTPUT_DIR%
    )
)

echo.
echo 部署包已准备就绪！
echo 请将文件发送给运维团队进行部署。
echo.
pause