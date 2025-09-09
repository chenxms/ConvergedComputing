@echo off
setlocal enabledelayedexpansion

echo ===================================
echo 教育统计分析服务 - 部署包打包工具
echo ===================================

:: 设置变量
set PACKAGE_NAME=converged-computing
for /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%c%%a%%b)
for /f "tokens=1-2 delims=/: " %%a in ('time /t') do (set mytime=%%a%%b)
set mytime=%mytime: =%
set VERSION=%mydate%_%mytime%
set OUTPUT_DIR=deployment_package_%VERSION%
set ARCHIVE_NAME=%PACKAGE_NAME%_%VERSION%.tar

echo 版本: %VERSION%
echo.

:: 创建打包目录
echo [1/7] 创建打包目录...
if exist %OUTPUT_DIR% rmdir /s /q %OUTPUT_DIR%
mkdir %OUTPUT_DIR%

:: 复制核心程序文件
echo [2/7] 复制核心程序文件...
xcopy /E /I /Q app %OUTPUT_DIR%\app
if exist scripts mkdir %OUTPUT_DIR%\scripts
if exist scripts\rewrite_subjects_v12.py copy scripts\rewrite_subjects_v12.py %OUTPUT_DIR%\scripts\ >nul
if exist scripts\acceptance_quick_check.py copy scripts\acceptance_quick_check.py %OUTPUT_DIR%\scripts\ >nul
if exist scripts\complete_questionnaire_labels.py copy scripts\complete_questionnaire_labels.py %OUTPUT_DIR%\scripts\ >nul
if exist docs xcopy /E /I /Q docs %OUTPUT_DIR%\docs

:: 复制Docker文件
echo [3/7] 复制Docker配置文件...
copy Dockerfile %OUTPUT_DIR%\ >nul
copy docker-compose.yml %OUTPUT_DIR%\ >nul
if exist .dockerignore copy .dockerignore %OUTPUT_DIR%\ >nul

:: 复制部署文件
echo [4/7] 复制部署文档和脚本...
copy .env.example %OUTPUT_DIR%\ >nul
copy deploy.sh %OUTPUT_DIR%\ >nul
copy package.sh %OUTPUT_DIR%\ >nul
copy DEPLOYMENT_GUIDE.md %OUTPUT_DIR%\ >nul
copy DEPLOYMENT_CHECKLIST.md %OUTPUT_DIR%\ >nul
copy DEPLOYMENT_FILES_CHECKLIST.md %OUTPUT_DIR%\ >nul
if exist nginx.conf.example copy nginx.conf.example %OUTPUT_DIR%\ >nul

:: 复制依赖文件
echo [5/7] 复制依赖文件...
if exist requirements.txt copy requirements.txt %OUTPUT_DIR%\ >nul
if exist pyproject.toml copy pyproject.toml %OUTPUT_DIR%\ >nul
if exist poetry.lock copy poetry.lock %OUTPUT_DIR%\ >nul

:: 创建必要目录
echo [6/7] 创建运行时目录...
mkdir %OUTPUT_DIR%\logs
mkdir %OUTPUT_DIR%\temp
mkdir %OUTPUT_DIR%\reports

:: 清理Python缓存
echo [7/7] 清理缓存文件...
for /d /r %OUTPUT_DIR% %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"
del /s /q %OUTPUT_DIR%\*.pyc 2>nul

echo.
echo =====================================
echo 打包完成！
echo 输出目录: %OUTPUT_DIR%
echo =====================================
echo.
echo 请使用以下工具创建压缩包：
echo 1. 使用7-Zip: 右键点击 %OUTPUT_DIR% 文件夹 -^> 7-Zip -^> 添加到压缩包
echo 2. 使用WinRAR: 右键点击 %OUTPUT_DIR% 文件夹 -^> 添加到压缩文件
echo 3. 使用Windows自带: 右键点击 %OUTPUT_DIR% 文件夹 -^> 发送到 -^> 压缩文件夹
echo.
pause