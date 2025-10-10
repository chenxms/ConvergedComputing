@echo off
REM G7-2025 汇聚重启预检查脚本 (Windows批处理版本)
REM
REM 用法：
REM   run_g7_precheck.bat                    # 完整预检查
REM   run_g7_precheck.bat quick              # 快速检查
REM   run_g7_precheck.bat backup-only        # 仅备份
REM   run_g7_precheck.bat validation-only    # 仅验证

echo =============================================
echo G7-2025 汇聚重启预检查套件
echo =============================================
echo 开始时间: %date% %time%
echo.

REM 检查Python环境
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python未安装或不在PATH中
    goto :error
)

REM 设置参数
set ARGS=
if "%1"=="quick" set ARGS=--quick
if "%1"=="backup-only" set ARGS=--backup-only
if "%1"=="validation-only" set ARGS=--validation-only

REM 执行预检查套件
echo 执行预检查套件...
python g7_precheck_suite.py %ARGS%

if errorlevel 1 (
    echo.
    echo [ERROR] 预检查失败，请查看上述错误信息
    goto :error
) else (
    echo.
    echo [SUCCESS] 预检查完成
    goto :success
)

:error
echo.
echo =============================================
echo 预检查失败，请修复问题后重新运行
echo =============================================
pause
exit /b 1

:success
echo.
echo =============================================
echo 预检查成功，可以继续进行G7-2025汇聚重启
echo =============================================
pause
exit /b 0