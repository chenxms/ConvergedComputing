# 团队公告｜G7-2025 汇聚重启执行指引（白名单-only）

- 生效时间：即日起
- 适用范围：后端 DEV、DBA、运维
- 目的：统一执行路径，避免旧流程对 G7-2025 的重复汇聚写入，保障新流程按期上线

## 链接
- DEV 实施步骤文档（唯一标准版本）：../G7_2025_汇聚重启实施步骤_DEV.md

## 关键点
- 本次采用“增强守卫 + 白名单-only”方案：仅放行专用 DB 用户（chenlei），不开维护模式，旧流程（非白名单）写入将被阻断。
- 先在本机按步骤完成演练，再在服务器重复相同命令，确保一致性。
- 权限已由 DBA 配置到位；如遇 CREATE/TRIGGER 报错，联系 DBA 校验授权。

## 可复制命令清单

### Bash（Linux/macOS/Git‑Bash）
```bash
# 1) 环境变量（替换 <host>/<port>/<db>）
export G7_DB_HOST=<host>
export G7_DB_PORT=<port>
export G7_DB_NAME=<db>
export G7_DB_PASSWORD='lujing2022'
export PRODUCTION_DATABASE_URL="mysql+pymysql://chenlei:${G7_DB_PASSWORD}@${G7_DB_HOST}:${G7_DB_PORT}/${G7_DB_NAME}?charset=utf8mb4"

# 2) 安装增强守卫 + 白名单-only（不开维护模式）
python scripts/enhanced_g7_guard.py install
python scripts/enhanced_g7_guard.py add-whitelist "chenlei%"
python scripts/enhanced_g7_guard.py disable-maintenance
python scripts/validate_g7_triggers.py --quick

# 3) 预检查与备份（调用干净版SQL）
python g7_precheck_suite.py --with-validation

# 4) 启动新流水线（包装器）
python run_g7_pipeline_wrapper.py --batch G7-2025 --env production

# 5) 数据与 API 验证
python validate_g7_data.py --compare-backup

# 6) 最终触发器校验（保护开启，白名单放行）
python scripts/validate_g7_triggers.py
```

### PowerShell（Windows）
```powershell
# 1) 环境变量（替换 <host>/<port>/<db>）
$env:G7_DB_HOST="<host>"
$env:G7_DB_PORT="<port>"
$env:G7_DB_NAME="<db>"
$env:G7_DB_PASSWORD="lujing2022"
$env:PRODUCTION_DATABASE_URL = "mysql+pymysql://chenlei:$env:G7_DB_PASSWORD@$($env:G7_DB_HOST):$($env:G7_DB_PORT)/$($env:G7_DB_NAME)?charset=utf8mb4"

# 2) 安装增强守卫 + 白名单-only（不开维护模式）
python scripts/enhanced_g7_guard.py install
python scripts/enhanced_g7_guard.py add-whitelist "chenlei%"
python scripts/enhanced_g7_guard.py disable-maintenance
python scripts/validate_g7_triggers.py --quick

# 3) 预检查与备份（调用干净版SQL）
python g7_precheck_suite.py --with-validation

# 4) 启动新流水线（包装器）
python run_g7_pipeline_wrapper.py --batch G7-2025 --env production

# 5) 数据与 API 验证
python validate_g7_data.py --compare-backup

# 6) 最终触发器校验（保护开启，白名单放行）
python scripts/validate_g7_triggers.py
```

### 一键脚本（可选）
- Bash：`bash scripts/run_g7_relaunch.sh`（需先 export G7_DB_* 环境变量）
- PowerShell：`powershell -ExecutionPolicy Bypass -File scripts/run_g7_relaunch.ps1 -DbHost <host> -DbPort <port> -DbName <db> -Password 'lujing2022'`

## 自检要点
- 触发器存在：`SHOW TRIGGERS LIKE 'statistical_aggregations'` → g7_enhanced_guard_insert / g7_enhanced_guard_update
- 守卫日志：`chenlei` 写入为 ALLOWED，其他账号写入 G7-2025 为 BLOCKED
- 验收模板：填写并归档 `docs/templates/validation_report.md`

> 如需帮助或发现偏差，请在工程群 @PM/@DBA 即时同步。

