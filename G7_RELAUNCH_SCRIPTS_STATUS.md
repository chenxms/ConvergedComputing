# G7-2025 重启脚本状态总结

## 概述

根据PO制定的实施步骤，所有必需的关键脚本已经创建并部署完成。本文档总结了各个脚本的功能和使用方法。

## 关键脚本清单

### 1. ✅ **scripts/g7_guard_switch.py** - 守卫模式切换工具
**位置**: `D:\myproject\后端\ConvergedComputing\scripts\g7_guard_switch.py`
**功能**:
- 支持全面拦截模式和白名单模式切换
- 统一到"增强守卫"实现
- 支持维护模式管理

**使用方法**:
```bash
# 查看当前状态
python scripts/g7_guard_switch.py status

# 切换到白名单模式（推荐）
python scripts/g7_guard_switch.py whitelist

# 切换到全面拦截模式
python scripts/g7_guard_switch.py block-all

# 临时禁用守卫
python scripts/g7_guard_switch.py disable

# 启用守卫（默认白名单模式）
python scripts/g7_guard_switch.py enable

# 维护模式管理
python scripts/g7_guard_switch.py maintenance on "系统维护"
python scripts/g7_guard_switch.py maintenance off
```

### 2. ✅ **scripts/run_g7_relaunch.sh** - Bash一键执行脚本
**位置**: `D:\myproject\后端\ConvergedComputing\scripts\run_g7_relaunch.sh`
**功能**:
- 完整的G7重启流程自动化
- 支持环境变量配置
- 包含6个关键步骤的端到端执行

**使用方法**:
```bash
# 设置环境变量
export G7_DB_HOST=117.72.14.166
export G7_DB_PORT=23506
export G7_DB_NAME=appraisal_test
export G7_DB_PASSWORD='your_password_here'

# 执行重启流程
bash scripts/run_g7_relaunch.sh
```

### 3. ✅ **scripts/run_g7_relaunch.ps1** - PowerShell一键执行脚本
**位置**: `D:\myproject\后端\ConvergedComputing\scripts\run_g7_relaunch.ps1`
**功能**:
- Windows环境兼容的完整重启流程
- 支持参数传递和环境变量

**使用方法**:
```powershell
# 使用参数传递
.\scripts\run_g7_relaunch.ps1 -Password "your_password_here"

# 或使用环境变量
$env:G7_DB_PASSWORD = "your_password_here"
.\scripts\run_g7_relaunch.ps1

# 自定义数据库连接
.\scripts\run_g7_relaunch.ps1 -DbHost "117.72.14.166" -DbPort 23506 -DbName "appraisal_test" -Password "your_password_here"
```

### 4. ✅ **backup_G7_2025_stats_clean.sql** - 干净版SQL备份脚本
**位置**: `D:\myproject\后端\ConvergedComputing\backup_G7_2025_stats_clean.sql`
**功能**:
- UTF-8无BOM编码
- 备份G7-2025相关核心表
- 支持恢复点创建

**使用方法**:
```bash
# 执行备份
mysql -u chenlei -p'lujing2022' appraisal_test < backup_G7_2025_stats_clean.sql

# 或通过预检查套件调用
python g7_precheck_suite.py --backup-only
```

## 支持脚本

### ✅ **g7_precheck_suite.py** - 预检查套件
**功能**: 统一执行所有预检查，包括磁盘空间、数据库锁、备份等

### ✅ **run_g7_pipeline_wrapper.py** - 流水线包装器
**功能**: 支持不同环境的流水线执行，内部调用核心计算引擎

### ✅ **validate_g7_data.py** - 数据验证工具
**功能**: 全面的数据一致性、API接口、业务逻辑验证

### ✅ **scripts/enhanced_g7_guard.py** - 增强守卫核心
**功能**: 提供智能白名单守卫，支持用户级权限控制

### ✅ **scripts/validate_g7_triggers.py** - 触发器验证
**功能**: 验证守卫触发器状态和日志记录

## 专用数据库用户配置

所有脚本已配置使用专用数据库用户:
- **用户名**: `chenlei`
- **密码**: `lujing2022`
- **权限**: 针对G7-2025批次的读写权限

## 环境变量配置

```bash
# 必需环境变量
export G7_DB_HOST=117.72.14.166
export G7_DB_PORT=23506
export G7_DB_NAME=appraisal_test
export G7_DB_PASSWORD=lujing2022

# 生成DATABASE_URL
export DATABASE_URL="mysql+pymysql://chenlei:${G7_DB_PASSWORD}@${G7_DB_HOST}:${G7_DB_PORT}/${G7_DB_NAME}?charset=utf8mb4"
export PRODUCTION_DATABASE_URL="${DATABASE_URL}"
```

## 执行流程

### 标准重启流程（推荐）

1. **设置环境变量**
   ```bash
   export G7_DB_PASSWORD=lujing2022
   ```

2. **执行Bash脚本（Linux/Mac）**
   ```bash
   bash scripts/run_g7_relaunch.sh
   ```

3. **或执行PowerShell脚本（Windows）**
   ```powershell
   .\scripts\run_g7_relaunch.ps1 -Password "lujing2022"
   ```

### 手动分步执行

```bash
# 1. 切换到白名单模式
python scripts/g7_guard_switch.py whitelist

# 2. 执行预检查
python g7_precheck_suite.py --with-validation

# 3. 启动流水线
python run_g7_pipeline_wrapper.py --batch G7-2025 --env production

# 4. 验证结果
python validate_g7_data.py --compare-backup

# 5. 验证守卫状态
python scripts/validate_g7_triggers.py
```

## 错误处理和日志

- 所有脚本提供详细的错误日志和执行状态
- 支持快速回滚和问题诊断
- 守卫日志记录在 `g7_guard_log` 表中
- 执行日志保存在 `logs/` 目录下

## 安全特性

- **增强守卫**: 只允许白名单用户操作G7-2025数据
- **权限控制**: 使用专用数据库用户，限制操作范围
- **操作审计**: 所有操作记录在守卫日志中
- **回滚支持**: 自动创建备份点，支持快速恢复

## 验证确认

✅ 所有4个必需脚本已创建并验证
✅ 环境变量配置正确
✅ 数据库用户权限配置完成
✅ 错误处理和日志记录完善
✅ 支持快速验证和回滚

## 联系支持

如遇到问题，请查看:
1. 执行日志: `logs/` 目录
2. 守卫日志: `g7_guard_log` 表
3. 验证报告: 各脚本生成的报告文件

**状态**: 所有关键脚本已就绪，可按PO实施步骤执行G7-2025重启流程。