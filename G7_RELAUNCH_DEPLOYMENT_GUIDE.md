# G7-2025 重启部署指南

## 🎯 概述

根据PO制定的实施步骤，所有必需的关键脚本已创建完成并通过验证。本指南提供完整的G7-2025重启流程部署说明。

## ✅ 验证状态

**最新验证结果**: 所有12个关键脚本已就绪
- ✅ 通过: 12
- ❌ 失败: 0
- ⚠️ 警告: 0

**验证时间**: 2025-09-19 14:06:21

## 📋 必需脚本清单

### 1. 核心执行脚本

| 脚本 | 位置 | 功能 | 大小 |
|------|------|------|------|
| `g7_guard_switch.py` | `scripts/` | 守卫模式切换工具 | 11.5KB |
| `run_g7_relaunch.sh` | `scripts/` | Bash一键执行脚本 | 1.9KB |
| `run_g7_relaunch.ps1` | `scripts/` | PowerShell一键执行脚本 | 1.7KB |
| `backup_G7_2025_stats_clean.sql` | 根目录 | 干净版SQL备份脚本 | 2.8KB |

### 2. 支持工具脚本

| 脚本 | 位置 | 功能 | 大小 |
|------|------|------|------|
| `g7_precheck_suite.py` | 根目录 | 预检查套件 | 15.2KB |
| `run_g7_pipeline_wrapper.py` | 根目录 | 流水线包装器 | 5.7KB |
| `validate_g7_data.py` | 根目录 | 数据验证工具 | 36.7KB |
| `enhanced_g7_guard.py` | `scripts/` | 增强守卫核心 | 22.4KB |
| `validate_g7_triggers.py` | `scripts/` | 触发器验证工具 | 21.6KB |

### 3. 预检查依赖脚本

| 脚本 | 位置 | 功能 | 大小 |
|------|------|------|------|
| `check_no_active_old_pipeline.py` | 根目录 | 旧流水线进程检查 | 12.3KB |
| `check_disk_space.py` | 根目录 | 磁盘空间检查 | 13.2KB |
| `check_db_locks_enhanced.py` | 根目录 | 数据库锁状态检查 | 24.1KB |

## 🚀 快速部署流程

### 环境准备

```bash
# 1. 导出必需环境变量
export G7_DB_HOST=117.72.14.166
export G7_DB_PORT=23506
export G7_DB_NAME=appraisal_test
export G7_DB_PASSWORD=lujing2022

# 2. 验证脚本就绪状态
python scripts/verify_g7_scripts_readiness.py
```

### Linux/Mac 环境（推荐）

```bash
# 一键执行完整重启流程
bash scripts/run_g7_relaunch.sh
```

### Windows 环境

```powershell
# PowerShell执行
.\scripts\run_g7_relaunch.ps1 -Password "lujing2022"
```

## 📝 标准重启流程（6步骤）

自动化脚本会按以下顺序执行:

1. **[1/6] 安装增强守卫** - 切换到白名单模式，允许chenlei用户操作
2. **[2/6] 触发器快速校验** - 验证守卫触发器正常工作
3. **[3/6] 预检查与备份** - 执行全面预检查并创建备份
4. **[4/6] 启动新流水线** - 执行G7-2025批次汇聚计算
5. **[5/6] 数据与API验证** - 验证结果数据和API接口
6. **[6/6] 触发器最终校验** - 确认守卫状态正常

## 🔧 手动分步执行（可选）

如需手动控制每个步骤:

```bash
# 1. 切换到白名单模式
python scripts/g7_guard_switch.py whitelist

# 2. 验证守卫状态
python scripts/g7_guard_switch.py status

# 3. 执行预检查
python g7_precheck_suite.py --with-validation

# 4. 启动流水线
python run_g7_pipeline_wrapper.py --batch G7-2025 --env production

# 5. 验证数据
python validate_g7_data.py --compare-backup

# 6. 最终验证
python scripts/validate_g7_triggers.py
```

## 🔐 安全特性

### 数据库访问控制
- **专用用户**: `chenlei` / `lujing2022`
- **权限范围**: 仅G7-2025批次相关操作
- **访问模式**: 白名单守卫保护

### 守卫机制
- **增强守卫**: 智能白名单，阻断非授权写入
- **操作审计**: 所有操作记录在`g7_guard_log`表
- **实时监控**: 守卫状态实时检查

### 数据保护
- **自动备份**: 执行前自动创建恢复点
- **版本控制**: 备份数据带时间戳
- **回滚支持**: 快速数据恢复能力

## 📊 监控和日志

### 执行日志
- **守卫日志**: `g7_guard_log` 数据库表
- **执行日志**: `logs/` 目录下的文件
- **验证报告**: `reports/` 目录下的JSON报告

### 状态检查命令
```bash
# 查看守卫状态
python scripts/g7_guard_switch.py status

# 验证触发器
python scripts/validate_g7_triggers.py --quick

# 检查脚本就绪状态
python scripts/verify_g7_scripts_readiness.py
```

## ⚠️ 注意事项

### 执行前检查
- [ ] 确保数据库连接正常
- [ ] 验证所有脚本存在且语法正确
- [ ] 检查磁盘空间充足
- [ ] 确认无其他汇聚进程运行

### 执行中监控
- [ ] 观察守卫日志，确认只有白名单用户写入
- [ ] 监控流水线执行进度
- [ ] 检查内存和CPU使用情况

### 执行后验证
- [ ] 验证数据一致性
- [ ] 测试API接口响应
- [ ] 确认守卫状态正常
- [ ] 检查业务指标正确性

## 🆘 故障排除

### 常见问题

1. **环境变量未设置**
   ```bash
   export G7_DB_PASSWORD=lujing2022
   ```

2. **数据库连接失败**
   ```bash
   # 检查网络连接和数据库服务状态
   mysql -h 117.72.14.166 -P 23506 -u chenlei -p
   ```

3. **守卫触发器异常**
   ```bash
   # 重新安装守卫
   python scripts/enhanced_g7_guard.py install
   ```

4. **磁盘空间不足**
   ```bash
   # 清理临时文件和日志
   python check_disk_space.py --cleanup
   ```

### 紧急回滚

如遇重大问题需要回滚:
```bash
# 1. 禁用守卫
python scripts/g7_guard_switch.py disable

# 2. 恢复备份数据
mysql -u chenlei -p appraisal_test < backup_restore_script.sql

# 3. 重新启用守卫
python scripts/g7_guard_switch.py whitelist
```

## 📞 支持联系

**脚本维护**: DevOps团队
**数据问题**: 数据工程师
**业务验证**: 产品团队

---

## 🎉 部署确认

✅ 所有必需脚本已创建并验证
✅ 环境变量配置说明完整
✅ 安全控制机制已实施
✅ 监控和日志记录完善
✅ 故障排除指南已提供

**状态**: 已就绪，可按PO实施步骤执行G7-2025重启流程

**最后更新**: 2025-09-19
**验证版本**: v1.0