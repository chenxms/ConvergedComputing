# G7-2025 汇聚重启预检查脚本套件

本套件为G7-2025批次汇聚重启任务提供完整的预检查、监控和回滚脚本，确保安全可靠的汇聚重启操作。

## 脚本概览

### 预检查脚本
1. **check_no_active_old_pipeline.py** - 检查旧流水线进程
2. **check_disk_space.py** - 检查磁盘空间
3. **check_db_locks_enhanced.py** - 检查数据库锁状态（增强版）
4. **backup_G7_2025_stats_clean.sql** - G7-2025数据备份脚本（UTF-8 无BOM）

### 监控脚本
5. **monitor_g7_pipeline.py** - G7汇聚流水线实时监控

### 验证脚本
6. **validate_g7_data.py** - G7-2025数据验证

### 回滚脚本
7. **quick_remove_g7_guard.sql** - 快速移除G7触发器
8. **scripts/uninstall_g7_guard.py** - 卸载G7写入守卫
9. **ultimate_stop.py** - 终极阻断方案

### 统一执行脚本
10. **g7_precheck_suite.py** - 预检查套件统一入口
11. **run_g7_precheck.bat** / **run_g7_precheck.sh** - 批处理执行脚本

## 使用指南

### 一键执行预检查（推荐）

#### Windows系统：
```bash
# 完整预检查
run_g7_precheck.bat

# 快速检查（跳过备份和验证）
run_g7_precheck.bat quick

# 仅执行备份
run_g7_precheck.bat backup-only

# 仅执行验证
run_g7_precheck.bat validation-only
```

#### Linux/macOS系统：
```bash
# 给脚本执行权限
chmod +x run_g7_precheck.sh

# 完整预检查
./run_g7_precheck.sh

# 快速检查
./run_g7_precheck.sh quick

# 仅执行备份
./run_g7_precheck.sh backup-only

# 仅执行验证
./run_g7_precheck.sh validation-only
```

### Python直接执行

```bash
# 完整预检查套件
python g7_precheck_suite.py

# 快速检查模式
python g7_precheck_suite.py --quick

# 仅备份
python g7_precheck_suite.py --backup-only

# 仅验证
python g7_precheck_suite.py --validation-only

# 包含验证的完整检查
python g7_precheck_suite.py --with-validation

# 自动清理数据库锁
python g7_precheck_suite.py --auto-kill-db-locks
```

### 单独执行各脚本

#### 1. 检查旧流水线进程
```bash
python check_no_active_old_pipeline.py
```

#### 2. 检查磁盘空间
```bash
python check_disk_space.py
```

#### 3. 检查数据库锁状态
```bash
# 标准检查
python check_db_locks_enhanced.py

# 专注G7相关检查
python check_db_locks_enhanced.py --g7-focus

# 自动清理阻塞进程
python check_db_locks_enhanced.py --auto-kill

# 持续监控模式
python check_db_locks_enhanced.py --continuous --interval 30 --duration 3600
```

#### 4. 执行数据备份
```bash
# 通过预检查套件执行（推荐）
python g7_precheck_suite.py --backup-only

# 或直接通过MySQL执行
mysql -u username -p database_name < backup_G7_2025_stats_clean.sql
```

#### 5. 实时监控G7流水线
```bash
# 标准监控
python monitor_g7_pipeline.py

# 仅显示告警
python monitor_g7_pipeline.py --alert-only

# 监控1小时
python monitor_g7_pipeline.py --duration 3600

# 启用紧急停止检测
python monitor_g7_pipeline.py --emergency-stop

# 自动紧急停止
python monitor_g7_pipeline.py --emergency-stop --auto-emergency-stop
```

#### 6. 数据验证
```bash
# 完整验证
python validate_g7_data.py

# 快速验证
python validate_g7_data.py --quick

# 仅验证API
python validate_g7_data.py --api-only

# 与备份数据对比
python validate_g7_data.py --compare-backup

# 指定API基础URL
python validate_g7_data.py --api-base-url http://localhost:8000
```

#### 7. 快速回滚操作
```bash
# 移除G7触发器
mysql -u username -p database_name < quick_remove_g7_guard.sql

# 卸载G7写入守卫
python scripts/uninstall_g7_guard.py

# 终极阻断（紧急情况）
python ultimate_stop.py

# 移除阻断触发器
python ultimate_stop.py --remove-triggers

# 测试阻断效果
python ultimate_stop.py --test
```

## 执行顺序建议

### G7-2025汇聚重启标准流程：

1. **预检查阶段**
   ```bash
   # 执行完整预检查
   python g7_precheck_suite.py --with-validation
   ```

2. **启动监控**
   ```bash
   # 在单独终端启动监控
   python monitor_g7_pipeline.py --emergency-stop --duration 7200
   ```

3. **执行汇聚重启**
   ```bash
   # 在主终端执行汇聚流程
   python run_full_batch_pipeline.py --batch G7-2025 --env production
   ```

4. **验证结果**
   ```bash
   # 汇聚完成后验证数据
   python validate_g7_data.py --compare-backup
   ```

5. **清理监控**
   ```bash
   # 停止监控脚本（Ctrl+C）
   # 如需要，执行最终验证
   python validate_g7_data.py --api-only
   ```

## 输出文件说明

### 报告文件
- `g7_precheck_report_YYYYMMDD_HHMMSS.txt` - 预检查总结报告
- `g7_monitor_report_YYYYMMDD_HHMMSS.txt` - 监控总结报告
- `g7_validation_report_YYYYMMDD_HHMMSS.txt` - 数据验证报告

### 日志文件
脚本可配置输出日志到指定文件：
```bash
python monitor_g7_pipeline.py --log-file /path/to/monitor.log
python check_db_locks_enhanced.py --report-file /path/to/db_status.txt
```

## 错误处理

### 常见问题及解决方案

1. **Python依赖缺失**
   ```bash
   pip install psutil requests pandas sqlalchemy
   ```

2. **数据库连接失败**
   - 检查环境变量配置
   - 确认数据库服务运行状态
   - 验证连接参数

3. **脚本权限问题**
   ```bash
   # Linux/macOS
   chmod +x *.sh
   chmod +x *.py
   ```

4. **磁盘空间不足**
   - 清理临时文件
   - 清理Docker镜像和容器
   - 压缩或删除旧日志文件

5. **数据库锁超时**
   ```bash
   # 自动清理阻塞进程
   python check_db_locks_enhanced.py --auto-kill
   ```

## 安全注意事项

1. **备份验证** - 执行任何操作前确认备份成功
2. **权限控制** - 确保脚本在适当权限下运行
3. **监控保障** - 重要操作期间保持监控运行
4. **回滚准备** - 准备好快速回滚方案
5. **环境隔离** - 在生产环境执行前先在测试环境验证

## 技术支持

如遇到问题，请按以下顺序排查：

1. **查看详细日志输出**
2. **检查先决条件是否满足**
3. **验证环境配置**
4. **查阅错误代码和消息**
5. **联系技术支持团队**

---

**重要提醒**: 本套脚本专门为G7-2025批次汇聚重启设计，请在充分理解各脚本功能和风险的基础上使用。生产环境操作前务必在测试环境充分验证。
