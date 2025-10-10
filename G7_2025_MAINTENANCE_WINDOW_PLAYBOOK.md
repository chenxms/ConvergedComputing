# G7-2025 维护窗口执行手册

> 🚨 **关键文档** - 请严格按照本手册执行，任何偏离需立即上报PM
>
> 版本：v1.0
> 批次：G7-2025
> 维护窗口：22:00-24:00（建议时段）
> 最后更新：2025-09-20

---

## 📋 执行前检查清单

### 团队就位确认
- [ ] PM - 总指挥，风险控制
- [ ] DEV - 执行操作，问题排查
- [ ] DBA - 数据库支持，性能监控
- [ ] QA - 数据验证，质量把关

### 环境准备确认
- [ ] 所有脚本已部署到生产环境
- [ ] 数据库连接配置已验证
- [ ] 备份存储空间充足（需要至少50GB）
- [ ] 监控终端已开启
- [ ] 回滚脚本已就绪

### 通知确认
- [ ] 业务方已知晓维护窗口
- [ ] 运维团队已准备就绪
- [ ] 紧急联系方式已更新

---

## 🕐 时间线与执行步骤

### T-30min：预准备阶段
```bash
# 1. 进入工作目录
cd D:\myproject\后端\ConvergedComputing

# 2. 验证环境变量
echo $DB_NAME
echo $DB_USER
echo $DB_PASSWORD

# 3. 执行预检查套件（包含验证与备份）
python g7_precheck_suite.py --with-validation

# 4. 检查预检查结果
cat logs/precheck_report_*.log
```

**检查点**：
- ✅ 所有预检查通过
- ✅ 备份文件已生成
- ✅ 无活跃的旧流水线进程

---

### T-10min：最终准备
```bash
# 1. 启动实时监控（在独立终端）
python monitor_g7_pipeline.py \
    --emergency-stop \
    --duration 7200 \
    --alert-threshold 100

# 2. 记录当前数据状态
python validate_g7_data.py --snapshot-only

# 3. 最后确认
echo "Ready to start maintenance window? (yes/no)"
```

---

### T0：维护窗口开始

#### 阶段1：停止旧流水线（T0 - T0+5min）
```bash
# 1. 检查并停止旧进程
python check_no_active_old_pipeline.py --kill-if-found

# 2. 验证停止状态
ps aux | grep -E "batch|aggregation|G7" | grep -v grep

# 3. 记录操作
echo "[$(date)] Old pipeline stopped" >> maintenance_log.txt
```

**决策点**：如发现无法停止的进程，执行紧急停止：
```bash
python ultimate_stop.py --force
```

---

#### 阶段2：调整触发器守卫（T0+5min - T0+10min）

**方案A：启用增强守卫 + 白名单模式（推荐）**
```bash
# 1. 启动维护窗口（120分钟）
python scripts/maintenance_window_manager.py start --duration 120

# 2. 安装增强守卫并切换到白名单模式（写入默认白名单）
python scripts/g7_guard_switch.py whitelist

# 3. 快速验证触发器与白名单
python scripts/validate_g7_triggers.py --quick
```

**方案B：临时移除守卫（备选）**
```bash
# 1. 移除触发器
mysql -u $DB_USER -p$DB_PASSWORD $DB_NAME < quick_remove_g7_guard.sql

# 2. 验证移除（查看当前触发器）
mysql -u $DB_USER -p$DB_PASSWORD $DB_NAME -e "SHOW TRIGGERS LIKE 'statistical_aggregations'\G"
```

**验证点**：
- ✅ 新流水线用户可写入
- ✅ 普通用户被拦截（方案A）或所有用户可写入（方案B）

---

#### 阶段3：执行新流水线（T0+10min - T0+40min）

```bash
# 1. 启动G7-2025批次汇聚
# 方案A：使用包装器脚本（推荐，支持 --batch 和 --env 参数）
python run_g7_pipeline_wrapper.py \
    --batch G7-2025 \
    --env production \
    --verbose \
    2>&1 | tee logs/g7_pipeline_$(date +%Y%m%d_%H%M%S).log

# 方案B：直接使用原始脚本（仅位置参数）
python run_full_batch_pipeline.py G7-2025 \
    2>&1 | tee logs/g7_pipeline_$(date +%Y%m%d_%H%M%S).log

# 2. 实时监控进度（在监控终端查看）
# monitor_g7_pipeline.py 会自动显示进度
```

**监控要点**：
- 📊 写入速度：应保持在1000-5000条/秒
- 💾 内存使用：不超过系统内存的70%
- ⚠️ 错误率：低于0.1%
- 🔄 重试次数：不超过3次

**异常处理**：
```bash
# 如发现异常，立即执行
python monitor_g7_pipeline.py --emergency-stop

# 查看错误详情
tail -n 100 logs/g7_pipeline_*.log | grep ERROR

# 决策：继续/回滚
# 如需回滚
python scripts/g7_guard_rollback.py --emergency
```

---

#### 阶段4：数据验证（T0+40min - T0+50min）

```bash
# 1. 执行全面验证
python validate_g7_data.py \
    --mode comprehensive \
    --compare-backup \
    --test-api \
    --output-report

# 2. 查看验证报告
cat reports/g7_validation_report_*.json

# 3. 测试关键API
curl -X GET "http://localhost:8000/api/v12/batch/G7-2025/regional"
curl -X GET "http://localhost:8000/api/v12/batch/G7-2025/school/SC001"
```

**验收标准**：
- ✅ 数据总量匹配（偏差<0.1%）
- ✅ 核心指标正确（平均分、等级分布）
- ✅ API响应正常
- ✅ 无数据缺失

---

#### 阶段5：收尾工作（T0+50min - T0+60min）

```bash
# 1. 确保增强守卫安装且白名单开启（如已卸载）
python scripts/g7_guard_switch.py whitelist

# 2. 关闭维护模式
python scripts/maintenance_window_manager.py stop

# 3. 最终验证（完整验证）
python scripts/validate_g7_triggers.py

# 4. 填写验收报告模板（docs/templates/validation_report.md）并归档
```

---

## 🔄 回滚方案

### 快速回滚（<5分钟）
```bash
# 1. 停止所有进程
python ultimate_stop.py --force

# 2. 恢复数据
mysql -u $DB_USER -p$DB_PASSWORD $DB_NAME < backups/g7_2025_backup_*.sql

# 3. 重新安装守卫
python scripts/install_g7_guard.py

# 4. 验证回滚
python validate_g7_data.py --compare-backup
```

### 部分回滚（保留部分数据）
```bash
# 1. 识别问题数据
python identify_corrupted_data.py --batch G7-2025

# 2. 选择性恢复
python selective_restore.py --tables "statistical_aggregations"

# 3. 重新计算
python recalculate_metrics.py --batch G7-2025 --metrics "failed_only"
```

---

## 📞 紧急联系方式

| 角色 | 姓名 | 电话 | 备注 |
|------|------|------|------|
| PM | 张经理 | 138-xxxx-xxxx | 总负责人 |
| DEV Lead | 李工 | 139-xxxx-xxxx | 技术执行 |
| DBA | 王工 | 137-xxxx-xxxx | 数据库支持 |
| 运维值班 | - | 400-xxx-xxxx | 7x24小时 |

---

## 📊 监控仪表板

### 关键指标监控
```bash
# 终端1：数据库监控
watch -n 1 'mysql -u $DB_USER -p$DB_PASSWORD -e "SHOW PROCESSLIST" | grep G7'

# 终端2：日志监控
tail -f logs/g7_pipeline_*.log | grep -E "ERROR|WARNING|SUCCESS"

# 终端3：系统资源
htop

# 终端4：实时监控脚本
python monitor_g7_pipeline.py
```

### 告警阈值
- CPU使用率 > 80%：警告
- 内存使用率 > 70%：警告
- 磁盘使用率 > 90%：严重
- 错误率 > 1%：严重
- 无响应时间 > 30秒：紧急

---

## ✅ 执行后检查清单

### 立即检查（T0+60min）
- [ ] 所有进程正常结束
- [ ] 数据验证全部通过
- [ ] 触发器守卫已恢复
- [ ] API接口响应正常
- [ ] 无异常日志

### 24小时内检查
- [ ] 用户反馈收集
- [ ] 性能指标分析
- [ ] 数据质量复检
- [ ] 备份文件归档
- [ ] 复盘会议召开

### 一周内完成
- [ ] 维护报告编写
- [ ] 流程优化建议
- [ ] 文档更新
- [ ] 知识库更新
- [ ] 下次维护计划

---

## 📝 操作日志模板

```
[时间] [操作人] [操作内容] [结果] [备注]
示例：
[22:00] [李工] 开始执行预检查 [通过] 所有检查项正常
[22:10] [李工] 启动G7批次汇聚 [进行中] PID:12345
[22:40] [王工] 数据验证 [通过] 核心指标偏差<0.1%
```

---

## ⚠️ 注意事项

1. **双人复核**：所有关键操作需要第二人确认
2. **及时记录**：每步操作都要记录到日志
3. **保持沟通**：团队保持实时沟通，使用统一的沟通群
4. **优先安全**：宁可延长窗口，不可冒险操作
5. **备份优先**：任何修改前必须有备份

---

## 🎯 成功标准

维护窗口成功的标志：
1. ✅ G7-2025数据完整更新
2. ✅ 无数据丢失或损坏
3. ✅ 所有API正常工作
4. ✅ 性能指标达标
5. ✅ 用户无感知（除维护通知外）

---

**文档维护**：本手册需要在每次维护后更新，记录实际执行情况和改进建议。

最后更新人：[待填写]
最后更新时间：[待填写]
