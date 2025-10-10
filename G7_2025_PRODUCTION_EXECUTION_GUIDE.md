# G7-2025 生产环境执行指南

> 准备完成时间：2025-09-19
> 目标：在生产环境安全执行 G7-2025 汇聚重启，使用“增强守卫 + 白名单-only”。

---

## 一、执行前确认清单

环境准备（必选）
- [x] 关键脚本与工具就绪（见 `docs/G7_2025_汇聚重启实施步骤_DEV.md`）
- [x] 数据库连接参数已确认：`chenlei/lujing2022@117.72.14.166:23506/appraisal_test`
- [x] Python 环境与依赖可用；网络连通

权限确认（必选）
- [ ] `TRIGGER, CREATE` ON `appraisal_test`.*
- [ ] 守卫相关表 `CREATE/SELECT/INSERT/UPDATE/DELETE`
- [ ]（可选）`PROCESS` 便于 `SHOW PROCESSLIST`

运行窗口
- [ ] 选择业务低峰；完成风控沟通与回滚预案共享

---

## 二、生产执行步骤

1) 设置环境变量
```bash
export PRODUCTION_DATABASE_URL='mysql+pymysql://chenlei:lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4'
export PYTHONPATH=.
```

2) 预检与备份
```bash
python g7_precheck_suite.py --with-validation
```

3) 安装增强守卫（白名单-only）
```bash
python scripts/g7_guard_switch.py whitelist     # 或按下列原子命令执行
python scripts/enhanced_g7_guard.py install
python scripts/enhanced_g7_guard.py add-whitelist "chenlei%"
python scripts/enhanced_g7_guard.py disable-maintenance
python scripts/enhanced_g7_guard.py status
python scripts/validate_g7_triggers.py --quick
```

4) 启动监控端
```bash
python monitor_g7_pipeline.py --emergency-stop
```

5) 执行 G7-2025 汇聚
```bash
python run_g7_pipeline_wrapper.py --batch G7-2025 --env production
```

6) 数据与 API 验证
```bash
python validate_g7_data.py --compare-backup
curl -s http://localhost:8000/api/v12/batch/G7-2025/regional | head -c 200
```

---

## 三、一键执行（可选）

Linux/Mac
```bash
export G7_DB_HOST=117.72.14.166
export G7_DB_PORT=23506
export G7_DB_NAME=appraisal_test
export G7_DB_PASSWORD='lujing2022'

bash scripts/run_g7_relaunch.sh
```

Windows
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_g7_relaunch.ps1 `
  -DbHost 117.72.14.166 -DbPort 23506 -DbName appraisal_test -Password 'lujing2022'
```

---

## 四、验收标记

技术验收
1. 触发器：
   ```sql
   SHOW TRIGGERS LIKE 'statistical_aggregations';
   -- 期望返回 g7_enhanced_guard_insert / g7_enhanced_guard_update
   ```
2. 白名单：
   ```sql
   SELECT user_pattern, is_active FROM g7_guard_whitelist;
   -- 应包含 'chenlei%' 且 is_active=1
   ```
3. 守卫日志：
   ```sql
   SELECT decision, is_whitelisted, current_user_name
   FROM g7_enhanced_guard_log
   ORDER BY id DESC LIMIT 10;
   -- chenlei* 为 ALLOWED，其余为 BLOCKED
   ```

业务验收
1. 数据完整性：核心统计指标偏差 < 0.1%
2. API 功能：关键接口 200 且数据一致
3. 性能指标：关键接口 P95 < 500ms（可选）

---

## 五、应急与回滚

紧急停机
```bash
python ultimate_stop.py --force
```

快速回滚
```bash
python scripts/g7_guard_rollback.py emergency
# 或清除触发器（如需）
mysql -u chenlei -p'lujing2022' -h 117.72.14.166 -P 23506 appraisal_test < quick_remove_g7_guard.sql
```

数据恢复
```bash
# 恢复备份数据（路径按预检输出）
mysql -u chenlei -p'lujing2022' -h 117.72.14.166 -P 23506 appraisal_test < backups/g7_2025_backup_*.sql
```

---

## 六、执行记录模板

执行信息
- 执行日期：____-__-__
- 执行时间：__:__ - __:__
- 执行人员：_________
- 环境：生产环境

检查项记录
| 步骤 | 开始 | 结束 | 结果 | 备注 |
|------|------|------|------|------|
| 预检 |      |      | 通过/失败 | |
| 守卫安装 |  |      | 通过/失败 | |
| 汇聚执行 |  |      | 通过/失败 | |
| 数据验证 |  |      | 通过/失败 | |
| API验证 |  |      | 通过/失败 | |

关键指标
- 处理数据量：____ 行
- 执行用时：____ 分钟
- 错误数量：____ 个
- 平均响应：____ ms

异常记录
| 时间 | 异常描述 | 处理措施 | 结果 |
|------|----------|----------|------|
|      |          |          |      |

---

状态：已准备，待生产窗口执行

