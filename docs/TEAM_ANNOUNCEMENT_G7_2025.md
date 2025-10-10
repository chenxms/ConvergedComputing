## 团队公告｜G7-2025 汇聚重启上线（白名单-only）

- 生效时间：发布当日 10:00（以群内通知为准）
- 指南入口：`docs/G7_2025_汇聚重启实施步骤_DEV.md`
- 生产指引：`G7_2025_PRODUCTION_EXECUTION_GUIDE.md`

---

## 可复制命令清单（便于快速、无差错执行）

说明：统一的“可复制命令清单”能减少人工输入错误、保证参数一致性，帮助 DEV 同事在本地和服务器侧用同一套步骤复现和执行，提高上线效率与可追溯性。

Linux/Mac（Bash）
```bash
export PRODUCTION_DATABASE_URL='mysql+pymysql://chenlei:lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4'
export PYTHONPATH=.

python g7_precheck_suite.py --with-validation
python scripts/enhanced_g7_guard.py install
python scripts/enhanced_g7_guard.py add-whitelist "chenlei%"
python scripts/enhanced_g7_guard.py disable-maintenance
python scripts/enhanced_g7_guard.py status
python scripts/validate_g7_triggers.py --quick
python run_g7_pipeline_wrapper.py --batch G7-2025 --env production
python monitor_g7_pipeline.py --emergency-stop
python validate_g7_data.py --compare-backup
```

Windows（PowerShell）
```powershell
$env:PRODUCTION_DATABASE_URL = 'mysql+pymysql://chenlei:lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4'
$env:PYTHONPATH = '.'

powershell -ExecutionPolicy Bypass -File scripts/run_g7_relaunch.ps1 `
  -DbHost 117.72.14.166 -DbPort 23506 -DbName appraisal_test -Password 'lujing2022'
```

---

## 关键提醒
- 仅允许 `chenlei%` 用户写入 G7-2025；旧流程如非该账号将被拦截
- 默认不启用维护模式；如需补写，请使用 `g7_guard_switch.py maintenance on/off`
- 生产执行与验收步骤详见上述两份文档
