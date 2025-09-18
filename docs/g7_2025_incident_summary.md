# G7-2025 汇聚异常排查手册

## 背景
- 监控到 `statistical_aggregations` 表内 `batch_code = 'G7-2025'` 持续被重写，影响后续全量汇聚结果的可靠性。
- 生产容器已陆续停用，怀疑仍有遗留进程或脚本在重复触发 `rewrite_subjects_v12.py`。

## 关键时间线
- **T0**：业务方发现 G7-2025 数据被覆盖，反馈给开发团队。
- **T0 +**：执行 `SHOW FULL PROCESSLIST`，确认库内多条 `root@171.213.*`、`root@183.221.*` 等连接均处于 `Sleep` 状态，无持续事务。
- **T0 +**：排查容器/进程，发现 `docker compose exec ... python scripts/rewrite_subjects_v12.py G7-2025` 长驻在 `subjects` 容器中。
- **T0 +**：重启 `subjects` 容器，结束残留进程；同时定位到本地存在 `materialize_g7_2025.bat` 等硬编码脚本。
- **T0 +**：全面禁用相关脚本，并持续观察数据库；截至最近一次检查，G7-2025 未再自动写入。

## 排查步骤
1. **数据库侧验证**
   - `SHOW FULL PROCESSLIST;` 观察是否存在活跃的 INSERT/UPDATE SQL。
   - 若未捕获到语句，使用 `performance_schema.events_statements_history_long` 或临时开启 General Log 定位具体来源。

2. **容器与进程排查**
   - `docker ps` / `docker top <container>` 观察是否有 Python 批处理脚本常驻。
   - `docker events --since 30m` 检查是否有人通过 `docker exec` 触发脚本。

3. **脚本与任务核查**
   - 在代码仓库和部署目录中搜索 `rewrite_subjects_v12.py G7-2025` 之类硬编码调用，尤其是 `.bat/.sh/.py` 工具脚本。
   - 检查本地/远端的计划任务、快捷方式、Shell 历史记录，确认无人或无程序自动触发。

4. **安全兜底**
   - 再次确认 `.env` 中 `DISABLE_WRITES_FOR_BATCHES` 未被清空。
   - 若需要临时止血，可使用 `scripts/install_g7_guard.py`（或触发器 SQL）阻断 G7-2025 的 INSERT/UPDATE。

## 根因总结
- 由于调试时执行的 `docker compose exec ... rewrite_subjects_v12.py G7-2025` 未退出，形成后台 bash 进程。
- 同时本地存在硬编码 G7-2025 的脚本（如 `materialize_g7_2025.bat`），任何无意执行都会再次拉起脚本覆盖数据。
- 导致在生产汇聚尚未完成前，旧输出不断回写 G7 批次，造成数据被反复清洗。

## 修复措施
1. 重启 `subjects-v12-api` 容器，终止所有残留 `rewrite_subjects_v12.py` 进程。
2. 将以下脚本改名为 `.disabled`，并要求显式传参：
   - `materialize_g7_2025.bat`
   - `unlock_and_materialize_g7_2025.py`
   - `unlock_and_materialize_g7_2025.sh`
3. 更新提示脚本：
   - `remove_g7_guard.py`、`unlock_g7_2025_database.py` 的示例改为占位符，提醒执行者自行指定批次。
4. 通知团队成员在其机器上执行同样的脚本/计划任务排查，避免远端再次触发。
5. 在重新汇聚前，统一流程：使用一次性的 `docker compose run --rm app python scripts/rewrite_subjects_v12.py <批次>`，避免 `docker exec` 常驻。

## 验证与监控
- 观察数据库：确认 `statistical_aggregations` 中 G7-2025 未再更新。
- 保留 `performance_schema` 查询或短期开启 General Log，用以捕捉后续异常写入。
- 若再次检测到写入，立即记录 SQL 与 `HOST` 信息，登录对应机器复盘并清理。

## 复盘结论与长期建议
- 核心失效点在于“脚本未显式传参 + docker exec 常驻”。将所有批处理改为显式白名单参数，并限制 `subjects` 容器的执行权限，可从流程层面避免类似问题。
- 建议建立脚本执行审计（命令行历史留存、容器 exec 操作告警）和数据库写入告警，第一时间发现异常。
- 汇聚完成且验收通过后，保持 `DISABLE_WRITES_FOR_BATCHES=G7-2025` 和触发器防线常开，作为最终兜底。

## 长期防范要求（2025-09 更新）
- **默认保持写入阻断**：在 `docker-compose.yml` 中为 `app`、`subjects`、`batch-processor` 服务设置 `DISABLE_WRITES_FOR_BATCHES=G7-2025`，并在运维手册中要求解除阻断必须登记负责人、起止时间及恢复确认。
- **数据库触发器兜底**：如需快速止血，优先执行以下 SQL（分别在所有写库容器中运行），确保任何 INSERT/UPDATE 都被拒绝：

  ```sql
  CREATE TRIGGER IF NOT EXISTS g7_guard_insert
  BEFORE INSERT ON statistical_aggregations
  FOR EACH ROW
  BEGIN
      IF NEW.batch_code = 'G7-2025' THEN
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by guard';
      END IF;
  END;

  CREATE TRIGGER IF NOT EXISTS g7_guard_update
  BEFORE UPDATE ON statistical_aggregations
  FOR EACH ROW
  BEGIN
      IF NEW.batch_code = 'G7-2025' THEN
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025 writes blocked by guard';
      END IF;
  END;
  ```

  解除时必须记录 DROP TRIGGER 的执行时间与责任人。
- **脚本执行规范**：所有批处理脚本（`rewrite_subjects_v12.py`、`unlock_and_materialize_g7_2025.py`、`fast_materialize_all_batches_v12.py` 等）必须显式传入批次并在运行前确认“守卫已有计划解除”；脚本头部需加入警告说明，禁止硬编码 G7-2025。
- **执行方式统一**：建议仅通过 `docker compose run --rm app python scripts/rewrite_subjects_v12.py <BATCH>` 方式单次运行，避免 `docker exec` 的常驻进程；执行完毕立即恢复 `DISABLE_WRITES_FOR_BATCHES` 并重启相关服务。
- **监控与告警**：数据库侧需对 `statistical_aggregations` 中 `batch_code='G7-2025'` 的写入建立告警，一旦有新增/更新记录立刻通知运维定位来源。
