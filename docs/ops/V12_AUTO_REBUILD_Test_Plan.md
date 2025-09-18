# v1.2 读时重建禁用（V12_AUTO_REBUILD=0）联调测试方案

目标：验证在禁用“读时重建”后，v1.2 接口不触发计算。

- 已部署代码包含环境开关；服务器已设置 `V12_AUTO_REBUILD=0` 并重启应用。
- 不触碰既有批次：`G4-2025`、`G7-2025`、`G8-2025`。
- 使用全新测试批次：`TEST-2025`（如该批次已存在可换名）。

---

## 0. 前置检查

- 确认运行环境（任选其一设置方式）。
  - Docker Compose：在 app 服务下添加 `environment: ["V12_AUTO_REBUILD=0"]` 并 `docker-compose up -d`。
  - Kubernetes：Deployment 容器 `env` 加入 `name: V12_AUTO_REBUILD, value: "0"`，滚动更新。
  - systemd：Unit 文件添加 `Environment=V12_AUTO_REBUILD=0`，`systemctl daemon-reload && systemctl restart <service>`。
  - 直接命令：`V12_AUTO_REBUILD=0 uvicorn app.main:app ...`。

> 验证：应用日志中不应出现大规模“开始计算/初始化计算系统/生成学校级统计”等计算日志。

---

## 1. 准备最小测试数据（仅测试批次）

在目标 MySQL 执行以下 SQL。根据环境实际库名/枚举/权限微调。

1) 主数据里新增一所测试学校（ACTIVE）：

```sql
INSERT INTO school_master_data (batch_code, school_id, standard_school_name, status)
VALUES ('TEST-2025', 'TEST001', '测试学校TEST001', 'ACTIVE');
```

2) 预置一条“旧结构”的区域级统计（作为已有数据，应当原样返回，不重建）：

```sql
INSERT INTO statistical_aggregations
  (batch_code, aggregation_level, school_id, school_name,
   statistics_data, data_version, calculation_status,
   total_students, total_schools, created_at, updated_at)
VALUES
  ('TEST-2025','REGIONAL', NULL, '区域汇总',
   JSON_OBJECT('schema_version','v1.1','batch_code','TEST-2025','subjects',JSON_ARRAY()),
   'v1.1','COMPLETED', 0, 0, NOW(), NOW());
```

3) 预置一条“旧结构”的学校级统计（作为已有数据，应当原样返回，不重建）：

```sql
INSERT INTO statistical_aggregations
  (batch_code, aggregation_level, school_id, school_name,
   statistics_data, data_version, calculation_status,
   total_students, total_schools, created_at, updated_at)
VALUES
  ('TEST-2025','SCHOOL', 'TEST001', '测试学校TEST001',
   JSON_OBJECT('schema_version','v1.1','batch_code','TEST-2025','school_id','TEST001','subjects',JSON_ARRAY()),
   'v1.1','COMPLETED', 0, 0, NOW(), NOW());
```

> 说明：若目标库对 `aggregation_level`/`calculation_status` 为 ENUM 类型，字符串取值须与库定义一致。

---

## 2. 接口测试

将 `HOST:PORT` 替换为服务器地址和端口。

1) 区域级（已有旧数据 → 应返回 200，且内容为旧结构，不触发重建）

```bash
curl -sS "http://HOST:PORT/api/v12/batch/TEST-2025/regional" | jq '.'
```

期望：
- HTTP 200；返回 JSON 中 `data.data_version` 为 v1.1 或无 v1.2 字段；
- 应用日志出现：`V12 auto rebuild disabled; return existing regional data for TEST-2025 as-is`；
- 不出现“大规模计算/初始化计算系统”等日志。

2) 学校级（已有旧数据 → 应返回 200，且内容为旧结构，不触发重建）

```bash
curl -sS "http://HOST:PORT/api/v12/batch/TEST-2025/school/TEST001" | jq '.'
```

期望：
- HTTP 200；返回 JSON 中 `data.data_version` 为 v1.1 或无 v1.2 字段；
- 应用日志出现：`V12 auto rebuild disabled; return existing school data for TEST-2025/TEST001 as-is`。

3) 空批次（不存在任何统计 → 应返回 404，不触发重建）

```bash
curl -i "http://HOST:PORT/api/v12/batch/TEST-2025-EMPTY/regional"
curl -i "http://HOST:PORT/api/v12/batch/TEST-2025-EMPTY/school/TEST001"
```

期望：
- HTTP 404；日志不出现计算相关语句；
- `statistical_aggregations` 表中无 `TEST-2025-EMPTY` 新增记录。

> 注意：不要调用 `POST /api/v12/batch/{batch_code}/materialize`。该接口设计为强制计算入库，不受 V12_AUTO_REBUILD 影响。

---

## 3. 验证数据库无意外改写

```sql
-- 应当只存在上面手工插入的 2 条 TEST-2025 记录
SELECT batch_code, aggregation_level, school_id, data_version, updated_at
FROM statistical_aggregations
WHERE batch_code IN ('TEST-2025','TEST-2025-EMPTY')
ORDER BY aggregation_level, school_id;
```

- 期望：`TEST-2025` 仅 2 条；`TEST-2025-EMPTY` 无记录。
- `updated_at` 不应频繁变化（避免被接口写回）。

---

## 4. 清理（可选）

测试完成后可移除测试数据：

```sql
DELETE FROM statistical_aggregations WHERE batch_code IN ('TEST-2025','TEST-2025-EMPTY');
DELETE FROM school_master_data WHERE batch_code='TEST-2025' AND school_id='TEST001';
```

---

## 5. 回滚/开启读时重建（如需）

- 将 `V12_AUTO_REBUILD` 设为 `1`（或删除该环境变量），重启服务；
- 此后 v1.2 接口会在数据不达标时执行重建计算并回写。

---

## 附录：排障要点

- 若 2.1/2.2 返回 404，检查：
  - `TEST-2025` 的两条统计记录是否成功写入；
  - 路由路径和 HOST:PORT 是否正确；
  - 服务环境变量是否为 `V12_AUTO_REBUILD=0` 并已重启；
- 若日志仍有大量计算痕迹，确认：
  - 是否误访问了 `materialize` 接口；
  - 其他批次（如 G4/G7/G8）是否被前端轮询命中。

