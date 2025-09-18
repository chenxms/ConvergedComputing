# 数据汇聚与计算流程研究（正式文档）

版本：v1.0  
目标读者：产品/运营/数据/研发（非技术读者也能读懂）

## 1. 研究目标（Research Objective）

- 彻底梳理“数据从进入到出数”的端到端流程与每个环节的细节。
- 找出“无法完成数据汇聚”和“汇聚后数据缺失严重”的根因位置与触发条件。
- 制定快速止血方案与根因级修复方案，形成可复用的质量闸门与排查脚本。

成功标准：
- 当日/次日出数成功率明显提升；关键表缺失率下降到阈值内（见§8）。
- 任意一天可用研究清单快速定位问题（<60 分钟得出原因与处理策略）。
- 建立分层与回补机制，Gold（汇聚层）可按 Silver（清洗层）稳定重算。

## 2. 背景与现状（Background Context）

业务反馈两个核心问题：
1) 作业经常“无法完成数据汇聚”（任务失败/卡住/早跑）。
2) 即便完成，汇聚结果“数据缺失严重”（指标偏低、行数不足、维表匹配差）。

常见诱因（假设）：
- 上游不可达/鉴权/限流、调度依赖未就绪、资源不足、队列积压。
- 模式（Schema）漂移、分区/路径错误、增量位点/检查点异常。
- 内连接/过严过滤导致行丢失、窗口太窄漏算晚到、去重键错误。
- 维表延迟/缺失、时区/事件时间错用、未做回补、过早聚合发布。

（注：以上为经验假设，后续以数据与日志验证。）

## 3. 通俗版全流程说明（给非技术同学）

- 采集来源：像“不同水源”——业务库、日志、接口、消息队列等。
- 传输落地（原始层/Raw/Bronze）：先把“所有水”完整装进桶里，暂不加工，避免“刚到就丢”。
- 清洗对齐（清洗层/Silver）：过滤杂质、统一名字与时间、去重补齐，得到“干净的水”。
- 汇聚计算（汇聚层/Gold）：按业务口径做汇总（按天/门店/用户求和/去重计数/均值等）。
- 出数服务：报表/API/模型读取；设置“完整性闸门”，没达到质量阈值就先不对外。
- 全程度量：每个桶有“刻度线”——进了多少、还差多少、是否有迟到“补水”。

## 4. 研究问题（Research Questions）

### 必答（Primary）
1. 端到端每一环节的输入/输出/依赖/失败重试策略是什么？
2. 哪个环节首次出现“无法完成/数据缺失”的偏差？触发条件与频率如何？
3. 数据缺失的主要类型：行丢失、列为空、维表未匹配、时间错分桶、去重误杀？
4. 事件时间、时区与迟到处理（Watermark/Allowed Lateness）如何设置？是否回补重算？
5. 汇聚口径（去重、过滤、关联、窗口）与数据契约是否被严格执行？
6. 质量闸门与告警是否覆盖：行数基线、主键覆盖、非空率、匹配率、水位线？

### 次要（Secondary）
1. 是否存在“早跑”（未等上游完整就开始聚合）与“重复跑”（位点异常）？
2. Schema 演进是否破坏兼容？新增字段默认值/可空策略如何？
3. DAG 是否具备数据感知触发（Sensors）与数据驱动的重算机制？

## 5. 研究方法（Methodology）

### 信息来源（Information Sources）
- 调度与运行：DAG 定义与依赖、运行日志、重试与失败原因、资源/队列指标。
- 存储与分区：Raw/Silver/Gold 路径与分区（天/小时），行数与基线对比。
- Schema 与契约：期望 Schema 与实际 Schema Diff、数据契约与指标口径文档。
- 样本数据：抽样近 7 天中的 3 天（含一日异常、一日对照、一日最新）。

### 分析框架（Analysis Frameworks）
- 分层对账：原始→清洗→汇聚逐层核对行数/非空率/匹配率，定位“第一处偏差”。
- 空值审计：聚合全部改“左连接+空值指标”，统计维表缺失与被过滤比例。
- 时间分析：事件时间 vs 处理时间分布、迟到比例、水位线与窗口触发关系。
- Schema Diff：字段新增/类型改变/可空性变化是否导致作业失败或过滤。
- 位点与幂等：CDC/Offset/LSN 连续性，写入幂等（唯一键+去重）验证。

### 数据质量要求（Data Requirements）
- 行数基线偏差 ≤ 10%（同周同比）；分区齐全率 100%。
- 主键覆盖率 ≥ 99.5%；关键字段非空率 ≥ 98%。
- 维表匹配率 ≥ 97%；迟到回补完成前不对外发布 Gold。

## 6. 期望交付物（Expected Deliverables）

### 管理层/非技术摘要（Executive Summary）
- 一页纸：问题概述、根因归类、影响范围、止血与修复路线、完成时间表。

### 详细分析（Detailed Analysis）
- 端到端流程图与数据血缘（Source→Raw→Silver→Gold→消费）。
- 分层对账表（3 天样本）：行数、主键覆盖、非空率、匹配率、水位/迟到比例。
- 空值审计报告：左连接引入的空值来源分布与Top N维度缺口。
- Schema Diff 报告：字段/类型/可空性变化与影响评估。
- 位点/分区/调度诊断：首错环节、触发条件、复现场景与风险。

### 支撑材料（Supporting Materials）
- 质量闸门规则清单（可配置阈值）。
- 回补与重算 Runbook（T+1/T+N与全量重算）。
- 排查脚本/SQL 模板与自动化检查列表。

## 7. 流程与环节清单（可直接用于排查）

1) 采集与落地（Raw/Bronze）
- 校验上游可达/鉴权/限流；检查分区是否生成、行数与同周基线对比。
- 对比期望 Schema；新增字段默认值/可空策略；拒绝“因新增字段直接失败”。

2) 清洗与对齐（Silver）
- 去重键选择与重复率；必填字段非空率；时间戳/时区一致性。
- 维表连接一律左连接；统计匹配率并输出空值审计结果。

3) 汇聚与计算（Gold）
- 窗口与迟到：事件时间+watermark，允许迟到并标记补算范围。
- 过滤逻辑：列出所有过滤条件，评估被过滤比例是否异常。
- 输出前的“完整性闸门”：行数/非空率/匹配率/水位阈值必须达标。

4) 出数与消费
- 报表/API 的数据版本与发布时间；是否支持回滚；异常时降级策略。

## 8. 质量闸门（发布前必过）

- 行数偏差 ≤ 10%（相对 4 周同周同日/同小时基线）。
- 分区齐全率 = 100%。
- 主键覆盖率 ≥ 99.5%，关键字段非空率 ≥ 98%。
- 维表匹配率 ≥ 97%。
- Watermark 达标；迟到回补完成或已标记将自动补算。

不达标即：不发布 Gold，对应 DAG 标红并触发告警与回补流程。

## 9. 修复路线（Roadmap）

短期（当日止血）
- 引入发布闸门；聚合改左连接并输出空值审计；放宽窗口并启用回补。

中期（两周内根因修复）
- Schema 注册与演进策略；事件时间/水位与迟到机制；位点精确与写入幂等。
- Raw/Silver/Gold 严格分层，Gold 禁止成为“唯一真相”，支持按 Silver 重算。

长期（一个月+）
- 数据契约与指标口径字典；数据感知调度（Sensors）；质量门控与告警仪表盘。

## 10. 时间与优先级（Timeline & Priority）

- Day 0–1：抽样 3 天分层对账 + 空值审计，立刻启用闸门（影子模式）。
- Day 2–5：定位首错环节与根因，完成当周修复与回补。
- Week 2：落地 Schema 演进、迟到与回补、位点幂等等核心机制。
- Week 4+：数据契约与数据感知调度、仪表盘与告警全面上线。

## 11. 输入清单（需要提供）

- 调度/运行：DAG 名称与依赖、最近一周的运行日志、失败样例。
- 存储/分区：Raw/Silver/Gold 的路径、分区命名与样本行数。
- Schema/口径：关键事实表主键、维表键、指标定义与过滤口径。
- 样本数据：近 7 天的 3 天样本（异常/对照/最新）。

## 12. 附录 A：样例审计与排查 SQL（伪代码）

1) 分层对账（行数核对）
```sql
-- Raw 层（按日）
SELECT dt, COUNT(*) AS cnt_raw FROM raw.fact_x WHERE dt BETWEEN :d1 AND :d2 GROUP BY dt;

-- Silver 层（按日）
SELECT dt, COUNT(*) AS cnt_silver FROM silver.fact_x WHERE dt BETWEEN :d1 AND :d2 GROUP BY dt;

-- Gold 层（按日）
SELECT dt, COUNT(*) AS cnt_gold FROM gold.agg_x WHERE dt BETWEEN :d1 AND :d2 GROUP BY dt;
```

2) 维表左连接与空值审计
```sql
SELECT 
  f.dt,
  COUNT(*)                         AS fact_rows,
  SUM(CASE WHEN d.dim_key IS NULL THEN 1 ELSE 0 END) AS dim_miss_rows,
  AVG(CASE WHEN d.dim_key IS NULL THEN 1.0 ELSE 0 END) AS dim_miss_ratio
FROM silver.fact_x f
LEFT JOIN dim.d_store d ON f.store_id = d.store_id
WHERE f.dt BETWEEN :d1 AND :d2
GROUP BY f.dt;
```

3) 事件时间分布与迟到比例
```sql
SELECT 
  dt,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY event_time - ingest_time) AS p50_lateness,
  AVG(CASE WHEN event_time > dt_end + INTERVAL 'X' HOUR THEN 1 ELSE 0 END) AS late_ratio
FROM silver.fact_x
WHERE dt BETWEEN :d1 AND :d2
GROUP BY dt;
```

4) 过滤与去重影响评估
```sql
-- 去重前后对比
WITH raw_dedup AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY event_time DESC) AS rn
  FROM silver.fact_x
)
SELECT dt,
  COUNT(*)                         AS before_cnt,
  SUM(CASE WHEN rn = 1 THEN 1 ELSE 0 END) AS after_cnt,
  1.0 - SUM(CASE WHEN rn = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS dedup_loss_ratio
FROM raw_dedup
WHERE dt BETWEEN :d1 AND :d2
GROUP BY dt;
```

## 13. 附录 B：完整性闸门（示例规则）

- 行数偏差 ≤ 10%（对齐历史同周同日/小时基线）。
- 分区齐全率 = 100%。
- 主键覆盖率 ≥ 99.5%，关键字段非空率 ≥ 98%。
- 维表匹配率 ≥ 97%。
- Watermark 达标；迟到回补未完成则暂缓发布 Gold。

## 14. 下一步

- 若认可本研究大纲：请提供§11输入清单，我将按该结构完成具体化分析与结论。
- 也可先行落地：我可基于你们的技术栈生成“审计脚本/作业改造示例（左连接+空值审计/闸门规则）”。

