# 数据汇聚实现与 API 契约（基于清洗数据）

本文档给出在现有“数据清洗与汇聚对接说明”基础上，汇聚（统计/报表）侧的实现思路、核心查询模板、持久化结构与对外 API 契约，保证在不追加上下文的情况下，工程师可直接实现并联调通过。

参考清洗说明：`docs/数据清洗与汇聚对接说明.md`

## 1. 汇聚目标与产出

- 按学校（SCHOOL）与区域（REGIONAL）两级，生成各科（含问卷）统计摘要：
  - 学生数、平均分、最小/最大、标准差（`STDDEV_POP`）、分位数（可选 `PERCENTILE_CONT` 或软件侧计算）
  - 题量（question_count）、满分（max_score）
  - 通过率/优秀率（基于阈值配置，可从元数据表拉取）
  - 维度统计（考试/互动）：每个维度的平均分与满分
  - 问卷统计：题目选项分布（option_level 占比）与学生级问卷总分统计
- 将统计结果持久化到 `statistical_aggregations` 表（已建模型），`statistics_data` 字段以 JSON 存储标准结构，供 API 直接输出。

## 2. 汇聚输入（清洗侧产物）

- 主输入：`student_cleaned_scores`
  - 字段：`batch_code, school_id, school_name, student_id, subject_id, subject_name, total_score, max_score, question_count, subject_type, dimension_scores, dimension_max_scores, ...`
  - subject_type：`exam`（含互动题）/ `questionnaire`
- 问卷细分：
  - `questionnaire_question_scores`（每生×每题原始分/scale_level/instrument_type）
  - `questionnaire_option_distribution`（题目×选项等级的计数）
- 阈值与标签（可选）：
  - `statistical_metadata`（阈值、等级等全局/年级配置）
  - `questionnaire_scale_options`（问卷选项标签映射）

## 3. 统计口径与配置

- 统一口径：
  - 考试/互动：总分 = 逐题求和（清洗已保证），满分 = 配置求和。
  - 问卷：总分 = 逐题求和（清洗已保证），满分 = 配置求和；选项分布由明细按比例映射物化。
- 阈值（示例）：
  - `pass_rate`: total_score / max_score >= 0.6
  - `excellent_rate`: total_score / max_score >= 0.85
  - 实际阈值可放入 `statistical_metadata`：`metadata_type='grade_config'` 或 `subject_config`，读取后按批次/年级应用。

## 4. SQL 模板与实现范式

以下以 `:batch` 代表批次、`:subject` 代表科目、`:school` 代表学校过滤参数。

### 4.1 学校级：科目总体统计（考试/互动）
```sql
SELECT 
  school_id,
  school_name,
  subject_name,
  COUNT(*)                    AS students,
  ROUND(AVG(total_score),2)   AS avg_score,
  ROUND(MIN(total_score),2)   AS min_score,
  ROUND(MAX(total_score),2)   AS max_score,
  ROUND(STDDEV_POP(total_score),2) AS stddev,
  MAX(max_score)              AS subject_max,
  MAX(question_count)         AS question_count,
  SUM(CASE WHEN total_score / NULLIF(max_score,0) >= 0.6 THEN 1 ELSE 0 END) / COUNT(*) AS pass_rate,
  SUM(CASE WHEN total_score / NULLIF(max_score,0) >= 0.85 THEN 1 ELSE 0 END) / COUNT(*) AS excellent_rate
FROM student_cleaned_scores
WHERE batch_code=:batch AND subject_type='exam'
GROUP BY school_id, school_name, subject_name;
```

### 4.2 学校级：维度统计（考试/互动）
维度在 `student_cleaned_scores.dimension_scores`（JSON）中，结构类似：`{"DIM-A": {"score": 12.5, "name": "维度A"}, ...}`。若列类型为 JSON，可直接 `JSON_EXTRACT`；若为 TEXT，先 `CAST(... AS JSON)`。

示例提取单维度（以 `DIM-A` 为例）：
```sql
SELECT 
  school_id, subject_name,
  ROUND(AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(CAST(dimension_scores AS JSON), '$."DIM-A".score')) AS DECIMAL(10,4))),2) AS dimA_avg,
  MAX(CAST(JSON_UNQUOTE(JSON_EXTRACT(CAST(dimension_max_scores AS JSON), '$."DIM-A".max_score')) AS DECIMAL(10,4))) AS dimA_max
FROM student_cleaned_scores
WHERE batch_code=:batch AND subject_type='exam'
GROUP BY school_id, subject_name;
```

如需“全维度”输出，建议在服务层将 `dimension_scores` JSON 展开（UNION ALL 拼接或通过 JSON_TABLE/应用层展开）再做 `GROUP BY` 聚合，最终回写到 `statistics_data.dimensions`。

### 4.3 学校级：问卷统计
1) 学生级问卷总分摘要：
```sql
SELECT 
  school_id,
  school_name,
  subject_name,
  COUNT(*) AS students,
  ROUND(AVG(total_score),2) AS avg_score,
  ROUND(STDDEV_POP(total_score),2) AS stddev,
  MAX(max_score) AS questionnaire_max
FROM student_cleaned_scores
WHERE batch_code=:batch AND subject_type='questionnaire'
GROUP BY school_id, school_name, subject_name;
```

2) 问卷题目选项分布（占比）：
```sql
SELECT 
  q.question_id,
  q.option_level,
  SUM(q.count)                         AS cnt,
  ROUND(SUM(q.count) * 100.0 / SUM(SUM(q.count)) OVER (PARTITION BY q.question_id), 2) AS pct
FROM questionnaire_option_distribution q
WHERE q.batch_code=:batch AND q.subject_name=:subject
GROUP BY q.question_id, q.option_level
ORDER BY q.question_id, q.option_level;
```

如需标准 `option_label`，在读端联表 `questionnaire_scale_options`：
```sql
SELECT d.question_id, d.option_level, o.option_label, d.cnt, d.pct
FROM (
  SELECT question_id, option_level,
         SUM(count) AS cnt,
         ROUND(SUM(count) * 100.0 / SUM(SUM(count)) OVER (PARTITION BY question_id), 2) AS pct
  FROM questionnaire_option_distribution
  WHERE batch_code=:batch AND subject_name=:subject
  GROUP BY question_id, option_level
) d
LEFT JOIN questionnaire_scale_options o
  ON o.instrument_type = :instrument_type
 AND o.scale_level = :scale_level
 AND o.option_level = d.option_level;
```

### 4.4 区域级统计
区域级与学校级类似，将 `GROUP BY school_id` 去除或对学校聚合后再汇总。建议做两段：
1) 先学校级聚合（保证每校权重一致或按学生数加权），
2) 再区域级聚合（如需等权学校，可取学校均值的均值；如需学生等权，直接对学生级汇总）。

示例（学生等权）：
```sql
SELECT 
  subject_name,
  COUNT(*) AS students,
  ROUND(AVG(total_score),2) AS avg_score,
  ROUND(STDDEV_POP(total_score),2) AS stddev,
  MAX(max_score) AS subject_max
FROM student_cleaned_scores
WHERE batch_code=:batch AND subject_type='exam'
GROUP BY subject_name;
```

## 5. 写入 `statistical_aggregations`（持久化契约）

- 表模型（已存在）：`statistical_aggregations`
  - 关键字段：`batch_code, aggregation_level (REGIONAL/SCHOOL), school_id, school_name, statistics_data(JSON), calculation_status, total_students, calculation_duration`
- 推荐 `statistics_data` 结构（示例）：
```json
{
  "summary": {
    "students": 1532,
    "avg_score": 72.35,
    "min": 12,
    "max": 100,
    "stddev": 13.27
  },
  "subjects": [
    {
      "name": "数学",
      "type": "exam",
      "max_score": 100,
      "question_count": 25,
      "avg": 76.21,
      "pass_rate": 0.68,
      "excellent_rate": 0.21,
      "dimensions": [
        {"code": "D-A", "name": "代数", "avg": 12.3, "max": 20},
        {"code": "D-G", "name": "几何", "avg": 10.1, "max": 15}
      ]
    },
    {
      "name": "问卷",
      "type": "questionnaire",
      "max_score": 112,
      "avg": 83.5,
      "option_distribution": [
        {"question_id": "q1", "level": 1, "pct": 3.1},
        {"question_id": "q1", "level": 2, "pct": 15.0}
      ]
    }
  ]
}
```

> 注意：具体 JSON 可按产品需要调整，但字段含义与来源必须与本说明一致，以确保前后端对齐。

## 6. 对外 API 契约（建议）

已存在路由：`app/api/aggregation_api.py`、`app/api/reporting_api.py` 可按下列语义输出。

- 批次概览（区域级）：`GET /api/v1/reporting/batches/{batch_code}/overview`
  - 返回：各科 `avg/max/min/stddev/pass/excellent`，总学生数
- 学校列表与对比：`GET /api/v1/reporting/batches/{batch_code}/schools`
  - 返回：每校统计摘要（同科目结构），支持分页/排序
- 科目维度：`GET /api/v1/reporting/batches/{batch_code}/dimensions?subject_name=数学`
  - 返回：维度均值与满分（支持学校过滤）
- 问卷分布：`GET /api/v1/reporting/batches/{batch_code}/questionnaire/options?subject_name=问卷`
  - 返回：题目选项分布（可选带 `option_label`）

> 如需按“学校-科目-维度”多层钻取，优先从 `statistical_aggregations.statistics_data` 直接取；若需要明细支撑，结合清洗表做即时查询。

## 7. 实现步骤（落地指南）

1) 读取配置阈值（如有）：从 `statistical_metadata` 拉取 pass/excellent 阈值；无配置则使用默认 0.6/0.85。
2) 对指定 `batch_code`：
   - 学校级：按 4.1/4.2/4.3 统计并组织 JSON；写入/更新 `statistical_aggregations(aggregation_level=SCHOOL, school_id)`。
   - 区域级：按 4.4 统计并组织 JSON；写入/更新 `statistical_aggregations(aggregation_level=REGIONAL)`。
3) 更新 `calculation_status` 与 `calculation_duration`；记录 `total_students/total_schools`。
4) 提供查询 API：从 `statistical_aggregations` 直接返回 `statistics_data`，避免重复计算。

## 8. 性能与稳定性建议

- 使用 `WHERE batch_code=:batch` 索引过滤；建议在清洗表上建好 `(batch_code, subject_name)`、`(batch_code, school_id)` 索引。
- 维度展开：大 JSON 建议在服务层展开再聚合，减少数据库 JSON 函数开销。
- 问卷分布：优先用已物化的 `questionnaire_option_distribution`，避免对明细重复扫描。
- 长事务与死锁：写入 `statistical_aggregations` 建议分科目/分学校批次化提交。

## 9. 联调验证清单

- 清洗已完成（校验 `student_cleaned_scores` 行数 > 0）。
- SAMPLE：G4-2025 / 问卷（当前快照）
  - 学生数 1532、问卷 max_score 112、分布行 112
- 汇聚结果：
  - `statistical_aggregations` 存在 REGIONAL 与若干 SCHOOL 级记录
  - `statistics_data.summary.students` 与清洗学生数一致；`subjects[].type` 与来源一致
  - 问卷 `option_distribution` 与 `questionnaire_option_distribution` 汇总结果一致

---

按本文档实现后，可直接满足报表端对“批次/学校/科目/维度/问卷分布”的查询需求，且与清洗侧契约完全对齐；如需新增指标或分组，仅需在 SQL 聚合与 `statistics_data` JSON 结构中补充字段，不影响既有接口。

