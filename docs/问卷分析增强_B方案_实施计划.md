# 问卷分析增强（方案B）实施计划与技术说明

面向“思维品质（问卷）”等问卷类科目，按你的分析需要，基于 v1.2 subjects 架构，对区域/学校两级输出进行增强，补齐“维度平均分、维度排名（学校级）、题目选项分布（按维度分组）”。本文档供新 DEV 工程师直接落地开发与联调使用。

---

## 1. 目标与范围

- 目标科目类型：问卷（subject.type = "questionnaire"），如“思维品质”。
- 输出层级：
  - 区域级（REGIONAL）：
    1) 各维度（好奇心、冒险性、想象力、挑战性、创造性自我效能感）全区平均分。
    2) 各维度下的题目选项分布（每道题各选项，如“非常同意”的比例）。
  - 学校级（SCHOOL）：
    1) 各维度本校平均分 + 维度区域排名（表现统计表）。
    2) 各维度“区域平均分”（用于对标）。
    3) 题目层选项分布（用于学生倾向分析）。
- 向后兼容：保持 v1.2 subjects 其他结构不变；问卷科目新增/扩展字段遵循下述契约。

---

## 2. 现状与差距

- 现状（已具备）：
  - 区域/学校科目主体字段、排名（subject-level）
  - 问卷（区域级）题目选项分布：`subjects[].questions[].option_distribution`
  - 量表标签映射与基础方法：`SubjectsBuilder._get_scale_label_map`
- 差距（需补齐）：
  - 区域/学校维度平均分（问卷）未产出
  - 学校级维度区域排名未产出（问卷）
  - 维度下按“题目”的选项分布（当前为科目下 questions 扁平，未按维度归组）
  - 学校级题目选项分布（已具备数据库条件，需汇聚输出）

---

## 3. 数据源与计算口径

### 3.1 主要数据表
- `questionnaire_question_scores`
  - 粒度：学生×题目（已修复学校ID完整性）
  - 字段建议：`batch_code, subject_name, school_id, student_id, question_id, original_score, max_score, scale_level, instrument_type`
- `question_dimension_mapping`
  - 粒度：题目→维度映射
  - 字段：`batch_code, subject_name, question_id, dimension_code`
- `subject_question_config`
  - 粒度：科目×题目配置（含题目 max_score、instrument_id 等）
- `questionnaire_option_distribution`
  - 粒度：题目×选项等级（支持学校/区域聚合，见“问卷数据学校ID修复总结报告”）
  - 字段建议：`batch_code, subject_name, school_id(可空), question_id, option_level, student_count, percentage`
- `batch_dimension_definition`
  - 维度元数据与名称：`batch_code, subject_name, dimension_code, dimension_name`

### 3.2 口径与公式
- 维度平均分（区域级）：
  - 从 `questionnaire_question_scores` 与 `question_dimension_mapping` 关联，将同一维度下的题目原始分按学生聚合（平均/求和均可，根据量表定义；此处建议“按题目原始分求均值后再对学生均值求均值”，简化处理可直接`AVG(original_score)`分组到 `dimension_code`）。
  - 若需得分率：`avg / dimension_max_score`，其中 `dimension_max_score` 可按题目 max_score 在同一维度求和（`subject_question_config` × `question_dimension_mapping`）。
- 维度区域排名（学校级）：
  - 先计算所有学校在该维度的平均分（同上口径），再 `DENSE_RANK() OVER (ORDER BY dim_avg DESC)`。
- 题目选项分布：
  - 来自 `questionnaire_option_distribution`（优先，避免明细全表扫描）。
  - 维度归组：`JOIN question_dimension_mapping`，按 `dimension_code, question_id, option_level` 聚合；输出 `pct` 或 `percentage`。
  - 学校级：条件 `WHERE school_id = :school`；区域级：`school_id IS NULL` 或聚合全体学校。

---

## 4. 对外 JSON 契约（v1.2 subjects 增强）

以下仅列“问卷科目”的新增/变更字段；其他字段保持现状。

### 4.1 区域级（REGIONAL）
```json
subjects[] = {
  "subject_name": "思维品质",
  "type": "questionnaire",
  "metrics": { ... },
  "school_rankings": [ ... ],
  "dimensions": [
    {
      "code": "CQX",               // 维度编码（好奇心等）
      "name": "好奇心",             // 来自 batch_dimension_definition
      "avg": 3.72,                  // 区域平均分（两位小数）
      "questions": [                // 维度下各题选项分布
        {
          "question_id": "Q101",
          "option_distribution": [
            {"option_level": 5, "option_label": "非常同意", "pct": 28.75},
            {"option_level": 4, "option_label": "同意",     "pct": 43.20},
            {"option_level": 3, "option_label": "一般",     "pct": 18.05},
            {"option_level": 2, "option_label": "不同意",   "pct": 7.10},
            {"option_level": 1, "option_label": "非常不同意", "pct": 2.90}
          ]
        }
      ]
    }
  ]
}
```

说明：
- 维度对象新增 `avg`（区域平均分）。
- `dimensions[].questions[]` 为新增，按维度归组题目，提供题目各选项占比。
- 保留（可选）历史 `subjects[].questions[]`（扁平题目分布）用于兼容，前端建议优先使用 `dimensions[].questions[]`。

### 4.2 学校级（SCHOOL）
```json
subjects[] = {
  "subject_name": "思维品质",
  "type": "questionnaire",
  "region_rank": 12,
  "total_schools": 56,
  "dimensions": [
    {
      "code": "CQX",
      "name": "好奇心",
      "avg": 3.61,                 // 本校维度平均分
      "regional_avg": 3.72,        // 区域维度平均分（用于对标）
      "rank": 18,                  // 本校在该维度的区域排名
      "questions": [               // （可选）本校题目选项分布
        {
          "question_id": "Q101",
          "option_distribution": [
            {"option_level": 5, "option_label": "非常同意", "pct": 24.12},
            {"option_level": 4, "option_label": "同意",     "pct": 45.33},
            {"option_level": 3, "option_label": "一般",     "pct": 20.45},
            {"option_level": 2, "option_label": "不同意",   "pct": 8.60},
            {"option_level": 1, "option_label": "非常不同意", "pct": 1.50}
          ]
        }
      ]
    }
  ]
}
```

说明：
- 学校级维度对象新增 `regional_avg` 与 `rank`。
- 学校级 `questions[]`（可选）给出本校题目选项分布；若数据量大，可按需开关。

---

## 5. 代码改造点（SubjectsBuilder）

文件：`app/services/subjects_builder.py`

### 5.1 新增/改造方法
- 区域级维度均分：
  - `def _compute_questionnaire_dimension_avg_regional(batch_code, subject_name) -> Dict[dim_code, avg]`
    - SQL：基于 `questionnaire_question_scores` × `question_dimension_mapping`
    - 出参：`{"CQX": 3.72, ...}`
- 学校级维度均分/排名：
  - `def _compute_questionnaire_dimension_avg_school(batch_code, subject_name, school_code) -> Dict[dim_code, avg]`
  - `def _compute_questionnaire_dimension_rank_school(batch_code, subject_name, school_code) -> Dict[dim_code, rank]`
    - SQL：先按学校求 `dim_avg`（CTE per_school），再 `DENSE_RANK()`
- 维度下题目选项分布（区域级）：
  - `def _compute_questionnaire_dimension_question_dist_regional(batch_code, subject_name) -> Dict[dim_code, List[{question_id, option_distribution}]]`
    - SQL：`questionnaire_option_distribution` × `question_dimension_mapping`（聚合/分组）
- 维度下题目选项分布（学校级）：
  - `def _compute_questionnaire_dimension_question_dist_school(batch_code, subject_name, school_code)` 同上，带 `WHERE school_id=:school`
- 量表标签映射（已存在）：
  - `_get_scale_label_map(batch_code, subject_name)`（复用）

### 5.2 subjects 组装逻辑
- `build_regional_subjects`：当 `s.type=='questionnaire'` 时：
  1) 写入 `dimensions[].avg`
  2) 注入 `dimensions[].questions[].option_distribution`
  3) 保持 `subjects[].questions[]`（兼容）或标记为废弃字段
- `build_school_subjects`：当 `s.type=='questionnaire'` 时：
  1) 写入 `dimensions[].avg`（本校）、`dimensions[].regional_avg`、`dimensions[].rank`
  2) （可选）注入 `dimensions[].questions[]` 的学校级分布

### 5.3 精度与字段风格
- 两位小数：使用 `round2, round2_json` 统一处理
- 百分比字段 `pct/percentage`：统一保留百分比整数（0-100，保留两位）

---

## 6. SQL 参考片段

### 6.1 区域级维度平均分
```sql
SELECT qdm.dimension_code,
       ROUND(AVG(qqs.original_score), 2) AS dim_avg
FROM questionnaire_question_scores qqs
JOIN question_dimension_mapping qdm
  ON qdm.batch_code=qqs.batch_code
 AND qdm.subject_name=qqs.subject_name
 AND qdm.question_id = qqs.question_id
WHERE qqs.batch_code=:batch AND qqs.subject_name=:subject
GROUP BY qdm.dimension_code;
```

### 6.2 学校级维度均分与排名
```sql
WITH per_school AS (
  SELECT qqs.school_id,
         qdm.dimension_code,
         ROUND(AVG(qqs.original_score), 2) AS dim_avg
  FROM questionnaire_question_scores qqs
  JOIN question_dimension_mapping qdm
    ON qdm.batch_code=qqs.batch_code
   AND qdm.subject_name=qqs.subject_name
   AND qdm.question_id = qqs.question_id
  WHERE qqs.batch_code=:batch AND qqs.subject_name=:subject
  GROUP BY qqs.school_id, qdm.dimension_code
)
SELECT 
  dimension_code,
  (SELECT dim_avg FROM per_school WHERE school_id=:school AND dimension_code=ps.dimension_code) AS my_avg,
  (SELECT DENSE_RANK() OVER (ORDER BY dim_avg DESC, school_id ASC) FROM per_school WHERE school_id=:school AND dimension_code=ps.dimension_code) AS my_rank
FROM per_school ps
GROUP BY dimension_code;
```

### 6.3 维度→题目选项分布（区域）
```sql
SELECT qdm.dimension_code, qqd.question_id, qqd.option_level,
       ROUND(qqd.percentage, 2) AS pct
FROM questionnaire_option_distribution qqd
JOIN question_dimension_mapping qdm
  ON qdm.batch_code=qqd.batch_code
 AND qdm.subject_name=qqd.subject_name
 AND qdm.question_id=qqd.question_id
WHERE qqd.batch_code=:batch AND qqd.subject_name=:subject
ORDER BY qdm.dimension_code, qqd.question_id, qqd.option_level;
```

### 6.4 维度→题目选项分布（学校）
```sql
SELECT qdm.dimension_code, qqd.question_id, qqd.option_level,
       ROUND(qqd.percentage, 2) AS pct
FROM questionnaire_option_distribution qqd
JOIN question_dimension_mapping qdm
  ON qdm.batch_code=qqd.batch_code
 AND qdm.subject_name=qqd.subject_name
 AND qdm.question_id=qqd.question_id
WHERE qqd.batch_code=:batch AND qqd.subject_name=:subject AND qqd.school_id=:school
ORDER BY qdm.dimension_code, qqd.question_id, qqd.option_level;
```

---

## 7. API 影响与兼容

- `GET /batch/{batch_code}/regional`：返回的问卷科目将包含 `dimensions[].avg` 与 `dimensions[].questions[]`。
- `GET /batch/{batch_code}/school/{school_code}`：问卷科目将包含 `dimensions[].avg/regional_avg/rank`，可选 `dimensions[].questions[]`。
- 兼容性：保留原 `subjects[].questions[]`（如存在），但前端应优先消费 `dimensions[].questions[]`。

---

## 8. 性能与索引建议

- 建议索引：
  - `questionnaire_question_scores (batch_code, subject_name, school_id, question_id)`
  - `question_dimension_mapping (batch_code, subject_name, question_id)`
  - `questionnaire_option_distribution (batch_code, subject_name, school_id, question_id, option_level)`
- 避免扫描：区域级优先使用 `questionnaire_option_distribution`，不要对明细进行全表统计。
- 并发与重写：保持区域级仅保留 1 条记录的策略（已在 `rewrite_subjects_v12_enhanced_fixed.py` 落地）。

---

## 9. 实施步骤（给 DEV 的任务清单）

### 9.1 开发改造
1) 在 `SubjectsBuilder` 增加 4 个查询方法（见 5.1）。
2) 在 `build_regional_subjects` 针对 `questionnaire`：
   - 组装 `dimensions[].avg`（映射中文名）
   - 组装 `dimensions[].questions[]`（带 `option_distribution`）
3) 在 `build_school_subjects` 针对 `questionnaire`：
   - 组装 `dimensions[].avg/regional_avg/rank`
   - （按需）组装 `dimensions[].questions[]`（学校级分布）
4) 统一两位小数，百分比 0-100 保留两位。
5) 保持现有考试科目逻辑不变。

### 9.2 验收与联调
1) 区域级：
   - 任取维度与题目，验证 `dimensions[].avg` 与 SQL 抽查一致。
   - `dimensions[].questions[].option_distribution` 各选项之和≈100%。
2) 学校级：
   - 验证 `dimensions[].avg`、`regional_avg`，以及 `rank` 与 SQL 结果一致。
   - 验证题目选项分布（如开启）。
3) 脚本辅助：
   - 扩展/复用 `scripts/acceptance_quick_check.py` 增加问卷维度与题目检查；或临时提供 `scripts/inspect_subjects_fields.py` 的问卷断言。

### 9.3 发布与回滚
- 发布：
  - 合并代码 → 重跑 `rewrite_subjects_v12_enhanced_fixed.py` 针对 G4/G7/G8 → 运行去重脚本确保 REGIONAL=1。
- 回滚：
  - 仅为 JSON 合并逻辑变更，无数据库 DDL 依赖；回滚即恢复旧版 SubjectsBuilder 并重写 subjects。

---

## 10. 风险与规避

- 学校级题目分布数据量较大：
  - 建议通过开关控制是否输出 `dimensions[].questions[]` 于学校级。
  - 或仅在“学校分析页面”单独 API 提供题目分布。
- 维度定义不一致：
  - 强依赖 `question_dimension_mapping` 与 `batch_dimension_definition` 完整性；上线前对这两张表做数据健康检查。
- 历史批次：
  - 若 `questionnaire_option_distribution` 曾缺少 `school_id`，需先按“问卷数据学校ID修复总结报告”执行修复与重建分布表。

---

## 11. 示例（片段）

### 11.1 区域级（REGIONAL）问卷科目片段
```json
{
  "subject_name": "思维品质",
  "type": "questionnaire",
  "dimensions": [
    {
      "code": "CQX",
      "name": "好奇心",
      "avg": 3.72,
      "questions": [
        {
          "question_id": "Q101",
          "option_distribution": [
            {"option_level": 5, "option_label": "非常同意", "pct": 28.75},
            {"option_level": 4, "option_label": "同意",     "pct": 43.20},
            {"option_level": 3, "option_label": "一般",     "pct": 18.05},
            {"option_level": 2, "option_label": "不同意",   "pct": 7.10},
            {"option_level": 1, "option_label": "非常不同意", "pct": 2.90}
          ]
        }
      ]
    }
  ]
}
```

### 11.2 学校级（SCHOOL）问卷科目片段
```json
{
  "subject_name": "思维品质",
  "type": "questionnaire",
  "dimensions": [
    {
      "code": "CQX",
      "name": "好奇心",
      "avg": 3.61,
      "regional_avg": 3.72,
      "rank": 18,
      "questions": [
        {
          "question_id": "Q101",
          "option_distribution": [
            {"option_level": 5, "option_label": "非常同意", "pct": 24.12},
            {"option_level": 4, "option_label": "同意",     "pct": 45.33},
            {"option_level": 3, "option_label": "一般",     "pct": 20.45},
            {"option_level": 2, "option_label": "不同意",   "pct": 8.60},
            {"option_level": 1, "option_label": "非常不同意", "pct": 1.50}
          ]
        }
      ]
    }
  ]
}
```

---

## 12. 交付清单与负责人

- 代码：`app/services/subjects_builder.py`（核心改造）
- SQL/验证：随实施附上抽查 SQL（第 6 节）
- 自动化脚本（可复用）：`scripts/rewrite_subjects_v12_enhanced_fixed.py`、`scripts/acceptance_quick_check.py`
- 文档：本实施计划（docs/问卷分析增强_B方案_实施计划.md）

---

如需我进一步拆分为具体代码任务（PR checklist）、或新增校验脚本以保障联调效率，请告知我再补充。 

