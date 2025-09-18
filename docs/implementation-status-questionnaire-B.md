# 问卷分析增强（方案B）实施进展与交付清单

本文为《问卷分析增强_B方案实施计划》的实施进展同步与交付物清单。对应原始实施计划文档（文件名含中文，路径在仓库 docs/ 目录），本文件汇总本次后端改造的范围、状态、接口影响与验证结果，便于产品、前端与测试对齐。

## 1. 范围与目标
- 科目类型：问卷（`type = "questionnaire"`）。
- 区域级（REGIONAL）：
  - 新增维度平均分 `dimensions[].avg`；
  - 新增按维度归组的题目选项分布 `dimensions[].questions[].option_distribution`；
  - 保留顶层 `subjects[].questions[]` 以兼容旧前端。
- 学校级（SCHOOL）：
  - 维度对象新增区域对标均分 `dimensions[].regional_avg`；
  - 可返回学校级题目选项分布（视底表是否具备 school 粒度）。

## 2. 已完成功能
- 区域级：
  - 维度均分计算（优先 `student_cleaned_scores.dimension_scores`，缺失时回退 `questionnaire_question_scores + question_dimension_mapping`）。
  - 按维度归组的“题目→选项分布”（优先 `questionnaire_option_distribution` 的 count 聚合）。
  - 继续输出顶层 `subjects[].questions[]`（兼容）。
- 学校级：
  - `dimensions[].regional_avg` 补齐；
  - 学校级题目选项分布（若底表有 school 维度，否则不输出）。
- API 接入：`app/api/subjects_v12_api.py` 已切换到增强版构建器。
- 精度统一：两位小数，百分比字段 0–100，两位小数。

## 3. 数据源与口径
- 维度均分（区域/学校）：
  - 首选 `student_cleaned_scores.dimension_scores` 中的 JSON 字段（`{"<dim>":{"score":x}}`）。
  - 回退口径：`questionnaire_question_scores` 与 `question_dimension_mapping` 关联后，按维度 `AVG(original_score)` 计算。
- 题目选项分布：
  - `questionnaire_option_distribution` 的 `count` 聚合为主，百分比由局部分母求得。
  - 选项标签通过量表表或明细回推获得，缺失时使用通用标签兜底。

## 4. 接口影响与兼容性
- 新增/变更（问卷科目）：
  - `dimensions[].avg`（区域、学校页均可出现，学校页为本校均分）；
  - `dimensions[].regional_avg`（学校页对标区域均分）；
  - `dimensions[].questions[]`（维度→题目→选项分布，区域页优先）；
  - 兼容保留 `subjects[].questions[]`（原有结构不移除）。
- 端点：
  - 区域：`GET /api/v12/batch/{batch_code}/regional`
  - 学校：`GET /api/v12/batch/{batch_code}/school/{school_code}`
  - 物化：`POST /api/v12/batch/{batch_code}/materialize`
- 版本标识：`schema_version = "v1.2"`。

## 5. 回归验证（批次：G4-2025）
- 区域页：问卷维度均分已呈现；维度→题目→选项分布正常；
- 学校页：`dimensions[].regional_avg` 正常；学校级题目分布根据底表 school 粒度自动呈现/省略；
- 百分比字段范围规范；返回结构与旧前端兼容。

## 6. 兼容与风控
- 字符集/排序规则（Collation）差异：已在 SQL 中就地规避（统一 `utf8mb4_unicode_ci`）；建议中长期统一库/表/列 Collation 以减轻运行开销；
- 当 `questionnaire_option_distribution` 缺少 `school_id` 列时：区域页维度→题目分布可用，学校级题目分布省略。

## 7. 示例（节选）
```json
{
  "subject_name": "思维品质",
  "type": "questionnaire",
  "dimensions": [
    {
      "code": "CZL-hqx",
      "name": "好奇心",
      "avg": 3.34,
      "questions": [
        { "question_id": "12_4", "option_distribution": [
          { "option_level": 1, "option_label": "非常不同意", "pct": 11.29 },
          { "option_level": 2, "option_label": "不同意",     "pct": 9.86  }
        ]}
      ]
    }
  ]
}
```

## 8. 下一步建议
- 增加基于 JSON Schema 的自动回归；
- 为前端提供默认可视化规格（色板、排序、断点）参考；
- 长期统一数据库字符集与 Collation。

