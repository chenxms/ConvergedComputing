# 汇聚接口规范 v1.2（subjects 统一结构）

版本：v1.2  
状态：可用（REGIONAL+SCHOOL 均已支持）

## 总览

- 统一输出结构：`subjects`（数组），包含考试与问卷（`type`: `exam`/`questionnaire`）。
- 排名：
  - 区域层 → `subjects[].school_rankings`
  - 学校层 → `subjects[].region_rank`, `subjects[].total_schools`
  - 维度层 → `subjects[].dimensions[].rank`（学校维度均分在区域内的名次）
- 问卷分布：
  - 维度：`subjects[].dimensions[].option_distribution = [{option_level, option_label?, pct}]`
  - 题目：`subjects[].questions[].option_distribution = [{option_level, option_label?, pct}]`
- 精度：数值统一两位小数；百分比统一 0–100 且两位小数。
- 版本标识：`schema_version = "v1.2"`

---

## 1. 区域级 subjects

GET `/api/v12/batch/{batch_code}/regional`

- 功能：返回区域级 subjects；若库中缺失，将即时生成并写回。
- 响应：
```
{
  "success": true,
  "message": "v1.2 区域级 subjects 已生成 {batch_code}",
  "data": {
    "schema_version": "v1.2",
    "batch_code": "G4-2025",
    "aggregation_level": "REGIONAL",
    "subjects": [
      {
        "subject_name": "数学",
        "type": "exam",
        "metrics": {"avg": 82.15, "stddev": 11.02, "max": 100.00, "min": 35.50, "difficulty": 0.82},
        "school_rankings": [
          {"school_code": "S001", "school_name": "一中", "avg": 88.32, "rank": 1},
          {"school_code": "S002", "school_name": "二中", "avg": 86.00, "rank": 2}
        ],
        "dimensions": [
          {"code": "ALG", "name": "代数", "max_score": 50.00, "avg": 41.23, "score_rate": 82.46, "rank": 3}
        ]
      },
      {
        "subject_name": "问卷",
        "type": "questionnaire",
        "metrics": {"avg": 76.40, "stddev": 8.50, "max": 100.00, "min": 40.00, "difficulty": 0.76},
        "school_rankings": [{"school_code": "S001", "school_name": "一中", "avg": 80.12, "rank": 1}],
        "dimensions": [
          {"code": "SAT", "name": "满意度", "avg": 78.20, "score_rate": 78.20, "rank": 2,
           "option_distribution": [
             {"option_level": 5, "option_label": "非常满意", "pct": 33.33},
             {"option_level": 4, "option_label": "满意", "pct": 50.00},
             {"option_level": 3, "option_label": "一般", "pct": 16.67}
           ]
          }
        ],
        "questions": [
          {"question_id": "Q1", "option_distribution": [
            {"option_level": 5, "option_label": "非常满意", "pct": 30.00},
            {"option_level": 4, "option_label": "满意", "pct": 70.00}
          ]}
        ]
      }
    ]
  },
  "code": 200
}
```

---

## 2. 学校级 subjects

GET `/api/v12/batch/{batch_code}/school/{school_code}`

- 功能：返回学校级 subjects；若库中缺失，将即时生成并写回。
- 响应（示例）：
```
{
  "success": true,
  "data": {
    "schema_version": "v1.2",
    "batch_code": "G4-2025",
    "aggregation_level": "SCHOOL",
    "school_code": "S001",
    "subjects": [
      {
        "subject_name": "数学",
        "type": "exam",
        "metrics": {"avg": 84.00, "stddev": 10.10, "max": 100.00, "min": 40.00, "difficulty": 0.84},
        "region_rank": 3,
        "total_schools": 57,
        "dimensions": [
          {"code": "ALG", "name": "代数", "avg": 42.10, "score_rate": 84.20, "rank": 4}
        ]
      },
      {
        "subject_name": "问卷",
        "type": "questionnaire",
        "metrics": {"avg": 78.50, "stddev": 7.90, "max": 100.00, "min": 50.00, "difficulty": 0.79},
        "region_rank": 1,
        "total_schools": 57,
        "dimensions": [
          {"code": "SAT", "name": "满意度", "avg": 80.00, "score_rate": 80.00, "rank": 1}
        ]
      }
    ]
  },
  "code": 200
}
```

---

## 3. 全量生成（可用于批次上线前）

POST `/api/v12/batch/{batch_code}/materialize`

- 功能：为指定批次生成/刷新 区域+所有学校 的 v1.2 subjects 产物。
- 响应：
```
{
  "success": true,
  "data": {"batch_code": "G4-2025", "schools_materialized": 165},
  "message": "v1.2 subjects 全量生成完成"
}
```

---

## 字段说明

- `schema_version`: 固定 `v1.2`
- `subjects[].type`: `exam` 或 `questionnaire`
- `metrics`: {`avg`, `stddev`, `max`, `min`, `difficulty`}
- `school_rankings`: [{`school_code`, `school_name`, `avg`, `rank`}]（区域层）
- `region_rank`, `total_schools`: 学校层我校名次与参与学校数
- `dimensions[]`: {`code`, `name`, `max_score?`, `avg`, `score_rate`, `rank?`, `option_distribution?`}
- `questions[]`: 问卷题目层；每题 `option_distribution`
- 精度：所有数值两位小数；`pct` 为 0–100 且两位小数；分布合计≈100%

---

## 错误码

- 4xx：参数/数据不可用（如批次不存在、学校代码不存在）
- 5xx：内部错误（SQL 失败、数据源不可用）

---

## 兼容策略

- 兼容期内保留 legacy 输出（如 academic_subjects/non_academic_subjects）但不建议消费。
- 前端应以 `schema_version` 判定并消费 `subjects`。

