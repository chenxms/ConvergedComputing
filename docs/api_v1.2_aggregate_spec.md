# 汇聚接口规范 v1.2（subjects 统一结构，含问卷增强B）

版本：v1.2  
层级：REGIONAL、SCHOOL 均支持

## 总则
- 统一返回结构 subjects: Subject[]，Subject.type 取值：exam | questionnaire；
- 区域层附带 subjects[].school_rankings；学校层附带 subjects[].region_rank/total_schools；
- 维度对象可带 ank（学校层）、score_rate（考试类）；
- 精度：数值两位小数；百分比 0–100，两位小数；
- 版本标识：schema_version = "v1.2"。

---

## 1. 区域级 subjects
GET /api/v12/batch/{batch_code}/regional

- 说明：返回区域层所有科目汇总；
- 重点（问卷增强B）：
  - dimensions[].avg：维度区域平均分；
  - dimensions[].questions[]：维度→题目→选项分布；
  - 保留 subjects[].questions[] 顶层题目分布（兼容）。

示例（节选，问卷科目）：
`json
{
  "schema_version": "v1.2",
  "batch_code": "G4-2025",
  "aggregation_level": "REGIONAL",
  "subjects": [
    {
      "subject_name": "思维品质",
      "type": "questionnaire",
      "metrics": { "avg": 75.12, "stddev": 8.23, "max": 98.5, "min": 40.0, "difficulty": 0.75 },
      "school_rankings": [ { "school_code": "5071", "school_name": "一中", "avg": 77.20, "rank": 3 } ],
      "dimensions": [
        {
          "code": "CZL-hqx", "name": "好奇心", "avg": 3.34,
          "questions": [
            {
              "question_id": "12_4",
              "option_distribution": [
                { "option_level": 1, "option_label": "非常不同意", "pct": 11.29 },
                { "option_level": 2, "option_label": "不同意",     "pct": 9.86  }
              ]
            }
          ]
        }
      ],
      "questions": [
        {
          "question_id": "12_4",
          "option_distribution": [ { "option_level": 1, "pct": 11.29 } ]
        }
      ]
    }
  ]
}
`

---

## 2. 学校级 subjects
GET /api/v12/batch/{batch_code}/school/{school_code}

- 说明：返回学校层所有科目汇总；
- 重点（问卷增强B）：
  - dimensions[].regional_avg：维度区域对标均分；
  - 可返回学校级 subjects[].questions[]（如底表具备 school 粒度）；

示例（节选，问卷科目）：
`json
{
  "schema_version": "v1.2",
  "batch_code": "G4-2025",
  "aggregation_level": "SCHOOL",
  "school_code": "5071",
  "subjects": [
    {
      "subject_name": "思维品质",
      "type": "questionnaire",
      "metrics": { "avg": 76.01, "stddev": 7.10, "max": 95.0, "min": 45.2, "difficulty": 0.76 },
      "region_rank": 3,
      "total_schools": 125,
      "dimensions": [
        { "code": "CZL-hqx", "name": "好奇心", "avg": 3.42, "regional_avg": 3.34 }
      ],
      "questions": [
        {
          "question_id": "12_4",
          "option_distribution": [ { "option_level": 1, "pct": 10.02 } ]
        }
      ]
    }
  ]
}
`

---

## 3. 物化（批量生成缓存）
POST /api/v12/batch/{batch_code}/materialize

- 说明：生成区域与全部学校的 subjects 结果；
- 返回：{ batch_code, schools_materialized }；
- 建议：批处理或定时任务触发，避免重复生成。

---

## 4. 字段与类型（核心）
- Subject（公共）：subject_name, 	ype, metrics, school_rankings?, egion_rank?, 	otal_schools?, dimensions?, questions?
- QuestionnaireDimension：code, 
ame, vg, egional_avg?, option_distribution?, questions?
- OptionDistribution：option_level, option_label?, pct（0–100，两位小数）
- Metrics：vg, stddev, max, min, difficulty

---

## 5. 精度与风格
- 数值统一两位小数；
- 百分比统一 0–100 两位小数；
- 保持 v1.2 schema 下字段命名与现有前端一致。

---

## 6. 兼容性说明
- 旧前端可继续消费 subjects[].questions[]；
- 新前端优先消费：
  - 区域：dimensions[].avg、dimensions[].questions[]；
  - 学校：dimensions[].regional_avg；
- 学校级题目分布是否返回取决于底表是否具备 school 粒度。
