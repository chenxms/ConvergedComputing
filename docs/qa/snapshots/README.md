此目录用于存放聚合 API v1.2 的回归快照（REGIONAL/SCHOOL 输出 JSON）。

建议命名：
- REGIONAL_{batch_code}_{YYYYMMDD}.json
- SCHOOL_{batch_code}_{school_id}_{YYYYMMDD}.json

如何生成与校验：
1) 将接口返回保存为 JSON 文件，放入本目录。
2) 本地或 CI 会自动运行 `ci/validate_aggregation_v12.py`：
   - 使用 `docs/qa/aggregation_v12_schema.json` 进行 JSON Schema 校验；
   - 额外检查：
     - subjects[].type 仅允许 exam/questionnaire；
     - exam.metrics：p10<=p50<=p90；difficulty/discipline∈[0,1]；四项等级比例合计≈100%；
     - questionnaire.metrics：score_rate∈[0,100]；
     - rankings/dimensions 的 rank 为整数且≥1；
   - 失败则退出非零，阻断流水线（若存在快照）。

注意：若本目录为空，CI 会跳过校验（不阻断）。为获得发布闸门能力，请提交至少一份代表性快照样本。

