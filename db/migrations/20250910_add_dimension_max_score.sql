-- 变更单：为 batch_dimension_definition 增加维度满分字段并回填
-- 目的：降低运行时 JSON 解析依赖，统一由结构化表提供维度聚合满分
-- 影响范围：读取 batch_dimension_definition 的读路径（已在 SubjectsBuilder 中优先读取该列，缺失则回退 JSON）

-- 1) 安全检查：如果列已存在则跳过
ALTER TABLE `batch_dimension_definition`
  ADD COLUMN IF NOT EXISTS `dimension_max_score` DECIMAL(10,2) NULL COMMENT '维度满分（题目映射聚合）' AFTER `dimension_code`;

-- 2) 回填：按题目与维度映射求和聚合到维度层
-- 依赖：question_dimension_mapping(qdm) + subject_question_config(sqc)
-- 说明：将同一批次/科目/维度下关联题目的 max_score 进行求和
UPDATE `batch_dimension_definition` bdd
JOIN (
  SELECT qdm.batch_code, qdm.subject_name, qdm.dimension_code,
         SUM(sqc.max_score) AS dimension_max_score
  FROM `question_dimension_mapping` qdm
  JOIN `subject_question_config` sqc
    ON sqc.batch_code    = qdm.batch_code
   AND sqc.subject_name  = qdm.subject_name
   AND sqc.question_id   = qdm.question_id
  GROUP BY qdm.batch_code, qdm.subject_name, qdm.dimension_code
) t
  ON t.batch_code      = bdd.batch_code
 AND t.subject_name    = bdd.subject_name
 AND t.dimension_code  = bdd.dimension_code
SET bdd.`dimension_max_score` = t.dimension_max_score;

-- 3) 索引（可选）：提高常用读路径性能
CREATE INDEX IF NOT EXISTS idx_bdd_batch_subject_dim ON `batch_dimension_definition` (batch_code, subject_name, dimension_code);

-- 4) 验证（只读）：抽查非空比例
-- SELECT subject_name,
--        COUNT(*) AS dims,
--        SUM(CASE WHEN dimension_max_score IS NOT NULL THEN 1 END) AS filled
-- FROM batch_dimension_definition
-- WHERE batch_code = 'G4-2025'
-- GROUP BY subject_name;

