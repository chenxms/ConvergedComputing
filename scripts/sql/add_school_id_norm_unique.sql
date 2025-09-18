-- 生成列 + 唯一索引（MySQL 5.7+/8.0+ / MariaDB 10.2+）
-- 注意：执行前需确保 normalized 唯一键下无重复，否则创建唯一索引会失败。
-- 可先运行：python scripts/fix_regional_duplicates.py --all（或 --batch G7-2025）

-- 1) 生成列（如已存在请跳过本节）
ALTER TABLE `statistical_aggregations`
  ADD COLUMN `school_id_norm` VARCHAR(60)
  GENERATED ALWAYS AS (COALESCE(`school_id`,'REGIONAL')) STORED;

-- 2) 唯一索引（如已存在请跳过本节）
CREATE UNIQUE INDEX `uk_batch_level_school_norm`
  ON `statistical_aggregations` (`batch_code`, `aggregation_level`, `school_id_norm`);

