-- 移除G7-2025触发器
DROP TRIGGER IF EXISTS g7_guard_insert;
DROP TRIGGER IF EXISTS g7_guard_update;
DROP TRIGGER IF EXISTS prevent_g7_2025_insert;
DROP TRIGGER IF EXISTS prevent_g7_2025_update;
DROP TRIGGER IF EXISTS g7_2025_guard;
DROP TRIGGER IF EXISTS block_g7_2025;

-- 显示剩余触发器
SHOW TRIGGERS LIKE 'statistical_aggregations';