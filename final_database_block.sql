-- 最终数据库层面阻断G7数据生成
-- 直接在数据库中执行这些SQL语句

-- 1. 创建阻断触发器
DELIMITER $$
CREATE TRIGGER prevent_g7_data_insert
BEFORE INSERT ON statistical_aggregations
FOR EACH ROW
BEGIN
    IF NEW.batch_code LIKE '%G7%' OR NEW.batch_code LIKE '%2025%' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7/2025批次数据插入被管理员阻断';
    END IF;
END$$

CREATE TRIGGER prevent_g7_data_update
BEFORE UPDATE ON statistical_aggregations
FOR EACH ROW
BEGIN
    IF NEW.batch_code LIKE '%G7%' OR NEW.batch_code LIKE '%2025%' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7/2025批次数据更新被管理员阻断';
    END IF;
END$$
DELIMITER ;

-- 2. 清理现有数据
DELETE FROM statistical_aggregations WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%';

-- 3. 验证触发器
-- 这个插入应该失败
INSERT INTO statistical_aggregations 
(batch_code, aggregation_level, school_id, statistics_data, data_version, calculation_status, created_at, updated_at)
VALUES ('G7-2025', 'REGIONAL', 'TEST', '{}', 'TEST', 'COMPLETED', NOW(), NOW());

-- 如果需要移除触发器，执行：
-- DROP TRIGGER IF EXISTS prevent_g7_data_insert;
-- DROP TRIGGER IF EXISTS prevent_g7_data_update;