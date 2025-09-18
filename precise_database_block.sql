-- 精确的数据库阻断 - 只阻断G7-2025批次
-- 更安全的版本，不会影响其他批次

-- 1. 创建精确阻断触发器
DELIMITER $$
CREATE TRIGGER prevent_g7_2025_insert
BEFORE INSERT ON statistical_aggregations
FOR EACH ROW
BEGIN
    IF NEW.batch_code = 'G7-2025' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025批次数据插入被管理员临时阻断';
    END IF;
END$$

CREATE TRIGGER prevent_g7_2025_update
BEFORE UPDATE ON statistical_aggregations
FOR EACH ROW
BEGIN
    IF NEW.batch_code = 'G7-2025' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025批次数据更新被管理员临时阻断';
    END IF;
END$$
DELIMITER ;

-- 2. 清理现有G7-2025数据
DELETE FROM statistical_aggregations WHERE batch_code = 'G7-2025';

-- 3. 验证触发器（这个插入应该失败）
-- INSERT INTO statistical_aggregations 
-- (batch_code, aggregation_level, school_id, statistics_data, data_version, calculation_status, created_at, updated_at)
-- VALUES ('G7-2025', 'REGIONAL', 'TEST', '{}', 'TEST', 'COMPLETED', NOW(), NOW());

-- 4. 测试其他批次不受影响（这个插入应该成功，然后立即删除）
-- INSERT INTO statistical_aggregations 
-- (batch_code, aggregation_level, school_id, statistics_data, data_version, calculation_status, created_at, updated_at)
-- VALUES ('TEST-2025', 'REGIONAL', 'TEST', '{}', 'TEST', 'COMPLETED', NOW(), NOW());
-- DELETE FROM statistical_aggregations WHERE batch_code = 'TEST-2025';

-- 移除触发器的命令（需要时执行）：
-- DROP TRIGGER IF EXISTS prevent_g7_2025_insert;
-- DROP TRIGGER IF EXISTS prevent_g7_2025_update;