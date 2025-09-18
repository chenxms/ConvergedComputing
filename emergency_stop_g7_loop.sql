-- 紧急停止G7-2025循环写入

-- 1. 创建触发器临时阻止写入
DELIMITER $$

CREATE TRIGGER stop_g7_2025_loop
BEFORE INSERT ON statistical_aggregations
FOR EACH ROW
BEGIN
    IF NEW.batch_code = 'G7-2025' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'G7-2025 temporarily blocked to stop loop';
    END IF;
END$$

CREATE TRIGGER stop_g7_2025_update_loop
BEFORE UPDATE ON statistical_aggregations
FOR EACH ROW
BEGIN
    IF NEW.batch_code = 'G7-2025' OR OLD.batch_code = 'G7-2025' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'G7-2025 temporarily blocked to stop loop';
    END IF;
END$$

DELIMITER ;

-- 2. 查看并终止所有相关进程
SELECT CONCAT('KILL ', id, ';') AS kill_command
FROM information_schema.PROCESSLIST
WHERE (INFO LIKE '%G7-2025%' OR INFO LIKE '%statistical_aggregations%')
AND COMMAND != 'Sleep';