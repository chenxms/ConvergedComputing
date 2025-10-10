-- G7-2025 批次关键表简单备份脚本（UTF-8 无 BOM）
-- 使用方法：mysql -u <user> -p<pass> <db> < backup_G7_2025_stats_clean.sql

SET @batch_code := 'G7-2025';
SET @backup_timestamp := NOW();

-- 0) 记录备份任务
CREATE TABLE IF NOT EXISTS backup_records (
    id INT AUTO_INCREMENT PRIMARY KEY,
    backup_type VARCHAR(100) NOT NULL,
    batch_code VARCHAR(50) NOT NULL,
    backup_timestamp DATETIME NOT NULL,
    records_count INT DEFAULT 0,
    backup_size_mb DECIMAL(10,2) DEFAULT 0,
    backup_status ENUM('STARTED','COMPLETED','FAILED') DEFAULT 'STARTED',
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO backup_records (backup_type, batch_code, backup_timestamp, backup_status, notes)
VALUES ('G7-2025_PRECHECK_BACKUP', @batch_code, @backup_timestamp, 'STARTED', 'Pre-pipeline restart backup');
SET @backup_id := LAST_INSERT_ID();

-- 1) 备份 statistical_aggregations（按批次）
SELECT 'Backing up statistical_aggregations...' AS status;
CREATE TABLE IF NOT EXISTS statistical_aggregations_backup_g7_2025 LIKE statistical_aggregations;
DELETE FROM statistical_aggregations_backup_g7_2025 WHERE batch_code = @batch_code;
INSERT INTO statistical_aggregations_backup_g7_2025
SELECT * FROM statistical_aggregations WHERE batch_code = @batch_code;
SET @stats_count := ROW_COUNT();

-- 2) 备份 school_master_data（按涉及学校）
SELECT 'Backing up school_master_data...' AS status;
CREATE TABLE IF NOT EXISTS school_master_data_backup_g7_2025 LIKE school_master_data;
DELETE FROM school_master_data_backup_g7_2025
WHERE school_id IN (
    SELECT DISTINCT school_id FROM statistical_aggregations WHERE batch_code = @batch_code
);
INSERT INTO school_master_data_backup_g7_2025
SELECT DISTINCT sm.*
FROM school_master_data sm
JOIN statistical_aggregations sa
  ON sa.school_id = sm.school_id AND sa.batch_code = @batch_code;
SET @school_count := ROW_COUNT();

-- 3) 备份 grade_aggregation_main（如存在）
SELECT 'Backing up grade_aggregation_main...' AS status;
CREATE TABLE IF NOT EXISTS grade_aggregation_backup_g7_2025 LIKE grade_aggregation_main;
DELETE FROM grade_aggregation_backup_g7_2025 WHERE batch_code = @batch_code;
INSERT INTO grade_aggregation_backup_g7_2025
SELECT * FROM grade_aggregation_main WHERE batch_code = @batch_code;
SET @grade_count := ROW_COUNT();

-- 4) 汇总与收尾
SET @records_total := COALESCE(@stats_count,0) + COALESCE(@school_count,0) + COALESCE(@grade_count,0);
UPDATE backup_records
SET records_count = @records_total,
    backup_status = 'COMPLETED',
    notes = CONCAT('Stats: ', COALESCE(@stats_count,0), ', Schools: ', COALESCE(@school_count,0), ', Grades: ', COALESCE(@grade_count,0))
WHERE id = @backup_id;

SELECT 'BACKUP COMPLETED' AS status, @records_total AS total_records, @backup_timestamp AS ts;

