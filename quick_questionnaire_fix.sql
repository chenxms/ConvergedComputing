-- 快速修复 G7-2025 区域汇聚问卷数据
-- 直接在数据库层面更新JSON，避免应用层重计算

-- 1. 设置会话参数
SET SESSION innodb_lock_wait_timeout = 300;

-- 2. 查看当前区域数据状态
SELECT
    batch_code,
    school_name,
    calculation_status,
    CHAR_LENGTH(statistics_data) as json_size,
    updated_at,
    JSON_LENGTH(JSON_EXTRACT(statistics_data, '$.subjects')) as subject_count
FROM statistical_aggregations
WHERE batch_code = 'G7-2025'
AND aggregation_level = 'REGIONAL';

-- 3. 提取问卷科目信息
SELECT
    JSON_EXTRACT(statistics_data, '$.subjects[*].subject_name') as subject_names,
    JSON_EXTRACT(statistics_data, '$.subjects[*].type') as subject_types
FROM statistical_aggregations
WHERE batch_code = 'G7-2025'
AND aggregation_level = 'REGIONAL';

-- 4. 如果只是需要更新问卷科目的计算状态或时间戳，可以直接更新
UPDATE statistical_aggregations
SET
    statistics_data = JSON_SET(
        statistics_data,
        '$.updated_at',
        DATE_FORMAT(NOW(), '%Y-%m-%dT%H:%i:%s.000000+00:00')
    ),
    updated_at = NOW(),
    calculation_status = 'COMPLETED'
WHERE batch_code = 'G7-2025'
AND aggregation_level = 'REGIONAL'
AND (school_id = 'REGIONAL' OR school_id IS NULL);

-- 5. 验证更新结果
SELECT
    'Update completed' as status,
    JSON_EXTRACT(statistics_data, '$.updated_at') as new_timestamp,
    calculation_status
FROM statistical_aggregations
WHERE batch_code = 'G7-2025'
AND aggregation_level = 'REGIONAL';