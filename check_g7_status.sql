-- 检查G7-2025当前状态
SELECT
    batch_code,
    aggregation_level,
    school_id,
    school_name,
    calculation_status,
    JSON_EXTRACT(statistics_data, '$.data_version') as data_version,
    JSON_EXTRACT(statistics_data, '$.schema_version') as schema_version,
    JSON_LENGTH(JSON_EXTRACT(statistics_data, '$.subjects')) as subject_count,
    CHAR_LENGTH(statistics_data) as json_size,
    updated_at
FROM statistical_aggregations
WHERE batch_code = 'G7-2025'
AND aggregation_level = 'REGIONAL';