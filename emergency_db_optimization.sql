-- 紧急数据库优化：减少锁竞争

-- 1. 临时增加锁等待时间（当前会话）
SET SESSION innodb_lock_wait_timeout = 300;

-- 2. 检查当前锁状态
SELECT
    r.trx_mysql_thread_id AS waiting_thread,
    b.trx_mysql_thread_id AS blocking_thread,
    r.trx_query AS waiting_query
FROM information_schema.innodb_lock_waits w
JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id
JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id;

-- 3. 清理可能的僵死事务
SELECT
    trx_mysql_thread_id,
    trx_state,
    TIMESTAMPDIFF(SECOND, trx_started, NOW()) as duration_seconds,
    trx_query
FROM information_schema.innodb_trx
WHERE trx_state = 'LOCK WAIT'
AND TIMESTAMPDIFF(SECOND, trx_started, NOW()) > 60;

-- 4. 如果需要，可以手动结束长时间锁等待的线程
-- KILL <thread_id>;

-- 5. 优化批次处理：分离问卷和考试科目的更新
-- 先更新问卷数据（减少JSON大小）
UPDATE statistical_aggregations
SET statistics_data = JSON_REPLACE(
    statistics_data,
    '$.updated_at',
    UTC_TIMESTAMP()
)
WHERE batch_code = 'G7-2025'
AND aggregation_level = 'REGIONAL'
AND JSON_EXTRACT(statistics_data, '$.subjects[*].type') LIKE '%questionnaire%';

-- 6. 查看当前区域数据状态
SELECT
    batch_code,
    school_id,
    school_name,
    calculation_status,
    CHAR_LENGTH(statistics_data) as data_size_bytes,
    updated_at
FROM statistical_aggregations
WHERE batch_code = 'G7-2025' AND aggregation_level = 'REGIONAL';