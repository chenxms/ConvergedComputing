-- 查看当前所有线程状态
SHOW PROCESSLIST;

-- 查看详细的线程信息
SELECT
    ID,
    USER,
    HOST,
    DB,
    COMMAND,
    TIME,
    STATE,
    INFO
FROM INFORMATION_SCHEMA.PROCESSLIST
WHERE COMMAND != 'Sleep'
ORDER BY TIME DESC;

-- 查看当前锁等待情况
SELECT
    r.trx_id waiting_trx_id,
    r.trx_mysql_thread_id waiting_thread,
    r.trx_query waiting_query,
    b.trx_id blocking_trx_id,
    b.trx_mysql_thread_id blocking_thread,
    b.trx_query blocking_query
FROM information_schema.innodb_lock_waits w
INNER JOIN information_schema.innodb_trx b
    ON b.trx_id = w.blocking_trx_id
INNER JOIN information_schema.innodb_trx r
    ON r.trx_id = w.requesting_trx_id;

-- 查看当前事务状态
SELECT
    trx_id,
    trx_state,
    trx_started,
    trx_mysql_thread_id,
    trx_query,
    trx_tables_locked,
    trx_rows_locked
FROM information_schema.innodb_trx
ORDER BY trx_started;

-- 查看锁等待统计
SHOW ENGINE INNODB STATUS\G

-- 杀死特定线程 (谨慎使用)
-- KILL CONNECTION thread_id;
-- KILL QUERY thread_id;