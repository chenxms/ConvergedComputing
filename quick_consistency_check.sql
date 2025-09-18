-- G7-2025批次数据一致性快速检查SQL脚本
-- 执行时间: 2025-09-18
-- 用途: 快速验证预聚合表与汇聚结果的关键指标一致性

-- ======================================================================
-- 1. 数据概览统计
-- ======================================================================

SELECT '=== G7-2025批次数据概览 ===' AS section;

SELECT
    '预聚合表统计' AS category,
    'subject_core_metrics' AS table_name,
    COUNT(*) AS record_count,
    GROUP_CONCAT(DISTINCT subject_name ORDER BY subject_name) AS subjects
FROM subject_core_metrics
WHERE batch_code = 'G7-2025'

UNION ALL

SELECT
    '预聚合表统计' AS category,
    'subject_school_rankings' AS table_name,
    COUNT(*) AS record_count,
    CONCAT(COUNT(DISTINCT school_code), ' 所学校') AS subjects
FROM subject_school_rankings
WHERE batch_code = 'G7-2025'

UNION ALL

SELECT
    '汇聚结果统计' AS category,
    CONCAT('statistical_aggregations (', aggregation_level, ')') AS table_name,
    COUNT(*) AS record_count,
    CASE
        WHEN aggregation_level = 'REGIONAL' THEN '区域级汇聚'
        ELSE CONCAT(COUNT(DISTINCT school_id), ' 所学校')
    END AS subjects
FROM statistical_aggregations
WHERE batch_code = 'G7-2025'
GROUP BY aggregation_level;

-- ======================================================================
-- 2. 区域级核心指标快速对比
-- ======================================================================

SELECT '=== 区域级核心指标对比 ===' AS section;

SELECT
    '核心指标对比' AS validation_type,
    scm.subject_name,
    scm.avg_score AS table_avg_score,
    ROUND(JSON_UNQUOTE(JSON_EXTRACT(
        JSON_EXTRACT(sa.statistics_data, '$.subjects[*]'),
        CONCAT('$[', FIND_IN_SET(scm.subject_name, (
            SELECT GROUP_CONCAT(
                JSON_UNQUOTE(JSON_EXTRACT(JSON_EXTRACT(statistics_data, '$.subjects[*]'), '$[*].subject_name'))
                ORDER BY JSON_UNQUOTE(JSON_EXTRACT(JSON_EXTRACT(statistics_data, '$.subjects[*]'), '$[*].subject_name'))
            )
            FROM statistical_aggregations sa2
            WHERE sa2.batch_code = 'G7-2025' AND sa2.aggregation_level = 'REGIONAL'
        )) - 1, '].metrics.avg')
    )), 3) AS json_avg_score,
    scm.difficulty_coefficient AS table_difficulty,
    ROUND(JSON_UNQUOTE(JSON_EXTRACT(
        JSON_EXTRACT(sa.statistics_data, '$.subjects[*]'),
        CONCAT('$[', FIND_IN_SET(scm.subject_name, (
            SELECT GROUP_CONCAT(
                JSON_UNQUOTE(JSON_EXTRACT(JSON_EXTRACT(statistics_data, '$.subjects[*]'), '$[*].subject_name'))
                ORDER BY JSON_UNQUOTE(JSON_EXTRACT(JSON_EXTRACT(statistics_data, '$.subjects[*]'), '$[*].subject_name'))
            )
            FROM statistical_aggregations sa2
            WHERE sa2.batch_code = 'G7-2025' AND sa2.aggregation_level = 'REGIONAL'
        )) - 1, '].metrics.difficulty')
    )), 4) AS json_difficulty,
    CASE
        WHEN ABS(scm.avg_score - JSON_UNQUOTE(JSON_EXTRACT(
            JSON_EXTRACT(sa.statistics_data, '$.subjects[*]'),
            CONCAT('$[', FIND_IN_SET(scm.subject_name, (
                SELECT GROUP_CONCAT(
                    JSON_UNQUOTE(JSON_EXTRACT(JSON_EXTRACT(statistics_data, '$.subjects[*]'), '$[*].subject_name'))
                    ORDER BY JSON_UNQUOTE(JSON_EXTRACT(JSON_EXTRACT(statistics_data, '$.subjects[*]'), '$[*].subject_name'))
                )
                FROM statistical_aggregations sa2
                WHERE sa2.batch_code = 'G7-2025' AND sa2.aggregation_level = 'REGIONAL'
            )) - 1, '].metrics.avg')
        ))) <= 0.01 THEN '✓ 一致'
        ELSE '✗ 不一致'
    END AS avg_score_status,
    CASE
        WHEN JSON_EXTRACT(sa.statistics_data, '$.subjects[0].metrics.student_count') IS NULL THEN '✗ JSON缺失student_count'
        ELSE '✓ 包含student_count'
    END AS student_count_status
FROM subject_core_metrics scm
CROSS JOIN statistical_aggregations sa
WHERE scm.batch_code = 'G7-2025'
  AND sa.batch_code = 'G7-2025'
  AND sa.aggregation_level = 'REGIONAL'
ORDER BY scm.subject_name;

-- ======================================================================
-- 3. 学校级抽样验证 (随机5所学校)
-- ======================================================================

SELECT '=== 学校级抽样验证 ===' AS section;

-- 显示随机选择的学校
SELECT
    '抽样信息' AS info_type,
    school_code AS sample_school,
    COUNT(DISTINCT subject_name) AS subjects_count,
    GROUP_CONCAT(DISTINCT subject_name ORDER BY subject_name LIMIT 3) AS sample_subjects
FROM subject_school_rankings
WHERE batch_code = 'G7-2025'
GROUP BY school_code
ORDER BY RAND()
LIMIT 5;

-- ======================================================================
-- 4. 数据完整性概览
-- ======================================================================

SELECT '=== 数据完整性概览 ===' AS section;

SELECT
    '覆盖率统计' AS stat_type,
    CONCAT(
        '学校覆盖: ',
        (SELECT COUNT(DISTINCT school_id) FROM statistical_aggregations WHERE batch_code = 'G7-2025' AND aggregation_level = 'SCHOOL'),
        '/',
        (SELECT COUNT(DISTINCT school_code) FROM subject_school_rankings WHERE batch_code = 'G7-2025'),
        ' (',
        ROUND((SELECT COUNT(DISTINCT school_id) FROM statistical_aggregations WHERE batch_code = 'G7-2025' AND aggregation_level = 'SCHOOL') * 100.0 /
              (SELECT COUNT(DISTINCT school_code) FROM subject_school_rankings WHERE batch_code = 'G7-2025'), 1),
        '%)'
    ) AS coverage_info,
    CASE
        WHEN (SELECT COUNT(DISTINCT school_id) FROM statistical_aggregations WHERE batch_code = 'G7-2025' AND aggregation_level = 'SCHOOL') =
             (SELECT COUNT(DISTINCT school_code) FROM subject_school_rankings WHERE batch_code = 'G7-2025')
        THEN '✓ 完整覆盖'
        ELSE '⚠ 部分缺失'
    END AS coverage_status;

-- ======================================================================
-- 5. JSON字段结构检查
-- ======================================================================

SELECT '=== JSON字段结构检查 ===' AS section;

SELECT
    'JSON结构检查' AS validation_type,
    aggregation_level,
    data_version,
    COUNT(*) AS total_records,
    SUM(CASE WHEN JSON_VALID(statistics_data) THEN 1 ELSE 0 END) AS valid_json_count,
    SUM(CASE WHEN JSON_EXTRACT(statistics_data, '$.subjects[0].metrics.avg') IS NOT NULL THEN 1 ELSE 0 END) AS has_avg_field,
    SUM(CASE WHEN JSON_EXTRACT(statistics_data, '$.subjects[0].metrics.student_count') IS NOT NULL THEN 1 ELSE 0 END) AS has_student_count,
    CASE
        WHEN data_version = 'v1.2' THEN '✓ 正确版本'
        ELSE '✗ 版本错误'
    END AS version_status
FROM statistical_aggregations
WHERE batch_code = 'G7-2025'
GROUP BY aggregation_level, data_version
ORDER BY aggregation_level;

-- ======================================================================
-- 6. 验证结果汇总
-- ======================================================================

SELECT '=== 验证结果汇总 ===' AS section;

SELECT
    '最终验证摘要' AS report_section,
    CONCAT(
        '✓ 区域级记录: ', (SELECT COUNT(*) FROM subject_core_metrics WHERE batch_code = 'G7-2025'), ' 条 | ',
        '✓ 学校级记录: ', (SELECT COUNT(*) FROM subject_school_rankings WHERE batch_code = 'G7-2025'), ' 条 | ',
        '✓ 汇聚结果: ', (SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = 'G7-2025'), ' 条'
    ) AS data_summary,
    CASE
        WHEN (SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = 'G7-2025' AND data_version != 'v1.2') = 0
        THEN '✓ Schema版本v1.2正确'
        ELSE '✗ 存在版本问题'
    END AS schema_status,
    CASE
        WHEN (SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = 'G7-2025' AND JSON_EXTRACT(statistics_data, '$.subjects[0].metrics.student_count') IS NULL) > 0
        THEN '⚠ JSON缺少student_count字段'
        ELSE '✓ JSON字段完整'
    END AS json_completeness,
    CONCAT('验证时间: ', NOW()) AS validation_timestamp;

-- 使用说明
SELECT '=== 使用说明 ===' AS section;
SELECT
    '快速检查完成' AS status,
    '1. 检查上述各项指标是否为 ✓ 状态' AS step1,
    '2. 如发现 ✗ 或 ⚠ 标记，运行详细Python验证脚本' AS step2,
    '3. 重点关注: 平均分一致性、学校覆盖率、JSON字段完整性' AS step3;