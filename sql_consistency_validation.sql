-- SQL数据一致性校验脚本
-- 验证预聚合表与汇聚结果的一致性
-- Author: PO测试方案执行
-- Date: 2025-09-18

-- ======================================================================
-- 1. 基础数据统计概览
-- ======================================================================

-- 1.1 各表记录数统计
SELECT
    '预聚合表统计' AS category,
    'subject_core_metrics' AS table_name,
    COUNT(*) AS record_count,
    COUNT(DISTINCT subject_name) AS unique_subjects
FROM subject_core_metrics
WHERE batch_id = 'G7-2025'

UNION ALL

SELECT
    '预聚合表统计' AS category,
    'subject_school_rankings' AS table_name,
    COUNT(*) AS record_count,
    COUNT(DISTINCT school_id) AS unique_schools
FROM subject_school_rankings
WHERE batch_id = 'G7-2025'

UNION ALL

SELECT
    '汇聚结果表统计' AS category,
    'statistical_aggregations (regional)' AS table_name,
    COUNT(*) AS record_count,
    COUNT(DISTINCT JSON_UNQUOTE(JSON_EXTRACT(statistics_data, '$.batch_id'))) AS unique_batches
FROM statistical_aggregations
WHERE batch_id = 'G7-2025' AND aggregation_level = 'regional'

UNION ALL

SELECT
    '汇聚结果表统计' AS category,
    'statistical_aggregations (school)' AS table_name,
    COUNT(*) AS record_count,
    COUNT(DISTINCT school_id) AS unique_schools
FROM statistical_aggregations
WHERE batch_id = 'G7-2025' AND aggregation_level = 'school';

-- ======================================================================
-- 2. 区域级数据一致性验证
-- ======================================================================

-- 2.1 区域级核心指标对比
SELECT
    '区域级对比' AS validation_type,
    scm.subject_name,
    scm.avg_score AS table_avg_score,
    ROUND(JSON_UNQUOTE(JSON_EXTRACT(
        JSON_EXTRACT(sa.statistics_data,
        CONCAT('$.subjects[', idx.idx, '].metrics.avg')), '$'
    )), 3) AS json_avg_score,
    ABS(scm.avg_score - JSON_UNQUOTE(JSON_EXTRACT(
        JSON_EXTRACT(sa.statistics_data,
        CONCAT('$.subjects[', idx.idx, '].metrics.avg')), '$'
    ))) AS avg_score_diff,
    scm.student_count AS table_student_count,
    JSON_UNQUOTE(JSON_EXTRACT(
        JSON_EXTRACT(sa.statistics_data,
        CONCAT('$.subjects[', idx.idx, '].metrics.student_count')), '$'
    )) AS json_student_count,
    scm.difficulty_coefficient AS table_difficulty,
    ROUND(JSON_UNQUOTE(JSON_EXTRACT(
        JSON_EXTRACT(sa.statistics_data,
        CONCAT('$.subjects[', idx.idx, '].metrics.difficulty_coefficient')), '$'
    )), 4) AS json_difficulty,
    CASE
        WHEN ABS(scm.avg_score - JSON_UNQUOTE(JSON_EXTRACT(
            JSON_EXTRACT(sa.statistics_data,
            CONCAT('$.subjects[', idx.idx, '].metrics.avg')), '$'
        ))) > 0.001 THEN '分数不一致'
        WHEN scm.student_count != JSON_UNQUOTE(JSON_EXTRACT(
            JSON_EXTRACT(sa.statistics_data,
            CONCAT('$.subjects[', idx.idx, '].metrics.student_count')), '$'
        )) THEN '学生数不一致'
        ELSE '一致'
    END AS consistency_status
FROM subject_core_metrics scm
JOIN statistical_aggregations sa ON sa.batch_id = 'G7-2025' AND sa.aggregation_level = 'regional'
JOIN (
    SELECT 0 AS idx UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
    UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9
) idx ON JSON_UNQUOTE(JSON_EXTRACT(
    JSON_EXTRACT(sa.statistics_data, CONCAT('$.subjects[', idx.idx, '].subject_name')), '$'
)) = scm.subject_name
WHERE scm.batch_id = 'G7-2025'
ORDER BY scm.subject_name;

-- 2.2 区域级schema版本检查
SELECT
    'Schema版本检查' AS validation_type,
    aggregation_level,
    schema_version,
    COUNT(*) AS record_count,
    CASE
        WHEN schema_version = 'v1.2' THEN '✓ 正确'
        ELSE '✗ 版本错误'
    END AS version_status
FROM statistical_aggregations
WHERE batch_id = 'G7-2025'
GROUP BY aggregation_level, schema_version
ORDER BY aggregation_level, schema_version;

-- ======================================================================
-- 3. 学校级数据一致性验证（随机抽样）
-- ======================================================================

-- 3.1 随机选择10所学校进行验证
SET @sample_schools = (
    SELECT GROUP_CONCAT(DISTINCT school_id ORDER BY RAND() LIMIT 10)
    FROM subject_school_rankings
    WHERE batch_id = 'G7-2025'
);

-- 3.2 随机选择3个科目进行验证
SET @sample_subjects = (
    SELECT GROUP_CONCAT(DISTINCT subject_name ORDER BY RAND() LIMIT 3)
    FROM subject_school_rankings
    WHERE batch_id = 'G7-2025'
);

-- 显示抽样信息
SELECT
    '抽样信息' AS info_type,
    '随机选择的学校' AS description,
    @sample_schools AS sample_data
UNION ALL
SELECT
    '抽样信息' AS info_type,
    '随机选择的科目' AS description,
    @sample_subjects AS sample_data;

-- 3.3 学校级核心指标对比（使用抽样数据）
SELECT
    '学校级对比' AS validation_type,
    ssr.school_id,
    ssr.subject_name,
    ssr.avg_score AS table_avg_score,
    ROUND(JSON_UNQUOTE(JSON_EXTRACT(
        JSON_EXTRACT(sa.statistics_data,
        CONCAT('$.subjects[', subj_idx.idx, '].metrics.avg')), '$'
    )), 3) AS json_avg_score,
    ssr.rank AS table_rank,
    JSON_UNQUOTE(JSON_EXTRACT(
        JSON_EXTRACT(sa.statistics_data,
        CONCAT('$.subjects[', subj_idx.idx, '].school_ranking')), '$'
    )) AS json_rank,
    ssr.student_count AS table_student_count,
    JSON_UNQUOTE(JSON_EXTRACT(
        JSON_EXTRACT(sa.statistics_data,
        CONCAT('$.subjects[', subj_idx.idx, '].metrics.student_count')), '$'
    )) AS json_student_count,
    CASE
        WHEN ABS(ssr.avg_score - JSON_UNQUOTE(JSON_EXTRACT(
            JSON_EXTRACT(sa.statistics_data,
            CONCAT('$.subjects[', subj_idx.idx, '].metrics.avg')), '$'
        ))) > 0.001 THEN '分数不一致'
        WHEN ssr.rank != JSON_UNQUOTE(JSON_EXTRACT(
            JSON_EXTRACT(sa.statistics_data,
            CONCAT('$.subjects[', subj_idx.idx, '].school_ranking')), '$'
        )) THEN '排名不一致'
        WHEN ssr.student_count != JSON_UNQUOTE(JSON_EXTRACT(
            JSON_EXTRACT(sa.statistics_data,
            CONCAT('$.subjects[', subj_idx.idx, '].metrics.student_count')), '$'
        )) THEN '学生数不一致'
        ELSE '一致'
    END AS consistency_status
FROM subject_school_rankings ssr
JOIN statistical_aggregations sa ON ssr.school_id = sa.school_id
    AND sa.batch_id = 'G7-2025' AND sa.aggregation_level = 'school'
JOIN (
    SELECT 0 AS idx UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
    UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9
) subj_idx ON JSON_UNQUOTE(JSON_EXTRACT(
    JSON_EXTRACT(sa.statistics_data, CONCAT('$.subjects[', subj_idx.idx, '].subject_name')), '$'
)) = ssr.subject_name
WHERE ssr.batch_id = 'G7-2025'
    AND FIND_IN_SET(ssr.school_id, @sample_schools) > 0
    AND FIND_IN_SET(ssr.subject_name, @sample_subjects) > 0
ORDER BY ssr.school_id, ssr.subject_name
LIMIT 30;  -- 最多30条记录进行详细对比

-- ======================================================================
-- 4. JSON字段完整性验证
-- ======================================================================

-- 4.1 检查JSON结构完整性
SELECT
    'JSON字段检查' AS validation_type,
    aggregation_level,
    COUNT(*) AS total_records,
    SUM(CASE WHEN JSON_VALID(statistics_data) THEN 1 ELSE 0 END) AS valid_json_count,
    SUM(CASE WHEN JSON_EXTRACT(statistics_data, '$.subjects') IS NOT NULL THEN 1 ELSE 0 END) AS has_subjects_field,
    SUM(CASE WHEN JSON_EXTRACT(statistics_data, '$.batch_id') = '"G7-2025"' THEN 1 ELSE 0 END) AS correct_batch_id,
    ROUND(SUM(CASE WHEN JSON_VALID(statistics_data) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS json_valid_rate
FROM statistical_aggregations
WHERE batch_id = 'G7-2025'
GROUP BY aggregation_level
ORDER BY aggregation_level;

-- 4.2 检查增强字段存在性（抽样检查）
SELECT
    'JSON增强字段检查' AS validation_type,
    sa.aggregation_level,
    sa.school_id,
    JSON_LENGTH(JSON_EXTRACT(sa.statistics_data, '$.subjects')) AS subjects_count,
    CASE WHEN JSON_EXTRACT(sa.statistics_data, '$.subjects[0].metrics.percentiles') IS NOT NULL
         THEN '✓' ELSE '✗' END AS has_percentiles,
    CASE WHEN JSON_EXTRACT(sa.statistics_data, '$.subjects[0].metrics.discrimination_index') IS NOT NULL
         THEN '✓' ELSE '✗' END AS has_discrimination,
    CASE WHEN JSON_EXTRACT(sa.statistics_data, '$.subjects[0].metrics.grade_distribution') IS NOT NULL
         THEN '✓' ELSE '✗' END AS has_grade_distribution,
    CASE WHEN JSON_EXTRACT(sa.statistics_data, '$.subjects[0].enhanced_fields') IS NOT NULL
         THEN '✓' ELSE '✗' END AS has_enhanced_fields
FROM statistical_aggregations sa
WHERE sa.batch_id = 'G7-2025'
    AND JSON_LENGTH(JSON_EXTRACT(sa.statistics_data, '$.subjects')) > 0
ORDER BY sa.aggregation_level, sa.school_id
LIMIT 20;

-- ======================================================================
-- 5. 数据覆盖完整性检查
-- ======================================================================

-- 5.1 学校覆盖率统计
SELECT
    '覆盖率统计' AS stat_type,
    '学校覆盖情况' AS description,
    rankings_schools.school_count AS rankings_school_count,
    aggregation_schools.school_count AS aggregation_school_count,
    ROUND(aggregation_schools.school_count * 100.0 / rankings_schools.school_count, 2) AS coverage_rate
FROM
    (SELECT COUNT(DISTINCT school_id) AS school_count
     FROM subject_school_rankings WHERE batch_id = 'G7-2025') rankings_schools,
    (SELECT COUNT(DISTINCT school_id) AS school_count
     FROM statistical_aggregations WHERE batch_id = 'G7-2025' AND aggregation_level = 'school') aggregation_schools;

-- 5.2 科目覆盖情况
SELECT
    '科目覆盖检查' AS validation_type,
    scm.subject_name,
    scm.student_count AS regional_student_count,
    rankings_count.school_count AS schools_with_rankings,
    agg_count.school_count AS schools_with_aggregation,
    CASE
        WHEN rankings_count.school_count = agg_count.school_count THEN '✓ 完整覆盖'
        ELSE CONCAT('✗ 覆盖缺失: ', (rankings_count.school_count - agg_count.school_count), ' 所学校')
    END AS coverage_status
FROM subject_core_metrics scm
LEFT JOIN (
    SELECT subject_name, COUNT(DISTINCT school_id) AS school_count
    FROM subject_school_rankings
    WHERE batch_id = 'G7-2025'
    GROUP BY subject_name
) rankings_count ON scm.subject_name = rankings_count.subject_name
LEFT JOIN (
    SELECT
        JSON_UNQUOTE(JSON_EXTRACT(subj.value, '$.subject_name')) AS subject_name,
        COUNT(DISTINCT sa.school_id) AS school_count
    FROM statistical_aggregations sa
    JOIN JSON_TABLE(
        JSON_EXTRACT(sa.statistics_data, '$.subjects'),
        '$[*]' COLUMNS (value JSON PATH '$')
    ) subj
    WHERE sa.batch_id = 'G7-2025' AND sa.aggregation_level = 'school'
    GROUP BY JSON_UNQUOTE(JSON_EXTRACT(subj.value, '$.subject_name'))
) agg_count ON scm.subject_name = agg_count.subject_name
WHERE scm.batch_id = 'G7-2025'
ORDER BY scm.subject_name;

-- ======================================================================
-- 6. 一致性问题汇总
-- ======================================================================

-- 6.1 不一致项统计
SELECT
    '问题汇总' AS summary_type,
    validation_category,
    issue_type,
    COUNT(*) AS issue_count
FROM (
    -- 区域级不一致
    SELECT
        '区域级验证' AS validation_category,
        CASE
            WHEN ABS(scm.avg_score - JSON_UNQUOTE(JSON_EXTRACT(
                JSON_EXTRACT(sa.statistics_data,
                CONCAT('$.subjects[', idx.idx, '].metrics.avg')), '$'
            ))) > 0.001 THEN '平均分不一致'
            WHEN scm.student_count != JSON_UNQUOTE(JSON_EXTRACT(
                JSON_EXTRACT(sa.statistics_data,
                CONCAT('$.subjects[', idx.idx, '].metrics.student_count')), '$'
            )) THEN '学生数不一致'
            ELSE NULL
        END AS issue_type
    FROM subject_core_metrics scm
    JOIN statistical_aggregations sa ON sa.batch_id = 'G7-2025' AND sa.aggregation_level = 'regional'
    JOIN (
        SELECT 0 AS idx UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9
    ) idx ON JSON_UNQUOTE(JSON_EXTRACT(
        JSON_EXTRACT(sa.statistics_data, CONCAT('$.subjects[', idx.idx, '].subject_name')), '$'
    )) = scm.subject_name
    WHERE scm.batch_id = 'G7-2025'

    UNION ALL

    -- Schema版本问题
    SELECT
        'Schema版本检查' AS validation_category,
        CASE
            WHEN schema_version != 'v1.2' THEN 'Schema版本错误'
            ELSE NULL
        END AS issue_type
    FROM statistical_aggregations
    WHERE batch_id = 'G7-2025'

    UNION ALL

    -- JSON有效性问题
    SELECT
        'JSON字段检查' AS validation_category,
        CASE
            WHEN NOT JSON_VALID(statistics_data) THEN 'JSON格式无效'
            WHEN JSON_EXTRACT(statistics_data, '$.subjects') IS NULL THEN '缺少subjects字段'
            ELSE NULL
        END AS issue_type
    FROM statistical_aggregations
    WHERE batch_id = 'G7-2025'
) issues
WHERE issue_type IS NOT NULL
GROUP BY validation_category, issue_type
ORDER BY validation_category, issue_count DESC;

-- ======================================================================
-- 7. 验证结果总结
-- ======================================================================

-- 7.1 最终验证摘要
SELECT
    '最终摘要' AS report_section,
    'G7-2025批次数据一致性验证' AS description,
    CONCAT(
        '区域级记录: ', (SELECT COUNT(*) FROM subject_core_metrics WHERE batch_id = 'G7-2025'), ' 条, ',
        '学校级记录: ', (SELECT COUNT(*) FROM subject_school_rankings WHERE batch_id = 'G7-2025'), ' 条, ',
        '汇聚结果: ', (SELECT COUNT(*) FROM statistical_aggregations WHERE batch_id = 'G7-2025'), ' 条'
    ) AS data_summary,
    CONCAT(
        '学校覆盖率: ',
        ROUND((SELECT COUNT(DISTINCT school_id) FROM statistical_aggregations WHERE batch_id = 'G7-2025' AND aggregation_level = 'school') * 100.0 /
              (SELECT COUNT(DISTINCT school_id) FROM subject_school_rankings WHERE batch_id = 'G7-2025'), 2), '%'
    ) AS coverage_summary,
    CASE
        WHEN EXISTS (
            SELECT 1 FROM statistical_aggregations
            WHERE batch_id = 'G7-2025' AND schema_version != 'v1.2'
        ) THEN '存在Schema版本问题'
        ELSE 'Schema版本正确'
    END AS schema_status,
    NOW() AS validation_timestamp;

-- 显示验证完成信息
SELECT
    'SQL数据一致性校验完成' AS status,
    '请查看以上各部分的验证结果' AS instruction,
    '如发现不一致项，请运行Python脚本获得详细分析' AS recommendation;