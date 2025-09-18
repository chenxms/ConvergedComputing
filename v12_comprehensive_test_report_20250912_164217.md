# v1.2汇聚指标修复完整测试报告

## 测试概览
- 批次代码: G4-2025
- 执行时间: 77.83s
- 开始时间: 2025-09-12T16:40:59.572007
- 结束时间: 2025-09-12T16:42:17.403266
- 总体状态: **FAILED**
- 总体成功率: **54.5%**
- 总测试数: 22
- 通过数: 12
- 失败数: 10

## 测试分类结果

| 测试类别 | 总数 | 通过 | 失败 | 成功率 | 状态 |
|---------|------|------|------|--------|------|
| 实施验证 | 10 | 8 | 2 | 80.0% | ❌ FAILED |
| SQL验证 | 7 | 2 | 5 | 28.6% | ❌ FAILED |
| 业务场景 | 5 | 2 | 3 | 40.0% | ❌ FAILED |

## 详细测试结果

### 1. 实施验证测试
**状态**: FAILED | **成功率**: 80.0%

#### T1: 结构收敛与清退
- ✅ **T1.1** 科目类型过滤: 所有科目类型正确
- ✅ **T1.2** rank字段整数类型: 所有rank字段为整数类型

#### T2: Metrics注入与字段转换
- ✅ **T2.1** 问卷score_rate格式: 问卷正确使用score_rate格式
- ✅ **T2.2** 考试difficulty保留: 考试科目正确保留difficulty字段
- ✅ **T2.3** 等级阈值修正: 小学优秀≥85.0%, 初中优秀≥80.0%

#### T3: 问卷指标与题目分布隔离
- ✅ **T3.1** 问卷questions[]移除: 问卷科目成功移除嵌入questions[]结构
- ❌ **T3.2** 独立分布表结构: 缺少字段: {'pct', 'school_id'}
- ✅ **T3.3** 学校级questions[]移除: 学校级问卷科目成功移除嵌入questions[]结构

#### T4: 数据质量检查
- ❌ **T4.1** 数据完整性检查: 发现 5 个数据完整性问题
- ✅ **T4.2** API接口验证: 发现 1 个问卷科目

### 2. SQL验证测试
**状态**: FAILED | **成功率**: 28.6%

#### SQL.1: 表结构验证
- ❌ **SQL.1.1** questionnaire_option_distribution表结构: 问题: ['id', 'school_id', 'option_label', 'n_total', 'pct', 'option_level: 期望bigint, 实际int', 'count: 期望bigint, 实际int']
- ❌ **SQL.1.2** questionnaire_option_distribution索引: 缺少索引: {'uk_questionnaire_option_distribution', 'idx_batch_school_subject', 'idx_question_option'}

#### SQL.2: 数据过滤验证
- ✅ **SQL.2.1** 科目类型过滤: 仅包含exam/questionnaire类型
- ❌ **SQL.2.1** 数据过滤验证: 验证异常: (pymysql.err.OperationalError) (1267, "Illegal mix of collations (utf8mb4_0900_ai_ci,IMPLICIT) and (utf8mb4_unicode_ci,IMPLICIT) for operation '='")
[SQL: 
                SELECT smd.status, COUNT(DISTINCT scs.school_code) as school_count
                FROM student_cleaned_scores scs
                JOIN school_master_data smd ON smd.batch_code = scs.batch_code 
                    AND smd.school_id = scs.school_code
                WHERE scs.batch_code = %(batch)s
                GROUP BY smd.status
                ]
[parameters: {'batch': 'G4-2025'}]
(Background on this error at: https://sqlalche.me/e/20/e3q8)

#### SQL.3: 指标计算验证
- ❌ **SQL.3.1** 指标计算验证: 验证异常: (pymysql.err.OperationalError) (1267, "Illegal mix of collations (utf8mb4_0900_ai_ci,IMPLICIT) and (utf8mb4_unicode_ci,IMPLICIT) for operation '='")
[SQL: 
                SELECT 
                    scs.subject_name,
                    AVG(scs.total_score) as avg_score,
                    MAX(sqc.max_score) as max_score,
                    ROUND(AVG(scs.total_score) / MAX(sqc.max_score) * 100, 2) as calculated_score_rate
                FROM student_cleaned_scores scs
                JOIN subject_question_config sqc ON sqc.batch_code = scs.batch_code 
                    AND sqc.subject_name = scs.subject_name
                WHERE scs.batch_code = %(batch)s 
                  AND scs.subject_type = 'questionnaire'
                GROUP BY scs.subject_name
                LIMIT 1
                ]
[parameters: {'batch': 'G4-2025'}]
(Background on this error at: https://sqlalche.me/e/20/e3q8)

#### SQL.4: 题目分布数据验证
- ✅ **SQL.4.1** 题目分布数据存在: 共 112 条记录
- ❌ **SQL.4.1** 题目分布数据验证: 验证异常: (pymysql.err.OperationalError) (1054, "Unknown column 'pct' in 'field list'")
[SQL: 
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN pct < 0 OR pct > 100 THEN 1 ELSE 0 END) as invalid_pct,
                        MIN(pct) as min_pct,
                        MAX(pct) as max_pct
                    FROM questionnaire_option_distribution
                    WHERE batch_code = %(batch)s
                    ]
[parameters: {'batch': 'G4-2025'}]
(Background on this error at: https://sqlalche.me/e/20/e3q8)

### 3. 业务场景测试
**状态**: FAILED | **成功率**: 40.0%

#### ❌ 区域级数据汇聚与清退
**测试结果**: 验证失败: ["AI交互: metrics缺少 {'rank'}", "数学: metrics缺少 {'rank'}", "科学: metrics缺少 {'rank'}"]

#### ❌ 学校级数据输出与对标
**测试结果**: 验证失败: ['AI交互: 缺少区域排名', '数学: 缺少区域排名', '科学: 缺少区域排名']

#### ❌ 问卷题目分布独立查询
**测试结果**: 场景执行异常: (pymysql.err.OperationalError) (1267, "Illegal mix of collations (utf8mb4_0900_ai_ci,IMPLICIT) and (utf8mb4_unicode_ci,IMPLICIT) for operation '='")
[SQL: 
                    SELECT DISTINCT smd.school_id, smd.standard_school_name
                    FROM school_master_data smd
                    JOIN student_cleaned_scores scs 
                      ON smd.batch_code = scs.batch_code 
                     AND smd.school_id = scs.school_code
                    WHERE smd.batch_code = %(batch)s 
                      AND scs.subject_name = %(subject)s
                      AND scs.subject_type = 'questionnaire'
                      AND smd.status = 'ACTIVE'
                ]
[parameters: {'batch': 'G4-2025', 'subject': '问卷'}]
(Background on this error at: https://sqlalche.me/e/20/e3q8)

#### ✅ 指标计算差异化处理
**测试结果**: 考试科目4个，问卷科目1个

#### ✅ 等级分布计算准确性
**测试结果**: 等级计算准确，小学优秀≥85%，初中优秀≥80%

## ⚠️ 失败项汇总
以下测试项未通过，需要进一步修正:

- **实施验证 - T3.2**: 缺少字段: {'pct', 'school_id'}
- **实施验证 - T4.1**: 发现 5 个数据完整性问题
- **SQL验证 - SQL.1.1**: 问题: ['id', 'school_id', 'option_label', 'n_total', 'pct', 'option_level: 期望bigint, 实际int', 'count: 期望bigint, 实际int']
- **SQL验证 - SQL.1.2**: 缺少索引: {'uk_questionnaire_option_distribution', 'idx_batch_school_subject', 'idx_question_option'}
- **SQL验证 - SQL.2.1**: 验证异常: (pymysql.err.OperationalError) (1267, "Illegal mix of collations (utf8mb4_0900_ai_ci,IMPLICIT) and (utf8mb4_unicode_ci,IMPLICIT) for operation '='")
[SQL: 
                SELECT smd.status, COUNT(DISTINCT scs.school_code) as school_count
                FROM student_cleaned_scores scs
                JOIN school_master_data smd ON smd.batch_code = scs.batch_code 
                    AND smd.school_id = scs.school_code
                WHERE scs.batch_code = %(batch)s
                GROUP BY smd.status
                ]
[parameters: {'batch': 'G4-2025'}]
(Background on this error at: https://sqlalche.me/e/20/e3q8)
- **SQL验证 - SQL.3.1**: 验证异常: (pymysql.err.OperationalError) (1267, "Illegal mix of collations (utf8mb4_0900_ai_ci,IMPLICIT) and (utf8mb4_unicode_ci,IMPLICIT) for operation '='")
[SQL: 
                SELECT 
                    scs.subject_name,
                    AVG(scs.total_score) as avg_score,
                    MAX(sqc.max_score) as max_score,
                    ROUND(AVG(scs.total_score) / MAX(sqc.max_score) * 100, 2) as calculated_score_rate
                FROM student_cleaned_scores scs
                JOIN subject_question_config sqc ON sqc.batch_code = scs.batch_code 
                    AND sqc.subject_name = scs.subject_name
                WHERE scs.batch_code = %(batch)s 
                  AND scs.subject_type = 'questionnaire'
                GROUP BY scs.subject_name
                LIMIT 1
                ]
[parameters: {'batch': 'G4-2025'}]
(Background on this error at: https://sqlalche.me/e/20/e3q8)
- **SQL验证 - SQL.4.1**: 验证异常: (pymysql.err.OperationalError) (1054, "Unknown column 'pct' in 'field list'")
[SQL: 
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN pct < 0 OR pct > 100 THEN 1 ELSE 0 END) as invalid_pct,
                        MIN(pct) as min_pct,
                        MAX(pct) as max_pct
                    FROM questionnaire_option_distribution
                    WHERE batch_code = %(batch)s
                    ]
[parameters: {'batch': 'G4-2025'}]
(Background on this error at: https://sqlalche.me/e/20/e3q8)
- **场景测试 - 场景1**: 验证失败: ["AI交互: metrics缺少 {'rank'}", "数学: metrics缺少 {'rank'}", "科学: metrics缺少 {'rank'}"]
- **场景测试 - 场景2**: 验证失败: ['AI交互: 缺少区域排名', '数学: 缺少区域排名', '科学: 缺少区域排名']
- **场景测试 - 场景3**: 场景执行异常: (pymysql.err.OperationalError) (1267, "Illegal mix of collations (utf8mb4_0900_ai_ci,IMPLICIT) and (utf8mb4_unicode_ci,IMPLICIT) for operation '='")
[SQL: 
                    SELECT DISTINCT smd.school_id, smd.standard_school_name
                    FROM school_master_data smd
                    JOIN student_cleaned_scores scs 
                      ON smd.batch_code = scs.batch_code 
                     AND smd.school_id = scs.school_code
                    WHERE smd.batch_code = %(batch)s 
                      AND scs.subject_name = %(subject)s
                      AND scs.subject_type = 'questionnaire'
                      AND smd.status = 'ACTIVE'
                ]
[parameters: {'batch': 'G4-2025', 'subject': '问卷'}]
(Background on this error at: https://sqlalche.me/e/20/e3q8)

## 总结
本次v1.2汇聚指标修复测试共执行了 **22** 项测试，
其中 **12** 项通过，**10** 项失败，
总体成功率为 **54.5%**。

⚠️ **注意！** v1.2规范实施存在问题，需要进一步修正。

### 建议修正步骤:
1. 重点关注失败的测试项目
2. 检查相关代码实现
3. 修正后重新运行测试
4. 确保所有测试通过后再部署
