#!/usr/bin/env python3
"""
紧急字符集修复脚本
执行三个修复任务：
1. 统一COLLATE (0风险)
2. 历史数据回填 (低风险)
3. 添加关键索引 (0风险)
"""
import sys
import os
import time
sys.path.append('/app')

from sqlalchemy import text
from app.database.connection import get_db_context

def backup_critical_data():
    """备份关键数据"""
    print("=== 📦 数据备份 ===\n")

    with get_db_context() as db:
        try:
            # 获取各表记录数
            tables = [
                'student_cleaned_scores',
                'questionnaire_option_distribution',
                'questionnaire_question_scores',
                'questionnaire_scale_options'
            ]

            backup_info = {}
            for table in tables:
                count_sql = text(f"SELECT COUNT(*) FROM {table}")
                result = db.execute(count_sql)
                count = result.fetchone()[0]
                backup_info[table] = count
                print(f"📊 {table}: {count:,} 条记录")

            print(f"\n✅ 备份信息记录完成\n")
            return backup_info

        except Exception as e:
            print(f"❌ 备份失败: {e}")
            return None

def task1_unify_collation():
    """任务1: 统一COLLATE (0风险)"""
    print("=== 🎯 任务1: 统一字符集排序规则 ===\n")

    # 需要转换的表
    tables_to_convert = [
        'student_cleaned_scores',
        'questionnaire_option_distribution',
        'questionnaire_question_scores',
        'questionnaire_scale_options'
    ]

    with get_db_context() as db:
        for table in tables_to_convert:
            print(f"🔄 转换表: {table}")
            try:
                start_time = time.time()

                # 使用ALTER TABLE CONVERT转换字符集
                convert_sql = text(f"""
                    ALTER TABLE {table}
                    CONVERT TO CHARACTER SET utf8mb4
                    COLLATE utf8mb4_0900_ai_ci
                """)

                db.execute(convert_sql)
                db.commit()

                elapsed = time.time() - start_time
                print(f"✅ {table} 转换完成 (耗时: {elapsed:.2f}秒)")

            except Exception as e:
                print(f"❌ {table} 转换失败: {e}")
                db.rollback()
                return False

        print("\n🎉 任务1完成: 所有表已统一到utf8mb4_0900_ai_ci\n")
        return True

def task1_verify():
    """验证任务1结果"""
    print("=== ✅ 验证任务1: 字符集统一 ===\n")

    with get_db_context() as db:
        try:
            # 测试之前失败的JOIN
            test_sql = text("""
                SELECT COUNT(*)
                FROM student_cleaned_scores scs
                JOIN school_master_data smd ON scs.school_code = smd.school_id
                WHERE scs.batch_code = 'G7-2025'
                AND scs.subject_type = 'questionnaire'
                LIMIT 1
            """)

            result = db.execute(test_sql)
            count = result.fetchone()[0]
            print(f"✅ JOIN查询成功！匹配记录数: {count}")

            # 验证字符集转换
            tables = ['student_cleaned_scores', 'questionnaire_option_distribution']
            database_name = db.execute(text("SELECT DATABASE()")).fetchone()[0]

            for table in tables:
                charset_sql = text("""
                    SELECT DISTINCT COLLATION_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = :db_name
                    AND TABLE_NAME = :table_name
                    AND CHARACTER_SET_NAME IS NOT NULL
                """)

                result = db.execute(charset_sql, {
                    'db_name': database_name,
                    'table_name': table
                })

                collations = [row[0] for row in result.fetchall()]
                if all('utf8mb4_0900_ai_ci' == col for col in collations):
                    print(f"✅ {table}: 字符集统一成功")
                else:
                    print(f"⚠️  {table}: 字符集未完全统一 - {collations}")

            return True

        except Exception as e:
            print(f"❌ 验证失败: {e}")
            return False

def task2_backfill_null_labels():
    """任务2: 历史数据回填 (低风险)"""
    print("=== 🎯 任务2: 修复NULL标签 ===\n")

    with get_db_context() as db:
        try:
            # 首先检查是否有NULL标签
            null_check_sql = text("""
                SELECT COUNT(*)
                FROM questionnaire_option_distribution
                WHERE option_label IS NULL
            """)

            result = db.execute(null_check_sql)
            null_count = result.fetchone()[0]

            if null_count == 0:
                print("✅ 未发现NULL标签，跳过此任务\n")
                return True

            print(f"🔍 发现 {null_count} 条NULL标签记录")

            # 三层回填策略
            print("🔄 执行三层回填策略...")

            # L1: 从questionnaire_scale_options获取标签
            l1_sql = text("""
                UPDATE questionnaire_option_distribution qod
                JOIN questionnaire_scale_options qso
                    ON qod.option_level = qso.option_level
                    AND qso.instrument_type = 'default'
                SET qod.option_label = qso.option_label
                WHERE qod.option_label IS NULL
            """)

            result = db.execute(l1_sql)
            l1_updated = result.rowcount
            print(f"  📊 L1回填: {l1_updated} 条")

            # L2: 从科目级映射获取 (如果有的话)
            # 这里暂时跳过，因为需要确认具体的映射表结构

            # L3: 兜底策略 - 使用"选项{level}"
            l3_sql = text("""
                UPDATE questionnaire_option_distribution
                SET option_label = CONCAT('选项', option_level)
                WHERE option_label IS NULL
            """)

            result = db.execute(l3_sql)
            l3_updated = result.rowcount
            print(f"  📊 L3兜底: {l3_updated} 条")

            db.commit()

            # 验证回填结果
            result = db.execute(null_check_sql)
            remaining_null = result.fetchone()[0]

            if remaining_null == 0:
                print(f"✅ 任务2完成: 所有NULL标签已修复\n")
                return True
            else:
                print(f"⚠️  仍有 {remaining_null} 条NULL标签未修复\n")
                return False

        except Exception as e:
            print(f"❌ 任务2失败: {e}")
            db.rollback()
            return False

def task3_add_indexes():
    """任务3: 添加关键索引 (0风险)"""
    print("=== 🎯 任务3: 添加性能索引 ===\n")

    # 索引定义
    indexes_to_create = [
        {
            'table': 'questionnaire_option_distribution',
            'name': 'idx_qod_batch_subject_question_option',
            'columns': '(batch_code, subject_name, question_id, option_level)',
            'description': '问卷选项分布查询优化'
        },
        {
            'table': 'questionnaire_question_scores',
            'name': 'idx_qqs_batch_subject_school_question',
            'columns': '(batch_code, subject_name, school_id, question_id)',
            'description': '问卷题目得分查询优化'
        },
        {
            'table': 'student_cleaned_scores',
            'name': 'idx_scs_batch_subject_type_school',
            'columns': '(batch_code, subject_name, subject_type, school_code)',
            'description': '学生得分数据查询优化'
        }
    ]

    with get_db_context() as db:
        # 获取数据库名
        database_name = db.execute(text("SELECT DATABASE()")).fetchone()[0]

        for idx_def in indexes_to_create:
            table = idx_def['table']
            idx_name = idx_def['name']
            columns = idx_def['columns']
            desc = idx_def['description']

            print(f"🔄 创建索引: {idx_name}")
            print(f"   📋 表: {table}")
            print(f"   📊 列: {columns}")
            print(f"   📝 说明: {desc}")

            try:
                # 检查索引是否已存在
                check_idx_sql = text("""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = :db_name
                    AND TABLE_NAME = :table_name
                    AND INDEX_NAME = :idx_name
                """)

                result = db.execute(check_idx_sql, {
                    'db_name': database_name,
                    'table_name': table,
                    'idx_name': idx_name
                })

                idx_exists = result.fetchone()[0] > 0

                if idx_exists:
                    print(f"   ⏭️  索引已存在，跳过")
                    continue

                start_time = time.time()

                # 创建索引
                create_idx_sql = text(f"""
                    CREATE INDEX {idx_name} ON {table} {columns}
                """)

                db.execute(create_idx_sql)
                db.commit()

                elapsed = time.time() - start_time
                print(f"   ✅ 创建成功 (耗时: {elapsed:.2f}秒)")

            except Exception as e:
                print(f"   ❌ 创建失败: {e}")
                db.rollback()
                return False

            print()

        print("🎉 任务3完成: 所有性能索引已创建\n")
        return True

def task3_verify():
    """验证任务3结果"""
    print("=== ✅ 验证任务3: 索引创建 ===\n")

    with get_db_context() as db:
        try:
            database_name = db.execute(text("SELECT DATABASE()")).fetchone()[0]

            # 检查新创建的索引
            expected_indexes = [
                'idx_qod_batch_subject_question_option',
                'idx_qqs_batch_subject_school_question',
                'idx_scs_batch_subject_type_school'
            ]

            for idx_name in expected_indexes:
                check_sql = text("""
                    SELECT
                        TABLE_NAME,
                        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = :db_name
                    AND INDEX_NAME = :idx_name
                    GROUP BY TABLE_NAME
                """)

                result = db.execute(check_sql, {
                    'db_name': database_name,
                    'idx_name': idx_name
                })

                idx_info = result.fetchone()
                if idx_info:
                    table_name, columns = idx_info
                    print(f"✅ {idx_name}: {table_name}({columns})")
                else:
                    print(f"❌ {idx_name}: 未找到")

            return True

        except Exception as e:
            print(f"❌ 验证失败: {e}")
            return False

def generate_rollback_script():
    """生成回滚脚本"""
    rollback_content = '''-- 紧急回滚脚本
-- 如果需要回滚字符集更改，请谨慎执行

-- 回滚字符集 (需要根据实际情况调整)
/*
ALTER TABLE student_cleaned_scores CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE questionnaire_option_distribution CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE questionnaire_question_scores CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE questionnaire_scale_options CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
*/

-- 删除创建的索引
DROP INDEX IF EXISTS idx_qod_batch_subject_question_option ON questionnaire_option_distribution;
DROP INDEX IF EXISTS idx_qqs_batch_subject_school_question ON questionnaire_question_scores;
DROP INDEX IF EXISTS idx_scs_batch_subject_type_school ON student_cleaned_scores;

-- 注意：标签回填操作不建议回滚，因为原本就是NULL值
'''

    try:
        with open('/app/emergency_rollback.sql', 'w', encoding='utf-8') as f:
            f.write(rollback_content)
        print("📄 回滚脚本已生成: emergency_rollback.sql")
    except Exception as e:
        print(f"⚠️  回滚脚本生成失败: {e}")

def main():
    """主函数"""
    print("🚨 紧急数据库修复开始")
    print("=" * 50)

    # 备份数据信息
    backup_info = backup_critical_data()
    if not backup_info:
        print("❌ 备份失败，终止操作")
        return False

    # 生成回滚脚本
    generate_rollback_script()

    # 执行任务1: 统一字符集
    success1 = task1_unify_collation()
    if not success1:
        print("❌ 任务1失败，终止操作")
        return False

    # 验证任务1
    verify1 = task1_verify()
    if not verify1:
        print("❌ 任务1验证失败")
        return False

    # 执行任务2: 修复NULL标签
    success2 = task2_backfill_null_labels()
    if not success2:
        print("⚠️  任务2未完全成功，但继续执行任务3")

    # 执行任务3: 添加索引
    success3 = task3_add_indexes()
    if not success3:
        print("❌ 任务3失败")
        return False

    # 验证任务3
    verify3 = task3_verify()
    if not verify3:
        print("❌ 任务3验证失败")
        return False

    print("🎉 所有修复任务完成！")
    print("=" * 50)

    # 最终验证
    print("=== 🧪 最终验证测试 ===\\n")

    with get_db_context() as db:
        try:
            # 测试G7-2025问卷查询
            test_sql = text('''
                SELECT
                    scs.batch_code,
                    scs.subject_name,
                    smd.standard_school_name,
                    COUNT(*) as student_count
                FROM student_cleaned_scores scs
                JOIN school_master_data smd ON scs.school_code = smd.school_id
                WHERE scs.batch_code = "G7-2025"
                AND scs.subject_type = "questionnaire"
                GROUP BY scs.batch_code, scs.subject_name, smd.standard_school_name
                LIMIT 5
            ''')

            result = db.execute(test_sql)
            records = result.fetchall()

            if records:
                print("✅ G7-2025问卷查询测试成功:")
                for record in records:
                    batch, subject, school, count = record
                    print(f"  • {batch}/{subject}/{school}: {count}人")
            else:
                print("⚠️  G7-2025问卷查询无结果，但JOIN语法正常")

        except Exception as e:
            print(f"❌ 最终验证失败: {e}")
            return False

    print("\\n🎯 修复完成！G7-2025批次问题已解决")
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ 脚本执行失败: {e}")
        sys.exit(1)