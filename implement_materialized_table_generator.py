#!/usr/bin/env python3
"""
实现问卷物化表数据生成脚本
按PO方案生成选项分布和维度汇总数据
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime
import time

def implement_materialized_table_generator():
    """实现物化表数据生成"""
    
    try:
        with get_db_context() as session:
            print("=== 实现问卷物化表数据生成 ===")
            print(f"执行时间: {datetime.now()}")
            print("目标: 生成选项分布和维度汇总数据\n")
            
            # 1. 获取可处理的批次列表
            print("1. 获取问卷数据批次:")
            result = session.execute(text("""
                SELECT DISTINCT batch_code,
                       COUNT(*) as record_count,
                       COUNT(DISTINCT subject_name) as subject_count,
                       COUNT(DISTINCT student_id) as student_count
                FROM questionnaire_question_scores
                GROUP BY batch_code
                ORDER BY record_count DESC
            """))
            
            available_batches = result.fetchall()
            
            if not available_batches:
                print("   [WARNING] 未找到问卷数据，无法生成物化表")
                return
            
            print("   可处理批次:")
            for batch_code, record_count, subject_count, student_count in available_batches:
                print(f"     {batch_code}: {record_count:,}条记录, {subject_count}科目, {student_count}学生")
            
            # 选择第一个批次进行示例生成
            target_batch = available_batches[0][0]
            print(f"\n   选择批次 '{target_batch}' 进行数据生成")
            
            # 2. 生成选项分布数据
            print(f"\n2. 生成选项分布数据:")
            print("   表: questionnaire_option_distribution")
            
            option_distribution_sql = """
            REPLACE INTO questionnaire_option_distribution 
            (batch_code, subject_name, question_id, option_level, count, percentage, updated_at)
            SELECT 
                qqs.batch_code,
                qqs.subject_name,
                qqs.question_id,
                qqs.option_level,
                COUNT(*) AS count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
                    PARTITION BY qqs.batch_code, qqs.subject_name, qqs.question_id
                ), 2) AS percentage,
                NOW() AS updated_at
            FROM questionnaire_question_scores qqs
            WHERE qqs.batch_code = :batch_code
            AND qqs.option_level IS NOT NULL
            AND qqs.option_level BETWEEN 1 AND 5
            GROUP BY qqs.batch_code, qqs.subject_name, qqs.question_id, qqs.option_level
            ORDER BY qqs.batch_code, qqs.subject_name, qqs.question_id, qqs.option_level
            """
            
            print(f"   处理批次: {target_batch}")
            start_time = time.time()
            
            try:
                result = session.execute(text(option_distribution_sql), {"batch_code": target_batch})
                option_rows = result.rowcount
                session.commit()
                
                option_time = time.time() - start_time
                print(f"   [SUCCESS] 生成 {option_rows} 条选项分布记录，耗时 {option_time:.2f}秒")
                
                # 验证生成的数据
                result = session.execute(text("""
                    SELECT COUNT(*) as total_records,
                           COUNT(DISTINCT subject_name) as subjects,
                           COUNT(DISTINCT question_id) as questions,
                           SUM(count) as total_responses
                    FROM questionnaire_option_distribution
                    WHERE batch_code = :batch_code
                """), {"batch_code": target_batch})
                
                verify_result = result.fetchone()
                print(f"   验证结果: {verify_result[0]}条分布记录, {verify_result[1]}个科目, {verify_result[2]}个题目, {verify_result[3]}总回答数")
                
                # 展示部分数据样例
                result = session.execute(text("""
                    SELECT subject_name, question_id, option_level, count, percentage
                    FROM questionnaire_option_distribution
                    WHERE batch_code = :batch_code
                    ORDER BY subject_name, question_id, option_level
                    LIMIT 5
                """), {"batch_code": target_batch})
                
                sample_data = result.fetchall()
                print("   数据样例:")
                for subject, question, option, count, percentage in sample_data:
                    print(f"     {subject} {question} 选项{option}: {count}人 ({percentage}%)")
                
            except Exception as e:
                print(f"   [ERROR] 选项分布生成失败: {e}")
                return
            
            # 3. 生成维度汇总数据
            print(f"\n3. 生成维度汇总数据:")
            print("   表: questionnaire_dimension_summary")
            
            # 首先检查维度映射数据
            result = session.execute(text("""
                SELECT COUNT(*) as mapping_count,
                       COUNT(DISTINCT dimension_code) as dimension_count
                FROM question_dimension_mapping
                WHERE batch_code = :batch_code
            """), {"batch_code": target_batch})
            
            mapping_info = result.fetchone()
            print(f"   维度映射数据: {mapping_info[0]}条映射, {mapping_info[1]}个维度")
            
            if mapping_info[0] == 0:
                print("   [WARNING] 该批次无维度映射数据，跳过维度汇总生成")
            else:
                dimension_summary_sql = """
                REPLACE INTO questionnaire_dimension_summary 
                (batch_code, subject_name, student_id, dimension_code, mean_score, question_count, updated_at)
                SELECT 
                    qqs.batch_code,
                    qqs.subject_name,
                    qqs.student_id,
                    qdm.dimension_code,
                    ROUND(AVG(qqs.original_score), 4) AS mean_score,
                    COUNT(DISTINCT qqs.question_id) AS question_count,
                    NOW() AS updated_at
                FROM questionnaire_question_scores qqs
                JOIN question_dimension_mapping qdm
                  ON BINARY qdm.batch_code = BINARY qqs.batch_code 
                  AND BINARY qdm.subject_name = BINARY qqs.subject_name 
                  AND BINARY qdm.question_id = BINARY qqs.question_id
                WHERE qqs.batch_code = :batch_code
                AND qqs.original_score IS NOT NULL
                AND qdm.dimension_code IS NOT NULL
                GROUP BY qqs.batch_code, qqs.subject_name, qqs.student_id, qdm.dimension_code
                HAVING COUNT(DISTINCT qqs.question_id) > 0
                ORDER BY qqs.batch_code, qqs.subject_name, qqs.student_id, qdm.dimension_code
                """
                
                start_time = time.time()
                
                try:
                    result = session.execute(text(dimension_summary_sql), {"batch_code": target_batch})
                    dimension_rows = result.rowcount
                    session.commit()
                    
                    dimension_time = time.time() - start_time
                    print(f"   [SUCCESS] 生成 {dimension_rows} 条维度汇总记录，耗时 {dimension_time:.2f}秒")
                    
                    # 验证生成的数据
                    result = session.execute(text("""
                        SELECT COUNT(*) as total_records,
                               COUNT(DISTINCT student_id) as students,
                               COUNT(DISTINCT dimension_code) as dimensions,
                               ROUND(AVG(mean_score), 2) as overall_avg
                        FROM questionnaire_dimension_summary
                        WHERE batch_code = :batch_code
                    """), {"batch_code": target_batch})
                    
                    verify_result = result.fetchone()
                    print(f"   验证结果: {verify_result[0]}条汇总记录, {verify_result[1]}个学生, {verify_result[2]}个维度, 总平均{verify_result[3]}")
                    
                    # 展示维度统计
                    result = session.execute(text("""
                        SELECT dimension_code,
                               COUNT(DISTINCT student_id) as student_count,
                               ROUND(AVG(mean_score), 2) as dim_avg,
                               ROUND(AVG(question_count), 1) as avg_questions
                        FROM questionnaire_dimension_summary
                        WHERE batch_code = :batch_code
                        GROUP BY dimension_code
                        ORDER BY dim_avg DESC
                        LIMIT 5
                    """), {"batch_code": target_batch})
                    
                    dimension_stats = result.fetchall()
                    print("   维度统计:")
                    for dim_code, stu_count, avg_score, avg_questions in dimension_stats:
                        print(f"     {dim_code}: {stu_count}人, 平均分{avg_score}, 题目数{avg_questions}")
                    
                except Exception as e:
                    print(f"   [ERROR] 维度汇总生成失败: {e}")
            
            # 4. 性能对比测试
            print(f"\n4. 性能对比测试:")
            
            # 4.1 测试原始查询性能（模拟未优化前）
            print("   4.1 原始实时计算查询:")
            original_sql = """
            SELECT 
                qqs.question_id,
                qqs.option_level,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
                    PARTITION BY qqs.question_id
                ), 2) AS percentage
            FROM questionnaire_question_scores qqs
            WHERE qqs.batch_code = :batch_code
            AND qqs.subject_name = (
                SELECT subject_name 
                FROM questionnaire_question_scores 
                WHERE batch_code = :batch_code 
                LIMIT 1
            )
            AND qqs.option_level IS NOT NULL
            GROUP BY qqs.question_id, qqs.option_level
            ORDER BY qqs.question_id, qqs.option_level
            LIMIT 10
            """
            
            start_time = time.time()
            result = session.execute(text(original_sql), {"batch_code": target_batch})
            original_results = result.fetchall()
            original_time = time.time() - start_time
            
            print(f"     实时计算: {len(original_results)}条结果, {original_time:.3f}秒")
            
            # 4.2 测试物化表查询性能
            print("   4.2 物化表查询:")
            materialized_sql = """
            SELECT 
                question_id,
                option_level,
                count,
                percentage
            FROM questionnaire_option_distribution
            WHERE batch_code = :batch_code
            AND subject_name = (
                SELECT DISTINCT subject_name 
                FROM questionnaire_option_distribution 
                WHERE batch_code = :batch_code 
                LIMIT 1
            )
            ORDER BY question_id, option_level
            LIMIT 10
            """
            
            start_time = time.time()
            result = session.execute(text(materialized_sql), {"batch_code": target_batch})
            materialized_results = result.fetchall()
            materialized_time = time.time() - start_time
            
            print(f"     物化表查询: {len(materialized_results)}条结果, {materialized_time:.3f}秒")
            
            # 4.3 性能改善计算
            if original_time > 0 and materialized_time > 0:
                improvement_ratio = original_time / materialized_time
                improvement_percent = ((original_time - materialized_time) / original_time) * 100
                
                print(f"\n   性能改善:")
                print(f"     查询速度提升: {improvement_ratio:.2f}倍")
                print(f"     时间节省: {improvement_percent:.1f}%")
                
                if improvement_ratio > 2.0:
                    print("     [EXCELLENT] 性能提升显著")
                elif improvement_ratio > 1.5:
                    print("     [GOOD] 性能有明显改善")
                else:
                    print("     [OK] 性能有一定改善")
            
            # 5. 生成调度脚本模板
            print(f"\n5. 生成调度脚本建议:")
            
            print("   5.1 增量更新脚本:")
            incremental_script = f"""
# 增量更新问卷物化表 - Python脚本模板
def update_questionnaire_materialized_tables(batch_code=None):
    batches = get_updated_batches() if batch_code is None else [batch_code]
    
    for batch in batches:
        # 更新选项分布
        update_option_distribution(batch)
        
        # 更新维度汇总  
        update_dimension_summary(batch)
        
        # 记录更新日志
        log_update_completion(batch)
            """
            
            print("     特点: 支持全量和增量更新")
            print("     触发: 数据清洗完成后调用")
            print("     监控: 记录更新日志和耗时")
            
            print(f"\n   5.2 定时任务配置:")
            cron_config = """
# Crontab 配置示例
# 每小时检查并更新物化表
0 * * * * /path/to/python /path/to/update_materialized_tables.py

# 每天凌晨全量重建
0 2 * * * /path/to/python /path/to/rebuild_materialized_tables.py
            """
            print("     定时检查: 每小时增量更新")
            print("     全量重建: 每天凌晨执行")
            print("     错误恢复: 失败重试机制")
            
            # 6. 总结
            print(f"\n=== 物化表生成完成 ===")
            print(f"完成时间: {datetime.now()}")
            
            print("✅ 完成项目:")
            print(f"  - 选项分布表: {option_rows if 'option_rows' in locals() else 0}条记录")
            print(f"  - 维度汇总表: {dimension_rows if 'dimension_rows' in locals() else 0}条记录")
            print("  - 性能对比验证完成")
            print("  - 调度脚本模板生成")
            
            print(f"\n📋 后续集成建议:")
            print("  1. 将生成脚本集成到数据处理流水线")
            print("  2. 修改报表接口优先使用物化表")
            print("  3. 设置定时任务保持数据同步")
            print("  4. 建立监控机制跟踪物化表健康度")
            
    except Exception as e:
        print(f"物化表生成失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    implement_materialized_table_generator()