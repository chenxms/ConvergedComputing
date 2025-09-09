#!/usr/bin/env python3
"""
按PO方案设计问卷物化汇总表
实现两个核心物化表：选项分布和维度汇总
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def design_questionnaire_materialized_tables():
    """按PO方案设计问卷物化表"""
    
    try:
        with get_db_context() as session:
            print("=== 设计问卷物化汇总表 ===")
            print(f"设计时间: {datetime.now()}")
            print("基于PO方案: 两个物化表降低查询成本\n")
            
            # 1. 检查现有问卷数据结构
            print("1. 检查现有问卷数据结构:")
            
            # 检查是否存在问卷相关表
            questionnaire_tables = [
                'questionnaire_question_scores',
                'question_dimension_mapping'
            ]
            
            existing_tables = {}
            for table in questionnaire_tables:
                try:
                    result = session.execute(text(f"""
                        SELECT COUNT(*) as count,
                               COUNT(DISTINCT batch_code) as batches
                        FROM {table}
                    """))
                    count_info = result.fetchone()
                    existing_tables[table] = {
                        'exists': True,
                        'records': count_info[0],
                        'batches': count_info[1]
                    }
                    print(f"   {table}: {count_info[0]:,}条记录, {count_info[1]}个批次")
                    
                except Exception as e:
                    existing_tables[table] = {'exists': False, 'error': str(e)}
                    print(f"   {table}: 不存在或无法访问 - {e}")
            
            # 2. 设计物化表1：选项分布表
            print(f"\n2. 设计物化表1: questionnaire_option_distribution")
            print("   用途: 存储问卷选项分布统计，避免实时GROUP BY计算")
            
            option_distribution_ddl = """
            CREATE TABLE IF NOT EXISTS questionnaire_option_distribution (
                batch_code VARCHAR(50) NOT NULL COMMENT '批次代码',
                subject_name VARCHAR(100) NOT NULL COMMENT '科目名称',
                question_id VARCHAR(50) NOT NULL COMMENT '题目ID',
                option_level INT NOT NULL COMMENT '选项等级(1-5)',
                count INT NOT NULL DEFAULT 0 COMMENT '选择该选项的人数',
                percentage DECIMAL(5,2) COMMENT '选项占比(%)',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                
                PRIMARY KEY (batch_code, subject_name, question_id, option_level),
                INDEX idx_batch_subject (batch_code, subject_name) COMMENT '科目聚合查询索引',
                INDEX idx_updated_at (updated_at) COMMENT '增量更新索引'
            ) ENGINE=InnoDB 
            CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci 
            COMMENT='问卷选项分布物化表';
            """
            
            print("   表结构设计:")
            print("     主键: (batch_code, subject_name, question_id, option_level)")
            print("     索引1: (batch_code, subject_name) - 科目聚合")
            print("     索引2: (updated_at) - 增量更新")
            print("     字段: batch_code, subject_name, question_id, option_level, count, percentage")
            
            # 3. 设计物化表2：维度汇总表  
            print(f"\n3. 设计物化表2: questionnaire_dimension_summary")
            print("   用途: 存储按维度汇总的学生得分，支持维度层统计分析")
            
            dimension_summary_ddl = """
            CREATE TABLE IF NOT EXISTS questionnaire_dimension_summary (
                batch_code VARCHAR(50) NOT NULL COMMENT '批次代码',
                subject_name VARCHAR(100) NOT NULL COMMENT '科目名称',
                student_id VARCHAR(100) NOT NULL COMMENT '学生ID',
                dimension_code VARCHAR(20) NOT NULL COMMENT '维度代码',
                mean_score DECIMAL(10,4) NOT NULL COMMENT '维度平均分',
                question_count INT NOT NULL DEFAULT 0 COMMENT '维度题目数量',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                
                PRIMARY KEY (batch_code, subject_name, student_id, dimension_code),
                INDEX idx_batch_subject_dim (batch_code, subject_name, dimension_code) COMMENT '维度统计索引',
                INDEX idx_dimension (dimension_code) COMMENT '维度查询索引',
                INDEX idx_updated_at (updated_at) COMMENT '增量更新索引'
            ) ENGINE=InnoDB 
            CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci 
            COMMENT='问卷维度汇总物化表';
            """
            
            print("   表结构设计:")
            print("     主键: (batch_code, subject_name, student_id, dimension_code)")
            print("     索引1: (batch_code, subject_name, dimension_code) - 维度统计")
            print("     索引2: (dimension_code) - 维度查询")
            print("     字段: batch_code, subject_name, student_id, dimension_code, mean_score, question_count")
            
            # 4. 创建物化表
            print(f"\n4. 创建物化表:")
            
            tables_to_create = [
                ("questionnaire_option_distribution", option_distribution_ddl),
                ("questionnaire_dimension_summary", dimension_summary_ddl)
            ]
            
            created_tables = []
            
            for table_name, ddl in tables_to_create:
                try:
                    print(f"   创建表: {table_name}")
                    session.execute(text(ddl))
                    session.commit()
                    print(f"   [SUCCESS] {table_name} 创建成功")
                    created_tables.append(table_name)
                    
                except Exception as e:
                    if "already exists" in str(e) or "Table" in str(e) and "already exists" in str(e):
                        print(f"   [INFO] {table_name} 已存在，跳过创建")
                        created_tables.append(table_name)
                    else:
                        print(f"   [ERROR] {table_name} 创建失败: {e}")
            
            # 5. 验证表创建结果
            print(f"\n5. 验证物化表结构:")
            
            for table_name in created_tables:
                try:
                    # 检查表结构
                    result = session.execute(text(f"DESCRIBE {table_name}"))
                    columns = result.fetchall()
                    
                    print(f"\n   表: {table_name}")
                    print("     字段结构:")
                    for col in columns:
                        field = col[0]
                        type_info = col[1]
                        null_info = col[2]
                        key_info = col[3]
                        default_info = col[4]
                        print(f"       {field}: {type_info} {null_info} {key_info}")
                    
                    # 检查索引
                    result = session.execute(text(f"SHOW INDEX FROM {table_name}"))
                    indexes = result.fetchall()
                    
                    print("     索引:")
                    current_index = None
                    for idx in indexes:
                        if len(idx) >= 3:
                            index_name = idx[2]
                            column_name = idx[4] if len(idx) > 4 else "unknown"
                            if index_name != current_index:
                                current_index = index_name
                                print(f"       {index_name}: {column_name}", end="")
                            else:
                                print(f", {column_name}", end="")
                    print()  # 换行
                    
                except Exception as e:
                    print(f"   [ERROR] 验证 {table_name} 失败: {e}")
            
            # 6. 生成数据填充SQL模板
            print(f"\n6. 数据填充SQL模板设计:")
            
            # 6.1 选项分布表填充SQL
            print("\n   6.1 选项分布表填充SQL:")
            option_fill_sql = """
            -- 填充选项分布数据
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
            WHERE qqs.batch_code = ? -- 参数化批次
            AND qqs.option_level IS NOT NULL
            GROUP BY qqs.batch_code, qqs.subject_name, qqs.question_id, qqs.option_level
            ORDER BY qqs.batch_code, qqs.subject_name, qqs.question_id, qqs.option_level;
            """
            print("     用途: 计算每个题目各选项的分布情况")
            print("     特点: 使用REPLACE INTO保证幂等性")
            print("     参数: batch_code (支持增量更新)")
            
            # 6.2 维度汇总表填充SQL
            print("\n   6.2 维度汇总表填充SQL:")
            dimension_fill_sql = """
            -- 填充维度汇总数据
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
              ON qdm.batch_code = qqs.batch_code 
              AND qdm.subject_name = qqs.subject_name 
              AND qdm.question_id = qqs.question_id
            WHERE qqs.batch_code = ? -- 参数化批次
            AND qqs.original_score IS NOT NULL
            AND qdm.dimension_code IS NOT NULL
            GROUP BY qqs.batch_code, qqs.subject_name, qqs.student_id, qdm.dimension_code
            ORDER BY qqs.batch_code, qqs.subject_name, qqs.student_id, qdm.dimension_code;
            """
            print("     用途: 计算学生在各维度的平均得分")
            print("     特点: 基于题目维度映射进行汇总")
            print("     参数: batch_code (支持增量更新)")
            
            # 7. 生成调度建议
            print(f"\n7. 物化表调度建议:")
            
            print("   触发时机:")
            print("     - 数据清洗完成后统一执行")
            print("     - 或设置定时任务(如每小时检查更新)")
            print("     - 支持按批次增量更新")
            
            print(f"\n   幂等策略:")
            print("     - 使用 REPLACE INTO 保证重复执行一致性")
            print("     - 基于 updated_at 字段支持增量同步")
            print("     - 出错重试机制不会产生重复数据")
            
            print(f"\n   性能优化:")
            print("     - 报表/接口优先读取物化表")
            print("     - 明细表仅用于深度钻取分析")
            print("     - 定期清理过期的物化数据")
            
            # 8. 总结
            print(f"\n=== 物化表设计完成 ===")
            print(f"完成时间: {datetime.now()}")
            
            if len(created_tables) == 2:
                print("✅ 所有物化表创建成功")
                print("✅ 问卷查询性能将显著提升")
                print("✅ 实时计算成本大幅降低")
                
                print(f"\n下一步:")
                print("1. 实现数据填充脚本")
                print("2. 集成到数据处理流水线")
                print("3. 修改报表接口使用物化表")
                print("4. 设置定期数据同步任务")
            else:
                print("⚠️  部分物化表创建失败，需要检查权限或表结构")
            
    except Exception as e:
        print(f"物化表设计失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    design_questionnaire_materialized_tables()