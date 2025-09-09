#!/usr/bin/env python3
"""
基于PO方案的数据库优化审计
审计当前索引和排序规则状态，为后续优化做准备
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def po_optimization_audit():
    """按PO方案审计数据库状态"""
    
    try:
        with get_db_context() as session:
            print("=== PO优化方案数据库审计 ===")
            print(f"审计时间: {datetime.now()}")
            print("基于: C:\\Users\\chenx\\Desktop\\改进方案.txt\n")
            
            # 1. 审计表级排序规则（按PO方案要求）
            print("1. 表级排序规则审计:")
            result = session.execute(text("""
                SELECT TABLE_NAME, TABLE_COLLATION 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME IN ('student_cleaned_scores', 'student_score_detail', 
                                   'subject_question_config', 'question_dimension_mapping')
                ORDER BY TABLE_NAME
            """))
            
            table_collations = result.fetchall()
            collation_summary = {}
            
            for table_name, collation in table_collations:
                print(f"   {table_name}: {collation}")
                if collation in collation_summary:
                    collation_summary[collation].append(table_name)
                else:
                    collation_summary[collation] = [table_name]
            
            print(f"\n   排序规则分布:")
            for collation, tables in collation_summary.items():
                print(f"     {collation}: {len(tables)}张表 ({', '.join(tables)})")
            
            # 2. 审计列级排序规则（重点关注JOIN键）
            print(f"\n2. 关键列排序规则审计:")
            join_columns = [
                ('student_cleaned_scores', ['batch_code', 'subject_id', 'subject_name', 'student_id']),
                ('student_score_detail', ['batch_code', 'subject_id', 'subject_name', 'student_id']),
                ('subject_question_config', ['batch_code', 'subject_id', 'subject_name']),
                ('question_dimension_mapping', ['batch_code', 'subject_name', 'question_id'])
            ]
            
            column_collations = {}
            
            for table_name, columns in join_columns:
                print(f"\n   表: {table_name}")
                for column in columns:
                    result = session.execute(text(f"""
                        SELECT COLLATION_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() 
                        AND TABLE_NAME = '{table_name}' 
                        AND COLUMN_NAME = '{column}'
                        AND COLLATION_NAME IS NOT NULL
                    """))
                    
                    col_info = result.fetchone()
                    if col_info:
                        collation = col_info[0]
                        print(f"     {column}: {collation}")
                        if collation in column_collations:
                            column_collations[collation] += 1
                        else:
                            column_collations[collation] = 1
                    else:
                        print(f"     {column}: 无排序规则(非文本类型)")
            
            print(f"\n   列级排序规则统计:")
            for collation, count in column_collations.items():
                print(f"     {collation}: {count}个列")
            
            # 3. 审计现有索引（按PO方案检查student_cleaned_scores）
            print(f"\n3. student_cleaned_scores索引审计:")
            result = session.execute(text("""
                SELECT INDEX_NAME, 
                       GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns,
                       NON_UNIQUE,
                       INDEX_TYPE
                FROM INFORMATION_SCHEMA.STATISTICS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = 'student_cleaned_scores'
                GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE
                ORDER BY INDEX_NAME
            """))
            
            existing_indexes = result.fetchall()
            
            # 检查PO推荐的索引是否存在
            recommended_index = "idx_scs_batch_subj_stu"
            po_index_columns = "batch_code,subject_id,student_id"
            po_index_exists = False
            similar_indexes = []
            
            for idx_name, columns, non_unique, idx_type in existing_indexes:
                print(f"   {idx_name}: ({columns}) - {'UNIQUE' if non_unique == 0 else 'NON-UNIQUE'}")
                
                if idx_name == recommended_index:
                    po_index_exists = True
                
                # 检查是否有类似的索引（覆盖PO推荐的列组合）
                if columns and po_index_columns in columns:
                    similar_indexes.append((idx_name, columns))
            
            print(f"\n   PO推荐索引分析:")
            print(f"     推荐索引: {recommended_index} (batch_code, subject_id, student_id)")
            if po_index_exists:
                print(f"     状态: ✅ 已存在")
            else:
                print(f"     状态: ❌ 不存在，需要创建")
            
            if similar_indexes:
                print(f"     相似索引:")
                for idx_name, columns in similar_indexes:
                    print(f"       {idx_name}: ({columns})")
            
            # 4. 审计问卷相关表结构（为物化表设计做准备）
            print(f"\n4. 问卷数据结构审计:")
            questionnaire_tables = [
                'questionnaire_question_scores',
                'question_dimension_mapping'
            ]
            
            for table in questionnaire_tables:
                print(f"\n   检查表: {table}")
                try:
                    result = session.execute(text(f"""
                        SELECT COUNT(*) as record_count
                        FROM {table}
                    """))
                    count = result.fetchone()[0]
                    print(f"     记录数: {count:,}")
                    
                    # 检查表结构
                    result = session.execute(text(f"""
                        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA = DATABASE() 
                        AND TABLE_NAME = '{table}'
                        ORDER BY ORDINAL_POSITION
                        LIMIT 10
                    """))
                    
                    columns = result.fetchall()
                    print(f"     主要字段:")
                    for col_name, data_type, nullable, default in columns:
                        nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                        print(f"       {col_name}: {data_type} {nullable_str}")
                        
                except Exception as e:
                    print(f"     状态: 表不存在或无法访问 - {e}")
            
            # 5. 生成优化建议
            print(f"\n=== 优化建议生成 ===")
            
            # 5.1 排序规则统一建议
            if len(collation_summary) > 1:
                print("📋 排序规则统一建议:")
                print("   发现多种排序规则，建议统一为 utf8mb4_0900_ai_ci")
                print("   优先级：")
                for i, (collation, tables) in enumerate(collation_summary.items(), 1):
                    if collation == 'utf8mb4_0900_ai_ci':
                        print(f"     {i}. 保持 {collation} - 已符合PO推荐")
                    else:
                        print(f"     {i}. 将 {collation} 转换为 utf8mb4_0900_ai_ci")
                        for table in tables:
                            print(f"        ALTER TABLE {table} CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;")
            else:
                print("✅ 排序规则统一: 表级排序规则已统一")
            
            # 5.2 索引优化建议
            print(f"\n📋 索引优化建议:")
            if not po_index_exists:
                print("   需要添加PO推荐的复合索引:")
                print(f"     ALTER TABLE student_cleaned_scores")
                print(f"     ADD INDEX {recommended_index} (batch_code, subject_id, student_id),")
                print(f"     ALGORITHM=INPLACE, LOCK=NONE;")
            else:
                print("   ✅ PO推荐的复合索引已存在")
            
            # 5.3 物化表设计建议
            print(f"\n📋 问卷物化表建议:")
            print("   按PO方案设计两个物化表:")
            print("     1. questionnaire_option_distribution - 选项分布统计")
            print("     2. questionnaire_dimension_summary - 维度汇总数据")
            
            # 计算优化完成度
            optimization_score = 0
            max_score = 100
            
            # 排序规则统一 (40分)
            if len(collation_summary) == 1 and 'utf8mb4_0900_ai_ci' in collation_summary:
                optimization_score += 40
                print("\n✅ 排序规则: 已统一为utf8mb4_0900_ai_ci (40/40分)")
            elif len(collation_summary) == 1:
                optimization_score += 20
                print("\n⚠️  排序规则: 已统一但非推荐规则 (20/40分)")
            else:
                print("\n❌ 排序规则: 需要统一 (0/40分)")
            
            # 索引优化 (40分)
            if po_index_exists:
                optimization_score += 40
                print("✅ 索引优化: PO推荐索引已存在 (40/40分)")
            elif similar_indexes:
                optimization_score += 20
                print("⚠️  索引优化: 存在相似索引但不完全匹配 (20/40分)")
            else:
                print("❌ 索引优化: 缺少PO推荐索引 (0/40分)")
            
            # 物化表准备 (20分)
            questionnaire_ready = any("questionnaire" in table for table in [t[0] for t in table_collations])
            if questionnaire_ready:
                optimization_score += 20
                print("✅ 物化表基础: 问卷表结构就绪 (20/20分)")
            else:
                print("❌ 物化表基础: 问卷表结构需要确认 (0/20分)")
            
            print(f"\n🎯 PO方案完成度评估: {optimization_score}/100分")
            
            if optimization_score >= 80:
                print("🏆 优化基础良好，可以直接实施PO方案")
            elif optimization_score >= 60:
                print("👍 优化基础可行，需要少量调整")
            else:
                print("⚠️  需要较多准备工作才能实施PO方案")
            
            print(f"\n完成时间: {datetime.now()}")
            
    except Exception as e:
        print(f"审计失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    po_optimization_audit()