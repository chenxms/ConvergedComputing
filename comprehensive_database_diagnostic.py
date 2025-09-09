#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
G7-2025数据库全面诊断脚本
根本原因分析和数据存在性验证
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db, test_connection, check_database_health
from sqlalchemy import text, inspect
import traceback
import json


def comprehensive_database_diagnostic():
    """全面数据库诊断"""
    print("=" * 60)
    print("G7-2025 数据库全面诊断报告")
    print("=" * 60)
    print()
    
    # 阶段1: 基础连接测试
    print("阶段1: 数据库连接和健康状态检查")
    print("-" * 40)
    
    # 测试连接
    connection_ok = test_connection()
    print(f"数据库连接测试: {'SUCCESS' if connection_ok else 'FAILED'}")
    
    if not connection_ok:
        print("CRITICAL: 数据库连接失败，无法继续诊断")
        return False
    
    # 健康检查
    health_info = check_database_health()
    print(f"数据库健康状态: {health_info.get('status', 'unknown')}")
    print(f"连接响应时间: {health_info.get('response_time_ms', 0):.2f}ms")
    
    print()
    
    # 阶段2: 数据库表结构检查
    print("阶段2: 核心表结构和存在性检查")
    print("-" * 40)
    
    db = next(get_db())
    
    try:
        # 检查核心表是否存在
        inspector = inspect(db.bind)
        tables = inspector.get_table_names()
        
        required_tables = [
            'student_score_detail', 
            'subject_question_config', 
            'batches',
            'statistical_aggregation',
            'grade_aggregation_main'
        ]
        
        print("核心表存在性检查:")
        table_status = {}
        for table in required_tables:
            exists = table in tables
            table_status[table] = exists
            print(f"  {table}: {'EXISTS' if exists else 'MISSING'}")
        
        if not all(table_status.values()):
            print("ERROR: 缺少必要的数据表")
            return False
        
        print()
        
        # 阶段3: G7-2025批次数据存在性检查
        print("阶段3: G7-2025批次数据全面检查")
        print("-" * 40)
        
        batch_code = "G7-2025"
        
        # 检查batches表
        result = db.execute(text("SELECT COUNT(*) FROM batches WHERE name = :batch_code"), 
                           {'batch_code': batch_code})
        batch_count = result.fetchone()[0]
        print(f"batches表中G7-2025记录: {batch_count}")
        
        if batch_count == 0:
            print("WARNING: batches表中没有G7-2025记录")
        
        # 检查student_score_detail表 - 核心学生数据
        try:
            result = db.execute(text("SELECT COUNT(*) FROM student_score_detail WHERE batch_code = :batch_code"), 
                               {'batch_code': batch_code})
            student_detail_count = result.fetchone()[0]
            print(f"student_score_detail表中G7-2025记录: {student_detail_count}")
            
            if student_detail_count > 0:
                # 获取详细统计
                result = db.execute(text("""
                    SELECT 
                        COUNT(DISTINCT student_id) as unique_students,
                        COUNT(DISTINCT school_id) as unique_schools,
                        COUNT(DISTINCT subject_name) as unique_subjects,
                        COUNT(*) as total_records
                    FROM student_score_detail 
                    WHERE batch_code = :batch_code
                """), {'batch_code': batch_code})
                
                stats = result.fetchone()
                print(f"  - 唯一学生数: {stats.unique_students}")
                print(f"  - 唯一学校数: {stats.unique_schools}")  
                print(f"  - 唯一科目数: {stats.unique_subjects}")
                print(f"  - 总记录数: {stats.total_records}")
                
                # 列出所有科目
                result = db.execute(text("""
                    SELECT DISTINCT subject_name 
                    FROM student_score_detail 
                    WHERE batch_code = :batch_code
                    ORDER BY subject_name
                """), {'batch_code': batch_code})
                
                subjects = [row[0] for row in result.fetchall()]
                print(f"  科目列表: {', '.join(subjects)}")
                
                # 抽样检查数据结构
                result = db.execute(text("""
                    SELECT student_id, student_name, school_id, school_code, 
                           school_name, subject_name, total_score
                    FROM student_score_detail 
                    WHERE batch_code = :batch_code
                    LIMIT 3
                """), {'batch_code': batch_code})
                
                print("  数据样本:")
                for i, row in enumerate(result.fetchall(), 1):
                    print(f"    样本{i}: 学生{row.student_id}, 学校{row.school_code}, "
                          f"科目{row.subject_name}, 分数{row.total_score}")
                          
            else:
                print("CRITICAL: student_score_detail表中没有G7-2025数据!")
                
        except Exception as e:
            print(f"ERROR: 查询student_score_detail失败: {e}")
        
        # 检查subject_question_config表 - 科目配置
        try:
            result = db.execute(text("SELECT COUNT(*) FROM subject_question_config WHERE batch_code = :batch_code"), 
                               {'batch_code': batch_code})
            subject_config_count = result.fetchone()[0]
            print(f"subject_question_config表中G7-2025记录: {subject_config_count}")
            
            if subject_config_count > 0:
                result = db.execute(text("""
                    SELECT subject_name, max_score 
                    FROM subject_question_config 
                    WHERE batch_code = :batch_code
                    ORDER BY subject_name
                """), {'batch_code': batch_code})
                
                configs = result.fetchall()
                print("  科目配置:")
                for config in configs:
                    print(f"    {config.subject_name}: 满分{config.max_score}")
                    
        except Exception as e:
            print(f"ERROR: 查询subject_question_config失败: {e}")
        
        # 检查statistical_aggregation表 - 已生成的统计数据
        try:
            result = db.execute(text("SELECT COUNT(*) FROM statistical_aggregation WHERE batch_code = :batch_code"), 
                               {'batch_code': batch_code})
            stats_count = result.fetchone()[0]
            print(f"statistical_aggregation表中G7-2025记录: {stats_count}")
            
            if stats_count > 0:
                result = db.execute(text("""
                    SELECT aggregation_level, school_id, COUNT(*) as count
                    FROM statistical_aggregation 
                    WHERE batch_code = :batch_code
                    GROUP BY aggregation_level, school_id
                """), {'batch_code': batch_code})
                
                print("  已生成统计数据:")
                for row in result.fetchall():
                    level = row.aggregation_level
                    school = row.school_id or "区域级"
                    count = row.count
                    print(f"    {level}: {school} ({count}条记录)")
                    
        except Exception as e:
            print(f"ERROR: 查询statistical_aggregation失败: {e}")
        
        print()
        
        # 阶段4: 数据质量和一致性检查
        print("阶段4: 数据质量和一致性验证")
        print("-" * 40)
        
        try:
            # 检查数据完整性
            result = db.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN total_score IS NULL THEN 1 END) as null_scores,
                    COUNT(CASE WHEN student_id IS NULL OR student_id = '' THEN 1 END) as null_students,
                    COUNT(CASE WHEN subject_name IS NULL OR subject_name = '' THEN 1 END) as null_subjects
                FROM student_score_detail 
                WHERE batch_code = :batch_code
            """), {'batch_code': batch_code})
            
            quality = result.fetchone()
            if quality and quality.total_records > 0:
                print(f"数据质量检查:")
                print(f"  总记录数: {quality.total_records}")
                print(f"  空分数记录: {quality.null_scores}")
                print(f"  空学生ID记录: {quality.null_students}")
                print(f"  空科目记录: {quality.null_subjects}")
                
                data_quality_ok = (quality.null_scores == 0 and 
                                 quality.null_students == 0 and 
                                 quality.null_subjects == 0)
                print(f"数据质量状态: {'GOOD' if data_quality_ok else 'ISSUES FOUND'}")
                
        except Exception as e:
            print(f"ERROR: 数据质量检查失败: {e}")
        
        print()
        
        # 阶段5: 最终诊断总结
        print("阶段5: 诊断总结和建议")
        print("-" * 40)
        
        if student_detail_count > 0:
            print(f"SUCCESS: 发现G7-2025批次数据")
            print(f"  实际学生数据量: {stats.unique_students if 'stats' in locals() else '未知'}")
            print(f"  数据记录总数: {student_detail_count}")
            print(f"  涉及学校数: {stats.unique_schools if 'stats' in locals() else '未知'}")
            print(f"  涉及科目数: {stats.unique_subjects if 'stats' in locals() else '未知'}")
            
            if student_detail_count < 1000:
                print("WARNING: 数据量似乎偏少，需要进一步调查")
            
            return True
        else:
            print("CRITICAL: 没有发现G7-2025批次的学生数据")
            print("可能的原因:")
            print("  1. 数据未导入")
            print("  2. batch_code字段值不匹配")
            print("  3. 数据在其他表中")
            print("  4. 权限问题")
            return False
            
    except Exception as e:
        print(f"CRITICAL ERROR: 诊断过程中发生严重错误: {e}")
        traceback.print_exc()
        return False
    finally:
        db.close()


def main():
    """主函数"""
    success = comprehensive_database_diagnostic()
    print()
    print("=" * 60)
    if success:
        print("诊断完成: 发现G7-2025数据，可以继续下一步处理")
    else:
        print("诊断完成: 发现严重问题，需要立即修复")
    print("=" * 60)
    return success


if __name__ == "__main__":
    main()