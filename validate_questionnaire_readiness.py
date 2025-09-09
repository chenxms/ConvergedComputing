#!/usr/bin/env python3
"""
验证问卷数据是否符合汇聚计算需求
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def validate_questionnaire_readiness():
    """验证问卷数据是否符合汇聚计算需求"""
    print("=== 验证问卷数据汇聚准备情况 ===\n")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        print("1. 检查问卷详细数据表（questionnaire_question_scores）...")
        
        # 检查每个批次的数据完整性
        detail_check_query = text("""
            SELECT 
                batch_code,
                COUNT(*) as total_records,
                COUNT(DISTINCT student_id) as student_count,
                COUNT(DISTINCT question_id) as question_count,
                COUNT(DISTINCT dimension_code) as dimension_count,
                COUNT(CASE WHEN dimension_code IS NOT NULL THEN 1 END) as with_dimension,
                COUNT(CASE WHEN dimension_name IS NOT NULL THEN 1 END) as with_dimension_name,
                COUNT(CASE WHEN option_label IS NOT NULL THEN 1 END) as with_option_label,
                COUNT(CASE WHEN option_level IS NOT NULL THEN 1 END) as with_option_level
            FROM questionnaire_question_scores
            GROUP BY batch_code
            ORDER BY batch_code
        """)
        
        detail_results = session.execute(detail_check_query).fetchall()
        
        print("   批次数据统计:")
        for row in detail_results:
            batch_code = row[0]
            total = row[1]
            students = row[2]
            questions = row[3]
            dimensions = row[4]
            with_dim = row[5]
            with_dim_name = row[6]
            with_option_label = row[7]
            with_option_level = row[8]
            
            print(f"   {batch_code}:")
            print(f"      总记录数: {total:,}")
            print(f"      学生数: {students:,}")
            print(f"      题目数: {questions}")
            print(f"      维度数: {dimensions}")
            print(f"      有维度代码: {with_dim:,} ({with_dim/total*100:.1f}%)")
            print(f"      有维度名称: {with_dim_name:,} ({with_dim_name/total*100:.1f}%)")
            print(f"      有选项标签: {with_option_label:,} ({with_option_label/total*100:.1f}%)")
            print(f"      有选项等级: {with_option_level:,} ({with_option_level/total*100:.1f}%)")
            
            # 检查数据完整性
            issues = []
            if with_dim < total:
                issues.append("维度代码缺失")
            if with_dim_name < total:
                issues.append("维度名称缺失")
            if with_option_label < total:
                issues.append("选项标签缺失")
            if with_option_level < total:
                issues.append("选项等级缺失")
            
            if issues:
                print(f"      [ISSUE] 问题: {', '.join(issues)}")
            else:
                print(f"      [OK] 数据完整")
        
        print("\n2. 检查学生汇总数据表（student_cleaned_scores）...")
        
        # 检查汇总数据
        summary_check_query = text("""
            SELECT 
                batch_code,
                COUNT(*) as student_count,
                COUNT(CASE WHEN dimension_scores IS NOT NULL AND dimension_scores != '{}' THEN 1 END) as with_dim_scores,
                COUNT(CASE WHEN dimension_max_scores IS NOT NULL AND dimension_max_scores != '{}' THEN 1 END) as with_dim_max_scores,
                AVG(total_score) as avg_total_score,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score
            FROM student_cleaned_scores
            WHERE subject_type = 'questionnaire' OR subject_name = '问卷'
            GROUP BY batch_code
            ORDER BY batch_code
        """)
        
        summary_results = session.execute(summary_check_query).fetchall()
        
        print("   汇总数据统计:")
        for row in summary_results:
            batch_code = row[0]
            student_count = row[1]
            with_dim_scores = row[2]
            with_dim_max_scores = row[3]
            avg_score = row[4]
            min_score = row[5]
            max_score = row[6]
            
            print(f"   {batch_code}:")
            print(f"      学生数: {student_count:,}")
            print(f"      有维度分数: {with_dim_scores:,} ({with_dim_scores/student_count*100:.1f}%)")
            print(f"      有维度满分: {with_dim_max_scores:,} ({with_dim_max_scores/student_count*100:.1f}%)")
            print(f"      平均分: {avg_score:.2f}")
            print(f"      分数范围: {min_score:.2f} - {max_score:.2f}")
            
            # 检查维度数据完整性
            if with_dim_scores < student_count or with_dim_max_scores < student_count:
                print(f"      [ISSUE] 维度数据不完整")
            else:
                print(f"      [OK] 维度数据完整")
        
        print("\n3. 检查维度分布情况...")
        
        # 检查每个批次的维度分布
        for batch in ['G4-2025', 'G7-2025', 'G8-2025']:
            dimension_query = text("""
                SELECT 
                    dimension_code,
                    dimension_name,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT student_id) as student_count
                FROM questionnaire_question_scores
                WHERE batch_code = :batch_code AND dimension_code IS NOT NULL
                GROUP BY dimension_code, dimension_name
                ORDER BY record_count DESC
            """)
            
            dim_results = session.execute(dimension_query, {'batch_code': batch}).fetchall()
            
            print(f"\n   {batch} 维度分布:")
            if dim_results:
                for dim_row in dim_results:
                    print(f"      {dim_row[0]} ({dim_row[1]}): {dim_row[2]:,}条记录, {dim_row[3]:,}学生")
            else:
                print(f"      [ISSUE] 无维度数据")
        
        print("\n4. 检查选项分布合理性...")
        
        # 检查选项分布
        for batch in ['G4-2025', 'G7-2025', 'G8-2025']:
            option_query = text("""
                SELECT 
                    option_label,
                    option_level,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                FROM questionnaire_question_scores
                WHERE batch_code = :batch_code
                GROUP BY option_label, option_level
                ORDER BY option_level
            """)
            
            opt_results = session.execute(option_query, {'batch_code': batch}).fetchall()
            
            print(f"\n   {batch} 选项分布:")
            if opt_results:
                for opt_row in opt_results:
                    print(f"      {opt_row[0]} (等级{opt_row[1]}): {opt_row[2]:,}条 ({opt_row[3]}%)")
            else:
                print(f"      [ISSUE] 无选项数据")
        
        print("\n5. 汇聚计算准备情况评估...")
        
        # 评估是否准备就绪
        ready_batches = []
        not_ready_batches = []
        
        for detail_row in detail_results:
            batch_code = detail_row[0]
            total = detail_row[1]
            with_dim = detail_row[5]
            
            if with_dim == total and with_dim > 0:
                ready_batches.append(batch_code)
            else:
                not_ready_batches.append(batch_code)
        
        print(f"   [OK] 准备就绪的批次: {', '.join(ready_batches) if ready_batches else '无'}")
        print(f"   [PENDING] 尚未准备的批次: {', '.join(not_ready_batches) if not_ready_batches else '无'}")
        
        if not not_ready_batches:
            print("\n[SUCCESS] 所有批次问卷数据已准备就绪，可以进行汇聚计算！")
            print("\n汇聚计算支持的功能:")
            print("   - 按维度统计选项分布（非常同意/同意/不同意/非常不同意）")
            print("   - 计算维度平均分和标准差")
            print("   - 按学校、班级、学生进行多层级汇聚")
            print("   - 生成维度雷达图数据")
            print("   - 进行维度间相关性分析")
        else:
            print(f"\n[WARNING] 还有 {len(not_ready_batches)} 个批次需要重新清洗")
        
        print("\n=== 验证完成 ===")
        
    except Exception as e:
        print(f"验证过程发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    validate_questionnaire_readiness()