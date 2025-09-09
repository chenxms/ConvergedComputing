#!/usr/bin/env python3
"""
维度计算深度调试脚本
分析维度计算不一致的原因
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def debug_dimension_calculation():
    """调试维度计算问题"""
    print("=== 维度计算深度调试 ===\n")
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    batch_code = 'G4-2025'
    
    try:
        # 1. 详细分析维度计算不一致的样本
        print("1. 分析维度计算不一致的具体原因\n")
        
        query = text("""
            SELECT scs.student_id, scs.subject_name, scs.total_score, 
                   scs.dimension_scores, scs.dimension_max_scores,
                   ssd.total_score as original_total_score,
                   ssd.subject_scores
            FROM student_cleaned_scores scs
            JOIN student_score_detail ssd ON scs.student_id = ssd.student_id 
                AND scs.batch_code = ssd.batch_code 
                AND scs.subject_name = ssd.subject_name
            WHERE scs.batch_code = :batch_code
                AND scs.dimension_scores IS NOT NULL 
                AND scs.dimension_scores != '{}'
            LIMIT 10
        """)
        
        result = session.execute(query, {'batch_code': batch_code})
        samples = result.fetchall()
        
        inconsistent_count = 0
        consistent_count = 0
        
        for i, row in enumerate(samples):
            student_id, subject_name, cleaned_total, dim_scores_json, dim_max_json, original_total, subject_scores_json = row
            
            print(f"样本 {i+1}: 学生 {student_id} ({subject_name})")
            
            try:
                # 解析数据
                dimension_scores = json.loads(dim_scores_json) if dim_scores_json else {}
                subject_scores = json.loads(subject_scores_json) if subject_scores_json else {}
                
                # 计算各项总分
                dimension_total = sum(dim_data.get('score', 0) for dim_data in dimension_scores.values() if isinstance(dim_data, dict))
                subject_total = sum(float(score) for score in subject_scores.values() if isinstance(score, (int, float, str)) and str(score).replace('.','').replace('-','').isdigit())
                
                print(f"  原始数据总分: {original_total}")
                print(f"  题目分数总和: {subject_total:.2f}")
                print(f"  维度分数总和: {dimension_total:.2f}")
                print(f"  清洗表总分: {cleaned_total}")
                
                # 详细分析维度分数
                print(f"  维度详情:")
                for dim_code, dim_data in dimension_scores.items():
                    if isinstance(dim_data, dict):
                        print(f"    {dim_code}: {dim_data.get('score', 0):.2f}")
                
                # 详细分析题目分数 (只显示前几个)
                print(f"  题目详情 (前5个):")
                for j, (q_id, score) in enumerate(subject_scores.items()):
                    if j < 5:
                        print(f"    {q_id}: {score}")
                    else:
                        print(f"    ... 等 {len(subject_scores)} 个题目")
                        break
                
                # 判断一致性
                dimension_vs_cleaned = abs(dimension_total - cleaned_total) < 0.1
                dimension_vs_original = abs(dimension_total - original_total) < 0.1
                subject_vs_original = abs(subject_total - original_total) < 0.1
                
                if dimension_vs_cleaned:
                    consistent_count += 1
                    print(f"  ✓ 维度计算一致")
                else:
                    inconsistent_count += 1
                    print(f"  ✗ 维度计算不一致")
                    print(f"    - 维度vs清洗表差异: {abs(dimension_total - cleaned_total):.2f}")
                    if not dimension_vs_original:
                        print(f"    - 维度vs原始差异: {abs(dimension_total - original_total):.2f}")
                    if not subject_vs_original:
                        print(f"    - 题目vs原始差异: {abs(subject_total - original_total):.2f}")
                
                print()
                        
            except Exception as e:
                print(f"  错误: 数据解析失败 - {e}\n")
        
        consistency_rate = consistent_count / len(samples) * 100 if len(samples) > 0 else 0
        print(f"样本一致性分析: {consistent_count}/{len(samples)} = {consistency_rate:.1f}%")
        
        # 2. 分析可能的数据问题
        print(f"\n2. 分析潜在数据问题\n")
        
        # 2.1 检查total_score为0的记录
        zero_score_query = text("""
            SELECT COUNT(*) as zero_count,
                   subject_name
            FROM student_cleaned_scores
            WHERE batch_code = :batch_code AND total_score = 0
            GROUP BY subject_name
            ORDER BY zero_count DESC
        """)
        
        zero_result = session.execute(zero_score_query, {'batch_code': batch_code})
        zero_scores = zero_result.fetchall()
        
        if zero_scores:
            print("发现零分记录:")
            for count, subject in zero_scores:
                print(f"  {subject}: {count} 条零分记录")
        else:
            print("没有发现零分记录")
        
        # 2.2 检查维度分数异常大的记录
        high_dimension_query = text("""
            SELECT student_id, subject_name, total_score, dimension_scores
            FROM student_cleaned_scores
            WHERE batch_code = :batch_code
                AND dimension_scores IS NOT NULL
                AND dimension_scores != '{}'
            ORDER BY total_score DESC
            LIMIT 3
        """)
        
        high_result = session.execute(high_dimension_query, {'batch_code': batch_code})
        high_scores = high_result.fetchall()
        
        print(f"\n分数最高的记录:")
        for student_id, subject, total, dim_json in high_scores:
            try:
                dim_data = json.loads(dim_json) if dim_json else {}
                dim_total = sum(d.get('score', 0) for d in dim_data.values() if isinstance(d, dict))
                print(f"  学生 {student_id} ({subject}): 总分 {total}, 维度总分 {dim_total:.2f}")
            except:
                print(f"  学生 {student_id} ({subject}): 总分 {total}, 维度解析失败")
        
        # 3. 统计维度计算整体情况
        print(f"\n3. 维度计算整体统计\n")
        
        stats_query = text("""
            SELECT 
                subject_name,
                COUNT(*) as total_records,
                AVG(total_score) as avg_total_score,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score,
                COUNT(CASE WHEN total_score = 0 THEN 1 END) as zero_scores
            FROM student_cleaned_scores
            WHERE batch_code = :batch_code
            GROUP BY subject_name
            ORDER BY subject_name
        """)
        
        stats_result = session.execute(stats_query, {'batch_code': batch_code})
        stats_data = stats_result.fetchall()
        
        print("各科目分数分布:")
        print("科目名称".ljust(10) + "记录数".ljust(8) + "平均分".ljust(10) + "最小分".ljust(8) + "最大分".ljust(8) + "零分数")
        print("-" * 60)
        
        for subject, total, avg_score, min_score, max_score, zero_count in stats_data:
            print(f"{subject[:9].ljust(10)} {str(total).ljust(8)} {avg_score:.2f}".ljust(10) + 
                  f"{min_score:.1f}".ljust(8) + f"{max_score:.1f}".ljust(8) + str(zero_count))
        
        print(f"\n=== 调试结论 ===")
        
        if zero_scores:
            print("⚠️  发现零分记录，可能影响维度计算一致性")
        
        if consistency_rate < 90:
            print("⚠️  维度计算存在不一致性，建议检查数据清洗逻辑")
            print("   可能原因:")
            print("   1. 原始数据中存在重复记录导致聚合不一致")
            print("   2. 维度映射与题目分数不匹配")
            print("   3. 数据清洗过程中的计算错误")
        else:
            print("✓ 维度计算基本一致")
        
    except Exception as e:
        print(f"调试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        session.close()

if __name__ == "__main__":
    debug_dimension_calculation()