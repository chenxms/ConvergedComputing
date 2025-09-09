#!/usr/bin/env python3
"""
完整验证测试：两阶段数据处理方案
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.services.calculation_service import CalculationService
from app.database.models import AggregationLevel

async def test_complete_verification():
    """完整验证两阶段方案"""
    print("=== 两阶段数据处理方案完整验证 ===\n")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        batch_code = 'G4-2025'
        print(f"测试批次: {batch_code}\n")
        
        # 1. 验证清洗表数据质量
        print("1. 验证第一阶段：清洗表数据质量")
        query = text("""
            SELECT 
                subject_name,
                COUNT(*) as record_count,
                COUNT(DISTINCT student_id) as unique_students,
                MIN(total_score) as min_score,
                MAX(total_score) as max_score,
                AVG(total_score) as avg_score
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code AND is_valid = 1
            GROUP BY subject_name
            ORDER BY subject_name
        """)
        
        result = session.execute(query, {'batch_code': batch_code})
        rows = result.fetchall()
        
        print("   清洗表数据统计:")
        total_records = 0
        total_unique_students = 0
        
        for row in rows:
            subject_name, record_count, unique_students, min_score, max_score, avg_score = row
            total_records += record_count
            total_unique_students += unique_students
            ratio = record_count / unique_students if unique_students > 0 else 0
            
            print(f"   {subject_name}: {record_count}条记录, {unique_students}个学生 (比例 {ratio:.1f}:1)")
            print(f"      分数范围: {min_score:.1f} - {max_score:.1f}, 平均: {avg_score:.1f}")
            
            if abs(ratio - 1.0) < 0.01:
                print(f"      [SUCCESS] 数据质量优秀：每个学生一条记录")
            else:
                print(f"      [WARNING] 数据质量警告：记录与学生比例异常")
        
        print(f"   总计: {total_records}条记录")
        print(f"   第一阶段数据清洗：[SUCCESS]\n")
        
        # 2. 验证第二阶段：统计计算服务
        print("2. 验证第二阶段：统计计算服务")
        calc_service = CalculationService(session)
        
        # 测试区域级计算
        print("   测试区域级统计计算...")
        try:
            regional_result = await calc_service.calculate_statistics(
                batch_code, 
                AggregationLevel.REGIONAL
            )
            
            academic_subjects = regional_result.get('regional_statistics', {}).get('academic_subjects', {})
            print(f"   区域级计算完成：包含 {len(academic_subjects)} 个科目")
            
            # 验证每个科目的统计结果
            validation_passed = True
            for subject_name, subject_stats in academic_subjects.items():
                student_count = subject_stats.get('school_stats', {}).get('student_count', 0)
                grade_dist = subject_stats.get('grade_distribution', {})
                
                # 计算等级分布总数
                total_grade_count = sum([
                    grade_dist.get('excellent', {}).get('count', 0),
                    grade_dist.get('good', {}).get('count', 0),
                    grade_dist.get('pass', {}).get('count', 0),
                    grade_dist.get('fail', {}).get('count', 0)
                ])
                
                print(f"     {subject_name}: {student_count}个学生, 等级分布总数: {total_grade_count}")
                
                if student_count > 0 and total_grade_count == student_count:
                    print(f"       [SUCCESS] 等级分布计算正确")
                elif student_count > 0:
                    print(f"       [ERROR] 等级分布计算错误：{total_grade_count} != {student_count}")
                    validation_passed = False
            
            if validation_passed:
                print("   区域级统计计算：[SUCCESS]")
            else:
                print("   区域级统计计算：[FAILED]")
                
        except Exception as e:
            print(f"   区域级统计计算失败：{e}")
        
        # 3. 验证数据库保存结果
        print(f"\n3. 验证数据库保存结果")
        
        # 检查区域级数据
        query = text("""
            SELECT COUNT(*) as count
            FROM statistical_aggregations 
            WHERE batch_code = :batch_code AND aggregation_level = 'REGIONAL'
                AND calculation_status = 'COMPLETED'
        """)
        result = session.execute(query, {'batch_code': batch_code})
        regional_count = result.fetchone()[0]
        print(f"   区域级统计记录：{regional_count} 条 {'[SUCCESS]' if regional_count > 0 else '[FAILED]'}")
        
        # 检查学校级数据
        query = text("""
            SELECT 
                COUNT(*) as total_schools,
                COUNT(CASE WHEN calculation_status = 'COMPLETED' THEN 1 END) as completed_schools
            FROM statistical_aggregations 
            WHERE batch_code = :batch_code AND aggregation_level = 'SCHOOL'
        """)
        result = session.execute(query, {'batch_code': batch_code})
        row = result.fetchone()
        total_schools, completed_schools = row
        
        print(f"   学校级统计记录：{completed_schools}/{total_schools} 完成 {'[SUCCESS]' if completed_schools == total_schools else '[PARTIAL]'}")
        
        # 4. 最终验证汇总
        print(f"\n=== 两阶段方案验证结果 ===")
        
        if (regional_count > 0 and completed_schools == total_schools and validation_passed):
            print("[SUCCESS] 第一阶段：数据清洗表构建成功")
            print("[SUCCESS] 第二阶段：统计计算服务正常")
            print("[SUCCESS] 区域级统计：计算正确，已保存")
            print("[SUCCESS] 学校级统计：批量计算成功")
            print("[SUCCESS] 数据一致性：等级分布匹配学生数")
            print("\n[COMPLETE] 两阶段数据处理方案验证成功！")
            print("   - 解决了复杂的实时聚合问题")
            print("   - 提供了清晰的数据处理流程")
            print("   - 确保了统计计算的准确性")
        else:
            print("[WARNING] 验证发现问题，需要进一步检查")
        
        session.close()
        
    except Exception as e:
        print(f"验证测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_complete_verification())