#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据质量监控系统测试脚本
验证核心功能是否正常工作
"""

import os
import sys
import json
import logging
from datetime import datetime

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database.connection import get_db
from app.utils.data_validation import DataValidator

# 配置日志输出为UTF-8
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('monitoring_test.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def test_basic_validation(db_session, batch_code="G4-2025"):
    """测试基础验证功能"""
    print("="*80)
    print(f"测试基础验证功能 - 批次: {batch_code}")
    print("="*80)
    
    try:
        validator = DataValidator(db_session)
        
        # 1. 测试基本的学校一致性检查
        print("1. 执行学校数据一致性检查...")
        result = validator.validate_batch_school_consistency(batch_code)
        
        print(f"   批次代码: {result['batch_code']}")
        print(f"   验证结果: {'通过' if result['is_valid'] else '失败'}")
        print(f"   统计信息:")
        for key, value in result['statistics'].items():
            print(f"     - {key}: {value}")
        
        if result['errors']:
            print("   错误:")
            for error in result['errors']:
                print(f"     - {error['type']}: {error['message']}")
        
        if result['warnings']:
            print("   警告:")
            for warning in result['warnings']:
                print(f"     - {warning['type']}: {warning['message']}")
        
        return result
        
    except Exception as e:
        print(f"   基础验证测试失败: {e}")
        return None


def test_school_count_check(db_session, batch_code="G4-2025"):
    """测试学校数量检查"""
    print("\n" + "="*80)
    print(f"测试学校数量检查 - 批次: {batch_code}")
    print("="*80)
    
    try:
        from sqlalchemy import text
        
        # 检查school_master_data中的学校数量
        query = text("""
            SELECT COUNT(DISTINCT school_id) as school_count
            FROM school_master_data 
            WHERE batch_code = :batch_code 
                AND status = 'ACTIVE'
        """)
        
        result = db_session.execute(query, {'batch_code': batch_code})
        master_count = result.scalar() or 0
        
        print(f"   school_master_data中的学校数: {master_count}")
        
        # 检查student_cleaned_scores中的学校数量
        query = text("""
            SELECT COUNT(DISTINCT school_code) as school_count
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
        """)
        
        result = db_session.execute(query, {'batch_code': batch_code})
        cleaned_count = result.scalar() or 0
        
        print(f"   student_cleaned_scores中的学校数: {cleaned_count}")
        
        # 检查孤立学校
        query = text("""
            SELECT COUNT(DISTINCT scs.school_code) as orphaned_count
            FROM student_cleaned_scores scs
            LEFT JOIN school_master_data smd 
                ON smd.batch_code = scs.batch_code
                AND smd.school_id = scs.school_code
                AND smd.status = 'ACTIVE'
            WHERE scs.batch_code = :batch_code
                AND smd.school_id IS NULL
        """)
        
        result = db_session.execute(query, {'batch_code': batch_code})
        orphaned_count = result.scalar() or 0
        
        print(f"   孤立学校数量: {orphaned_count}")
        
        if orphaned_count > 0:
            print("   ❌ 发现孤立学校！这是关键问题")
            return False
        else:
            print("   ✅ 没有孤立学校")
            return True
            
    except Exception as e:
        print(f"   学校数量检查失败: {e}")
        return False


def test_school_name_completeness(db_session, batch_code="G4-2025"):
    """测试学校名称完整性"""
    print("\n" + "="*80)
    print(f"测试学校名称完整性 - 批次: {batch_code}")
    print("="*80)
    
    try:
        from sqlalchemy import text
        
        # 检查student_cleaned_scores中的NULL名称
        query = text("""
            SELECT COUNT(*) as null_count
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
            AND (school_name IS NULL OR school_name = '' OR TRIM(school_name) = '')
        """)
        
        result = db_session.execute(query, {'batch_code': batch_code})
        cleaned_null_count = result.scalar() or 0
        
        print(f"   student_cleaned_scores中NULL名称数量: {cleaned_null_count}")
        
        # 检查school_master_data中的NULL名称
        query = text("""
            SELECT COUNT(*) as null_count
            FROM school_master_data 
            WHERE batch_code = :batch_code
            AND (standard_school_name IS NULL OR standard_school_name = '' OR TRIM(standard_school_name) = '')
        """)
        
        result = db_session.execute(query, {'batch_code': batch_code})
        master_null_count = result.scalar() or 0
        
        print(f"   school_master_data中NULL名称数量: {master_null_count}")
        
        total_null = cleaned_null_count + master_null_count
        
        if total_null > 0:
            print("   ❌ 发现NULL学校名称！这是关键问题")
            return False
        else:
            print("   ✅ 所有学校名称都完整")
            return True
            
    except Exception as e:
        print(f"   学校名称完整性检查失败: {e}")
        return False


def test_data_distribution(db_session, batch_code="G4-2025"):
    """测试数据分布"""
    print("\n" + "="*80)
    print(f"测试数据分布 - 批次: {batch_code}")
    print("="*80)
    
    try:
        from sqlalchemy import text
        
        # 检查学生数据分布
        query = text("""
            SELECT 
                school_code,
                school_name,
                COUNT(*) as student_count
            FROM student_cleaned_scores 
            WHERE batch_code = :batch_code
            GROUP BY school_code, school_name
            ORDER BY student_count DESC
            LIMIT 10
        """)
        
        result = db_session.execute(query, {'batch_code': batch_code})
        top_schools = result.fetchall()
        
        print("   学生数最多的前10所学校:")
        for i, (school_code, school_name, student_count) in enumerate(top_schools, 1):
            print(f"   {i:2d}. {school_code} - {school_name}: {student_count}人")
        
        # 检查学生数过少的学校
        query = text("""
            SELECT COUNT(*) as low_count
            FROM (
                SELECT 
                    school_code,
                    COUNT(*) as student_count
                FROM student_cleaned_scores 
                WHERE batch_code = :batch_code
                GROUP BY school_code
                HAVING student_count < 10
            ) as low_schools
        """)
        
        result = db_session.execute(query, {'batch_code': batch_code})
        low_count = result.scalar() or 0
        
        print(f"   学生数少于10人的学校: {low_count}所")
        
        if low_count > 5:
            print("   ⚠️  较多学校学生数过少，需要检查")
        else:
            print("   ✅ 学生数分布正常")
        
        return True
        
    except Exception as e:
        print(f"   数据分布检查失败: {e}")
        return False


def test_aggregation_status(db_session, batch_code="G4-2025"):
    """测试汇聚状态"""
    print("\n" + "="*80)
    print(f"测试汇聚状态 - 批次: {batch_code}")
    print("="*80)
    
    try:
        from sqlalchemy import text
        
        # 检查statistical_aggregations表是否存在
        try:
            query = text("""
                SELECT COUNT(*) as agg_count,
                       COUNT(CASE WHEN school_name IS NULL OR school_name = '' THEN 1 END) as null_name_count
                FROM statistical_aggregations 
                WHERE batch_code = :batch_code
            """)
            
            result = db_session.execute(query, {'batch_code': batch_code}).fetchone()
            agg_count = result[0] if result else 0
            null_name_count = result[1] if result else 0
            
            print(f"   statistical_aggregations中的记录数: {agg_count}")
            print(f"   其中NULL名称记录数: {null_name_count}")
            
            if agg_count > 0:
                print("   ✅ 已有汇聚数据")
                
                if null_name_count > 0:
                    print("   ❌ 汇聚结果中有NULL学校名称！这是关键问题")
                    return False
                else:
                    print("   ✅ 汇聚结果中学校名称完整")
                    return True
            else:
                print("   ⚠️  暂无汇聚数据")
                return True
                
        except Exception:
            print("   ⚠️  statistical_aggregations表不存在或无法访问")
            return True
            
    except Exception as e:
        print(f"   汇聚状态检查失败: {e}")
        return False


def generate_summary_report(test_results, batch_code):
    """生成测试摘要报告"""
    print("\n" + "="*80)
    print(f"监控系统测试摘要 - 批次: {batch_code}")
    print("="*80)
    
    total_tests = len(test_results)
    passed_tests = sum(1 for result in test_results.values() if result)
    failed_tests = total_tests - passed_tests
    
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总测试数: {total_tests}")
    print(f"通过测试: {passed_tests}")
    print(f"失败测试: {failed_tests}")
    print(f"成功率: {passed_tests/total_tests*100:.1f}%")
    print()
    
    print("各项测试结果:")
    test_names = {
        'basic_validation': '基础验证功能',
        'school_count': '学校数量检查',
        'name_completeness': '学校名称完整性',
        'data_distribution': '数据分布检查',
        'aggregation_status': '汇聚状态检查'
    }
    
    for test_key, result in test_results.items():
        test_name = test_names.get(test_key, test_key)
        status = "✅ 通过" if result else "❌ 失败"
        print(f"  - {test_name}: {status}")
    
    print()
    
    # 生成建议
    critical_issues = []
    warnings = []
    
    if not test_results.get('basic_validation'):
        critical_issues.append("基础验证失败，数据质量有严重问题")
    
    if not test_results.get('school_count'):
        critical_issues.append("发现孤立学校，必须修复后才能继续处理")
    
    if not test_results.get('name_completeness'):
        critical_issues.append("发现NULL学校名称，会导致前端显示错误")
    
    if not test_results.get('aggregation_status'):
        critical_issues.append("汇聚结果中有NULL学校名称")
    
    if critical_issues:
        print("🔴 关键问题:")
        for issue in critical_issues:
            print(f"  - {issue}")
        print("\n建议: 立即停止处理并修复这些问题！")
    else:
        print("✅ 未发现关键问题，数据质量良好")
        
        if not test_results.get('data_distribution'):
            warnings.append("数据分布需要关注")
        
        if warnings:
            print("\n⚠️  警告:")
            for warning in warnings:
                print(f"  - {warning}")
    
    return {
        'batch_code': batch_code,
        'test_timestamp': datetime.now().isoformat(),
        'total_tests': total_tests,
        'passed_tests': passed_tests,
        'failed_tests': failed_tests,
        'success_rate': passed_tests/total_tests*100,
        'test_results': test_results,
        'critical_issues': critical_issues,
        'warnings': warnings
    }


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="数据质量监控系统测试")
    parser.add_argument("batch_code", nargs='?', default="G4-2025", help="批次代码，如 G4-2025")
    parser.add_argument("--save-report", action="store_true", help="保存测试报告")
    
    args = parser.parse_args()
    
    try:
        print("启动数据质量监控系统测试...")
        print(f"目标批次: {args.batch_code}")
        print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        with next(get_db()) as db:
            # 执行各项测试
            test_results = {}
            
            # 1. 基础验证测试
            result = test_basic_validation(db, args.batch_code)
            test_results['basic_validation'] = result is not None and result.get('is_valid', False)
            
            # 2. 学校数量检查
            test_results['school_count'] = test_school_count_check(db, args.batch_code)
            
            # 3. 学校名称完整性
            test_results['name_completeness'] = test_school_name_completeness(db, args.batch_code)
            
            # 4. 数据分布检查
            test_results['data_distribution'] = test_data_distribution(db, args.batch_code)
            
            # 5. 汇聚状态检查
            test_results['aggregation_status'] = test_aggregation_status(db, args.batch_code)
            
            # 生成摘要报告
            summary = generate_summary_report(test_results, args.batch_code)
            
            # 保存报告
            if args.save_report:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"monitoring_test_report_{args.batch_code}_{timestamp}.json"
                
                os.makedirs("test_reports", exist_ok=True)
                filepath = os.path.join("test_reports", filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(summary, f, ensure_ascii=False, indent=2)
                
                print(f"\n测试报告已保存到: {filepath}")
            
            # 设置退出代码
            if summary['critical_issues']:
                print("\n⚠️  测试发现关键问题！")
                sys.exit(1)
            else:
                print("\n✅ 测试完成，监控系统工作正常")
                sys.exit(0)
                
    except Exception as e:
        print(f"\n❌ 测试过程失败: {e}")
        sys.exit(2)


if __name__ == "__main__":
    main()