#!/usr/bin/env python3
"""
测试数据质量验证机制
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.services.calculation_service import CalculationService

async def test_data_validation():
    """测试数据质量验证"""
    print("=== 测试数据质量验证机制 ===\n")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 创建计算服务
        calc_service = CalculationService(session)
        
        # 测试批次
        batch_code = 'G4-2025'
        print(f"验证批次: {batch_code}")
        
        # 执行数据质量验证
        validation_results = await calc_service.validate_batch_data_quality(batch_code)
        
        # 打印验证结果
        print(f"\n=== 验证结果 ===")
        print(f"整体状态: {validation_results['overall_status']}")
        print(f"发现问题: {validation_results['issues_found']} 个")
        print(f"发现警告: {validation_results['warnings_found']} 个")
        
        # 基础数据检查结果
        print(f"\n=== 基础数据检查 ===")
        basic_checks = validation_results.get('basic_checks', {})
        for table, check in basic_checks.items():
            status = "[OK]" if check['exists'] else "[FAIL]"
            print(f"{status} {table}: {check['count']} 条记录")
            if check['issues']:
                for issue in check['issues']:
                    print(f"   问题: {issue}")
        
        # 科目数据质量检查结果
        print(f"\n=== 科目数据质量检查 ===")
        subjects = validation_results.get('subjects', {})
        for subject_name, subject_validation in subjects.items():
            status_icon = {"OK": "[OK]", "WARNINGS": "[WARN]", "ISSUES": "[FAIL]", "ERROR": "[ERROR]"}.get(
                subject_validation['status'], "[?]"
            )
            print(f"{status_icon} {subject_name} ({subject_validation['status']})")
            
            stats = subject_validation.get('statistics', {})
            if stats:
                print(f"   记录数: {stats['total_records']} | 学生数: {stats['unique_students']}")
                print(f"   分数范围: {stats['min_score']:.1f} - {stats['max_score']:.1f} (平均: {stats['avg_score']:.1f})")
                
                if stats['negative_scores'] > 0 or stats['overmax_scores'] > 0 or stats['null_scores'] > 0:
                    print(f"   异常分数: 负数 {stats['negative_scores']} | 超范围 {stats['overmax_scores']} | 空值 {stats['null_scores']}")
            
            if subject_validation['issues']:
                for issue in subject_validation['issues']:
                    print(f"   [!] 问题: {issue}")
            
            if subject_validation['warnings']:
                for warning in subject_validation['warnings']:
                    print(f"   [*] 警告: {warning}")
        
        # 维度数据检查结果
        print(f"\n=== 维度数据检查 ===")
        dimension_checks = validation_results.get('dimension_checks', {})
        status_icon = {"OK": "[OK]", "WARNINGS": "[WARN]", "ISSUES": "[FAIL]", "ERROR": "[ERROR]"}.get(
            dimension_checks['status'], "[?]"
        )
        print(f"{status_icon} 维度数据 ({dimension_checks['status']})")
        print(f"   检查科目: {dimension_checks['subjects_checked']} 个")
        print(f"   有维度的科目: {dimension_checks['subjects_with_dimensions']} 个")
        print(f"   总维度数: {dimension_checks['total_dimensions']} 个")
        
        if dimension_checks['issues']:
            for issue in dimension_checks['issues']:
                print(f"   [!] 问题: {issue}")
        
        if dimension_checks['warnings']:
            for warning in dimension_checks['warnings']:
                print(f"   [*] 警告: {warning}")
        
        # 汇总建议
        print(f"\n=== 汇总建议 ===")
        summary = validation_results.get('summary', {})
        print(f"建议: {summary.get('recommendation', '无建议')}")
        
        # 保存详细结果到文件
        with open('validation_results.json', 'w', encoding='utf-8') as f:
            json.dump(validation_results, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n详细验证结果已保存到: validation_results.json")
        
        session.close()
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_data_validation())