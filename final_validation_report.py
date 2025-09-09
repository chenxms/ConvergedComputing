#!/usr/bin/env python3
"""
最终验证报告 - 检查汇聚数据是否符合所有要求
"""
import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database.connection import get_db
from sqlalchemy import text

def validate_batch_data(batch_code):
    """验证批次数据质量"""
    print(f"\n{'='*60}")
    print(f"验证批次: {batch_code}")
    print('='*60)
    
    db = next(get_db())
    issues = []
    
    # 1. 检查基础统计
    print("\n1. 基础统计检查:")
    query = text("""
        SELECT 
            aggregation_level,
            COUNT(*) as count,
            AVG(total_students) as avg_students,
            AVG(total_schools) as avg_schools
        FROM statistical_aggregations
        WHERE batch_code = :batch_code
        GROUP BY aggregation_level
    """)
    results = db.execute(query, {'batch_code': batch_code}).fetchall()
    
    if not results:
        print(f"   [FAIL] 无汇聚数据")
        issues.append("无汇聚数据")
        return issues
    
    for row in results:
        level = row[0]
        count = row[1]
        avg_students = row[2] or 0
        avg_schools = row[3] or 0
        
        print(f"   {level}: {count}条")
        print(f"     - total_students平均值: {avg_students:.0f}")
        print(f"     - total_schools平均值: {avg_schools:.0f}")
        
        if avg_students == 0:
            issues.append(f"{level}层级total_students为0")
            print(f"     [FAIL] total_students为0")
        else:
            print(f"     [OK] total_students正常")
            
        if level == 'REGIONAL' and avg_schools == 0:
            issues.append(f"{level}层级total_schools为0")
            print(f"     [FAIL] total_schools为0")
        elif level == 'REGIONAL':
            print(f"     [OK] total_schools正常")
    
    # 2. 检查区域层数据质量
    print("\n2. 区域层数据质量检查:")
    query = text("""
        SELECT statistics_data
        FROM statistical_aggregations
        WHERE batch_code = :batch_code
        AND aggregation_level = 'REGIONAL'
        LIMIT 1
    """)
    result = db.execute(query, {'batch_code': batch_code}).fetchone()
    
    if result:
        data = result[0] if isinstance(result[0], dict) else json.loads(result[0])
        
        # 检查schema版本
        schema_version = data.get('schema_version')
        if schema_version == 'v1.2':
            print(f"   [OK] Schema版本: {schema_version}")
        else:
            print(f"   [FAIL] Schema版本错误: {schema_version}")
            issues.append(f"Schema版本不是v1.2")
        
        # 检查subjects结构
        if 'subjects' not in data:
            print(f"   [FAIL] 缺少subjects字段")
            issues.append("缺少subjects字段")
        else:
            subjects = data['subjects']
            print(f"   [OK] 包含 {len(subjects)} 个科目")
            
            if subjects:
                # 检查第一个科目
                subject = subjects[0]
                print(f"\n   检查第一个科目: {subject.get('subject_name')}")
                
                # 检查metrics必要字段
                metrics = subject.get('metrics', {})
                required_metrics = ['avg', 'difficulty', 'stddev', 'discrimination', 
                                  'max', 'min', 'p10', 'p50', 'p90']
                missing_metrics = [f for f in required_metrics if f not in metrics]
                
                if missing_metrics:
                    print(f"   [FAIL] Metrics缺少字段: {missing_metrics}")
                    issues.append(f"Metrics缺少: {missing_metrics}")
                else:
                    print(f"   [OK] Metrics字段完整")
                    # 检查数值
                    for field in ['avg', 'difficulty', 'discrimination']:
                        value = metrics.get(field)
                        if value is not None:
                            print(f"     - {field}: {value}")
                
                # 检查school_rankings
                rankings = subject.get('school_rankings', [])
                if not rankings:
                    print(f"   [FAIL] 缺少school_rankings")
                    issues.append("缺少school_rankings")
                else:
                    print(f"   [OK] 包含 {len(rankings)} 所学校排名")
                    if rankings[0].get('school_code'):
                        print(f"     第一名: {rankings[0]['school_code']}")
                    else:
                        print(f"     [FAIL] 学校排名缺少school_code")
                        issues.append("学校排名缺少school_code")
                
                # 检查dimensions
                dimensions = subject.get('dimensions', [])
                if not dimensions:
                    print(f"   [FAIL] 缺少dimensions")
                    issues.append("缺少dimensions")
                else:
                    print(f"   [OK] 包含 {len(dimensions)} 个维度")
                    dim = dimensions[0]
                    required_dim = ['avg', 'score_rate']
                    missing_dim = [f for f in required_dim if f not in dim]
                    if missing_dim:
                        print(f"     [FAIL] 维度缺少字段: {missing_dim}")
                        issues.append(f"维度缺少: {missing_dim}")
                    else:
                        print(f"     [OK] 维度字段完整")
                
                # 检查是否有问卷
                questionnaire_found = False
                for s in subjects:
                    if s.get('type') == 'questionnaire':
                        questionnaire_found = True
                        print(f"\n   [OK] 找到问卷科目: {s.get('subject_name')}")
                        # 检查选项分布
                        if 'option_distribution' in s or any('option_distribution' in d for d in s.get('dimensions', [])):
                            print(f"     [OK] 包含选项分布")
                        else:
                            print(f"     [FAIL] 缺少选项分布")
                            issues.append("问卷缺少选项分布")
                        break
                
                if not questionnaire_found:
                    print(f"   [WARNING] 未找到问卷科目（可能该批次无问卷）")
    
    # 3. 检查学校层数据质量
    print("\n3. 学校层数据质量检查:")
    query = text("""
        SELECT statistics_data
        FROM statistical_aggregations
        WHERE batch_code = :batch_code
        AND aggregation_level = 'SCHOOL'
        LIMIT 1
    """)
    result = db.execute(query, {'batch_code': batch_code}).fetchone()
    
    if result:
        data = result[0] if isinstance(result[0], dict) else json.loads(result[0])
        
        if 'subjects' in data and data['subjects']:
            subject = data['subjects'][0]
            
            # 检查region_rank和total_schools
            if 'region_rank' not in subject:
                print(f"   [FAIL] 缺少region_rank")
                issues.append("学校层缺少region_rank")
            else:
                print(f"   [OK] region_rank: {subject['region_rank']}")
            
            if 'total_schools' not in subject:
                print(f"   [FAIL] 缺少total_schools")
                issues.append("学校层缺少total_schools")
            else:
                print(f"   [OK] total_schools: {subject['total_schools']}")
            
            # 检查维度排名
            dimensions = subject.get('dimensions', [])
            if dimensions and 'rank' in dimensions[0]:
                print(f"   [OK] 维度包含rank字段")
            else:
                print(f"   [FAIL] 维度缺少rank字段")
                issues.append("维度缺少rank字段")
    
    # 4. 检查数据精度
    print("\n4. 数据精度检查:")
    query = text("""
        SELECT statistics_data
        FROM statistical_aggregations
        WHERE batch_code = :batch_code
        LIMIT 1
    """)
    result = db.execute(query, {'batch_code': batch_code}).fetchone()
    
    if result:
        data_str = str(result[0])[:2000]  # 只检查前2000字符
        import re
        # 查找超过2位小数的数字
        pattern = r'\d+\.\d{3,}'
        matches = re.findall(pattern, data_str)
        if matches:
            print(f"   [FAIL] 发现超过2位小数: {matches[:5]}")
            issues.append(f"数据精度问题: {len(matches)}个超过2位小数")
        else:
            print(f"   [OK] 数据精度符合要求（2位小数）")
    
    db.close()
    
    return issues


def main():
    """生成最终验证报告"""
    print("\n" + "="*70)
    print("  汇聚模块修复实施方案 v1.2 - 最终验证报告")
    print("="*70)
    
    batches = ['G4-2025', 'G7-2025', 'G8-2025']
    all_issues = {}
    
    for batch_code in batches:
        issues = validate_batch_data(batch_code)
        all_issues[batch_code] = issues
    
    # 生成总结报告
    print("\n" + "="*70)
    print("  验证总结")
    print("="*70)
    
    total_issues = 0
    for batch_code, issues in all_issues.items():
        if issues:
            print(f"\n[FAIL] {batch_code}: {len(issues)} 个问题")
            for issue in issues:
                print(f"  - {issue}")
            total_issues += len(issues)
        else:
            print(f"\n[OK] {batch_code}: 完全符合要求")
    
    print("\n" + "="*70)
    if total_issues == 0:
        print("  [OK] 所有批次完全符合汇聚模块修复实施方案 v1.2 要求！")
    else:
        print(f"  [WARNING] 发现 {total_issues} 个问题需要修复")
    print("="*70)
    
    # 保存报告
    report = {
        'validation_time': datetime.now().isoformat(),
        'batches': all_issues,
        'total_issues': total_issues,
        'status': 'PASS' if total_issues == 0 else 'FAIL'
    }
    
    report_file = f"final_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n验证报告已保存至: {report_file}")


if __name__ == "__main__":
    from datetime import datetime
    main()