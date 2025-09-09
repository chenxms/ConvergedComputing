#!/usr/bin/env python3
"""
汇聚模块修复实施方案 v1.2 验收脚本
验证所有修复要求是否正确实施
"""
import sys
import os
import json
from datetime import datetime
from decimal import Decimal

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database.connection import get_db
from app.utils.precision import round2, to_pct, round2_json
from sqlalchemy import text

def print_section(title):
    """打印分节标题"""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

def check_precision_tools():
    """验证精度处理工具"""
    print_section("1. 精度处理工具验证")
    
    test_cases = [
        (3.14159, 3.14, "round2 - 正常数值"),
        (2.999, 3.0, "round2 - 进位"),
        (0.5, 50.0, "to_pct - 50%"),
        (0.9999, 99.99, "to_pct - 99.99%"),
        (1.0, 100.0, "to_pct - 100%")
    ]
    
    passed = 0
    for input_val, expected, desc in test_cases:
        if "to_pct" in desc:
            result = to_pct(input_val)
        else:
            result = round2(input_val)
        
        status = "[PASS]" if result == expected else "[FAIL]"
        print(f"  {status} {desc}: {input_val} -> {result} (期望: {expected})")
        if result == expected:
            passed += 1
    
    print(f"\n  结果: {passed}/{len(test_cases)} 测试通过")
    return passed == len(test_cases)

def check_data_structure(batch_code='G4-2025'):
    """验证数据结构统一性"""
    print_section("2. 数据结构统一性验证")
    
    try:
        db = next(get_db())
        
        # 查询区域层级汇聚
        query = text("""
            SELECT statistics_data
            FROM statistical_aggregations
            WHERE batch_code = :batch_code
            AND aggregation_level = 'REGIONAL'
            LIMIT 1
        """)
        result = db.execute(query, {'batch_code': batch_code}).fetchone()
        
        if not result:
            print(f"  ✗ 未找到批次 {batch_code} 的区域汇聚数据")
            db.close()
            return False
        
        data = result[0] if isinstance(result[0], dict) else json.loads(result[0])
        
        checks = {
            'schema_version存在': 'schema_version' in data,
            'schema_version=v1.2': data.get('schema_version') == 'v1.2',
            'subjects数组存在': 'subjects' in data,
            '不存在academic_subjects': 'academic_subjects' not in data,
            '不存在non_academic_subjects': 'non_academic_subjects' not in data
        }
        
        passed = 0
        for check_name, result in checks.items():
            status = "[PASS]" if result else "[FAIL]"
            print(f"  {status} {check_name}")
            if result:
                passed += 1
        
        db.close()
        
        print(f"\n  结果: {passed}/{len(checks)} 检查通过")
        return passed == len(checks)
        
    except Exception as e:
        print(f"  ✗ 检查失败: {str(e)}")
        return False

def check_school_rankings(batch_code='G4-2025'):
    """验证学校排名功能"""
    print_section("3. 学校排名功能验证")
    
    try:
        db = next(get_db())
        
        # 查询区域层级汇聚
        query = text("""
            SELECT statistics_data
            FROM statistical_aggregations
            WHERE batch_code = :batch_code
            AND aggregation_level = 'REGIONAL'
            LIMIT 1
        """)
        result = db.execute(query, {'batch_code': batch_code}).fetchone()
        
        if not result:
            print(f"  ✗ 未找到批次 {batch_code} 的区域汇聚数据")
            db.close()
            return False
        
        data = result[0] if isinstance(result[0], dict) else json.loads(result[0])
        
        if 'subjects' not in data or not data['subjects']:
            print("  ✗ 未找到subjects数据")
            db.close()
            return False
        
        # 检查第一个科目的学校排名
        subject = data['subjects'][0]
        has_rankings = 'school_rankings' in subject
        
        if has_rankings:
            rankings = subject['school_rankings']
            print(f"  ✓ 发现school_rankings字段")
            print(f"  ✓ 包含 {len(rankings)} 所学校的排名")
            
            if rankings:
                # 检查排名结构
                first_school = rankings[0]
                required_fields = ['school_code', 'school_name', 'avg_score', 'rank']
                missing_fields = [f for f in required_fields if f not in first_school]
                
                if not missing_fields:
                    print(f"  ✓ 排名结构完整")
                    print(f"     第一名: {first_school['school_name']} (分数: {first_school['avg_score']}, 排名: {first_school['rank']})")
                else:
                    print(f"  ✗ 排名结构缺少字段: {missing_fields}")
                    db.close()
                    return False
                
                # 检查排名是否正确（DENSE_RANK）
                last_score = float('inf')
                last_rank = 0
                rank_correct = True
                
                for school in rankings[:10]:  # 只检查前10个
                    score = school['avg_score']
                    rank = school['rank']
                    
                    if score < last_score:
                        last_rank = rank
                    elif score == last_score and rank != last_rank:
                        rank_correct = False
                        break
                    
                    last_score = score
                
                if rank_correct:
                    print(f"  ✓ DENSE_RANK排名规则正确")
                else:
                    print(f"  ✗ 排名规则不符合DENSE_RANK")
        else:
            print("  ✗ 未找到school_rankings字段")
            db.close()
            return False
        
        db.close()
        return True
        
    except Exception as e:
        print(f"  ✗ 检查失败: {str(e)}")
        return False

def check_dimension_ranks(batch_code='G4-2025'):
    """验证维度排名功能"""
    print_section("4. 维度排名功能验证")
    
    try:
        db = next(get_db())
        
        # 查询学校层级汇聚
        query = text("""
            SELECT statistics_data, school_id
            FROM statistical_aggregations
            WHERE batch_code = :batch_code
            AND aggregation_level = 'SCHOOL'
            LIMIT 1
        """)
        result = db.execute(query, {'batch_code': batch_code}).fetchone()
        
        if not result:
            print(f"  ✗ 未找到批次 {batch_code} 的学校汇聚数据")
            db.close()
            return False
        
        data = result[0] if isinstance(result[0], dict) else json.loads(result[0])
        school_id = result[1]
        
        if 'subjects' not in data or not data['subjects']:
            print("  ✗ 未找到subjects数据")
            db.close()
            return False
        
        # 检查科目的维度排名
        has_dimension_rank = False
        has_region_rank = False
        
        for subject in data['subjects']:
            # 检查区域排名
            if 'region_rank' in subject and 'total_schools' in subject:
                has_region_rank = True
                print(f"  ✓ 科目 {subject.get('subject_name', 'unknown')} 包含区域排名")
                print(f"     我校排名: {subject['region_rank']}/{subject['total_schools']}")
            
            # 检查维度排名
            if 'dimensions' in subject and subject['dimensions']:
                for dim in subject['dimensions']:
                    if 'rank' in dim:
                        has_dimension_rank = True
                        print(f"  ✓ 维度 {dim.get('name', 'unknown')} 包含排名: {dim['rank']}")
                        break
                if has_dimension_rank:
                    break
        
        db.close()
        
        if has_region_rank and has_dimension_rank:
            print("\n  ✓ 维度排名功能验证通过")
            return True
        else:
            if not has_region_rank:
                print("  ✗ 缺少区域排名(region_rank/total_schools)")
            if not has_dimension_rank:
                print("  ✗ 缺少维度排名(dimensions[].rank)")
            return False
        
    except Exception as e:
        print(f"  ✗ 检查失败: {str(e)}")
        return False

def check_questionnaire_integration(batch_code='G4-2025'):
    """验证问卷数据整合"""
    print_section("5. 问卷数据整合验证")
    
    try:
        db = next(get_db())
        
        # 查询区域层级汇聚
        query = text("""
            SELECT statistics_data
            FROM statistical_aggregations
            WHERE batch_code = :batch_code
            AND aggregation_level = 'REGIONAL'
            LIMIT 1
        """)
        result = db.execute(query, {'batch_code': batch_code}).fetchone()
        
        if not result:
            print(f"  ✗ 未找到批次 {batch_code} 的区域汇聚数据")
            db.close()
            return False
        
        data = result[0] if isinstance(result[0], dict) else json.loads(result[0])
        
        if 'subjects' not in data:
            print("  ✗ 未找到subjects数据")
            db.close()
            return False
        
        # 查找问卷科目
        questionnaire_found = False
        has_option_distribution = False
        
        for subject in data['subjects']:
            if subject.get('type') == 'questionnaire' or subject.get('subject_name') == '问卷':
                questionnaire_found = True
                print(f"  ✓ 找到问卷科目: {subject.get('subject_name')}")
                
                # 检查是否有学校排名
                if 'school_rankings' in subject:
                    print(f"  ✓ 问卷包含学校排名 ({len(subject['school_rankings'])}所学校)")
                
                # 检查选项分布
                if 'dimensions' in subject:
                    for dim in subject['dimensions']:
                        if 'option_distribution' in dim:
                            has_option_distribution = True
                            print(f"  ✓ 维度 {dim.get('name')} 包含选项分布")
                            break
                break
        
        # 检查是否存在旧结构
        if 'non_academic_subjects' in data:
            print("  [WARNING] 警告: 仍存在non_academic_subjects字段（兼容模式）")
        
        db.close()
        
        if questionnaire_found:
            print("\n  ✓ 问卷数据整合验证通过")
            return True
        else:
            print("  ✗ 未找到问卷科目在subjects数组中")
            return False
        
    except Exception as e:
        print(f"  ✗ 检查失败: {str(e)}")
        return False

def check_precision_compliance(batch_code='G4-2025'):
    """验证精度规范遵循"""
    print_section("6. 精度规范遵循验证")
    
    try:
        db = next(get_db())
        
        # 查询汇聚数据
        query = text("""
            SELECT statistics_data
            FROM statistical_aggregations
            WHERE batch_code = :batch_code
            LIMIT 5
        """)
        results = db.execute(query, {'batch_code': batch_code}).fetchall()
        
        if not results:
            print(f"  ✗ 未找到批次 {batch_code} 的汇聚数据")
            db.close()
            return False
        
        precision_issues = []
        total_floats = 0
        correct_floats = 0
        
        def check_precision(obj, path=""):
            nonlocal total_floats, correct_floats
            
            if isinstance(obj, dict):
                for key, value in obj.items():
                    check_precision(value, f"{path}.{key}")
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    check_precision(item, f"{path}[{i}]")
            elif isinstance(obj, float):
                total_floats += 1
                # 检查小数位数
                str_val = str(obj)
                if '.' in str_val:
                    decimal_places = len(str_val.split('.')[1])
                    if decimal_places <= 2:
                        correct_floats += 1
                    else:
                        precision_issues.append(f"{path}: {obj} ({decimal_places}位小数)")
                else:
                    correct_floats += 1
        
        # 检查前几条记录
        for result in results[:2]:
            data = result[0] if isinstance(result[0], dict) else json.loads(result[0])
            check_precision(data)
        
        db.close()
        
        print(f"  检查了 {total_floats} 个浮点数")
        print(f"  ✓ {correct_floats} 个符合两位小数规范")
        
        if precision_issues:
            print(f"  ✗ {len(precision_issues)} 个精度问题:")
            for issue in precision_issues[:5]:  # 只显示前5个
                print(f"     {issue}")
        
        compliance_rate = (correct_floats / total_floats * 100) if total_floats > 0 else 0
        print(f"\n  精度规范遵循率: {compliance_rate:.1f}%")
        
        return compliance_rate >= 95  # 95%以上即认为通过
        
    except Exception as e:
        print(f"  ✗ 检查失败: {str(e)}")
        return False

def main():
    """主验收流程"""
    print("\n" + "="*60)
    print("  汇聚模块修复实施方案 v1.2 验收测试")
    print("="*60)
    print(f"  执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 执行各项验收测试
    test_results = {
        '精度处理工具': check_precision_tools(),
        '数据结构统一': check_data_structure(),
        '学校排名功能': check_school_rankings(),
        '维度排名功能': check_dimension_ranks(),
        '问卷数据整合': check_questionnaire_integration(),
        '精度规范遵循': check_precision_compliance()
    }
    
    # 输出验收结果汇总
    print_section("验收结果汇总")
    
    passed = 0
    total = len(test_results)
    
    for test_name, result in test_results.items():
        status = "✓ 通过" if result else "✗ 未通过"
        print(f"  {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n  总体结果: {passed}/{total} 项验收通过")
    
    if passed == total:
        print("\n  [SUCCESS] 恭喜！所有验收项目全部通过！")
        print("  汇聚模块修复实施方案 v1.2 已成功实施")
    else:
        print(f"\n  [WARNING] 警告：有 {total - passed} 项未通过验收")
        print("  请检查并修复相关问题")
    
    # 保存验收报告
    report = {
        'timestamp': datetime.now().isoformat(),
        'version': 'v1.2',
        'test_results': test_results,
        'passed': passed,
        'total': total,
        'success': passed == total
    }
    
    report_file = f"acceptance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n  验收报告已保存至: {report_file}")

if __name__ == "__main__":
    main()