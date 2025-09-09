#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证汇聚数据存在的问题
根据PO验证报告核查数据库中的问题
"""
import json
import re
from decimal import Decimal
from sqlalchemy import create_engine, text
from collections import Counter

def check_decimal_places(value):
    """检查数值的小数位数"""
    if isinstance(value, (int, float)):
        str_val = str(value)
        if '.' in str_val:
            decimal_part = str_val.split('.')[1]
            # 去除尾部的0
            decimal_part = decimal_part.rstrip('0')
            return len(decimal_part)
    return 0

def analyze_json_precision(data, path="", precision_issues=None):
    """递归分析JSON中的数值精度问题"""
    if precision_issues is None:
        precision_issues = []
    
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = f"{path}.{key}" if path else key
            analyze_json_precision(value, new_path, precision_issues)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            new_path = f"{path}[{i}]"
            analyze_json_precision(item, new_path, precision_issues)
    elif isinstance(data, float):
        decimal_places = check_decimal_places(data)
        if decimal_places > 2:
            precision_issues.append({
                'path': path,
                'value': data,
                'decimal_places': decimal_places
            })
    
    return precision_issues

def verify_aggregation_issues():
    """核查汇聚数据问题"""
    print("="*80)
    print("汇聚数据问题核查报告")
    print("="*80)
    
    # 创建数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    
    with engine.connect() as conn:
        # 1. 检查入库覆盖情况
        print("\n1. 入库覆盖情况检查：")
        print("-"*40)
        result = conn.execute(text("""
            SELECT batch_code, aggregation_level, COUNT(*) as count
            FROM statistical_aggregations
            WHERE batch_code IN ('G4-2025','G7-2025','G8-2025')
            GROUP BY batch_code, aggregation_level
            ORDER BY batch_code, aggregation_level
        """))
        
        coverage = {}
        for row in result:
            if row.batch_code not in coverage:
                coverage[row.batch_code] = {}
            coverage[row.batch_code][row.aggregation_level] = row.count
        
        for batch in ['G4-2025', 'G7-2025', 'G8-2025']:
            if batch in coverage:
                regional = coverage[batch].get('REGIONAL', 0)
                school = coverage[batch].get('SCHOOL', 0)
                print(f"   {batch}: REGIONAL={regional}, SCHOOL={school}")
            else:
                print(f"   {batch}: 未入库")
        
        # 2. 检查G4-2025的数值精度问题
        print("\n2. 数值精度问题检查（G4-2025）：")
        print("-"*40)
        
        # 获取G4-2025区域级数据
        result = conn.execute(text("""
            SELECT statistics_data
            FROM statistical_aggregations
            WHERE batch_code = 'G4-2025' 
            AND aggregation_level = 'REGIONAL'
            LIMIT 1
        """))
        
        row = result.fetchone()
        if row and row.statistics_data:
            stats_data = json.loads(row.statistics_data)
            precision_issues = analyze_json_precision(stats_data)
            
            print(f"   发现精度问题数量: {len(precision_issues)}")
            
            # 统计问题分布
            path_patterns = Counter()
            for issue in precision_issues:
                # 提取路径模式
                path = issue['path']
                # 简化路径，忽略数组索引
                simplified_path = re.sub(r'\[\d+\]', '[]', path)
                path_patterns[simplified_path] += 1
            
            print(f"\n   精度问题分布（前10个）：")
            for path, count in path_patterns.most_common(10):
                print(f"      {path}: {count}次")
            
            # 显示几个具体例子
            print(f"\n   具体示例（前5个）：")
            for issue in precision_issues[:5]:
                print(f"      路径: {issue['path']}")
                print(f"      值: {issue['value']}")
                print(f"      小数位: {issue['decimal_places']}")
        
        # 3. 检查学校排名字段是否存在
        print("\n3. 学校排名字段检查：")
        print("-"*40)
        
        if row and row.statistics_data:
            stats_data = json.loads(row.statistics_data)
            
            # 检查是否有school_rankings字段
            has_school_rankings = False
            if 'subjects' in stats_data:
                for subject in stats_data.get('subjects', []):
                    if 'school_rankings' in subject:
                        has_school_rankings = True
                        break
            
            print(f"   区域级school_rankings字段: {'存在' if has_school_rankings else '缺失'}")
            
            # 检查学校级的region_rank
            result = conn.execute(text("""
                SELECT statistics_data
                FROM statistical_aggregations
                WHERE batch_code = 'G4-2025' 
                AND aggregation_level = 'SCHOOL'
                LIMIT 1
            """))
            
            school_row = result.fetchone()
            if school_row and school_row.statistics_data:
                school_stats = json.loads(school_row.statistics_data)
                has_region_rank = False
                has_total_schools = False
                
                if 'subjects' in school_stats:
                    for subject in school_stats.get('subjects', []):
                        if 'region_rank' in subject:
                            has_region_rank = True
                        if 'total_schools' in subject:
                            has_total_schools = True
                
                print(f"   学校级region_rank字段: {'存在' if has_region_rank else '缺失'}")
                print(f"   学校级total_schools字段: {'存在' if has_total_schools else '缺失'}")
        
        # 4. 检查维度排名字段
        print("\n4. 维度排名字段检查：")
        print("-"*40)
        
        if row and row.statistics_data:
            stats_data = json.loads(row.statistics_data)
            has_dimension_rank = False
            
            if 'subjects' in stats_data:
                for subject in stats_data.get('subjects', []):
                    if 'dimensions' in subject:
                        for dimension in subject['dimensions']:
                            if 'rank' in dimension:
                                has_dimension_rank = True
                                break
            
            print(f"   维度rank字段: {'存在' if has_dimension_rank else '缺失'}")
        
        # 5. 检查问卷数据结构
        print("\n5. 问卷数据结构检查：")
        print("-"*40)
        
        if row and row.statistics_data:
            stats_data = json.loads(row.statistics_data)
            
            # 检查是否有non_academic_subjects
            has_non_academic = 'non_academic_subjects' in stats_data
            print(f"   non_academic_subjects字段: {'存在（不符合契约）' if has_non_academic else '不存在'}")
            
            # 检查问卷是否在subjects中
            questionnaire_in_subjects = False
            if 'subjects' in stats_data:
                for subject in stats_data.get('subjects', []):
                    if subject.get('subject_name') == '问卷' or subject.get('type') == 'questionnaire':
                        questionnaire_in_subjects = True
                        
                        # 检查是否有选项占比
                        has_option_distribution = False
                        if 'dimensions' in subject:
                            for dim in subject['dimensions']:
                                if 'option_distribution' in dim:
                                    has_option_distribution = True
                                    break
                        
                        print(f"   问卷在subjects中: 是")
                        print(f"   维度选项占比字段: {'存在' if has_option_distribution else '缺失'}")
                        break
            
            if not questionnaire_in_subjects:
                print(f"   问卷在subjects中: 否（不符合契约）")
        
        # 6. 检查字段命名规范
        print("\n6. 字段命名规范检查：")
        print("-"*40)
        
        if row and row.statistics_data:
            stats_data = json.loads(row.statistics_data)
            
            # 检查科目层metrics结构
            has_metrics = False
            if 'subjects' in stats_data:
                for subject in stats_data.get('subjects', []):
                    if 'metrics' in subject:
                        has_metrics = True
                        metrics = subject['metrics']
                        expected_fields = ['avg', 'stddev', 'max', 'min', 'difficulty', 
                                         'discrimination', 'p10', 'p50', 'p90']
                        missing_fields = [f for f in expected_fields if f not in metrics]
                        if missing_fields:
                            print(f"   metrics缺失字段: {', '.join(missing_fields)}")
                        break
            
            if not has_metrics:
                print(f"   metrics字段结构: 缺失或不规范")
        
        print("\n" + "="*80)
        print("核查完成")
        print("="*80)

if __name__ == "__main__":
    verify_aggregation_issues()