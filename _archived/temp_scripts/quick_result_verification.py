#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速验证G7-2025计算结果
"""
import os
import sys
sys.path.insert(0, os.getcwd())

from app.database.connection import get_db
from sqlalchemy import text
import json


def quick_result_verification():
    """快速验证计算结果"""
    print("G7-2025 快速结果验证")
    print("=" * 50)
    
    db = next(get_db())
    batch_code = "G7-2025"
    
    try:
        # 检查statistical_aggregations表
        result = db.execute(text("SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = :batch_code"), 
                           {'batch_code': batch_code})
        stats_count = result.fetchone()[0]
        print(f"统计记录总数: {stats_count}")
        
        if stats_count > 0:
            # 区域级数据
            result = db.execute(text("""
                SELECT COUNT(*) FROM statistical_aggregations 
                WHERE batch_code = :batch_code AND aggregation_level = 'REGIONAL'
            """), {'batch_code': batch_code})
            regional_count = result.fetchone()[0]
            print(f"区域级统计: {regional_count}条")
            
            # 学校级数据
            result = db.execute(text("""
                SELECT COUNT(*) FROM statistical_aggregations 
                WHERE batch_code = :batch_code AND aggregation_level = 'SCHOOL'
            """), {'batch_code': batch_code})
            school_count = result.fetchone()[0]
            print(f"学校级统计: {school_count}条")
            
            # 查看最新的区域级数据内容
            if regional_count > 0:
                result = db.execute(text("""
                    SELECT statistics_data FROM statistical_aggregations 
                    WHERE batch_code = :batch_code AND aggregation_level = 'REGIONAL'
                    ORDER BY created_at DESC
                    LIMIT 1
                """), {'batch_code': batch_code})
                
                data_row = result.fetchone()
                if data_row and data_row.statistics_data:
                    try:
                        stats_data = json.loads(data_row.statistics_data) if isinstance(data_row.statistics_data, str) else data_row.statistics_data
                        if 'academic_subjects' in stats_data:
                            subjects = stats_data['academic_subjects']
                            print(f"区域级包含科目: {len(subjects)}个")
                            
                            for subject_name, subject_data in list(subjects.items())[:3]:  # 显示前3个科目
                                if 'school_stats' in subject_data:
                                    stats = subject_data['school_stats']
                                    avg_score = stats.get('avg_score', 0)
                                    student_count = stats.get('student_count', 0)
                                    
                                    # 检查百分位数
                                    percentiles = subject_data.get('percentiles', {})
                                    p10 = percentiles.get('P10', 0)
                                    p50 = percentiles.get('P50', 0)
                                    p90 = percentiles.get('P90', 0)
                                    
                                    print(f"  {subject_name}: 平均{avg_score:.1f}, {student_count}学生, P10/P50/P90: {p10:.1f}/{p50:.1f}/{p90:.1f}")
                    except Exception as e:
                        print(f"数据解析错误: {e}")
            
            print("\n状态评估:")
            if stats_count >= 44:  # 1区域级 + 43学校级
                print("SUCCESS: 计算已完成!")
            elif stats_count > 0:
                print(f"PROGRESS: 计算进行中 ({stats_count}/44)")
            else:
                print("WAITING: 等待计算开始...")
        else:
            print("WAITING: 等待计算开始...")
            
    except Exception as e:
        print(f"验证失败: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    quick_result_verification()