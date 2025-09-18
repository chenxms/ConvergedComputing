#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化的G4批次汇聚重建脚本
"""

import os
import sys
import time
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.database.connection import get_db
from app.database.repositories import StatisticalAggregationRepository
from app.database.enums import AggregationLevel as DBAggregationLevel, CalculationStatus
from app.services.subjects_builder import SubjectsBuilder
from app.utils.precision import round2_json
from sqlalchemy import text


def simple_rebuild_g4():
    """简化的G4批次重建"""
    batch_code = 'G4-2025'
    print(f"=== 开始重建 {batch_code} 批次汇聚数据 ===")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    db = next(get_db())
    try:
        repo = StatisticalAggregationRepository(db)
        builder = SubjectsBuilder()
        
        # 1. 清理已有数据
        print("1. 清理已有汇聚数据...")
        db.execute(text("DELETE FROM statistical_aggregations WHERE batch_code = :batch_code"), 
                  {"batch_code": batch_code})
        db.commit()
        print("已清理旧数据")
        
        # 2. 构建区域级数据
        print("2. 构建区域级统计数据...")
        regional_subjects = builder.build_regional_subjects(batch_code)
        regional_payload = {
            'schema_version': 'v1.2',
            'batch_code': batch_code,
            'aggregation_level': 'REGIONAL',
            'subjects': regional_subjects,
        }
        
        # 获取总学校数
        total_schools = db.execute(text("""
            SELECT COUNT(*) 
            FROM school_master_data 
            WHERE batch_code = :batch_code AND status = 'ACTIVE'
        """), {"batch_code": batch_code}).scalar() or 0
        
        repo.upsert_statistics({
            'batch_code': batch_code,
            'aggregation_level': DBAggregationLevel.REGIONAL,
            'school_id': None,
            'school_name': '区域汇总',
            'statistics_data': round2_json(regional_payload),
            'calculation_status': CalculationStatus.COMPLETED,
            'total_schools': total_schools,
        })
        print("区域级数据已生成")
        
        # 3. 构建学校级数据
        print("3. 构建学校级统计数据...")
        school_rows = db.execute(text("""
            SELECT school_id, standard_school_name 
            FROM school_master_data 
            WHERE batch_code = :batch_code AND status = 'ACTIVE' 
            ORDER BY school_id
            LIMIT 10
        """), {"batch_code": batch_code}).fetchall()
        
        print(f"处理 {len(school_rows)} 所学校...")
        successful = 0
        
        for i, school_row in enumerate(school_rows):
            school_id = school_row[0]
            school_name = school_row[1]
            
            try:
                # 检查学生数据
                student_count = db.execute(text("""
                    SELECT COUNT(DISTINCT student_id)
                    FROM student_cleaned_scores
                    WHERE batch_code = :batch_code AND school_code = :school_id
                """), {"batch_code": batch_code, "school_id": school_id}).scalar() or 0
                
                if student_count == 0:
                    print(f"跳过 {school_id}: 无学生数据")
                    continue
                
                # 构建学校级数据
                school_subjects = builder.build_school_subjects(batch_code, school_id)
                school_payload = {
                    'schema_version': 'v1.2',
                    'batch_code': batch_code,
                    'aggregation_level': 'SCHOOL',
                    'school_code': school_id,
                    'subjects': school_subjects,
                }
                
                repo.upsert_statistics({
                    'batch_code': batch_code,
                    'aggregation_level': DBAggregationLevel.SCHOOL,
                    'school_id': school_id,
                    'school_name': school_name,
                    'statistics_data': round2_json(school_payload),
                    'calculation_status': CalculationStatus.COMPLETED,
                    'total_students': student_count,
                    'total_schools': total_schools,
                })
                
                successful += 1
                print(f"完成 {successful}/{len(school_rows)}: {school_id}({school_name})")
                
            except Exception as e:
                print(f"学校 {school_id} 失败: {str(e)}")
        
        # 4. 验证结果
        print("4. 验证结果...")
        result = db.execute(text("""
            SELECT 
                aggregation_level,
                COUNT(*) as count,
                COUNT(CASE WHEN school_name IS NOT NULL THEN 1 END) as with_names
            FROM statistical_aggregations 
            WHERE batch_code = :batch_code
            GROUP BY aggregation_level
        """), {"batch_code": batch_code}).fetchall()
        
        print("汇聚结果:")
        for row in result:
            print(f"  {row[0]}: {row[1]}条记录, {row[2]}有名称")
        
        print(f"\n完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("G4批次汇聚重建完成!")
        
    except Exception as e:
        print(f"重建失败: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    simple_rebuild_g4()