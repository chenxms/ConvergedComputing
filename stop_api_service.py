#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
阻断API服务的数据生成功能
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db


def disable_api_data_generation():
    """通过修改API依赖的数据来阻止API生成数据"""
    
    with next(get_db()) as db:
        print("=== 阻断API数据生成 ===")
        
        try:
            # 方法1: 重命名所有G7相关的基础数据，API找不到数据就无法生成
            print("1. 重命名基础数据源...")
            
            # 重命名school_master_data中的G7批次
            result1 = db.execute(text("""
                UPDATE school_master_data 
                SET batch_code = 'G7-2025-API-BLOCKED' 
                WHERE batch_code IN ('G7-2025', 'G7-2025-STOPPED', 'G7-2025-DISABLED')
            """))
            print(f"  - 已重命名school_master_data: {result1.rowcount}")
            
            # 重命名student_cleaned_scores中的G7批次  
            result2 = db.execute(text("""
                UPDATE student_cleaned_scores 
                SET batch_code = 'G7-2025-API-BLOCKED'
                WHERE batch_code IN ('G7-2025', 'G7-2025-STOPPED', 'G7-2025-DISABLED')
            """))
            print(f"  - 已重命名student_cleaned_scores: {result2.rowcount}")
            
            # 删除所有相关的汇聚数据
            result3 = db.execute(text("""
                DELETE FROM statistical_aggregations 
                WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
            """))
            print(f"  - 已删除所有G7/2025汇聚数据: {result3.rowcount}")
            
            # 方法2: 创建一个API阻断标记
            result4 = db.execute(text("""
                INSERT INTO statistical_aggregations 
                (batch_code, aggregation_level, school_id, school_name, statistics_data, 
                 data_version, calculation_status, created_at, updated_at)
                VALUES 
                ('G7-2025', 'REGIONAL', 'API-BLOCKED', 'API已阻断', 
                 '{"error": "API数据生成已被阻断", "blocked_at": "2025-09-12T00:02:00Z"}',
                 'BLOCKED', 'FAILED', NOW(), NOW())
                ON DUPLICATE KEY UPDATE 
                    statistics_data = VALUES(statistics_data),
                    calculation_status = 'FAILED',
                    updated_at = NOW()
            """))
            print(f"  - 已创建API阻断标记")
            
            db.commit()
            print("API数据生成已被阻断!")
            print("现在API调用将无法找到基础数据，无法重新生成汇聚数据")
            
        except Exception as e:
            db.rollback()
            print(f"阻断失败: {str(e)}")
            raise


def test_api_blocked():
    """测试API是否被成功阻断"""
    
    import requests
    
    print("\n=== 测试API阻断效果 ===")
    
    try:
        # 测试materialize端点
        response = requests.post("http://117.72.14.166:8011/api/v12/batch/G7-2025/materialize", timeout=10)
        print(f"Materialize API响应: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"  - 消息: {data.get('message', 'N/A')}")
            if data.get('data'):
                schools_count = data['data'].get('schools_materialized', 0)
                print(f"  - 学校数量: {schools_count}")
                if schools_count == 0:
                    print("  ✅ API无法找到数据，阻断成功!")
                else:
                    print("  ❌ API仍在生成数据!")
        
        # 测试regional端点
        response2 = requests.get("http://117.72.14.166:8011/api/v12/batch/G7-2025/regional", timeout=10)
        print(f"Regional API响应: {response2.status_code}")
        
    except Exception as e:
        print(f"测试API时出错: {str(e)}")


def restore_data_sources():
    """恢复数据源（如需要的话）"""
    
    with next(get_db()) as db:
        print("=== 恢复数据源 ===")
        
        try:
            # 恢复school_master_data
            result1 = db.execute(text("""
                UPDATE school_master_data 
                SET batch_code = 'G7-2025' 
                WHERE batch_code = 'G7-2025-API-BLOCKED'
            """))
            print(f"已恢复school_master_data: {result1.rowcount}")
            
            # 恢复student_cleaned_scores
            result2 = db.execute(text("""
                UPDATE student_cleaned_scores 
                SET batch_code = 'G7-2025' 
                WHERE batch_code = 'G7-2025-API-BLOCKED'
            """))
            print(f"已恢复student_cleaned_scores: {result2.rowcount}")
            
            # 删除阻断标记
            result3 = db.execute(text("""
                DELETE FROM statistical_aggregations 
                WHERE school_id = 'API-BLOCKED'
            """))
            print(f"已删除阻断标记: {result3.rowcount}")
            
            db.commit()
            print("数据源已恢复!")
            
        except Exception as e:
            db.rollback()
            print(f"恢复失败: {str(e)}")
            raise


if __name__ == '__main__':
    try:
        if len(sys.argv) > 1 and sys.argv[1] == '--restore':
            restore_data_sources()
        elif len(sys.argv) > 1 and sys.argv[1] == '--test':
            test_api_blocked()
        else:
            disable_api_data_generation()
            test_api_blocked()
            
    except Exception as e:
        print(f"执行出错: {str(e)}")
        import traceback
        traceback.print_exc()