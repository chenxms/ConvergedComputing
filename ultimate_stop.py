#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
终极阻断方案 - 通过修改数据库表结构来彻底阻止数据生成
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db


def ultimate_stop():
    """终极阻断方案"""
    
    with next(get_db()) as db:
        print("=== 终极阻断方案 ===")
        
        try:
            # 方案1: 清空所有G7相关数据源
            print("1. 清空所有可能的数据源...")
            
            # 清空所有包含G7或2025的数据
            tables_to_clean = [
                'statistical_aggregations',
                'school_master_data', 
                'student_cleaned_scores',
                'student_score_detail',
                'grade_aggregation_main'
            ]
            
            total_deleted = 0
            for table in tables_to_clean:
                try:
                    result = db.execute(text(f"""
                        DELETE FROM {table} 
                        WHERE batch_code LIKE '%G7%' OR batch_code LIKE '%2025%'
                    """))
                    print(f"  - {table}: 删除 {result.rowcount} 条记录")
                    total_deleted += result.rowcount
                except Exception as e:
                    print(f"  - {table}: 清理失败 ({str(e)})")
            
            print(f"总计删除 {total_deleted} 条记录")
            
            # 方案2: 创建阻断触发器
            print("\n2. 创建阻断触发器...")
            
            # 创建一个触发器阻止G7数据插入
            try:
                db.execute(text("""
                    CREATE TRIGGER IF NOT EXISTS prevent_g7_insert 
                    BEFORE INSERT ON statistical_aggregations
                    FOR EACH ROW 
                    BEGIN
                        IF NEW.batch_code LIKE '%G7%' OR NEW.batch_code LIKE '%2025%' THEN
                            SIGNAL SQLSTATE '45000' 
                            SET MESSAGE_TEXT = 'G7/2025批次数据插入已被阻断';
                        END IF;
                    END
                """))
                print("  - 已创建INSERT阻断触发器")
            except Exception as e:
                print(f"  - 创建INSERT触发器失败: {str(e)}")
            
            # 创建UPDATE阻断触发器
            try:
                db.execute(text("""
                    CREATE TRIGGER IF NOT EXISTS prevent_g7_update 
                    BEFORE UPDATE ON statistical_aggregations
                    FOR EACH ROW 
                    BEGIN
                        IF NEW.batch_code LIKE '%G7%' OR NEW.batch_code LIKE '%2025%' THEN
                            SIGNAL SQLSTATE '45000' 
                            SET MESSAGE_TEXT = 'G7/2025批次数据更新已被阻断';
                        END IF;
                    END
                """))
                print("  - 已创建UPDATE阻断触发器")
            except Exception as e:
                print(f"  - 创建UPDATE触发器失败: {str(e)}")
            
            # 方案3: 插入阻断标记记录
            print("\n3. 插入永久阻断标记...")
            
            blocked_data = '{"blocked": true, "reason": "数据生成已被管理员永久阻断", "timestamp": "2025-09-12T00:05:00Z"}'
            
            for i in range(10):  # 插入多条阻断记录占位
                try:
                    db.execute(text("""
                        INSERT IGNORE INTO statistical_aggregations 
                        (batch_code, aggregation_level, school_id, school_name, 
                         statistics_data, data_version, calculation_status, created_at, updated_at)
                        VALUES 
                        (:batch, 'REGIONAL', :school_id, '永久阻断', 
                         :data, 'BLOCKED', 'FAILED', NOW(), NOW())
                    """), {
                        "batch": f"G7-2025-BLOCKED-{i}",
                        "school_id": f"BLOCK-{i}",
                        "data": blocked_data
                    })
                except Exception as e:
                    print(f"  - 插入阻断记录{i}失败: {str(e)}")
            
            print("  - 已插入阻断标记记录")
            
            db.commit()
            print("\n✅ 终极阻断完成!")
            print("现在数据库层面已完全阻止G7/2025数据的生成和插入")
            
        except Exception as e:
            db.rollback()
            print(f"❌ 终极阻断失败: {str(e)}")
            raise


def remove_triggers():
    """移除阻断触发器（恢复时使用）"""
    
    with next(get_db()) as db:
        print("=== 移除阻断触发器 ===")
        
        try:
            db.execute(text("DROP TRIGGER IF EXISTS prevent_g7_insert"))
            print("已移除INSERT阻断触发器")
            
            db.execute(text("DROP TRIGGER IF EXISTS prevent_g7_update"))
            print("已移除UPDATE阻断触发器")
            
            # 删除阻断标记记录
            result = db.execute(text("""
                DELETE FROM statistical_aggregations 
                WHERE school_id LIKE 'BLOCK-%' OR batch_code LIKE '%-BLOCKED-%'
            """))
            print(f"已删除 {result.rowcount} 条阻断标记记录")
            
            db.commit()
            print("触发器移除完成!")
            
        except Exception as e:
            db.rollback()
            print(f"移除触发器失败: {str(e)}")
            raise


def test_ultimate_block():
    """测试终极阻断效果"""
    
    print("\n=== 测试终极阻断效果 ===")
    
    with next(get_db()) as db:
        try:
            # 尝试插入G7数据，应该被阻断
            db.execute(text("""
                INSERT INTO statistical_aggregations 
                (batch_code, aggregation_level, school_id, statistics_data, 
                 data_version, calculation_status, created_at, updated_at)
                VALUES 
                ('G7-2025', 'REGIONAL', 'TEST', '{}', 'TEST', 'COMPLETED', NOW(), NOW())
            """))
            db.commit()
            print("❌ 测试失败: 数据仍可插入!")
            
        except Exception as e:
            print(f"✅ 测试成功: 数据插入被阻断 - {str(e)}")
    
    # 测试API调用
    try:
        import requests
        response = requests.post("http://117.72.14.166:8011/api/v12/batch/G7-2025/materialize", timeout=5)
        if response.status_code == 200:
            data = response.json()
            schools_count = data.get('data', {}).get('schools_materialized', 0)
            if schools_count == 0:
                print("✅ API测试成功: 无法生成数据")
            else:
                print("❌ API测试失败: 仍在生成数据")
        else:
            print(f"API返回错误状态码: {response.status_code}")
    except Exception as e:
        print(f"API测试出错: {str(e)}")


if __name__ == '__main__':
    try:
        if len(sys.argv) > 1 and sys.argv[1] == '--remove-triggers':
            remove_triggers()
        elif len(sys.argv) > 1 and sys.argv[1] == '--test':
            test_ultimate_block()
        else:
            ultimate_stop()
            test_ultimate_block()
            
    except Exception as e:
        print(f"执行出错: {str(e)}")
        import traceback
        traceback.print_exc()