#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据库层面彻底阻断G7-2025数据生成
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db


def database_level_block():
    """数据库层面阻断"""
    
    with next(get_db()) as db:
        print("=== 数据库层面彻底阻断G7-2025 ===")
        
        try:
            # 1. 重命名表（暂时阻断写入）
            print("1. 临时重命名statistical_aggregations表...")
            db.execute(text("RENAME TABLE statistical_aggregations TO statistical_aggregations_temp"))
            
            # 2. 创建一个新的同名表，但是只允许非G7数据
            print("2. 创建新的restricted表...")
            db.execute(text("""
                CREATE TABLE statistical_aggregations LIKE statistical_aggregations_temp
            """))
            
            # 3. 将所有非G7数据迁移回来
            print("3. 迁移非G7数据...")
            result = db.execute(text("""
                INSERT INTO statistical_aggregations 
                SELECT * FROM statistical_aggregations_temp 
                WHERE batch_code NOT LIKE '%G7%' AND batch_code NOT LIKE '%2025%'
            """))
            
            migrated_count = result.rowcount
            print(f"   已迁移 {migrated_count} 条非G7记录")
            
            # 4. 删除临时表
            print("4. 清理临时表...")
            db.execute(text("DROP TABLE statistical_aggregations_temp"))
            
            # 5. 创建一个阻断触发器（只针对G7）
            print("5. 创建G7阻断触发器...")
            db.execute(text("""
                CREATE TRIGGER block_g7_insert 
                BEFORE INSERT ON statistical_aggregations
                FOR EACH ROW 
                BEGIN
                    IF NEW.batch_code LIKE '%G7%' OR NEW.batch_code LIKE '%2025%' THEN
                        SIGNAL SQLSTATE '45000' 
                        SET MESSAGE_TEXT = 'G7/2025批次数据写入已被数据库级别阻断';
                    END IF;
                END
            """))
            
            db.commit()
            print("✅ 数据库层面阻断完成!")
            print("现在任何程序都无法向数据库插入G7/2025相关数据")
            
        except Exception as e:
            db.rollback()
            print(f"❌ 阻断失败: {str(e)}")
            # 如果失败，尝试恢复表名
            try:
                db.execute(text("RENAME TABLE statistical_aggregations_temp TO statistical_aggregations"))
                db.commit()
                print("已恢复原始表结构")
            except:
                pass
            raise


def remove_database_block():
    """移除数据库层面的阻断"""
    
    with next(get_db()) as db:
        print("=== 移除数据库层面阻断 ===")
        
        try:
            # 删除触发器
            db.execute(text("DROP TRIGGER IF EXISTS block_g7_insert"))
            print("已移除G7阻断触发器")
            
            db.commit()
            print("✅ 数据库阻断已移除")
            
        except Exception as e:
            db.rollback()
            print(f"❌ 移除阻断失败: {str(e)}")
            raise


def test_database_block():
    """测试数据库阻断效果"""
    
    with next(get_db()) as db:
        print("=== 测试数据库阻断效果 ===")
        
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
            print("❌ 测试失败: G7数据仍可插入!")
            
        except Exception as e:
            print(f"✅ 测试成功: G7数据插入被阻断 - {str(e)}")
        
        # 测试非G7数据是否正常
        try:
            db.execute(text("""
                INSERT INTO statistical_aggregations 
                (batch_code, aggregation_level, school_id, statistics_data, 
                 data_version, calculation_status, created_at, updated_at)
                VALUES 
                ('TEST-BATCH', 'REGIONAL', 'TEST', '{}', 'TEST', 'COMPLETED', NOW(), NOW())
            """))
            
            # 立即删除测试数据
            db.execute(text("DELETE FROM statistical_aggregations WHERE batch_code = 'TEST-BATCH'"))
            db.commit()
            print("✅ 非G7数据插入正常")
            
        except Exception as e:
            print(f"❌ 非G7数据插入也被阻断: {str(e)}")


if __name__ == '__main__':
    try:
        if len(sys.argv) > 1 and sys.argv[1] == '--remove':
            remove_database_block()
        elif len(sys.argv) > 1 and sys.argv[1] == '--test':
            test_database_block()
        else:
            database_level_block()
            test_database_block()
            print("\n如需移除阻断，请运行：")
            print("python database_level_block.py --remove")
            
    except Exception as e:
        print(f"执行出错: {str(e)}")
        import traceback
        traceback.print_exc()