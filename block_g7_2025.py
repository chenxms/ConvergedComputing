#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
阻断G7-2025数据生成脚本
同时生成恢复脚本
"""

import sys
import os
from sqlalchemy import text
from datetime import datetime

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db


def create_restore_script():
    """创建恢复脚本"""
    
    restore_script_content = f'''#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025阻断恢复脚本
创建时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""

import sys
import os
from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if CURR_DIR not in sys.path:
    sys.path.insert(0, CURR_DIR)

from app.database.connection import get_db


def restore_g7_2025():
    """恢复G7-2025数据生成能力"""
    
    with next(get_db()) as db:
        print("=== G7-2025阻断恢复 ===")
        print(f"恢复时间: {{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}")
        print()
        
        try:
            # 1. 删除INSERT阻断触发器
            print("1. 删除INSERT阻断触发器...")
            db.execute(text("DROP TRIGGER IF EXISTS prevent_g7_2025_insert"))
            
            # 2. 删除UPDATE阻断触发器
            print("2. 删除UPDATE阻断触发器...")
            db.execute(text("DROP TRIGGER IF EXISTS prevent_g7_2025_update"))
            
            # 3. 检查触发器是否已删除
            print("3. 验证触发器删除...")
            triggers = db.execute(text("""
                SELECT TRIGGER_NAME 
                FROM information_schema.TRIGGERS 
                WHERE TRIGGER_SCHEMA = DATABASE() 
                  AND TRIGGER_NAME LIKE '%g7_2025%'
            """)).fetchall()
            
            if triggers:
                print("   警告: 仍有相关触发器存在:")
                for trigger in triggers:
                    print(f"     - {{trigger[0]}}")
            else:
                print("   OK: 所有G7-2025阻断触发器已删除")
            
            # 4. 测试恢复效果
            print("4. 测试G7-2025数据插入能力...")
            try:
                db.execute(text("""
                    INSERT INTO statistical_aggregations 
                    (batch_code, aggregation_level, school_id, school_name, 
                     statistics_data, data_version, calculation_status, created_at, updated_at)
                    VALUES 
                    ('G7-2025', 'REGIONAL', 'RESTORE_TEST', '恢复测试', 
                     '{{"message": "G7-2025阻断已成功恢复", "restored_at": "{{datetime.now().isoformat()}}"}}', 
                     'RESTORED', 'COMPLETED', NOW(), NOW())
                """))
                
                # 立即删除测试记录
                db.execute(text("""
                    DELETE FROM statistical_aggregations 
                    WHERE school_id = 'RESTORE_TEST' AND batch_code = 'G7-2025'
                """))
                
                print("   OK: G7-2025数据可以正常插入")
                
            except Exception as e:
                print(f"   ERROR: G7-2025数据插入仍被阻断: {{str(e)}}")
            
            db.commit()
            
            print()
            print("✅ G7-2025阻断恢复完成!")
            print("现在可以正常处理G7-2025批次数据")
            
        except Exception as e:
            db.rollback()
            print(f"❌ 恢复失败: {{str(e)}}")
            raise


if __name__ == '__main__':
    try:
        restore_g7_2025()
    except Exception as e:
        print(f"恢复脚本执行出错: {{str(e)}}")
        import traceback
        traceback.print_exc()
'''
    
    # 写入恢复脚本文件
    restore_file = os.path.join(CURR_DIR, "restore_g7_2025.py")
    with open(restore_file, 'w', encoding='utf-8') as f:
        f.write(restore_script_content)
    
    print(f"✅ 恢复脚本已创建: {restore_file}")
    return restore_file


def block_g7_2025():
    """执行G7-2025阻断"""
    
    with next(get_db()) as db:
        print("=== G7-2025数据生成阻断 ===")
        print(f"阻断时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        try:
            # 1. 清理现有G7-2025数据
            print("1. 清理现有G7-2025数据...")
            result = db.execute(text("DELETE FROM statistical_aggregations WHERE batch_code = 'G7-2025'"))
            deleted_count = result.rowcount
            print(f"   已删除 {deleted_count} 条G7-2025记录")
            
            # 2. 创建INSERT阻断触发器
            print("2. 创建INSERT阻断触发器...")
            db.execute(text("""
                CREATE TRIGGER prevent_g7_2025_insert
                BEFORE INSERT ON statistical_aggregations
                FOR EACH ROW
                BEGIN
                    IF NEW.batch_code = 'G7-2025' THEN
                        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025批次数据插入被管理员临时阻断';
                    END IF;
                END
            """))
            print("   INSERT阻断触发器创建成功")
            
            # 3. 创建UPDATE阻断触发器
            print("3. 创建UPDATE阻断触发器...")
            db.execute(text("""
                CREATE TRIGGER prevent_g7_2025_update
                BEFORE UPDATE ON statistical_aggregations
                FOR EACH ROW
                BEGIN
                    IF NEW.batch_code = 'G7-2025' THEN
                        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'G7-2025批次数据更新被管理员临时阻断';
                    END IF;
                END
            """))
            print("   UPDATE阻断触发器创建成功")
            
            # 4. 测试阻断效果
            print("4. 测试阻断效果...")
            try:
                db.execute(text("""
                    INSERT INTO statistical_aggregations 
                    (batch_code, aggregation_level, school_id, statistics_data, 
                     data_version, calculation_status, created_at, updated_at)
                    VALUES 
                    ('G7-2025', 'REGIONAL', 'TEST', '{}', 'TEST', 'COMPLETED', NOW(), NOW())
                """))
                print("   ❌ 阻断测试失败: G7-2025数据仍可插入")
            except Exception as e:
                if "G7-2025批次数据插入被管理员临时阻断" in str(e):
                    print("   ✅ 阻断测试成功: G7-2025数据插入被正确阻断")
                else:
                    print(f"   ⚠️  阻断测试出现未知错误: {str(e)}")
            
            # 5. 验证触发器存在
            print("5. 验证触发器...")
            triggers = db.execute(text("""
                SELECT TRIGGER_NAME, EVENT_MANIPULATION 
                FROM information_schema.TRIGGERS 
                WHERE TRIGGER_SCHEMA = DATABASE() 
                  AND TRIGGER_NAME LIKE '%g7_2025%'
            """)).fetchall()
            
            print(f"   已创建触发器:")
            for trigger in triggers:
                print(f"     - {trigger[0]} ({trigger[1]})")
            
            db.commit()
            
            print()
            print("🛡️  G7-2025阻断设置完成!")
            print("任何程序尝试插入或更新G7-2025数据都将被阻止")
            
        except Exception as e:
            db.rollback()
            print(f"❌ 阻断设置失败: {str(e)}")
            raise


def monitor_block_effect():
    """监控阻断效果"""
    
    print("\n=== 监控阻断效果 ===")
    
    with next(get_db()) as db:
        print("开始5分钟监控...")
        
        import time
        for minute in range(5):
            print(f"\n第 {minute + 1} 分钟:")
            
            for i in range(12):  # 每5秒检查一次
                count = db.execute(text("SELECT COUNT(*) FROM statistical_aggregations WHERE batch_code = 'G7-2025'")).scalar()
                
                if count > 0:
                    print(f"  {i*5}s: ❌ 发现 {count} 条G7-2025记录! 阻断可能失效")
                    
                    # 显示最新记录
                    latest = db.execute(text("""
                        SELECT id, created_at, school_id 
                        FROM statistical_aggregations 
                        WHERE batch_code = 'G7-2025' 
                        ORDER BY created_at DESC 
                        LIMIT 1
                    """)).fetchone()
                    
                    if latest:
                        print(f"    最新记录: ID={latest[0]}, 时间={latest[1]}, 学校={latest[2]}")
                    
                    return False
                elif i % 6 == 0:  # 每30秒报告一次
                    print(f"  {i*5}s: ✅ 无G7-2025记录")
                
                time.sleep(5)
        
        print("\n🎉 5分钟监控完成: G7-2025数据生成已完全阻断!")
        return True


if __name__ == '__main__':
    try:
        # 1. 创建恢复脚本
        restore_file = create_restore_script()
        
        # 2. 执行阻断
        block_g7_2025()
        
        # 3. 监控效果
        success = monitor_block_effect()
        
        print(f"\n📋 总结:")
        print(f"   阻断状态: {'✅ 成功' if success else '❌ 失败'}")
        print(f"   恢复脚本: {restore_file}")
        print(f"\n📝 恢复方法:")
        print(f"   python restore_g7_2025.py")
        
    except Exception as e:
        print(f"❌ 阻断脚本执行出错: {str(e)}")
        import traceback
        traceback.print_exc()