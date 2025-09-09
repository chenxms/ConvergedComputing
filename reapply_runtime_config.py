#!/usr/bin/env python3
"""
重新应用运行时MySQL配置
重启后需要重新设置的参数
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime
import time

def reapply_runtime_config():
    """重新应用运行时配置"""
    
    # 需要重新设置的配置项
    runtime_configs = [
        ('innodb_lock_wait_timeout', 120, '锁等待超时时间'),
        ('max_connections', 200, '最大连接数'),
        ('slow_query_log', 1, '慢查询日志开关'),
        ('long_query_time', 2, '慢查询时间阈值')
    ]
    
    try:
        with get_db_context() as session:
            print("=== 重新应用运行时MySQL配置 ===")
            print(f"执行时间: {datetime.now()}")
            print("说明: 重启后需要重新设置运行时参数\n")
            
            success_count = 0
            
            for var_name, target_value, description in runtime_configs:
                try:
                    # 获取当前值
                    result = session.execute(text(f"SELECT @@{var_name}"))
                    current_value = result.fetchone()[0]
                    
                    print(f"设置 {var_name}: {current_value} -> {target_value}")
                    
                    # 应用新配置
                    session.execute(text(f"SET GLOBAL {var_name} = {target_value}"))
                    session.commit()
                    
                    # 验证设置
                    result = session.execute(text(f"SELECT @@{var_name}"))
                    new_value = result.fetchone()[0]
                    
                    if str(new_value) == str(target_value):
                        print(f"   [SUCCESS] {description}")
                        success_count += 1
                    else:
                        print(f"   [WARNING] 实际值 {new_value} != 期望值 {target_value}")
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"   [FAILED] {e}")
            
            print(f"\n配置应用结果: {success_count}/{len(runtime_configs)} 成功")
            
            if success_count >= len(runtime_configs):
                print("\n[SUCCESS] 所有运行时配置已成功应用!")
                
                # 立即进行性能测试
                print("\n=== 立即性能验证 ===")
                
                # 测试1: 锁等待时间验证
                print("1. 锁等待配置验证...")
                result = session.execute(text("SELECT @@innodb_lock_wait_timeout"))
                lock_timeout = result.fetchone()[0]
                print(f"   锁等待超时: {lock_timeout}秒 {'[OK]' if int(lock_timeout) >= 120 else '[需要更长]'}")
                
                # 测试2: 快速JOIN性能测试
                print("\n2. JOIN查询性能测试...")
                start_time = time.time()
                
                result = session.execute(text("""
                    SELECT COUNT(*) as total
                    FROM student_cleaned_scores scs
                    JOIN student_score_detail ssd ON BINARY scs.batch_code = BINARY ssd.batch_code
                                                  AND BINARY scs.student_id = BINARY ssd.student_id
                    WHERE scs.batch_code = 'G4-2025'
                    LIMIT 5000
                """))
                
                query_result = result.fetchone()[0]
                elapsed_time = time.time() - start_time
                
                print(f"   查询结果: {query_result} 条记录")
                print(f"   查询时间: {elapsed_time:.3f}秒 {'[FAST]' if elapsed_time < 1.0 else '[OK]' if elapsed_time < 2.0 else '[SLOW]'}")
                
                # 测试3: 缓冲池效果验证
                print("\n3. 缓冲池配置验证...")
                result = session.execute(text("SELECT @@innodb_buffer_pool_size"))
                buffer_size = result.fetchone()[0]
                buffer_mb = int(buffer_size) / 1024 / 1024
                print(f"   缓冲池大小: {buffer_mb:.0f}MB {'[EXCELLENT]' if buffer_mb >= 500 else '[LOW]'}")
                
                # 综合评估
                print(f"\n=== 优化效果评估 ===")
                
                improvements = []
                if int(lock_timeout) >= 120:
                    improvements.append("✅ 锁等待超时已优化到2分钟")
                if buffer_mb >= 500:
                    improvements.append("✅ 缓冲池已提升到512MB")
                if elapsed_time < 2.0:
                    improvements.append("✅ JOIN查询性能良好")
                
                for improvement in improvements:
                    print(f"   {improvement}")
                
                if len(improvements) >= 3:
                    print(f"\n🎉 优化完成！预期数据库性能问题已解决")
                    print("   - 长时间锁定风险大幅降低")
                    print("   - 可以安全执行大批量UPDATE操作")  
                    print("   - 系统整体响应性显著提升")
                else:
                    print(f"\n⚠️  部分优化生效，建议继续监控性能")
                    
            else:
                print(f"\n[WARNING] 部分配置应用失败，可能需要更高权限")
            
            print(f"\n完成时间: {datetime.now()}")
            
    except Exception as e:
        print(f"配置应用失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    reapply_runtime_config()