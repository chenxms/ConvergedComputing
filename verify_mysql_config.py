#!/usr/bin/env python3
"""
验证MySQL配置修改结果
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def verify_mysql_config():
    """验证MySQL配置状态"""
    
    try:
        with get_db_context() as session:
            print("=== MySQL配置验证 ===")
            print(f"验证时间: {datetime.now()}\n")
            
            # 验证关键配置项
            config_items = [
                ('innodb_lock_wait_timeout', 120, '锁等待超时时间'),
                ('wait_timeout', 3600, '连接超时时间'),
                ('interactive_timeout', 3600, '交互超时时间'),
                ('max_connections', 200, '最大连接数'),
                ('slow_query_log', 1, '慢查询日志'),
                ('long_query_time', 2.0, '慢查询阈值'),
                ('innodb_buffer_pool_size', 134217728, '缓冲池大小(需重启)'),
            ]
            
            print("关键配置验证结果:")
            optimized_count = 0
            need_attention = []
            
            for var_name, target_value, description in config_items:
                try:
                    result = session.execute(text(f"SELECT @@{var_name}"))
                    current_value = result.fetchone()[0]
                    
                    # 判断是否达到目标
                    if var_name == 'slow_query_log':
                        # 布尔类型特殊处理
                        is_optimized = int(current_value) == 1
                        status = "[ON]" if is_optimized else "[OFF]"
                    elif var_name == 'innodb_buffer_pool_size':
                        # 缓冲池大小特殊处理
                        is_optimized = int(current_value) >= 512*1024*1024  # 512MB
                        current_mb = int(current_value) / 1024 / 1024
                        status = f"[{current_mb:.0f}MB]"
                    else:
                        # 数值类型
                        is_optimized = float(current_value) >= float(target_value) * 0.9  # 允许10%误差
                        status = f"[{current_value}]"
                    
                    if is_optimized:
                        print(f"   [OK] {var_name}: {status} - {description}")
                        optimized_count += 1
                    else:
                        print(f"   [NEED] {var_name}: {status} - {description} (目标: {target_value})")
                        need_attention.append((var_name, current_value, target_value, description))
                        
                except Exception as e:
                    print(f"   [ERROR] {var_name}: 无法获取 - {e}")
            
            # 测试实际性能改善
            print(f"\n性能测试:")
            
            # 测试1: 简单JOIN查询
            print("   测试简单JOIN查询性能...")
            start_time = datetime.now()
            
            try:
                result = session.execute(text("""
                    SELECT COUNT(*)
                    FROM student_cleaned_scores scs
                    JOIN student_score_detail ssd ON BINARY scs.batch_code = BINARY ssd.batch_code
                                                  AND BINARY scs.student_id = BINARY ssd.student_id
                    WHERE scs.batch_code = 'G4-2025'
                    LIMIT 10
                """))
                
                count = result.fetchone()[0]
                elapsed = (datetime.now() - start_time).total_seconds()
                
                print(f"       结果: {count} 行")
                print(f"       耗时: {elapsed:.3f} 秒 {'[FAST]' if elapsed < 1.0 else '[SLOW]'}")
                
            except Exception as e:
                print(f"       测试失败: {e}")
            
            # 总结和建议
            print(f"\n=== 配置优化总结 ===")
            print(f"已优化配置: {optimized_count}/{len(config_items)}")
            
            if need_attention:
                print(f"\n需要进一步优化的配置:")
                
                immediate_fixes = []
                restart_required = []
                
                for var_name, current, target, desc in need_attention:
                    if var_name in ['innodb_buffer_pool_size', 'innodb_buffer_pool_instances']:
                        restart_required.append((var_name, current, target, desc))
                    else:
                        immediate_fixes.append((var_name, current, target, desc))
                
                if immediate_fixes:
                    print("   可立即修改:")
                    for var_name, current, target, desc in immediate_fixes:
                        print(f"     SET GLOBAL {var_name} = {target};")
                
                if restart_required:
                    print("   需要重启MySQL:")
                    for var_name, current, target, desc in restart_required:
                        if var_name == 'innodb_buffer_pool_size':
                            current_mb = int(current) / 1024 / 1024
                            print(f"     {var_name}: {current_mb:.0f}MB -> 512MB")
                        else:
                            print(f"     {var_name}: {current} -> {target}")
            
            # 给出下一步建议
            print(f"\n📋 下一步行动建议:")
            
            if optimized_count >= len(config_items) * 0.7:  # 70%以上配置已优化
                print("   ✅ 主要配置已优化，预期性能显著提升")
                print("   📝 建议将配置写入my.cnf永久保存")
                print("   🔍 监控slow query log发现更多优化机会")
            else:
                print("   ⚠️  仍有重要配置需要优化")
                print("   🔧 联系运维协助修改配置文件和重启MySQL")
                print("   📊 当前优化已能缓解部分性能问题")
            
            print(f"\n💡 应用层建议:")
            print("   1. 继续使用分批UPDATE策略避免长时间锁定")
            print("   2. 监控慢查询日志识别性能瓶颈")
            print("   3. 在UPDATE操作中添加适当的LIMIT子句")
            
    except Exception as e:
        print(f"验证失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_mysql_config()