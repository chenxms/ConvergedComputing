#!/usr/bin/env python3
"""
立即应用MySQL性能优化配置
基于检测到的SUPER权限执行配置优化
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime
import time

def apply_mysql_optimization():
    """应用MySQL性能优化配置"""
    
    # 优化配置项 (变量名, 建议值, 当前值变量, 描述)
    optimizations = [
        ('innodb_lock_wait_timeout', 120, None, '增加锁等待超时时间到2分钟'),
        ('wait_timeout', 3600, None, '增加连接超时时间到1小时'),
        ('interactive_timeout', 3600, None, '增加交互式连接超时时间'),
        ('max_connections', 200, None, '增加最大连接数'),
        ('slow_query_log', 'ON', None, '开启慢查询日志'),
        ('long_query_time', 2, None, '设置慢查询阈值为2秒')
    ]
    
    try:
        with get_db_context() as session:
            print("=== MySQL性能优化配置应用 ===")
            print(f"执行时间: {datetime.now()}")
            print("权限: root@% (SUPER权限)\n")
            
            # 1. 获取当前配置值
            print("1. 检查当前配置...")
            for i, (var_name, new_value, _, description) in enumerate(optimizations):
                try:
                    result = session.execute(text(f"SELECT @@{var_name}"))
                    current_value = result.fetchone()[0]
                    optimizations[i] = (var_name, new_value, current_value, description)
                    print(f"   {var_name}: {current_value} -> {new_value}")
                except Exception as e:
                    print(f"   {var_name}: 无法获取当前值 - {e}")
            
            print()
            
            # 2. 应用优化配置
            print("2. 应用优化配置...")
            success_count = 0
            failed_items = []
            
            for var_name, new_value, current_value, description in optimizations:
                if current_value is None:
                    continue
                    
                try:
                    print(f"   正在设置 {var_name} = {new_value}...")
                    
                    # 执行SET GLOBAL命令
                    if isinstance(new_value, str) and new_value.upper() in ['ON', 'OFF']:
                        session.execute(text(f"SET GLOBAL {var_name} = '{new_value}'"))
                    else:
                        session.execute(text(f"SET GLOBAL {var_name} = {new_value}"))
                    
                    session.commit()
                    
                    # 验证设置是否生效
                    result = session.execute(text(f"SELECT @@{var_name}"))
                    actual_value = result.fetchone()[0]
                    
                    if str(actual_value) == str(new_value):
                        print(f"       [SUCCESS] {description}")
                        success_count += 1
                    else:
                        print(f"       [WARNING] 设置值 {actual_value} != 期望值 {new_value}")
                    
                    time.sleep(0.5)  # 短暂延迟
                    
                except Exception as e:
                    failed_items.append((var_name, str(e)))
                    print(f"       [FAILED] {e}")
            
            # 3. 处理需要重启的配置
            print(f"\n3. 需要重启的关键配置:")
            restart_configs = [
                ('innodb_buffer_pool_size', '512M', '提升缓冲池大小到512MB'),
                ('innodb_buffer_pool_instances', '4', '优化缓冲池实例数')
            ]
            
            for var_name, recommended_value, description in restart_configs:
                try:
                    result = session.execute(text(f"SELECT @@{var_name}"))
                    current_value = result.fetchone()[0]
                    print(f"   {var_name}: {current_value} (建议: {recommended_value})")
                    print(f"       {description}")
                except:
                    print(f"   {var_name}: 无法获取当前值")
            
            # 4. 验证优化效果
            print(f"\n4. 验证优化效果...")
            
            # 测试锁等待时间
            result = session.execute(text("SELECT @@innodb_lock_wait_timeout"))
            lock_timeout = result.fetchone()[0]
            
            # 测试连接超时
            result = session.execute(text("SELECT @@wait_timeout"))
            wait_timeout = result.fetchone()[0]
            
            print(f"   锁等待超时: {lock_timeout}秒 {'[OK]' if lock_timeout >= 120 else '[需要更长]'}")
            print(f"   连接超时: {wait_timeout}秒 {'[OK]' if wait_timeout >= 3600 else '[需要更长]'}")
            
            # 5. 生成配置文件建议
            print(f"\n5. 永久配置建议 (my.cnf):")
            print("   # 在 [mysqld] 部分添加以下配置以永久保存:")
            print("   [mysqld]")
            for var_name, new_value, current_value, description in optimizations:
                if current_value is not None and success_count > 0:
                    print(f"   {var_name} = {new_value}")
            
            print("   # 需要重启的关键配置:")
            for var_name, recommended_value, description in restart_configs:
                print(f"   {var_name} = {recommended_value}")
            
            # 6. 结果总结
            print(f"\n=== 优化结果总结 ===")
            print(f"✓ 成功应用配置: {success_count}/{len([x for x in optimizations if x[2] is not None])}")
            
            if failed_items:
                print(f"✗ 失败的配置项:")
                for var_name, error in failed_items:
                    print(f"  - {var_name}: {error[:50]}...")
            
            if success_count > 0:
                print(f"\n🎯 预期效果:")
                print("  - UPDATE JOIN操作超时风险大幅降低")
                print("  - 锁等待时间容忍度提升到2分钟")
                print("  - 连接稳定性显著改善")
                print("  - 慢查询日志可用于后续优化")
                
                print(f"\n⚠️  重要提醒:")
                print("  1. 这些配置在MySQL重启后会丢失")
                print("  2. 建议将配置写入my.cnf文件永久保存")
                print("  3. innodb_buffer_pool_size需要重启才能修改")
            else:
                print(f"\n❌ 配置应用失败，可能需要运维协助")
            
            print(f"\n完成时间: {datetime.now()}")
            
    except Exception as e:
        print(f"优化过程失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("[INFO] MySQL性能优化配置工具")
    print("[INFO] 检测到SUPER权限，可以直接修改全局配置")
    print("[INFO] 注意：这些修改在重启后会丢失，需要配置文件永久保存")
    print()
    
    confirm = input("是否立即应用MySQL优化配置？(输入 'yes' 确认): ")
    if confirm.lower() == 'yes':
        apply_mysql_optimization()
    else:
        print("操作已取消")