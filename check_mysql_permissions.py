#!/usr/bin/env python3
"""
检查当前MySQL连接的权限和可调整的配置项
"""

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def check_mysql_permissions():
    """检查MySQL权限和可调整的配置"""
    
    try:
        with get_db_context() as session:
            print("=== MySQL权限和配置检查 ===")
            print(f"检查时间: {datetime.now()}\n")
            
            # 1. 检查当前用户和主机
            print("1. 当前连接信息:")
            result = session.execute(text("SELECT USER(), @@hostname, @@port"))
            conn_info = result.fetchone()
            print(f"   当前用户: {conn_info[0]}")
            print(f"   数据库主机: {conn_info[1]}")
            print(f"   端口: {conn_info[2]}")
            
            # 2. 检查用户权限
            print(f"\n2. 当前用户权限:")
            try:
                result = session.execute(text("SHOW GRANTS FOR CURRENT_USER()"))
                grants = result.fetchall()
                for grant in grants:
                    print(f"   {grant[0]}")
            except Exception as e:
                print(f"   无法查看权限: {e}")
            
            # 3. 检查是否可以修改全局变量
            print(f"\n3. 全局变量修改权限测试:")
            modifiable_vars = [
                ('innodb_lock_wait_timeout', '120'),
                ('wait_timeout', '3600'),
                ('interactive_timeout', '3600'),
                ('max_connections', '200')
            ]
            
            can_modify = []
            cannot_modify = []
            
            for var_name, test_value in modifiable_vars:
                try:
                    # 获取当前值
                    result = session.execute(text(f"SELECT @@{var_name}"))
                    current_value = result.fetchone()[0]
                    
                    # 测试是否可以修改(先改回原值测试)
                    session.execute(text(f"SET GLOBAL {var_name} = {current_value}"))
                    session.commit()
                    
                    can_modify.append((var_name, current_value, test_value))
                    print(f"   ✓ {var_name}: 当前={current_value}, 可修改为={test_value}")
                    
                except Exception as e:
                    cannot_modify.append((var_name, str(e)))
                    print(f"   ✗ {var_name}: 无法修改 - {str(e)[:50]}...")
            
            # 4. 检查需要重启的配置项
            print(f"\n4. 需要重启的配置项:")
            restart_required = [
                'innodb_buffer_pool_size',
                'innodb_buffer_pool_instances'
            ]
            
            for var_name in restart_required:
                try:
                    result = session.execute(text(f"SELECT @@{var_name}"))
                    current_value = result.fetchone()[0]
                    print(f"   {var_name}: {current_value} (需要重启MySQL)")
                except:
                    print(f"   {var_name}: 无法获取")
            
            # 5. 检查是否是云数据库
            print(f"\n5. 数据库环境检测:")
            try:
                result = session.execute(text("SELECT @@version_comment"))
                version_comment = result.fetchone()[0]
                print(f"   版本信息: {version_comment}")
                
                # 检查是否是云服务
                cloud_indicators = ['RDS', 'Cloud', 'Aliyun', 'Tencent', 'AWS', 'Azure']
                is_cloud = any(indicator.lower() in version_comment.lower() for indicator in cloud_indicators)
                
                if is_cloud:
                    print("   ⚠️  检测到云数据库服务，部分配置需要通过控制台修改")
                else:
                    print("   📍 自建MySQL服务器，可通过配置文件修改")
                    
            except:
                print("   无法确定数据库环境")
            
            # 6. 提供建议
            print(f"\n=== 配置修改建议 ===")
            
            if can_modify:
                print("🟢 可以立即修改的配置:")
                for var_name, current, suggested in can_modify:
                    print(f"   SET GLOBAL {var_name} = {suggested};  -- 当前: {current}")
                print("\n   ⚠️  注意: 这些修改重启后会丢失，需要写入配置文件永久保存")
            
            if cannot_modify:
                print(f"\n🔴 需要运维协助的配置:")
                for var_name, error in cannot_modify:
                    print(f"   {var_name}: 需要管理员权限")
            
            print(f"\n🔧 推荐的修改方式:")
            if can_modify:
                print("   1. 立即执行: 修改可调整的运行时参数")
                print("   2. 联系运维: 将配置写入my.cnf永久保存")
            else:
                print("   1. 联系运维同事修改MySQL配置文件")
                print("   2. 或申请临时提升数据库配置权限")
            
            # 7. 生成配置修改脚本
            if can_modify:
                print(f"\n📝 立即可执行的优化脚本:")
                print("   -- MySQL优化配置")
                for var_name, current, suggested in can_modify:
                    print(f"   SET GLOBAL {var_name} = {suggested};")
                print("   -- 查看修改结果")
                for var_name, current, suggested in can_modify:
                    print(f"   SELECT '{var_name}', @@{var_name};")
            
    except Exception as e:
        print(f"检查失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_mysql_permissions()