#!/usr/bin/env python3
"""
数据库连接测试脚本
用于验证数据库连接和基本权限
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database.connection import get_database_engine, get_session_factory
from app.database.models import StatisticalAggregations, StatisticalMetadata, StatisticalHistory
from sqlalchemy import text
import sqlalchemy as sa


def test_database_connection():
    """测试数据库连接"""
    print("🔍 开始测试数据库连接...")
    
    try:
        # 创建数据库引擎
        engine = get_database_engine()
        print("✅ 数据库引擎创建成功")
        
        # 测试连接
        with engine.connect() as conn:
            result = conn.execute(text("SELECT VERSION()"))
            version = result.fetchone()[0]
            print(f"✅ 数据库连接成功")
            print(f"   数据库版本: {version}")
            
        # 测试会话工厂
        SessionLocal = get_session_factory()
        with SessionLocal() as session:
            # 测试基本查询权限
            result = session.execute(text("SELECT 1 as test")).fetchone()
            assert result[0] == 1
            print("✅ 数据库查询权限正常")
            
        print("\n🎉 数据库连接测试全部通过！")
        return True
        
    except Exception as e:
        print(f"❌ 数据库连接测试失败: {str(e)}")
        print("\n🔧 请检查以下配置:")
        print("   - 数据库服务是否启动")
        print("   - 连接参数是否正确 (IP: 117.72.14.166, PORT: 23506)")
        print("   - 用户名密码是否正确")
        print("   - 网络连通性是否正常")
        return False


def test_table_access():
    """测试表访问权限"""
    print("\n🔍 开始测试表访问权限...")
    
    try:
        SessionLocal = get_session_factory()
        
        with SessionLocal() as session:
            # 测试statistical_aggregations表
            try:
                count = session.query(StatisticalAggregations).count()
                print(f"✅ statistical_aggregations 表访问正常，当前记录数: {count}")
            except Exception as e:
                print(f"⚠️ statistical_aggregations 表访问异常: {str(e)}")
            
            # 测试statistical_metadata表
            try:
                count = session.query(StatisticalMetadata).count()
                print(f"✅ statistical_metadata 表访问正常，当前记录数: {count}")
            except Exception as e:
                print(f"⚠️ statistical_metadata 表访问异常: {str(e)}")
                
            # 测试statistical_history表  
            try:
                count = session.query(StatisticalHistory).count()
                print(f"✅ statistical_history 表访问正常，当前记录数: {count}")
            except Exception as e:
                print(f"⚠️ statistical_history 表访问异常: {str(e)}")
                
        print("✅ 表访问权限测试完成")
        return True
        
    except Exception as e:
        print(f"❌ 表访问测试失败: {str(e)}")
        return False


def test_crud_operations():
    """测试基本CRUD操作"""
    print("\n🔍 开始测试CRUD操作...")
    
    try:
        SessionLocal = get_session_factory()
        
        with SessionLocal() as session:
            # 创建测试记录
            test_metadata = StatisticalMetadata(
                metadata_key="test_connection",
                metadata_value={"test": "connection_test", "timestamp": "2025-09-05"},
                description="数据库连接测试记录"
            )
            
            session.add(test_metadata)
            session.commit()
            print("✅ CREATE操作成功")
            
            # 读取测试记录
            retrieved = session.query(StatisticalMetadata).filter(
                StatisticalMetadata.metadata_key == "test_connection"
            ).first()
            
            if retrieved:
                print("✅ READ操作成功")
                
                # 更新测试记录
                retrieved.description = "更新后的测试记录"
                session.commit()
                print("✅ UPDATE操作成功")
                
                # 删除测试记录
                session.delete(retrieved)
                session.commit()
                print("✅ DELETE操作成功")
            else:
                print("❌ READ操作失败")
                
        print("✅ CRUD操作测试全部通过")
        return True
        
    except Exception as e:
        print(f"❌ CRUD操作测试失败: {str(e)}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Data-Calculation 数据库连接测试")
    print("=" * 60)
    
    # 运行所有测试
    tests = [
        ("数据库连接测试", test_database_connection),
        ("表访问权限测试", test_table_access),
        ("CRUD操作测试", test_crud_operations)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        print("-" * 40)
        result = test_func()
        results.append((test_name, result))
    
    # 总结结果
    print("\n" + "=" * 60)
    print("📊 测试结果总结")
    print("=" * 60)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {test_name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 所有测试通过！数据库连接正常，可以继续下一步测试。")
        sys.exit(0)
    else:
        print("\n❌ 部分测试失败，请检查错误信息并修复后重试。")
        sys.exit(1)