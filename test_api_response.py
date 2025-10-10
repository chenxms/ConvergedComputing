#!/usr/bin/env python3
"""测试API响应时间脚本"""

import time
import requests
import asyncio
from app.database.connection import test_connection, get_database_info

def test_database_connection():
    """测试数据库连接"""
    print("1. 测试数据库连接...")
    start_time = time.time()
    success = test_connection()
    response_time = (time.time() - start_time) * 1000

    if success:
        print(f"[成功] 数据库连接成功 ({response_time:.2f}ms)")

        # 获取数据库信息
        db_info = get_database_info()
        print(f"  - 连接池大小: {db_info.get('pool_size', 'N/A')}")
        print(f"  - 已检出连接: {db_info.get('checked_out', 'N/A')}")
        print(f"  - 已检入连接: {db_info.get('checked_in', 'N/A')}")
    else:
        print(f"[失败] 数据库连接失败 ({response_time:.2f}ms)")

    return success

def test_api_endpoints():
    """测试API端点"""
    base_url = "http://localhost:8000"

    endpoints = [
        "/",
        "/health",
        "/batches",
        "/reports/batch/G7_2025/all-schools"
    ]

    print("\n2. 测试API端点响应...")

    for endpoint in endpoints:
        try:
            start_time = time.time()
            response = requests.get(f"{base_url}{endpoint}", timeout=10)
            response_time = (time.time() - start_time) * 1000

            if response.status_code == 200:
                print(f"[成功] {endpoint} - {response.status_code} ({response_time:.2f}ms)")
            else:
                print(f"[警告] {endpoint} - {response.status_code} ({response_time:.2f}ms)")

        except requests.exceptions.ConnectionError:
            print(f"[失败] {endpoint} - 连接失败 (服务未启动)")
        except requests.exceptions.Timeout:
            print(f"[失败] {endpoint} - 请求超时")
        except Exception as e:
            print(f"[错误] {endpoint} - 错误: {e}")

def main():
    print("=== API响应测试 ===")

    # 测试数据库
    db_success = test_database_connection()

    if db_success:
        # 测试API
        test_api_endpoints()

        print("\n3. 性能建议:")
        print("- 数据库连接池已优化至10+15配置")
        print("- 连接超时设置为10秒")
        print("- 读取超时保持5分钟以支持复杂查询")
        print("- 写入超时设置为3分钟")
        print("- 如果前端仍然缓慢，建议检查具体的查询语句")
    else:
        print("\n数据库连接失败，请检查配置")

if __name__ == "__main__":
    main()