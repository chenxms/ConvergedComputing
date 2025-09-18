#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
使用正确路径测试API禁用功能
"""

import requests

BASE_URL = "http://localhost:8010"

def test_api_restrictions():
    print("=== API禁用功能测试 ===\n")

    # 测试写入类API（应该返回403）
    write_apis = [
        ("POST", f"{BASE_URL}/api/v1/management/batches/G7-2025/clean", "批次数据清洗"),
    ]

    print("1. 测试写入类API（期望403禁用）:")
    for method, url, name in write_apis:
        print(f"\n测试 {name}:")
        print(f"URL: {url}")

        try:
            response = requests.post(url, timeout=10)
            print(f"状态码: {response.status_code}")

            if response.status_code == 403:
                print("✓ 正确返回403禁用状态")
                print(f"响应内容: {response.text}")
            elif response.status_code == 422:
                print("! 返回422参数错误，API存在但可能需要参数")
                print(f"响应内容: {response.text}")
            else:
                print(f"✗ 期望403，实际返回{response.status_code}")
                print(f"响应内容: {response.text}")

        except Exception as e:
            print(f"✗ 请求失败: {e}")

    # 测试读取类API（应该正常工作）
    read_apis = [
        ("GET", f"{BASE_URL}/health", "系统健康检查"),
        ("GET", f"{BASE_URL}/api/v1/management/batches", "获取批次列表"),
    ]

    print("\n2. 测试读取类API（期望正常工作）:")
    for method, url, name in read_apis:
        print(f"\n测试 {name}:")
        print(f"URL: {url}")

        try:
            response = requests.get(url, timeout=10)
            print(f"状态码: {response.status_code}")

            if response.status_code == 200:
                print("✓ 正常返回200状态")
                content = response.text[:150] + "..." if len(response.text) > 150 else response.text
                print(f"响应片段: {content}")
            elif response.status_code == 404:
                print("? 返回404，路径可能不存在")
            else:
                print(f"状态码: {response.status_code}")
                print(f"响应: {response.text}")

        except Exception as e:
            print(f"✗ 请求失败: {e}")

def check_api_routes():
    """检查可用的API路由"""
    print("\n=== 检查API路由结构 ===\n")

    try:
        # 获取OpenAPI规范
        response = requests.get(f"{BASE_URL}/openapi.json", timeout=10)
        if response.status_code == 200:
            openapi = response.json()
            paths = openapi.get('paths', {})

            print("发现的API路径:")
            for path, methods in paths.items():
                if 'clean' in path or 'materialize' in path:
                    print(f"  {path}: {list(methods.keys())}")

            print(f"\n总计发现 {len(paths)} 个API路径")

        else:
            print(f"无法获取API规范: {response.status_code}")

    except Exception as e:
        print(f"检查API路由失败: {e}")

def main():
    print("测试API禁用触发计算的修改成果")
    print("=" * 50)

    # 检查API可用性
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            print("API服务运行正常\n")
        else:
            print(f"API服务状态异常: {response.status_code}")
            return
    except Exception as e:
        print(f"无法连接到API服务: {e}")
        return

    # 检查API路由
    check_api_routes()

    # 测试API限制
    test_api_restrictions()

    print("\n" + "=" * 50)
    print("测试完成")

if __name__ == "__main__":
    main()