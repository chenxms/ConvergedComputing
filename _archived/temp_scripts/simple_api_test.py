#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
简化API测试脚本
"""

import requests

BASE_URL = "http://localhost:8010"

def test_write_api_forbidden():
    """测试写入API禁用"""
    print("=== 测试写入类API禁用 ===")

    apis = [
        ("POST", f"{BASE_URL}/api/v1/batches/G7-2025/clean", "批次清洗"),
        ("POST", f"{BASE_URL}/api/v1/subjects-v12/batch/G7-2025/materialize", "批次物化"),
        ("POST", f"{BASE_URL}/api/v1/subjects-v12/batch/G7-2025/school/5001/materialize", "学校物化")
    ]

    for method, url, name in apis:
        print(f"\n测试 {name}: {url}")
        try:
            response = requests.post(url, timeout=10)
            print(f"状态码: {response.status_code}")

            if response.status_code == 403:
                print("OK: 正确返回403禁用")
                print(f"响应: {response.text}")
            else:
                print(f"ERROR: 期望403，实际{response.status_code}")
                print(f"响应: {response.text}")

        except Exception as e:
            print(f"ERROR: {e}")

def test_read_api_normal():
    """测试读取API正常"""
    print("\n=== 测试读取类API正常 ===")

    apis = [
        (f"{BASE_URL}/api/v1/batches", "批次列表"),
        (f"{BASE_URL}/health", "健康检查"),
        (f"{BASE_URL}/api/v1/batches/G7-2025", "批次信息")
    ]

    for url, name in apis:
        print(f"\n测试 {name}: {url}")
        try:
            response = requests.get(url, timeout=10)
            print(f"状态码: {response.status_code}")

            if response.status_code == 200:
                print("OK: 正常返回200")
                content = response.text[:200] + "..." if len(response.text) > 200 else response.text
                print(f"响应片段: {content}")
            else:
                print(f"状态: {response.status_code}")
                print(f"响应: {response.text}")

        except Exception as e:
            print(f"ERROR: {e}")

def main():
    print("API禁用功能测试")

    # 检查服务可用性
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        print(f"API服务状态: {response.status_code}")
        if response.status_code != 200:
            print("API服务不可用")
            return
    except Exception as e:
        print(f"无法连接API服务: {e}")
        return

    test_write_api_forbidden()
    test_read_api_normal()

    print("\n测试完成")

if __name__ == "__main__":
    main()