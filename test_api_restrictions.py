#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试API禁用触发计算的修改成果
"""

import requests
import json

# API基础URL
BASE_URL = "http://localhost:8010"  # app容器的端口

def test_write_apis_forbidden():
    """测试写入类API应该返回403禁用"""
    print("=== 测试写入类API禁用状态 ===\n")

    write_apis = [
        {
            "name": "批次数据清洗",
            "method": "POST",
            "url": f"{BASE_URL}/api/v1/batches/G7-2025/clean",
            "description": "触发批次数据清洗"
        },
        {
            "name": "批次物化",
            "method": "POST",
            "url": f"{BASE_URL}/api/v1/subjects-v12/batch/G7-2025/materialize",
            "description": "触发批次数据物化"
        },
        {
            "name": "单个学校物化",
            "method": "POST",
            "url": f"{BASE_URL}/api/v1/subjects-v12/batch/G7-2025/school/5001/materialize",
            "description": "触发单个学校物化"
        }
    ]

    for api in write_apis:
        print(f"测试: {api['name']}")
        print(f"URL: {api['url']}")

        try:
            if api['method'] == 'POST':
                response = requests.post(api['url'], timeout=10)
            else:
                response = requests.get(api['url'], timeout=10)

            print(f"状态码: {response.status_code}")

            if response.status_code == 403:
                print("✅ 正确返回403禁用状态")
                try:
                    data = response.json()
                    if "数据任务触发已禁用" in str(data) or "禁用" in str(data):
                        print(f"✅ 禁用提示信息正确: {data}")
                    else:
                        print(f"⚠ 禁用信息可能不完整: {data}")
                except:
                    print(f"响应内容: {response.text}")
            else:
                print(f"❌ 期望403，实际返回{response.status_code}")
                print(f"响应: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"❌ 请求失败: {e}")

        print("-" * 50)

def test_read_apis_normal():
    """测试读取类API应该保持正常响应"""
    print("\n=== 测试读取类API正常状态 ===\n")

    read_apis = [
        {
            "name": "获取批次列表",
            "method": "GET",
            "url": f"{BASE_URL}/api/v1/batches",
            "description": "获取所有批次列表"
        },
        {
            "name": "获取批次信息",
            "method": "GET",
            "url": f"{BASE_URL}/api/v1/batches/G7-2025",
            "description": "获取特定批次信息"
        },
        {
            "name": "健康检查",
            "method": "GET",
            "url": f"{BASE_URL}/health",
            "description": "系统健康检查"
        },
        {
            "name": "批次统计数据",
            "method": "GET",
            "url": f"{BASE_URL}/api/v1/subjects-v12/batch/G7-2025/statistics",
            "description": "获取批次统计数据"
        }
    ]

    for api in read_apis:
        print(f"测试: {api['name']}")
        print(f"URL: {api['url']}")

        try:
            response = requests.get(api['url'], timeout=10)
            print(f"状态码: {response.status_code}")

            if response.status_code == 200:
                print("✅ 正常返回200状态")
                try:
                    data = response.json()
                    if isinstance(data, dict) and data:
                        print(f"✅ 返回有效JSON数据")
                    elif isinstance(data, list):
                        print(f"✅ 返回JSON数组，长度: {len(data)}")
                    else:
                        print(f"⚠ 返回数据可能为空: {data}")
                except:
                    print(f"✅ 返回文本响应: {response.text[:100]}...")

            elif response.status_code == 404:
                print("⚠ 返回404，可能资源不存在")
            else:
                print(f"⚠ 返回状态码: {response.status_code}")
                print(f"响应: {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"❌ 请求失败: {e}")

        print("-" * 50)

def test_api_health():
    """测试API服务可用性"""
    print("\n=== 测试API服务可用性 ===\n")

    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        print(f"服务状态: {response.status_code}")

        if response.status_code == 200:
            print("✅ API服务正常运行")
            return True
        else:
            print(f"⚠ API服务状态异常: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ 无法连接到API服务: {e}")
        return False

def main():
    """主测试函数"""
    print("开始测试API禁用触发计算的修改成果")
    print("=" * 60)

    # 首先检查API服务可用性
    if not test_api_health():
        print("\n❌ API服务不可用，无法进行测试")
        return

    # 测试写入类API禁用
    test_write_apis_forbidden()

    # 测试读取类API正常
    test_read_apis_normal()

    print("\n" + "=" * 60)
    print("API禁用测试完成")

if __name__ == "__main__":
    main()