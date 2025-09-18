#!/usr/bin/env python3
"""
API端点测试脚本 - 诊断404问题
"""
import requests
import json

# API基础地址
API_BASE = "http://117.72.14.166:8000"

def test_endpoint(url, method="GET", headers=None, data=None):
    """测试单个端点"""
    print(f"\n测试: {method} {url}")
    print("-" * 60)

    try:
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=10)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=data, timeout=10)
        else:
            response = requests.request(method, url, headers=headers, json=data, timeout=10)

        print(f"状态码: {response.status_code}")
        print(f"响应头: {dict(response.headers)}")

        if response.status_code == 200:
            print("✅ 成功")
            try:
                data = response.json()
                print(f"响应数据: {json.dumps(data, ensure_ascii=False, indent=2)[:500]}...")
            except:
                print(f"响应文本: {response.text[:500]}...")
        elif response.status_code == 404:
            print("❌ 404 Not Found")
            print(f"响应内容: {response.text}")
        elif response.status_code == 401:
            print("⚠️ 401 Unauthorized - 需要认证")
            print(f"响应内容: {response.text}")
        elif response.status_code == 403:
            print("⚠️ 403 Forbidden - 权限不足")
            print(f"响应内容: {response.text}")
        else:
            print(f"⚠️ 其他状态码")
            print(f"响应内容: {response.text}")

        return response

    except requests.exceptions.ConnectionError:
        print("❌ 连接失败 - 服务可能未运行")
        return None
    except requests.exceptions.Timeout:
        print("❌ 请求超时")
        return None
    except Exception as e:
        print(f"❌ 错误: {e}")
        return None

def main():
    print("="*60)
    print("API端点测试 - 诊断404问题")
    print("="*60)

    # 1. 测试基础端点
    print("\n### 1. 基础端点测试 ###")
    test_endpoint(f"{API_BASE}/")
    test_endpoint(f"{API_BASE}/health")

    # 2. 测试API文档
    print("\n### 2. API文档端点 ###")
    test_endpoint(f"{API_BASE}/docs")
    test_endpoint(f"{API_BASE}/openapi.json")

    # 3. 测试v1.2 API路径
    print("\n### 3. v1.2 API路径测试 ###")

    # 3.1 不带任何参数
    test_endpoint(f"{API_BASE}/api/v12/batch/G7-2025/regional")

    # 3.2 带API Key Header（虽然可能不需要）
    headers_with_key = {
        "X-API-Key": "JDCIWWDAODAJJFAAFAJFJjdsmdjf23232"
    }
    test_endpoint(f"{API_BASE}/api/v12/batch/G7-2025/regional", headers=headers_with_key)

    # 3.3 测试其他batch code
    test_endpoint(f"{API_BASE}/api/v12/batch/G4-2024/regional")

    # 4. 测试其他API路径
    print("\n### 4. 其他API路径测试 ###")
    test_endpoint(f"{API_BASE}/api/v1/management/batch")
    test_endpoint(f"{API_BASE}/api/v1/reporting/test")

    # 5. 分析OpenAPI规范
    print("\n### 5. 分析可用的路径 ###")
    response = requests.get(f"{API_BASE}/openapi.json", timeout=10)
    if response.status_code == 200:
        openapi = response.json()
        paths = openapi.get("paths", {})

        print("\n可用的API路径：")
        for path in sorted(paths.keys()):
            methods = list(paths[path].keys())
            print(f"  {path} - 方法: {', '.join(methods).upper()}")

        # 特别查找v12相关路径
        v12_paths = [p for p in paths.keys() if "v12" in p]
        if v12_paths:
            print("\n✅ 找到v1.2相关路径：")
            for path in v12_paths:
                print(f"  {path}")
        else:
            print("\n❌ 没有找到v1.2相关路径！")

            # 查找包含batch的路径
            batch_paths = [p for p in paths.keys() if "batch" in p.lower()]
            if batch_paths:
                print("\n找到包含'batch'的路径：")
                for path in batch_paths:
                    print(f"  {path}")

if __name__ == "__main__":
    main()
    print("\n" + "="*60)
    print("测试完成！")
    print("="*60)