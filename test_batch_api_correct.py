#!/usr/bin/env python3
"""
正确的批次管理API测试脚本
解决了数据格式验证问题
"""

import requests
import json
from datetime import datetime

# API基础URL
BASE_URL = "http://localhost:8000"

def test_batch_creation():
    """测试批次创建 - 使用正确的数据格式"""
    
    print("🚀 测试批次创建 - 正确数据格式")
    print("=" * 50)
    
    # 1. 测试最小化数据格式
    minimal_data = {
        "batch_code": "MINIMAL_TEST_001",
        "aggregation_level": "regional", 
        "statistics_data": {
            "batch_info": {
                "batch_code": "MINIMAL_TEST_001",
                "total_students": 100,
                "total_schools": 2
            },
            "academic_subjects": [
                {
                    "subject_id": 1,
                    "subject_name": "语文",
                    "statistics": {
                        "total_score": 800,
                        "average_score": 650,
                        "difficulty_coefficient": 0.812,
                        "discrimination": 0.45
                    }
                }
            ]
        },
        "total_students": 100,
        "total_schools": 2
    }
    
    try:
        response = requests.post(f"{BASE_URL}/batches", json=minimal_data)
        print(f"响应状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        
        if response.status_code == 200:
            print("✅ 批次创建成功！")
            return response.json()
        else:
            print(f"❌ 批次创建失败: {response.status_code}")
            return None
            
    except requests.exceptions.ConnectionError:
        print("❌ 连接失败，请确认服务器运行在 localhost:8000")
        return None
    except Exception as e:
        print(f"❌ 请求异常: {str(e)}")
        return None

def test_batch_list():
    """测试批次列表查询"""
    print("\n📋 测试批次列表查询")
    print("=" * 50)
    
    try:
        response = requests.get(f"{BASE_URL}/batches")
        print(f"响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            batches = response.json()
            print(f"✅ 查询成功，共找到 {len(batches)} 个批次")
            if batches:
                print("批次列表:")
                for batch in batches:
                    print(f"  - {batch.get('batch_code')} ({batch.get('aggregation_level')})")
            else:
                print("  当前没有批次数据")
        else:
            print(f"❌ 查询失败: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ 请求异常: {str(e)}")

def test_comprehensive_data():
    """测试完整数据格式"""
    print("\n🎯 测试完整数据格式")
    print("=" * 50)
    
    comprehensive_data = {
        "batch_code": "COMPREHENSIVE_TEST_001",
        "aggregation_level": "regional",
        "statistics_data": {
            "batch_info": {
                "batch_code": "COMPREHENSIVE_TEST_001",
                "total_students": 800,
                "total_schools": 5,
                "analysis_date": datetime.now().isoformat()
            },
            "academic_subjects": [
                {
                    "subject_id": 1,
                    "subject_name": "语文",
                    "subject_code": "CHN",
                    "statistics": {
                        "total_score": 800,
                        "average_score": 650,
                        "difficulty_coefficient": 0.812,
                        "discrimination": 0.45,
                        "score_distribution": {
                            "excellent": 0.25,
                            "good": 0.35,
                            "pass": 0.30,
                            "fail": 0.10
                        },
                        "percentiles": {
                            "p25": 580,
                            "p50": 650,
                            "p75": 720,
                            "p90": 760
                        }
                    },
                    "dimensions": [
                        {
                            "dimension_id": 1,
                            "dimension_name": "阅读理解",
                            "average_score": 32.5,
                            "max_score": 40,
                            "difficulty_coefficient": 0.813
                        },
                        {
                            "dimension_id": 2,
                            "dimension_name": "语言运用",
                            "average_score": 26.0,
                            "max_score": 35,
                            "difficulty_coefficient": 0.743
                        }
                    ]
                },
                {
                    "subject_id": 2,
                    "subject_name": "数学",
                    "subject_code": "MATH",
                    "statistics": {
                        "total_score": 100,
                        "average_score": 78.5,
                        "difficulty_coefficient": 0.785,
                        "discrimination": 0.52,
                        "score_distribution": {
                            "excellent": 0.30,
                            "good": 0.40,
                            "pass": 0.25,
                            "fail": 0.05
                        },
                        "percentiles": {
                            "p25": 65,
                            "p50": 78,
                            "p75": 92,
                            "p90": 96
                        }
                    },
                    "dimensions": [
                        {
                            "dimension_id": 3,
                            "dimension_name": "数与代数",
                            "average_score": 42.3,
                            "max_score": 50,
                            "difficulty_coefficient": 0.846
                        }
                    ]
                }
            ],
            "regional_summary": {
                "total_subjects": 2,
                "average_participation_rate": 0.95,
                "overall_performance": "良好",
                "improvement_areas": ["语言运用", "几何图形"]
            }
        },
        "data_version": "1.0",
        "total_students": 800,
        "total_schools": 5,
        "change_reason": "测试批次创建",
        "triggered_by": "test_user"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/batches", json=comprehensive_data)
        print(f"响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ 完整数据格式创建成功！")
            result = response.json()
            print(f"批次ID: {result.get('data', {}).get('batch_id')}")
        else:
            print(f"❌ 创建失败: {response.status_code}")
            print(f"错误详情: {response.text}")
            
    except Exception as e:
        print(f"❌ 请求异常: {str(e)}")

def main():
    """主测试函数"""
    print("🏆 批次管理API正确格式测试")
    print(f"测试时间: {datetime.now().isoformat()}")
    print("=" * 60)
    
    # 1. 先测试基础连通性
    test_batch_list()
    
    # 2. 测试最小化数据创建
    test_batch_creation()
    
    # 3. 再次查询确认创建成功
    test_batch_list()
    
    # 4. 测试完整数据格式
    test_comprehensive_data()
    
    # 5. 最终查询
    test_batch_list()
    
    print("\n" + "=" * 60)
    print("🎯 测试完成！")
    print("\n📋 解决方案总结:")
    print("1. 问题根源：statistics_data 字段必须包含 'batch_info' 和 'academic_subjects' 字段")
    print("2. 前4次修复失败是因为修错了地方（修架构而不是数据格式）")
    print("3. 运维说API正常是因为GET请求正常，但POST请求数据格式不对")
    print("4. 使用正确的数据格式即可解决问题")

if __name__ == "__main__":
    main()