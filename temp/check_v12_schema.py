#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""检查v1.2接口数据格式"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from app.database.connection import get_db_context
from sqlalchemy import text
import json

def check_v12_schema():
    """检查REGIONAL记录的schema_version"""
    print("[CHECK] 验证DB中REGIONAL记录的schema_version...")

    query = text("""
        SELECT id, updated_at,
               JSON_EXTRACT(statistics_data,'$.schema_version') AS ver,
               JSON_KEYS(statistics_data) as json_keys
        FROM statistical_aggregations
        WHERE batch_code='G7-2025' AND aggregation_level='REGIONAL'
        ORDER BY updated_at DESC LIMIT 1
    """)

    try:
        with get_db_context() as db:
            result = db.execute(query)
            row = result.fetchone()

            if not row:
                print("[ERROR] 未找到G7-2025批次的区域级统计记录")
                return False

            record_id, updated_at, version, json_keys = row
            print(f"[INFO] 记录ID: {record_id}")
            print(f"[INFO] 更新时间: {updated_at}")
            print(f"[INFO] Schema版本: {version}")
            print(f"[INFO] JSON顶层字段: {json_keys}")

            if version == '"v1.2"' or version == 'v1.2':
                print("[SUCCESS] 确认为v1.2格式数据")
                return True
            else:
                print(f"[WARNING] 版本不匹配，当前版本: {version}")
                return False

    except Exception as e:
        print(f"[ERROR] 查询失败: {e}")
        return False

def test_v12_api():
    """测试v1.2 API接口"""
    print("\n[CHECK] 测试v1.2 API接口...")

    import requests

    try:
        # 假设服务在localhost:8000运行
        url = "http://localhost:8000/api/v12/batch/G7-2025/regional?allow_partial=false"

        response = requests.get(url, timeout=30)

        if response.status_code == 200:
            data = response.json()

            # 检查schema_version
            schema_version = data.get('schema_version')
            print(f"[INFO] API返回的schema_version: {schema_version}")

            if schema_version == 'v1.2':
                print("[SUCCESS] v1.2 API接口正常工作")

                # 检查是否包含subjects
                subjects = data.get('subjects', [])
                print(f"[INFO] 包含科目数量: {len(subjects)}")

                # 查找问卷科目
                questionnaire_subjects = [s for s in subjects if s.get('subject_type') == 'questionnaire']
                if questionnaire_subjects:
                    print(f"[INFO] 找到问卷科目: {len(questionnaire_subjects)}个")

                    # 检查第一个问卷科目的结构
                    first_q = questionnaire_subjects[0]
                    print(f"[INFO] 问卷科目名称: {first_q.get('subject_name')}")

                    dimensions = first_q.get('dimensions', [])
                    if dimensions:
                        first_dim = dimensions[0]
                        print(f"[INFO] 第一个维度: {first_dim.get('dimension_name')}")

                        questions = first_dim.get('questions', [])
                        target_question = None
                        for q in questions:
                            if q.get('question_id') == '21_1':
                                target_question = q
                                break

                        if target_question:
                            option_dist = target_question.get('option_distribution', {})
                            print(f"[INFO] 问题21_1的选项分布: {option_dist}")

                            if option_dist:
                                total = sum(option_dist.values())
                                print(f"[INFO] 选项分布总和: {total:.2f}")
                                print(f"[INFO] 选项数量: {len(option_dist)}")
                        else:
                            print("[WARNING] 未找到问题21_1")
                    else:
                        print("[WARNING] 问卷科目无维度数据")
                else:
                    print("[WARNING] 未找到问卷类型科目")

                return True
            else:
                print(f"[ERROR] API返回版本不正确: {schema_version}")
                return False
        else:
            print(f"[ERROR] API调用失败，状态码: {response.status_code}")
            print(f"[ERROR] 响应内容: {response.text[:500]}")
            return False

    except requests.exceptions.ConnectionError:
        print("[WARNING] 无法连接到API服务，可能服务未启动")
        return False
    except Exception as e:
        print(f"[ERROR] API测试失败: {e}")
        return False

def main():
    print("[START] 检查v1.2接口数据格式...")

    # 检查数据库中的数据格式
    db_ok = check_v12_schema()

    # 测试API接口
    api_ok = test_v12_api()

    print("\n" + "="*50)
    print("[SUMMARY] v1.2格式检查结果:")
    print(f"  数据库记录格式: {'[OK]' if db_ok else '[ERROR]'}")
    print(f"  API接口响应: {'[OK]' if api_ok else '[ERROR]'}")

    overall_ok = db_ok and api_ok
    print(f"\n[RESULT] v1.2格式确认: {'[SUCCESS]' if overall_ok else '[FAILED]'}")

    return overall_ok

if __name__ == "__main__":
    main()