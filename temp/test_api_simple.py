#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""简单API测试"""

import requests
import json

def test_api():
    try:
        url = "http://localhost:8000/api/v12/batch/G7-2025/regional?allow_partial=false"
        response = requests.get(url, timeout=10)

        print(f"[INFO] 状态码: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print(f"[INFO] 响应键: {list(data.keys())}")

            if 'data' in data:
                data_content = data['data']
                print(f"[INFO] data内容键: {list(data_content.keys())}")

                # 检查schema_version
                schema_version = data_content.get('schema_version')
                print(f"[INFO] schema_version: {schema_version}")

                # 检查subjects
                subjects = data_content.get('subjects', [])
                print(f"[INFO] subjects数量: {len(subjects)}")

                # 找问卷科目
                questionnaire_subjects = []
                for s in subjects:
                    if s.get('type') == 'questionnaire':
                        questionnaire_subjects.append(s)

                print(f"[INFO] 问卷科目数量: {len(questionnaire_subjects)}")

                if questionnaire_subjects:
                    q_subject = questionnaire_subjects[0]
                    print(f"[INFO] 问卷科目名: {q_subject.get('subject_name', 'Unknown')}")

                    # 检查维度
                    dimensions = q_subject.get('dimensions', [])
                    print(f"[INFO] 维度数量: {len(dimensions)}")

                    if dimensions:
                        first_dim = dimensions[0]
                        print(f"[INFO] 第一个维度名: {first_dim.get('dimension_name', 'Unknown')}")

                        # 检查问题
                        questions = first_dim.get('questions', [])
                        print(f"[INFO] 问题数量: {len(questions)}")

                        # 查找question_id为21_1的问题
                        target_q = None
                        for q in questions:
                            if q.get('question_id') == '21_1':
                                target_q = q
                                break

                        if target_q:
                            print(f"[INFO] 找到问题21_1")
                            option_dist = target_q.get('option_distribution', {})
                            print(f"[INFO] 选项分布: {option_dist}")

                            if option_dist:
                                total = sum(option_dist.values())
                                count = len(option_dist)
                                print(f"[INFO] 选项总和: {total:.2f}, 选项数量: {count}")

                                if 4 <= count <= 5 and 95 <= total <= 105:
                                    print("[SUCCESS] 选项分布符合预期")
                                else:
                                    print("[WARNING] 选项分布不符合预期")
                        else:
                            print("[WARNING] 未找到问题21_1")
                else:
                    print("[WARNING] 未找到问卷科目")
            else:
                print("[ERROR] 响应中没有data字段")
        else:
            print(f"[ERROR] API调用失败: {response.status_code}")
            print(f"[ERROR] 响应: {response.text[:200]}")

    except Exception as e:
        print(f"[ERROR] 测试失败: {e}")

if __name__ == "__main__":
    test_api()