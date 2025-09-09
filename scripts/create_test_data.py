#!/usr/bin/env python3
"""
创建测试数据脚本
为系统生成完整的测试数据，包括学生答题记录、科目配置、维度映射等
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database.connection import get_session_factory
from app.database.models import StatisticalMetadata
import random
import json
from datetime import datetime, timedelta


def create_statistical_metadata():
    """创建统计元数据"""
    print("🔍 创建统计元数据...")
    
    try:
        SessionLocal = get_session_factory()
        
        with SessionLocal() as session:
            # 检查是否已有测试元数据
            existing = session.query(StatisticalMetadata).filter(
                StatisticalMetadata.metadata_key == "test_batch_config"
            ).first()
            
            if existing:
                print("✅ 测试元数据已存在")
                return True
            
            # 创建测试批次配置
            test_config = StatisticalMetadata(
                metadata_key="test_batch_config",
                metadata_value={
                    "batch_code": "TEST_2025_001",
                    "subjects": ["数学", "语文", "英语", "科学"],
                    "grade_levels": ["3rd_grade", "4th_grade", "5th_grade"],
                    "total_students": 1000,
                    "schools": ["TEST_SCHOOL_001", "TEST_SCHOOL_002", "TEST_SCHOOL_003"]
                },
                description="测试批次基础配置"
            )
            
            # 创建科目配置
            subject_config = StatisticalMetadata(
                metadata_key="test_subject_config",
                metadata_value={
                    "数学": {
                        "max_score": 100,
                        "question_count": 20,
                        "dimensions": ["数值运算", "几何图形", "应用题"]
                    },
                    "语文": {
                        "max_score": 100,
                        "question_count": 25,
                        "dimensions": ["阅读理解", "语言文字", "写作表达"]
                    },
                    "英语": {
                        "max_score": 100,
                        "question_count": 30,
                        "dimensions": ["听力理解", "语法词汇", "阅读能力"]
                    },
                    "科学": {
                        "max_score": 100,
                        "question_count": 15,
                        "dimensions": ["观察实验", "科学思维", "知识理解"]
                    }
                },
                description="测试科目配置信息"
            )
            
            # 创建问卷配置
            survey_config = StatisticalMetadata(
                metadata_key="test_survey_config",
                metadata_value={
                    "dimensions": {
                        "好奇心": {
                            "questions": ["Q1", "Q2", "Q3"],
                            "forward_questions": ["Q1", "Q3"],
                            "reverse_questions": ["Q2"]
                        },
                        "观察能力": {
                            "questions": ["Q4", "Q5", "Q6"],
                            "forward_questions": ["Q4", "Q6"],
                            "reverse_questions": ["Q5"]
                        }
                    },
                    "scale_type": "likert_5",
                    "total_questions": 6
                },
                description="测试问卷配置信息"
            )
            
            session.add_all([test_config, subject_config, survey_config])
            session.commit()
            
            print("✅ 统计元数据创建成功")
            return True
            
    except Exception as e:
        print(f"❌ 统计元数据创建失败: {str(e)}")
        return False


def generate_student_scores():
    """生成学生成绩数据"""
    print("🔍 生成学生成绩测试数据...")
    
    try:
        # 这里生成内存中的测试数据，用于后续计算测试
        subjects = ["数学", "语文", "英语", "科学"]
        grade_levels = ["3rd_grade", "4th_grade", "5th_grade"]
        schools = ["TEST_SCHOOL_001", "TEST_SCHOOL_002", "TEST_SCHOOL_003"]
        
        student_data = []
        
        for i in range(100):  # 生成100个测试学生
            student_id = f"STU_{i+1:04d}"
            grade_level = random.choice(grade_levels)
            school_id = random.choice(schools)
            
            student_record = {
                "student_id": student_id,
                "grade_level": grade_level,
                "school_id": school_id,
                "scores": {}
            }
            
            # 为每个科目生成分数
            for subject in subjects:
                # 根据年级调整分数分布
                if grade_level == "3rd_grade":
                    base_score = random.normalvariate(75, 15)
                elif grade_level == "4th_grade":
                    base_score = random.normalvariate(80, 12)
                else:  # 5th_grade
                    base_score = random.normalvariate(85, 10)
                
                # 限制分数范围
                score = max(0, min(100, int(base_score)))
                student_record["scores"][subject] = score
            
            student_data.append(student_record)
        
        # 保存到临时文件
        with open("test_student_data.json", "w", encoding="utf-8") as f:
            json.dump(student_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 学生成绩数据生成成功，共 {len(student_data)} 条记录")
        print("   数据已保存到: test_student_data.json")
        return True, student_data
        
    except Exception as e:
        print(f"❌ 学生成绩数据生成失败: {str(e)}")
        return False, []


def generate_survey_data():
    """生成问卷调查数据"""
    print("🔍 生成问卷调查测试数据...")
    
    try:
        survey_data = []
        
        for i in range(100):
            student_id = f"STU_{i+1:04d}"
            
            # 生成6个问题的答案（1-5分）
            responses = {}
            for q in range(1, 7):
                # 模拟真实问卷响应分布
                if random.random() < 0.1:  # 10%极端回答
                    responses[f"Q{q}"] = random.choice([1, 5])
                else:  # 90%正常分布
                    responses[f"Q{q}"] = random.choices([1, 2, 3, 4, 5], 
                                                      weights=[5, 15, 40, 30, 10])[0]
            
            survey_record = {
                "student_id": student_id,
                "responses": responses,
                "completion_time": random.randint(60, 300)  # 1-5分钟
            }
            
            survey_data.append(survey_record)
        
        # 保存到临时文件
        with open("test_survey_data.json", "w", encoding="utf-8") as f:
            json.dump(survey_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 问卷调查数据生成成功，共 {len(survey_data)} 条记录")
        print("   数据已保存到: test_survey_data.json")
        return True, survey_data
        
    except Exception as e:
        print(f"❌ 问卷调查数据生成失败: {str(e)}")
        return False, []


def create_calculation_test_data():
    """创建计算引擎测试数据"""
    print("🔍 创建计算引擎测试数据...")
    
    try:
        # 加载学生数据
        with open("test_student_data.json", "r", encoding="utf-8") as f:
            student_data = json.load(f)
        
        # 加载问卷数据
        with open("test_survey_data.json", "r", encoding="utf-8") as f:
            survey_data = json.load(f)
        
        # 为计算引擎准备数据格式
        calc_test_data = {
            "batch_code": "TEST_2025_001",
            "subjects": {
                "数学": [s["scores"]["数学"] for s in student_data],
                "语文": [s["scores"]["语文"] for s in student_data],
                "英语": [s["scores"]["英语"] for s in student_data],
                "科学": [s["scores"]["科学"] for s in student_data]
            },
            "survey_responses": survey_data,
            "metadata": {
                "total_students": len(student_data),
                "subjects_count": 4,
                "max_score": 100,
                "grade_levels": ["3rd_grade", "4th_grade", "5th_grade"]
            }
        }
        
        # 保存计算测试数据
        with open("calculation_test_data.json", "w", encoding="utf-8") as f:
            json.dump(calc_test_data, f, ensure_ascii=False, indent=2)
        
        print("✅ 计算引擎测试数据创建成功")
        print("   数据已保存到: calculation_test_data.json")
        
        # 显示数据概览
        print("\n📊 测试数据概览:")
        print(f"   总学生数: {calc_test_data['metadata']['total_students']}")
        print(f"   科目数: {calc_test_data['metadata']['subjects_count']}")
        for subject, scores in calc_test_data['subjects'].items():
            avg_score = sum(scores) / len(scores)
            print(f"   {subject}平均分: {avg_score:.1f}")
        
        return True
        
    except Exception as e:
        print(f"❌ 计算引擎测试数据创建失败: {str(e)}")
        return False


def cleanup_old_test_data():
    """清理旧的测试数据"""
    print("🔍 清理旧的测试数据...")
    
    files_to_remove = [
        "test_student_data.json",
        "test_survey_data.json", 
        "calculation_test_data.json"
    ]
    
    for file_name in files_to_remove:
        if os.path.exists(file_name):
            os.remove(file_name)
            print(f"   清理: {file_name}")
    
    print("✅ 旧测试数据清理完成")


def main():
    print("=" * 60)
    print("🚀 Data-Calculation 测试数据创建")
    print("=" * 60)
    
    # 清理旧数据
    cleanup_old_test_data()
    
    # 运行数据创建任务
    tasks = [
        ("创建统计元数据", create_statistical_metadata),
        ("生成学生成绩数据", lambda: generate_student_scores()[0]),
        ("生成问卷调查数据", lambda: generate_survey_data()[0]),
        ("创建计算测试数据", create_calculation_test_data)
    ]
    
    results = []
    for task_name, task_func in tasks:
        print(f"\n📋 {task_name}")
        print("-" * 40)
        result = task_func()
        results.append((task_name, result))
    
    # 总结结果
    print("\n" + "=" * 60)
    print("📊 测试数据创建总结")
    print("=" * 60)
    
    all_passed = True
    for task_name, passed in results:
        status = "✅ 完成" if passed else "❌ 失败"
        print(f"   {task_name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 所有测试数据创建完成！")
        print("\n📋 生成的文件:")
        print("   - test_student_data.json: 学生成绩数据")
        print("   - test_survey_data.json: 问卷调查数据")
        print("   - calculation_test_data.json: 计算引擎测试数据")
        print("\n🚀 现在可以运行端到端测试:")
        print("   python scripts/end_to_end_test.py")
    else:
        print("\n❌ 部分测试数据创建失败，请检查错误信息。")
        sys.exit(1)


if __name__ == "__main__":
    main()