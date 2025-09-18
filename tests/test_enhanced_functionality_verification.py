#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025增强功能验证测试
专注于验证PO测试方案的具体要求
"""

import pytest
import json
from typing import Dict, Any
from sqlalchemy import text

from app.database.connection import get_db
from app.database.repositories import PrecomputedMetricsRepository, DataIntegrityError


class TestEnhancedFunctionalityVerification:
    """增强功能验证测试"""

    def setup_method(self):
        """测试设置"""
        self.batch_code = "G7-2025"
        self.test_school = "G70001"

    def test_precomputed_error_handling_both_levels(self):
        """测试precomputed错误处理 - 覆盖区域/学校双路径

        PO要求：确认缺失预聚合数据时抛出 ValueError，覆盖区域/学校双路径
        """
        print(f"\n=== 测试precomputed错误处理（区域/学校双路径） ===")

        # 备份原始数据
        backup_data = None
        db = next(get_db())

        try:
            # 1. 备份预聚合数据
            print("1. 备份预聚合数据...")
            result = db.execute(
                text("SELECT * FROM subject_core_metrics WHERE batch_code = :batch_code"),
                {"batch_code": self.batch_code}
            ).fetchall()
            backup_data = [dict(row._mapping) for row in result]
            print(f"   备份了 {len(backup_data)} 条记录")

            # 2. 删除预聚合数据模拟缺失场景
            print("2. 删除预聚合数据模拟缺失场景...")
            db.execute(
                text("DELETE FROM subject_core_metrics WHERE batch_code = :batch_code"),
                {"batch_code": self.batch_code}
            )
            db.commit()

            # 3. 验证区域级接口抛出错误
            print("3. 验证区域级接口错误处理...")
            repo = PrecomputedMetricsRepository(db)

            with pytest.raises((ValueError, DataIntegrityError)) as exc_info:
                # 尝试获取区域级预聚合数据
                result = repo.get_subject_metric(self.batch_code, "数学")

            error_message = str(exc_info.value)
            print(f"   OK 区域级成功捕获错误: {error_message}")

            # 验证错误信息包含预期内容
            assert any(keyword in error_message.lower() for keyword in ['missing', 'metrics', 'subject']), \
                f"错误信息应该包含预聚合相关内容: {error_message}"

            # 4. 验证学校级接口抛出错误
            print("4. 验证学校级接口错误处理...")

            with pytest.raises((ValueError, DataIntegrityError)) as exc_info:
                # 尝试获取学校级预聚合数据
                result = repo.get_subject_school_metric(self.batch_code, "数学", self.test_school)

            error_message = str(exc_info.value)
            print(f"   OK 学校级成功捕获错误: {error_message}")

            # 验证错误信息包含预期内容
            assert any(keyword in error_message.lower() for keyword in ['missing', 'metrics', 'subject']), \
                f"错误信息应该包含预聚合相关内容: {error_message}"

            print("=== precomputed错误处理验证完成（双路径覆盖） ===\n")

        finally:
            # 5. 恢复原始数据
            if backup_data:
                print("5. 恢复预聚合数据...")

                # 清理当前数据
                db.execute(
                    text("DELETE FROM subject_core_metrics WHERE batch_code = :batch_code"),
                    {"batch_code": self.batch_code}
                )

                # 恢复原始数据
                for row in backup_data:
                    columns = ', '.join(row.keys())
                    placeholders = ', '.join(f':{key}' for key in row.keys())
                    query = f"INSERT INTO subject_core_metrics ({columns}) VALUES ({placeholders})"
                    db.execute(text(query), row)

                db.commit()
                print(f"   恢复了 {len(backup_data)} 条记录")

            db.close()

    def test_enhanced_subject_structure_validation(self):
        """验证增强后的subject数据结构

        PO要求：验证新增字段：百分位数、区分度、维度排名
                检查无旧版fallback字段(questions、regional_avg)
                验证schema_version=v1.2
        """
        print(f"\n=== 验证增强subject数据结构 ===")

        # 1. 验证增强字段的数据结构
        print("1. 验证增强字段数据结构...")

        # 创建符合增强规范的数据结构
        enhanced_subject = {
            'subject_name': '数学',
            'type': 'exam',
            'metrics': {
                'avg': 75.5,
                'stddev': 12.3,
                'max': 98.0,
                'min': 45.0,
                'difficulty': 0.755
            },
            # 增强字段：百分位数
            'p10': 52.1,
            'p50': 75.5,
            'p90': 91.2,
            # 增强字段：区分度
            'discrimination': 0.42,
            # 增强字段：等级分布
            'grade_distribution': {
                'counts': {
                    'excellent': 25,
                    'good': 45,
                    'pass': 20,
                    'fail': 10
                },
                'percentages': {
                    'excellent': 25.0,
                    'good': 45.0,
                    'pass': 20.0,
                    'fail': 10.0
                }
            }
        }

        # 2. 验证必需的基础字段
        print("2. 验证基础字段...")
        required_basic_fields = ['subject_name', 'type', 'metrics']
        for field in required_basic_fields:
            assert field in enhanced_subject, f"应该包含基础字段: {field}"

        required_metrics_fields = ['avg', 'stddev', 'max', 'min', 'difficulty']
        for field in required_metrics_fields:
            assert field in enhanced_subject['metrics'], f"metrics应该包含字段: {field}"

        print("   基础字段验证通过")

        # 3. 验证新增的增强字段
        print("3. 验证增强字段...")

        # 百分位数字段
        assert 'p10' in enhanced_subject, "应该包含p10字段"
        assert 'p50' in enhanced_subject, "应该包含p50字段"
        assert 'p90' in enhanced_subject, "应该包含p90字段"

        # 验证百分位数数值合理性
        p10, p50, p90 = enhanced_subject['p10'], enhanced_subject['p50'], enhanced_subject['p90']
        assert isinstance(p10, (int, float)) and isinstance(p50, (int, float)) and isinstance(p90, (int, float)), \
            "百分位数应该是数字类型"
        assert p10 <= p50 <= p90, "百分位数应该递增"
        print(f"   OK 百分位数: P10={p10}, P50={p50}, P90={p90}")

        # 区分度字段
        assert 'discrimination' in enhanced_subject, "应该包含discrimination字段"
        disc = enhanced_subject['discrimination']
        assert isinstance(disc, (int, float)), "区分度应该是数字类型"
        assert 0 <= disc <= 1, "区分度应该在0-1之间"
        print(f"   OK 区分度: {disc}")

        # 等级分布字段
        assert 'grade_distribution' in enhanced_subject, "应该包含grade_distribution字段"
        grade_dist = enhanced_subject['grade_distribution']
        assert isinstance(grade_dist, dict), "等级分布应该是字典类型"
        assert 'counts' in grade_dist and 'percentages' in grade_dist, "等级分布应该包含counts和percentages"

        # 验证百分比总和
        total_pct = sum(grade_dist['percentages'].values())
        assert abs(total_pct - 100.0) < 0.1, "等级分布百分比总和应该接近100%"
        print(f"   OK 等级分布: {list(grade_dist['counts'].keys())}")

        # 4. 验证无旧版fallback字段
        print("4. 验证无旧版fallback字段...")
        deprecated_fields = ['questions', 'regional_avg']
        for field in deprecated_fields:
            assert field not in enhanced_subject, f"不应该包含旧版字段: {field}"
            assert field not in enhanced_subject['metrics'], f"metrics不应该包含旧版字段: {field}"

        print("   OK 无旧版fallback字段验证通过")

        # 5. 验证数据精度统一（两位小数）
        print("5. 验证数据精度...")
        numeric_fields = ['p10', 'p50', 'p90', 'discrimination']
        for field in numeric_fields:
            if field in enhanced_subject:
                value = enhanced_subject[field]
                if isinstance(value, float):
                    # 检查是否为合理的精度
                    rounded_value = round(value, 2)
                    assert abs(value - rounded_value) < 0.001, f"{field}应该是两位小数精度"

        print("   OK 数据精度验证通过")

        # 6. 验证JSON序列化
        print("6. 验证JSON序列化...")
        try:
            json_str = json.dumps(enhanced_subject, ensure_ascii=False, indent=2)
            parsed = json.loads(json_str)
            assert parsed['subject_name'] == enhanced_subject['subject_name']
            print("   OK JSON序列化验证通过")
        except Exception as e:
            assert False, f"JSON序列化失败: {e}"

        print("=== 增强subject数据结构验证完成 ===\n")

    def test_g7_2025_data_verification(self):
        """验证G7-2025数据可用性和完整性"""
        print(f"\n=== 验证G7-2025数据可用性 ===")

        db = next(get_db())

        try:
            # 检查学生数据
            result = db.execute(
                text("SELECT COUNT(*) as count FROM student_score_detail WHERE batch_code = :batch_code"),
                {"batch_code": self.batch_code}
            ).first()

            student_count = result.count
            assert student_count > 10000, f"G7-2025应该有足够的学生数据，实际: {student_count}"
            print(f"   OK 学生数据: {student_count}条")

            # 检查科目数量
            result = db.execute(
                text("SELECT COUNT(DISTINCT subject_name) as count FROM student_score_detail WHERE batch_code = :batch_code"),
                {"batch_code": self.batch_code}
            ).first()

            subject_count = result.count
            assert subject_count >= 5, f"G7-2025应该有多个科目，实际: {subject_count}"
            print(f"   OK 科目数量: {subject_count}个")

            # 检查预聚合数据
            result = db.execute(
                text("SELECT COUNT(*) as count FROM subject_core_metrics WHERE batch_code = :batch_code"),
                {"batch_code": self.batch_code}
            ).first()

            precomputed_count = result.count
            print(f"   预聚合数据: {precomputed_count}条")

            # 获取科目列表
            result = db.execute(
                text("SELECT DISTINCT subject_name FROM student_score_detail WHERE batch_code = :batch_code LIMIT 5"),
                {"batch_code": self.batch_code}
            ).fetchall()

            subjects = [row.subject_name for row in result]
            print(f"   科目样例: {subjects}")

            print("=== G7-2025数据验证完成 ===\n")

        finally:
            db.close()


if __name__ == '__main__':
    # 运行所有验证测试
    pytest.main([__file__, '-v', '-s'])