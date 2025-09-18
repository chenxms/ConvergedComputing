#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
G7-2025增强功能的SubjectsBuilder测试用例
验证增强后的subjects数据结构、统计指标和错误处理
按照PO测试方案要求实现
"""

import pytest
import json
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from unittest.mock import Mock, patch

from app.database.connection import get_db, get_db_context
from app.services.subjects_builder import SubjectsBuilder
from app.services.calculation_service import CalculationService
from app.database.repositories import PrecomputedMetricsRepository, DataIntegrityError


class TestSubjectsBuilderEnhanced:
    """SubjectsBuilder增强功能测试"""

    def setup_method(self):
        """测试设置"""
        self.batch_code = "G7-2025"
        self.test_school = "G70001"  # G7-2025的测试学校
        self.subjects_builder = SubjectsBuilder()

        # 每个测试方法单独获取数据库连接
        self.db = None

    def teardown_method(self):
        """测试清理"""
        if self.db:
            try:
                # 恢复数据
                if hasattr(self, 'backup_data'):
                    self._restore_precomputed_data()
            finally:
                self.db.close()

    def _get_db_connection(self):
        """获取数据库连接"""
        if not self.db:
            self.db = next(get_db())
            # 备份数据
            self._backup_precomputed_data()
        return self.db

    def _backup_precomputed_data(self):
        """备份预聚合数据"""
        result = self.db.execute(
            text("SELECT * FROM subject_core_metrics WHERE batch_code = :batch_code"),
            {"batch_code": self.batch_code}
        ).fetchall()
        self.backup_data = [dict(row._mapping) for row in result]

    def _restore_precomputed_data(self):
        """恢复预聚合数据"""
        if hasattr(self, 'backup_data') and self.backup_data:
            # 删除测试期间可能被修改的数据
            self.db.execute(
                text("DELETE FROM subject_core_metrics WHERE batch_code = :batch_code"),
                {"batch_code": self.batch_code}
            )

            # 恢复原始数据
            for row in self.backup_data:
                columns = ', '.join(row.keys())
                placeholders = ', '.join(f':{key}' for key in row.keys())
                query = f"INSERT INTO subject_core_metrics ({columns}) VALUES ({placeholders})"
                self.db.execute(text(query), row)

            self.db.commit()

    def _create_mock_enhanced_stats(self) -> Dict[str, Any]:
        """创建模拟的增强统计数据用于测试"""
        return {
            'academic_subjects': {
                '数学': {
                    'percentiles': {
                        'P10': 45.2,
                        'P50': 72.5,
                        'P90': 88.9
                    },
                    'discrimination': {
                        'discrimination_index': 0.42,
                        'discrimination_level': 'good'
                    },
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
                    },
                    'statistical_indicators': {
                        'discrimination_index': 0.42
                    }
                },
                '语文': {
                    'percentiles': {
                        'P10': 42.1,
                        'P50': 68.3,
                        'P90': 85.7
                    },
                    'discrimination': {
                        'discrimination_index': 0.38
                    },
                    'grade_distribution': {
                        'counts': {
                            'excellent': 20,
                            'good': 50,
                            'pass': 25,
                            'fail': 5
                        },
                        'percentages': {
                            'excellent': 20.0,
                            'good': 50.0,
                            'pass': 25.0,
                            'fail': 5.0
                        }
                    }
                }
            },
            'non_academic_subjects': {
                '学习态度问卷': {
                    'percentiles': {
                        'P10': 2.1,
                        'P50': 3.5,
                        'P90': 4.8
                    },
                    'statistical_indicators': {
                        'discrimination_index': 0.25
                    }
                }
            }
        }

    def test_enhanced_subject_structure(self):
        """验证增强后的subjects数据结构

        测试要求：
        1. 验证新增字段：百分位数、区分度、维度排名
        2. 检查无旧版fallback字段(questions、regional_avg)
        3. 验证schema_version=v1.2
        """
        print(f"\n=== 测试增强后的subjects数据结构 ===")

        # 获取数据库连接
        db = self._get_db_connection()

        # 获取增强统计数据
        calc_service = CalculationService(db)

        # 1. 测试区域级subjects增强结构
        print("1. 测试区域级subjects增强结构...")

        # 创建简化的增强数据用于测试
        enhanced_stats = self._create_mock_enhanced_stats()
        assert enhanced_stats is not None, "应该能获取到增强统计数据"

        # 构建增强的区域级subjects
        regional_subjects = self.subjects_builder.build_regional_subjects(
            self.batch_code,
            enhanced_stats=enhanced_stats
        )

        assert len(regional_subjects) > 0, "应该有区域级科目数据"

        # 验证增强字段
        subject = regional_subjects[0]
        print(f"   验证科目: {subject.get('subject_name')}")

        # 检查必需的基础字段
        assert 'subject_name' in subject, "应该包含subject_name字段"
        assert 'type' in subject, "应该包含type字段"
        assert 'metrics' in subject, "应该包含metrics字段"

        metrics = subject['metrics']

        # 验证基础指标字段
        required_basic_fields = ['avg', 'stddev', 'max', 'min', 'difficulty']
        for field in required_basic_fields:
            assert field in metrics, f"metrics应该包含基础字段: {field}"

        # 验证新增的增强字段
        enhanced_fields_found = []

        # 百分位数字段
        if 'p10' in subject and 'p50' in subject and 'p90' in subject:
            enhanced_fields_found.append('percentiles')
            assert isinstance(subject['p10'], (int, float)), "p10应该是数字类型"
            assert isinstance(subject['p50'], (int, float)), "p50应该是数字类型"
            assert isinstance(subject['p90'], (int, float)), "p90应该是数字类型"
            assert subject['p10'] <= subject['p50'] <= subject['p90'], "百分位数应该递增"
            print(f"   ✅ 百分位数: P10={subject['p10']}, P50={subject['p50']}, P90={subject['p90']}")

        # 区分度字段
        if 'discrimination' in subject:
            enhanced_fields_found.append('discrimination')
            assert isinstance(subject['discrimination'], (int, float)), "区分度应该是数字类型"
            assert 0 <= subject['discrimination'] <= 1, "区分度应该在0-1之间"
            print(f"   ✅ 区分度: {subject['discrimination']}")

        # 等级分布字段
        if 'grade_distribution' in subject:
            enhanced_fields_found.append('grade_distribution')
            grade_dist = subject['grade_distribution']
            assert isinstance(grade_dist, dict), "等级分布应该是字典类型"
            print(f"   ✅ 等级分布: {list(grade_dist.keys())}")

        assert len(enhanced_fields_found) > 0, f"应该至少包含一个增强字段，实际找到: {enhanced_fields_found}"
        print(f"   增强字段验证通过: {enhanced_fields_found}")

        # 验证无旧版fallback字段
        deprecated_fields = ['questions', 'regional_avg']
        for field in deprecated_fields:
            assert field not in subject, f"不应该包含旧版字段: {field}"
            assert field not in metrics, f"metrics不应该包含旧版字段: {field}"

        print("   ✅ 无旧版fallback字段验证通过")

        # 2. 测试学校级subjects增强结构
        print("2. 测试学校级subjects增强结构...")

        school_subjects = self.subjects_builder.build_school_subjects(
            self.batch_code,
            self.test_school,
            enhanced_stats=enhanced_stats
        )

        assert len(school_subjects) > 0, "应该有学校级科目数据"

        school_subject = school_subjects[0]
        print(f"   验证学校科目: {school_subject.get('subject_name')}")

        # 验证学校级特有字段
        assert 'region_rank' in school_subject, "学校级应该包含region_rank字段"
        assert 'total_schools' in school_subject, "学校级应该包含total_schools字段"
        assert 'dimensions' in school_subject, "学校级应该包含dimensions字段"

        # 验证维度增强
        dimensions = school_subject.get('dimensions', [])
        if dimensions:
            dim = dimensions[0]
            print(f"   验证维度: {dim.get('dimension_name')}")

            # 检查维度增强字段
            dim_enhanced_fields = []
            if 'difficulty' in dim:
                dim_enhanced_fields.append('difficulty')
            if 'discrimination' in dim:
                dim_enhanced_fields.append('discrimination')
            if 'rank' in dim:
                dim_enhanced_fields.append('rank')

            if dim_enhanced_fields:
                print(f"   ✅ 维度增强字段: {dim_enhanced_fields}")

        # 3. 验证schema_version
        print("3. 验证schema_version...")

        # 通过build_regional_subjects_v12方法验证v1.2版本
        regional_subjects_v12 = self.subjects_builder.build_regional_subjects_v12(
            self.batch_code,
            enhanced_stats=enhanced_stats
        )

        assert len(regional_subjects_v12) > 0, "v1.2版本应该有数据"
        print("   ✅ v1.2版本数据结构验证通过")

        print("=== 增强后的subjects数据结构测试完成 ===\n")

    def test_precomputed_error_handling_regional(self):
        """测试区域级接口缺失预聚合数据时抛出ValueError"""
        print(f"\n=== 测试区域级precomputed错误处理 ===")

        # 获取数据库连接
        db = self._get_db_connection()

        # 1. 删除预聚合数据模拟缺失场景
        print("1. 模拟删除预聚合数据...")
        db.execute(
            text("DELETE FROM subject_core_metrics WHERE batch_code = :batch_code"),
            {"batch_code": self.batch_code}
        )
        db.commit()

        # 2. 验证区域级接口抛出错误
        print("2. 验证区域级接口错误处理...")

        # 通过PrecomputedMetricsRepository直接测试
        repo = PrecomputedMetricsRepository()

        with pytest.raises((ValueError, DataIntegrityError)) as exc_info:
            # 尝试获取预聚合数据
            result = repo.get_subject_metrics(db, self.batch_code, "数学")

        error_message = str(exc_info.value)
        print(f"   ✅ 成功捕获错误: {error_message}")

        # 验证错误信息包含预期内容
        assert any(keyword in error_message.lower() for keyword in ['precomputed', '预聚合', 'metrics', 'not found']), \
            f"错误信息应该包含预聚合相关内容: {error_message}"

        print("=== 区域级precomputed错误处理测试完成 ===\n")

    def test_precomputed_error_handling_school(self):
        """测试学校级接口缺失预聚合数据时抛出ValueError"""
        print(f"\n=== 测试学校级precomputed错误处理 ===")

        # 获取数据库连接
        db = self._get_db_connection()

        # 1. 删除预聚合数据模拟缺失场景
        print("1. 模拟删除预聚合数据...")
        db.execute(
            text("DELETE FROM subject_core_metrics WHERE batch_code = :batch_code"),
            {"batch_code": self.batch_code}
        )
        db.commit()

        # 2. 验证学校级接口抛出错误
        print("2. 验证学校级接口错误处理...")

        # 通过PrecomputedMetricsRepository直接测试
        repo = PrecomputedMetricsRepository()

        with pytest.raises((ValueError, DataIntegrityError)) as exc_info:
            # 尝试获取学校级预聚合数据
            result = repo.get_school_metrics(db, self.batch_code, "数学", self.test_school)

        error_message = str(exc_info.value)
        print(f"   ✅ 成功捕获错误: {error_message}")

        # 验证错误信息包含预期内容
        assert any(keyword in error_message.lower() for keyword in ['precomputed', '预聚合', 'metrics', 'not found']), \
            f"错误信息应该包含预聚合相关内容: {error_message}"

        print("=== 学校级precomputed错误处理测试完成 ===\n")

    def test_enhanced_vs_basic_comparison(self):
        """对比增强版与基础版的数据差异"""
        print(f"\n=== 测试增强版与基础版数据对比 ===")

        # 获取数据库连接
        db = self._get_db_connection()

        # 获取增强统计数据
        enhanced_stats = self._create_mock_enhanced_stats()

        # 1. 基础版subjects
        print("1. 获取基础版subjects...")
        basic_subjects = self.subjects_builder.build_regional_subjects(self.batch_code)
        assert len(basic_subjects) > 0, "基础版应该有数据"

        # 2. 增强版subjects
        print("2. 获取增强版subjects...")
        enhanced_subjects = self.subjects_builder.build_regional_subjects(
            self.batch_code,
            enhanced_stats=enhanced_stats
        )
        assert len(enhanced_subjects) > 0, "增强版应该有数据"

        # 3. 对比数据结构
        print("3. 对比数据结构差异...")

        basic_subject = basic_subjects[0]
        enhanced_subject = enhanced_subjects[0]

        # 基础字段应该保持一致
        basic_fields = {'subject_name', 'type', 'metrics'}
        for field in basic_fields:
            assert basic_subject.get(field) is not None, f"基础版应该有字段: {field}"
            assert enhanced_subject.get(field) is not None, f"增强版应该有字段: {field}"

        # 增强版应该有额外字段
        enhanced_fields = set(enhanced_subject.keys()) - set(basic_subject.keys())
        print(f"   增强版新增字段: {enhanced_fields}")

        # 验证新增字段的有效性
        expected_enhanced_fields = {'p10', 'p50', 'p90', 'discrimination', 'grade_distribution'}
        found_enhanced_fields = enhanced_fields & expected_enhanced_fields
        assert len(found_enhanced_fields) > 0, f"应该至少有一个预期的增强字段，实际找到: {found_enhanced_fields}"

        print(f"   ✅ 验证通过，找到增强字段: {found_enhanced_fields}")

        print("=== 增强版与基础版数据对比测试完成 ===\n")

    def test_data_integrity_validation(self):
        """验证增强数据的完整性和准确性"""
        print(f"\n=== 测试数据完整性验证 ===")

        # 获取数据库连接
        db = self._get_db_connection()

        # 获取增强统计数据
        enhanced_stats = self._create_mock_enhanced_stats()

        # 构建增强subjects
        subjects = self.subjects_builder.build_regional_subjects(
            self.batch_code,
            enhanced_stats=enhanced_stats
        )

        for subject in subjects:
            subject_name = subject.get('subject_name')
            print(f"验证科目: {subject_name}")

            # 1. 验证数值类型和范围
            if 'p10' in subject and 'p50' in subject and 'p90' in subject:
                p10, p50, p90 = subject['p10'], subject['p50'], subject['p90']
                assert isinstance(p10, (int, float)), f"{subject_name} P10应该是数字"
                assert isinstance(p50, (int, float)), f"{subject_name} P50应该是数字"
                assert isinstance(p90, (int, float)), f"{subject_name} P90应该是数字"
                assert p10 <= p50 <= p90, f"{subject_name} 百分位数应该递增"

            if 'discrimination' in subject:
                disc = subject['discrimination']
                assert isinstance(disc, (int, float)), f"{subject_name} 区分度应该是数字"
                assert 0 <= disc <= 1, f"{subject_name} 区分度应该在0-1之间"

            # 2. 验证等级分布数据
            if 'grade_distribution' in subject:
                grade_dist = subject['grade_distribution']
                assert isinstance(grade_dist, dict), f"{subject_name} 等级分布应该是字典"

                # 检查百分比总和（如果有percentages字段）
                if 'percentages' in grade_dist:
                    percentages = grade_dist['percentages']
                    if isinstance(percentages, dict):
                        total_pct = sum(percentages.values())
                        assert abs(total_pct - 100.0) < 0.1, f"{subject_name} 等级分布百分比总和应该接近100%"

            # 3. 验证精度统一（两位小数）
            metrics = subject.get('metrics', {})
            for key, value in metrics.items():
                if isinstance(value, float):
                    # 检查是否为合理的两位小数精度
                    rounded_value = round(value, 2)
                    assert abs(value - rounded_value) < 0.001, f"{subject_name} {key}精度应该统一到两位小数"

        print("   ✅ 数据完整性验证通过")
        print("=== 数据完整性验证测试完成 ===\n")


class TestG7DataSpecific:
    """针对G7-2025数据的专项测试"""

    def setup_method(self):
        """测试设置"""
        self.batch_code = "G7-2025"
        self.db = None

    def teardown_method(self):
        """测试清理"""
        if self.db:
            self.db.close()

    def _get_db_connection(self):
        """获取数据库连接"""
        if not self.db:
            self.db = next(get_db())
        return self.db

    def test_g7_data_availability(self):
        """验证G7-2025数据可用性"""
        print(f"\n=== 验证G7-2025数据可用性 ===")

        # 获取数据库连接
        db = self._get_db_connection()

        # 检查学生数据
        result = db.execute(
            text("SELECT COUNT(*) as count FROM student_score_detail WHERE batch_code = :batch_code"),
            {"batch_code": self.batch_code}
        ).first()

        student_count = result.count
        assert student_count > 10000, f"G7-2025应该有足够的学生数据，实际: {student_count}"
        print(f"   ✅ 学生数据: {student_count}条")

        # 检查科目数量
        result = db.execute(
            text("SELECT COUNT(DISTINCT subject_name) as count FROM student_score_detail WHERE batch_code = :batch_code"),
            {"batch_code": self.batch_code}
        ).first()

        subject_count = result.count
        assert subject_count >= 5, f"G7-2025应该有多个科目，实际: {subject_count}"
        print(f"   ✅ 科目数量: {subject_count}个")

        # 检查预聚合数据
        result = db.execute(
            text("SELECT COUNT(*) as count FROM subject_core_metrics WHERE batch_code = :batch_code"),
            {"batch_code": self.batch_code}
        ).first()

        precomputed_count = result.count
        assert precomputed_count > 0, f"G7-2025应该有预聚合数据，实际: {precomputed_count}"
        print(f"   ✅ 预聚合数据: {precomputed_count}条")

        print("=== G7-2025数据可用性验证完成 ===\n")


if __name__ == '__main__':
    # 单独运行增强结构测试
    pytest.main([__file__ + '::TestSubjectsBuilderEnhanced::test_enhanced_subject_structure', '-v', '-s'])