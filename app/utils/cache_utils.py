#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
缓存工具类：键规范化、分层统计、完整性审计
"""

from typing import Any, Dict, List, Optional, Tuple
import difflib
import unicodedata
import re
from collections import defaultdict


class KeyNormalizer:
    """键规范化器，处理类型不一致、空白、大小写等问题"""

    @staticmethod
    def normalize_school_id(key: Any) -> str:
        """
        学校ID规范化
        - 转换为字符串
        - 去除前后空白
        - 处理None值
        """
        if key is None:
            return ""
        # 统一转为字符串并去除空白
        normalized = str(key).strip()
        # 去除零宽字符
        normalized = ''.join(ch for ch in normalized if unicodedata.category(ch)[0] != 'C')
        return normalized

    @staticmethod
    def normalize_subject_name(key: Any) -> str:
        """
        科目名规范化
        - 去除所有空白（包括内部）
        - 统一全角/半角
        - 保持原始大小写（中文科目名）
        """
        if key is None:
            return ""
        normalized = str(key).strip()
        # 去除所有空白字符
        normalized = re.sub(r'\s+', '', normalized)
        # 全角转半角（仅针对ASCII字符）
        normalized = normalized.replace("（", "(").replace("）", ")")
        normalized = normalized.replace("，", ",").replace("。", ".")
        # 去除零宽字符
        normalized = ''.join(ch for ch in normalized if unicodedata.category(ch)[0] != 'C')
        return normalized

    @staticmethod
    def normalize_dimension_code(key: Any) -> str:
        """
        维度代码规范化
        - 统一大写
        - 统一使用连字符
        - 去除空白
        """
        if key is None:
            return ""
        normalized = str(key).strip().upper()
        # 统一使用连字符（替换下划线）
        normalized = normalized.replace("_", "-")
        # 去除空白
        normalized = re.sub(r'\s+', '', normalized)
        # 去除零宽字符
        normalized = ''.join(ch for ch in normalized if unicodedata.category(ch)[0] != 'C')
        return normalized


class CacheHitStats:
    """分层缓存命中统计器"""

    def __init__(self):
        self.stats = {
            'l1_school_hit': 0,
            'l1_school_miss': 0,
            'l2_subject_hit': 0,
            'l2_subject_miss': 0,
            'l3_dimension_hit': 0,
            'l3_dimension_miss': 0
        }
        self.miss_samples = {
            'l1': [],  # 学校层未命中样例
            'l2': [],  # 科目层未命中样例
            'l3': []   # 维度层未命中样例
        }
        self.collision_warnings = []  # 键碰撞警告

    def record_l1_hit(self):
        """记录学校层命中"""
        self.stats['l1_school_hit'] += 1

    def record_l1_miss(self, original: str, normalized: str, available_keys: List[str]):
        """记录学校层未命中"""
        self.stats['l1_school_miss'] += 1
        if len(self.miss_samples['l1']) < 10:  # 只记录前10个样例
            # 使用difflib找出最相似的键
            close_matches = difflib.get_close_matches(normalized, available_keys, n=3, cutoff=0.6)
            self.miss_samples['l1'].append({
                'original': original,
                'normalized': normalized,
                'type': type(original).__name__,
                'close_matches': close_matches,
                'available_count': len(available_keys),
                'available_sample': available_keys[:3] if available_keys else []
            })

    def record_l2_hit(self):
        """记录科目层命中"""
        self.stats['l2_subject_hit'] += 1

    def record_l2_miss(self, school: str, subject: str, normalized_subject: str, available_subjects: List[str]):
        """记录科目层未命中"""
        self.stats['l2_subject_miss'] += 1
        if len(self.miss_samples['l2']) < 10:
            close_matches = difflib.get_close_matches(normalized_subject, available_subjects, n=3, cutoff=0.6)
            self.miss_samples['l2'].append({
                'school': school,
                'original_subject': subject,
                'normalized_subject': normalized_subject,
                'close_matches': close_matches,
                'available_subjects': available_subjects
            })

    def record_l3_hit(self):
        """记录维度层命中"""
        self.stats['l3_dimension_hit'] += 1

    def record_l3_miss(self, school: str, subject: str, dimension: str, normalized_dim: str, available_dims: List[str]):
        """记录维度层未命中"""
        self.stats['l3_dimension_miss'] += 1
        if len(self.miss_samples['l3']) < 10:
            close_matches = difflib.get_close_matches(normalized_dim, available_dims, n=3, cutoff=0.6)
            self.miss_samples['l3'].append({
                'school': school,
                'subject': subject,
                'original_dimension': dimension,
                'normalized_dimension': normalized_dim,
                'close_matches': close_matches,
                'available_dimensions': available_dims[:5] if available_dims else []
            })

    def record_collision(self, level: str, original1: str, original2: str, normalized: str):
        """记录键碰撞（两个不同的原始键规范化后相同）"""
        self.collision_warnings.append({
            'level': level,
            'original1': original1,
            'original2': original2,
            'normalized': normalized
        })

    def get_summary(self) -> Dict[str, Any]:
        """获取统计摘要"""
        l1_total = self.stats['l1_school_hit'] + self.stats['l1_school_miss']
        l2_total = self.stats['l2_subject_hit'] + self.stats['l2_subject_miss']
        l3_total = self.stats['l3_dimension_hit'] + self.stats['l3_dimension_miss']

        return {
            'l1_school': {
                'hit': self.stats['l1_school_hit'],
                'miss': self.stats['l1_school_miss'],
                'total': l1_total,
                'hit_rate': (self.stats['l1_school_hit'] / l1_total * 100) if l1_total > 0 else 0
            },
            'l2_subject': {
                'hit': self.stats['l2_subject_hit'],
                'miss': self.stats['l2_subject_miss'],
                'total': l2_total,
                'hit_rate': (self.stats['l2_subject_hit'] / l2_total * 100) if l2_total > 0 else 0
            },
            'l3_dimension': {
                'hit': self.stats['l3_dimension_hit'],
                'miss': self.stats['l3_dimension_miss'],
                'total': l3_total,
                'hit_rate': (self.stats['l3_dimension_hit'] / l3_total * 100) if l3_total > 0 else 0
            },
            'miss_samples': self.miss_samples,
            'collisions': len(self.collision_warnings),
            'collision_details': self.collision_warnings[:5]  # 只显示前5个碰撞
        }


class CacheAuditor:
    """缓存完整性审计器"""

    @staticmethod
    def audit_cache(cache: Dict[str, Any], expected_schools: int = None) -> Dict[str, Any]:
        """
        审计缓存完整性
        返回缓存覆盖率、样例数据等信息
        """
        if not cache:
            return {
                'status': 'EMPTY',
                'total_schools': 0,
                'message': '缓存为空，可能构建失败'
            }

        # 统计基本信息
        total_schools = len(cache)
        school_samples = list(cache.keys())[:5]

        # 统计每个学校的科目数
        subjects_per_school = {}
        dimensions_per_subject = defaultdict(list)

        for school_id in list(cache.keys())[:10]:  # 取前10所学校样例
            school_data = cache[school_id]
            if isinstance(school_data, dict):
                subjects_per_school[school_id] = list(school_data.keys())

                for subject_name, subject_data in school_data.items():
                    if isinstance(subject_data, dict) and 'dimensions' in subject_data:
                        dims = subject_data['dimensions']
                        if isinstance(dims, dict):
                            dimensions_per_subject[subject_name].append(len(dims))

        # 计算维度统计
        total_dimensions = 0
        dimension_samples = {}
        for school_id, school_data in cache.items():
            if isinstance(school_data, dict):
                for subject_name, subject_data in school_data.items():
                    if isinstance(subject_data, dict) and 'dimensions' in subject_data:
                        dims = subject_data.get('dimensions', {})
                        if isinstance(dims, dict):
                            total_dimensions += len(dims)
                            if subject_name not in dimension_samples and dims:
                                dimension_samples[subject_name] = list(dims.keys())[:3]

        # 检查覆盖率
        coverage_status = 'GOOD'
        warnings = []

        if expected_schools and total_schools < expected_schools * 0.9:
            coverage_status = 'INCOMPLETE'
            warnings.append(f'学校覆盖率低：{total_schools}/{expected_schools} ({total_schools/expected_schools*100:.1f}%)')

        if total_schools > 0 and total_dimensions == 0:
            coverage_status = 'NO_DIMENSIONS'
            warnings.append('缓存中没有维度数据')

        # 检查数据一致性
        subjects_set = set()
        for school_data in list(cache.values())[:10]:
            if isinstance(school_data, dict):
                subjects_set.update(school_data.keys())

        return {
            'status': coverage_status,
            'total_schools': total_schools,
            'school_samples': school_samples,
            'subjects_found': list(subjects_set),
            'subjects_per_school': subjects_per_school,
            'total_dimensions': total_dimensions,
            'dimension_samples': dimension_samples,
            'avg_dimensions_per_subject': {
                subj: sum(counts) / len(counts) if counts else 0
                for subj, counts in dimensions_per_subject.items()
            },
            'warnings': warnings,
            'expected_schools': expected_schools,
            'coverage_rate': (total_schools / expected_schools * 100) if expected_schools else None
        }

    @staticmethod
    def compare_keys(original_keys: List[Any], normalized_keys: List[str]) -> Dict[str, Any]:
        """比较原始键和规范化后的键，检测碰撞"""
        if len(original_keys) != len(normalized_keys):
            return {
                'error': '键数量不匹配',
                'original_count': len(original_keys),
                'normalized_count': len(normalized_keys)
            }

        collisions = defaultdict(list)
        changes = []

        for orig, norm in zip(original_keys, normalized_keys):
            orig_str = str(orig)
            if orig_str != norm:
                changes.append({
                    'original': orig_str,
                    'original_type': type(orig).__name__,
                    'normalized': norm
                })
            collisions[norm].append(orig_str)

        # 找出碰撞（多个原始键映射到同一个规范化键）
        actual_collisions = {
            norm: origs for norm, origs in collisions.items()
            if len(origs) > 1
        }

        return {
            'total_keys': len(original_keys),
            'changed_keys': len(changes),
            'change_samples': changes[:10],
            'collision_count': len(actual_collisions),
            'collision_samples': dict(list(actual_collisions.items())[:5])
        }