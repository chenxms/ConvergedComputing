#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
快速诊断缓存问题
"""

import sys
import os

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db_context
from app.services.subjects_builder import SubjectsBuilder
from app.utils.cache_utils import KeyNormalizer, CacheAuditor
from sqlalchemy import text


def quick_diagnosis(batch_code: str):
    """快速诊断缓存问题"""
    print(f"\n=== 快速诊断批次 {batch_code} ===\n")

    sb = SubjectsBuilder()
    normalizer = KeyNormalizer()

    # 1. 获取学校列表
    print("步骤1: 获取学校列表...")
    with get_db_context() as db:
        schools = db.execute(
            text("""
                SELECT school_id, standard_school_name
                FROM school_master_data
                WHERE batch_code=:b AND status='ACTIVE'
                LIMIT 5
            """),
            {"b": batch_code}
        ).fetchall()

    print(f"  找到 {len(schools)} 所学校（限制5所）")
    for school_id, school_name in schools:
        print(f"    - {school_id} ({type(school_id).__name__}): {school_name}")

    # 2. 构建缓存
    print("\n步骤2: 构建维度排名缓存...")
    import time
    t0 = time.time()
    raw_cache = sb.build_dimension_rank_cache(batch_code)
    elapsed = time.time() - t0
    print(f"  耗时: {elapsed:.2f}秒")

    # 3. 审计缓存
    print("\n步骤3: 审计原始缓存...")
    audit = CacheAuditor.audit_cache(raw_cache, expected_schools=len(schools))

    print(f"  缓存状态: {audit['status']}")
    print(f"  学校数量: {audit['total_schools']}")
    print(f"  维度总数: {audit['total_dimensions']}")

    if audit['school_samples']:
        print(f"  学校样例键:")
        for key in audit['school_samples'][:3]:
            print(f"    - '{key}' (类型: {type(key).__name__})")

    if audit['subjects_found']:
        print(f"  发现的科目: {audit['subjects_found']}")

    if audit['warnings']:
        print(f"  警告:")
        for warning in audit['warnings']:
            print(f"    - {warning}")

    # 4. 测试键匹配
    if schools and raw_cache:
        print("\n步骤4: 测试键匹配...")
        test_school_id = schools[0][0]
        print(f"  测试学校ID: '{test_school_id}' (类型: {type(test_school_id).__name__})")

        # 原始查找
        found_raw = test_school_id in raw_cache
        print(f"  原始缓存中存在: {found_raw}")

        # 字符串查找
        found_str = str(test_school_id) in raw_cache
        print(f"  str()转换后存在: {found_str}")

        # 规范化查找
        norm_id = normalizer.normalize_school_id(test_school_id)
        print(f"  规范化后: '{norm_id}'")

        # 显示缓存中的实际键
        cache_keys = list(raw_cache.keys())[:5]
        print(f"  缓存中的前5个键:")
        for key in cache_keys:
            print(f"    - '{key}' (类型: {type(key).__name__})")

        # 5. 测试科目层
        if found_str or found_raw:
            cache_key = test_school_id if found_raw else str(test_school_id)
            school_cache = raw_cache[cache_key]
            print(f"\n  学校缓存中的科目: {list(school_cache.keys())}")

            # 测试维度层
            if school_cache:
                first_subject = list(school_cache.keys())[0]
                subject_data = school_cache[first_subject]
                if 'dimensions' in subject_data:
                    dims = subject_data['dimensions']
                    print(f"  科目'{first_subject}'的维度数: {len(dims)}")
                    if dims:
                        sample_dims = list(dims.keys())[:3]
                        print(f"  维度样例: {sample_dims}")

    print("\n=== 诊断完成 ===\n")

    # 6. 建议
    print("诊断建议:")
    if audit['total_schools'] == 0:
        print("  ❌ 缓存构建失败，检查SQL查询")
    elif audit['total_dimensions'] == 0:
        print("  ❌ 缓存中没有维度数据，检查维度查询")
    elif not found_raw and found_str:
        print("  ⚠️ 键类型不匹配，需要统一使用字符串")
    else:
        print("  ✅ 缓存结构正常")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_cache_diagnosis.py <batch_code>")
        sys.exit(1)

    batch_code = sys.argv[1]
    quick_diagnosis(batch_code)