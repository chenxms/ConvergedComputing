#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
缓存命中率为0的问题分析与修复方案
"""

# 问题分析：
# 1. 缓存构建的结构：
#    cache[school_id][subject_name]['dimensions'][dimension_code] = {'rank': int, 'avg': float}
#
# 2. 使用时传递的数据：
#    pre_dim_ranks = dimension_rankings_cache.get(school_code)
#    # 这返回的是: {subject_name: {'dimensions': {code: {...}}}}
#
# 3. 函数内部的使用方式：
#    if isinstance(precomputed_dim_ranks, dict) and subject_name in precomputed_dim_ranks:
#        sub_entry = precomputed_dim_ranks.get(subject_name) or {}
#        pre_subject_dims = sub_entry.get('dimensions') or {}
#
# 问题：传递的已经是单个学校的数据，包含所有科目
#      函数内部再次按subject_name查找，所以能正确找到数据
#      理论上缓存应该能命中！

# 进一步分析：
# 可能的原因：
# 1. school_code在缓存中是字符串，但查询时可能是数字
# 2. subject_name可能有大小写或空格差异
# 3. dimension_code可能有格式差异

print("""
诊断步骤：

1. 检查缓存构建时的数据类型：
   - school_code是字符串还是数字？
   - subject_name的精确值是什么？

2. 检查使用时的数据类型：
   - 传入的school_code类型
   - 传入的subject_name值

3. 添加调试日志来验证
""")

# 修复建议：
fix_suggestion = """
修复方案：

1. 在缓存构建时统一数据类型：
   cache[str(school_code)][str(subject_name)]...

2. 在缓存使用时统一数据类型：
   pre_dim_ranks = dimension_rankings_cache.get(str(school_code))

3. 添加详细的调试日志：
   - 记录缓存的keys
   - 记录查询的keys
   - 记录匹配结果
"""

print(fix_suggestion)