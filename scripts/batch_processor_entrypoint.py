#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
批处理容器入口点脚本
支持通过环境变量 BATCH_CODE 指定处理的批次
"""

import os
import sys
from pathlib import Path

# 添加项目根路径到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def main():
    # 从环境变量获取批次代码（必须显式提供，防止误跑默认批次）
    batch_code = os.getenv('BATCH_CODE')

    if not batch_code:
        print("[batch-processor] 环境变量 BATCH_CODE 未设置，跳过执行。")
        print("用法: docker compose run --rm -e BATCH_CODE=G4-2025 batch-processor")
        # 返回0以避免在某些重启策略下被反复拉起
        return 0

    print(f"批处理器启动，处理批次: {batch_code}")
    print(f"环境变量 BATCH_CODE: {os.getenv('BATCH_CODE', 'not set')}")

    # 导入并执行重写脚本
    try:
        from scripts.rewrite_subjects_v12 import rewrite_batch
        print(f"开始重写批次 {batch_code} 的统计数据...")
        rewrite_batch(batch_code)
        print(f"批次 {batch_code} 处理完成")
        return 0
    except Exception as e:
        print(f"批处理失败: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
