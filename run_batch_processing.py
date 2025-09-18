#!/usr/bin/env python3
"""
直接运行批处理脚本，避开Docker Compose的复杂性
"""
import os
import sys
import asyncio
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from scripts.rewrite_subjects_v12 import rewrite_batch

async def main():
    batch_code = "G4-2025"
    
    # 设置数据库连接
    os.environ["DATABASE_URL"] = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    os.environ["PYTHONPATH"] = str(project_root)
    
    print(f"开始处理批次: {batch_code}")
    print(f"数据库连接: {os.environ.get('DATABASE_URL', 'NOT SET')}")
    
    try:
        # 直接调用重写函数
        result = rewrite_batch(batch_code)
        print(f"批处理完成: {result}")
        
    except Exception as e:
        print(f"批处理失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())