#!/usr/bin/env python3
"""
简化的批次数据清洗脚本
执行所有批次的数据清洗：G4-2025, G7-2025, G8-2025
"""
import os
import sys
import asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from data_cleaning_service import DataCleaningService


async def clean_single_batch(batch_code: str, session):
    """清洗单个批次的数据"""
    print(f"\n{'='*50}")
    print(f"开始清洗批次: {batch_code}")
    print(f"{'='*50}")
    
    try:
        cleaner = DataCleaningService(session)
        result = await cleaner.clean_batch_scores(batch_code)
        print(f"\n{batch_code} 清洗完成. 结果摘要:")
        print(result)
        return True
    except Exception as e:
        print(f"ERROR: {batch_code} 清洗失败: {e}")
        return False


async def main():
    """主执行函数"""
    db_url = os.getenv("DATABASE_URL", "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4")
    if not db_url:
        print("ERROR: DATABASE_URL 环境变量未设置.")
        sys.exit(2)
    
    # 要清洗的批次列表
    batches = ["G4-2025", "G7-2025", "G8-2025"]
    
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        success_count = 0
        total_count = len(batches)
        
        for batch_code in batches:
            success = await clean_single_batch(batch_code, session)
            if success:
                success_count += 1
        
        print(f"\n{'='*50}")
        print(f"批次清洗总结:")
        print(f"总计批次: {total_count}")
        print(f"成功批次: {success_count}")
        print(f"失败批次: {total_count - success_count}")
        print(f"{'='*50}")
        
        if success_count == total_count:
            print("✓ 所有批次清洗成功!")
        else:
            print("⚠ 部分批次清洗失败，请检查错误信息")
            
    finally:
        session.close()


if __name__ == "__main__":
    asyncio.run(main())