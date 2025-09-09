#!/usr/bin/env python3
"""
验证G4-2025批次清洗结果
"""
import sys
import os
import asyncio
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from data_cleaning_service import DataCleaningService, verify_cleaning_result

async def verify_g4_cleaning():
    """验证G4-2025批次清洗结果"""
    print("=== 验证G4-2025批次清洗结果 ===\n")
    
    batch_code = "G4-2025"
    
    # 直接设置数据库连接
    DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
    
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    
    try:
        print(f"正在验证批次 {batch_code} 的清洗结果...\n")
        
        # 验证清洗结果
        verification = await verify_cleaning_result(session, batch_code)
        print(verification)
        
        print("\n" + "="*50)
        print("验证完成！")
        
    except Exception as e:
        print(f"验证过程出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()

if __name__ == "__main__":
    asyncio.run(verify_g4_cleaning())