#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
单独测试G4-2025批次汇聚功能
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.services.calculation_service import CalculationService

async def test_g4_batch():
    """测试G4-2025批次"""
    print("="*60)
    print("G4-2025批次汇聚功能测试")
    print("="*60)
    
    start_time = datetime.now()
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        calc_service = CalculationService(session)
        batch_code = 'G4-2025'
        
        print(f"[1/3] 执行区域级汇聚...")
        try:
            regional_result = await calc_service.calculate_batch_statistics(batch_code)
            print(f"   区域级汇聚: 成功 ✓")
            regional_success = True
        except Exception as e:
            print(f"   区域级汇聚: 失败 - {str(e)}")
            regional_success = False
        
        print(f"\n[2/3] 获取学校列表...")
        schools = await calc_service._get_batch_schools(batch_code)
        print(f"   批次学校数: {len(schools)}")
        
        print(f"\n[3/3] 执行学校级汇聚测试（前5个学校）...")
        school_success = 0
        school_fail = 0
        
        for i, school_id in enumerate(schools[:5], 1):
            try:
                print(f"   测试学校 {i}/5: {school_id}", end=" ")
                school_result = await calc_service.calculate_school_statistics(batch_code, school_id)
                school_success += 1
                print("✓")
            except Exception as e:
                school_fail += 1
                print(f"✗ - {str(e)[:50]}")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n{'='*60}")
        print("测试结果汇总")
        print(f"{'='*60}")
        print(f"批次代码: {batch_code}")
        print(f"耗时: {int(duration//60)}分{int(duration%60)}秒")
        print(f"区域级汇聚: {'成功 ✓' if regional_success else '失败 ✗'}")
        print(f"学校级汇聚: 成功{school_success}/5个")
        
        overall_success = regional_success and school_success > 0
        
        if overall_success:
            print(f"\n[SUCCESS] G4-2025批次汇聚测试完全成功！")
        else:
            print(f"\n[PARTIAL] G4-2025批次汇聚测试部分成功")
        
        session.close()
        return overall_success
        
    except Exception as e:
        print(f"[ERROR] G4-2025测试异常: {e}")
        return False

if __name__ == "__main__":
    result = asyncio.run(test_g4_batch())
    exit(0 if result else 1)