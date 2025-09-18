#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速验证汇聚功能是否正常
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import asyncio
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.services.calculation_service import CalculationService

async def quick_validation():
    """快速验证汇聚功能"""
    print("快速验证汇聚功能是否正常")
    print("="*50)
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        calc_service = CalculationService(session)
        
        test_batches = ['G4-2025', 'G7-2025', 'G8-2025']
        results = {}
        
        for batch_code in test_batches:
            print(f"\n测试批次: {batch_code}")
            start_time = datetime.now()
            
            try:
                # 只测试区域级汇聚（较快）
                print(f"  区域级汇聚...", end="")
                regional_result = await calc_service.calculate_batch_statistics(batch_code)
                regional_time = (datetime.now() - start_time).total_seconds()
                print(f" 成功 ({regional_time:.1f}s)")
                
                # 测试获取学校列表
                schools = await calc_service._get_batch_schools(batch_code)
                print(f"  学校数量: {len(schools)}")
                
                # 只测试1个学校（最快）
                if schools:
                    school_start = datetime.now()
                    print(f"  测试学校汇聚...", end="")
                    school_result = await calc_service.calculate_school_statistics(batch_code, schools[0])
                    school_time = (datetime.now() - school_start).total_seconds()
                    print(f" 成功 ({school_time:.1f}s)")
                    
                    results[batch_code] = {
                        'status': 'success',
                        'regional_time': regional_time,
                        'school_time': school_time,
                        'total_schools': len(schools)
                    }
                else:
                    results[batch_code] = {
                        'status': 'partial', 
                        'regional_time': regional_time,
                        'error': '无学校数据'
                    }
                    
            except Exception as e:
                results[batch_code] = {
                    'status': 'failed',
                    'error': str(e)[:100]
                }
                print(f" 失败 - {str(e)[:50]}")
        
        # 汇总结果
        print(f"\n{'='*50}")
        print("验证结果汇总:")
        print(f"{'='*50}")
        
        success_count = 0
        for batch_code, result in results.items():
            status = result['status']
            print(f"{batch_code}: {status}")
            if status == 'success':
                success_count += 1
                print(f"  区域级: {result['regional_time']:.1f}s")
                print(f"  学校级: {result['school_time']:.1f}s")
                print(f"  学校数: {result['total_schools']}")
            elif 'error' in result:
                print(f"  错误: {result['error']}")
        
        overall_success = success_count == len(test_batches)
        
        if overall_success:
            print(f"\n[SUCCESS] 所有批次汇聚功能正常！")
        else:
            print(f"\n[PARTIAL] {success_count}/{len(test_batches)}批次成功")
        
        session.close()
        return overall_success
        
    except Exception as e:
        print(f"[ERROR] 验证过程异常: {e}")
        return False

if __name__ == "__main__":
    result = asyncio.run(quick_validation())
    print(f"\n验证结果: {'通过' if result else '失败'}")
    exit(0 if result else 1)