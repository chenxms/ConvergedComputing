#!/usr/bin/env python3
"""
直接测试EducationalMetricsStrategy
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
import asyncio

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.calculation.formulas import EducationalMetricsStrategy

async def direct_strategy_test():
    """直接测试策略"""
    print("=== 直接测试EducationalMetricsStrategy ===\n")
    
    try:
        # 获取真实的G4-2025艺术科目数据
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        from sqlalchemy import text
        query = text("""
            SELECT total_score
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025' 
                AND subject_name = '艺术'
            LIMIT 1000
        """)
        
        result = session.execute(query)
        scores = [row[0] for row in result.fetchall()]
        session.close()
        
        print(f"获取到 {len(scores)} 个分数样本")
        print(f"分数范围: {min(scores):.1f} - {max(scores):.1f}")
        print(f"平均分: {sum(scores)/len(scores):.1f}")
        
        # 直接测试策略
        strategy = EducationalMetricsStrategy()
        data = pd.DataFrame({'score': scores})
        config = {
            'max_score': 200.0,
            'grade_level': '4th_grade'
        }
        
        print(f"\n直接调用策略:")
        print(f"配置: {config}")
        
        result = strategy.calculate(data, config)
        print(f"\n完整结果:")
        for key, value in result.items():
            if isinstance(value, dict):
                print(f"  {key}:")
                for sub_key, sub_value in value.items():
                    print(f"    {sub_key}: {sub_value}")
            else:
                print(f"  {key}: {value}")
                
        # 检查关键指标
        print(f"\n关键指标检查:")
        print(f"  优秀率: {result.get('excellent_rate', 'MISSING')}")
        print(f"  及格率: {result.get('pass_rate', 'MISSING')}")
        print(f"  等级分布: {'存在' if 'grade_distribution' in result else 'MISSING'}")
        
        if 'grade_distribution' in result:
            grade_dist = result['grade_distribution']
            total_count = sum([
                grade_dist.get('excellent_count', 0),
                grade_dist.get('good_count', 0),
                grade_dist.get('pass_count', 0),
                grade_dist.get('fail_count', 0)
            ])
            print(f"  等级分布总数: {total_count} (应该等于数据量 {len(scores)})")
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(direct_strategy_test())