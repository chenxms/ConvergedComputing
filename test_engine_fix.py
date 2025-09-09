#!/usr/bin/env python3
"""
测试计算引擎修复
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
import asyncio

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.services.calculation_service import CalculationService

async def test_engine_fix():
    """测试计算引擎修复"""
    print("=== 测试计算引擎educational_metrics修复 ===\n")
    
    try:
        # 创建数据库连接
        DATABASE_URL = "mysql+pymysql://root:mysql_Lujing2022@117.72.14.166:23506/appraisal_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # 创建计算服务
        calc_service = CalculationService(session)
        
        # 获取G4-2025艺术科目数据（大数据集，会触发分块处理）
        from sqlalchemy import text
        query = text("""
            SELECT total_score
            FROM student_score_detail 
            WHERE batch_code = 'G4-2025' 
                AND subject_name = '艺术'
        """)
        
        result = session.execute(query)
        scores = [row[0] for row in result.fetchall()]
        session.close()
        
        print(f"获取到 {len(scores)} 个分数（大于chunk_size=10000，应该禁用分块处理）")
        
        # 创建测试DataFrame
        data = pd.DataFrame({'score': scores})
        config = {
            'max_score': 200.0,
            'grade_level': '4th_grade',
            'percentiles': [10, 25, 50, 75, 90],
            'required_columns': ['score']
        }
        
        print(f"\n通过计算引擎测试educational_metrics:")
        result = calc_service.engine.calculate('educational_metrics', data, config)
        
        print(f"结果包含的键: {list(result.keys())}")
        
        # 检查关键指标
        key_metrics = ['excellent_rate', 'pass_rate', 'grade_distribution', 'difficulty_coefficient']
        print(f"\n关键指标检查:")
        for metric in key_metrics:
            if metric in result:
                if metric == 'grade_distribution':
                    print(f"  ✅ {metric}: 存在")
                    grade_dist = result[metric]
                    total = sum([
                        grade_dist.get('excellent_count', 0),
                        grade_dist.get('good_count', 0), 
                        grade_dist.get('pass_count', 0),
                        grade_dist.get('fail_count', 0)
                    ])
                    print(f"     总人数: {total} (应该等于 {len(scores)})")
                else:
                    print(f"  ✅ {metric}: {result[metric]}")
            else:
                print(f"  ❌ {metric}: MISSING")
        
        # 验证优秀率计算
        expected_excellent = sum(1 for score in scores if score >= 200 * 0.85) / len(scores)
        actual_excellent = result.get('excellent_rate', 0)
        print(f"\n优秀率验证:")
        print(f"  期望优秀率: {expected_excellent:.3f}")
        print(f"  实际优秀率: {actual_excellent:.3f}")
        print(f"  差异: {abs(expected_excellent - actual_excellent):.6f}")
        
        if abs(expected_excellent - actual_excellent) < 0.001:
            print("  ✅ 优秀率计算正确!")
        else:
            print("  ❌ 优秀率计算错误!")
            
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_engine_fix())