#!/usr/bin/env python3
"""
清洗所有批次的问卷数据
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from fixed_questionnaire_clean import clean_questionnaire_batch

if __name__ == "__main__":
    # 清洗所有批次
    batches = ['G7-2025', 'G8-2025']  # G4-2025已完成
    
    for batch_code in batches:
        print(f"\n{'='*60}")
        print(f"开始清洗批次: {batch_code}")
        print(f"{'='*60}\n")
        
        try:
            clean_questionnaire_batch(batch_code)
        except Exception as e:
            print(f"批次 {batch_code} 清洗失败: {e}")
            continue
    
    print("\n所有批次清洗完成！")