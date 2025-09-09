#!/usr/bin/env python3
"""
清洗所有批次的问卷数据，包含维度信息
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from enhanced_questionnaire_clean import clean_questionnaire_with_dimensions

def clean_all_batches():
    """清洗所有批次"""
    batches = ['G7-2025', 'G8-2025']  # G4-2025已完成
    
    for batch_code in batches:
        print(f"\n{'='*80}")
        print(f"开始清洗批次: {batch_code} (包含维度信息)")
        print(f"{'='*80}\n")
        
        try:
            clean_questionnaire_with_dimensions(batch_code)
            print(f"\n[SUCCESS] 批次 {batch_code} 清洗完成")
        except Exception as e:
            print(f"\n[ERROR] 批次 {batch_code} 清洗失败: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\n{'='*80}")
    print("所有批次问卷数据清洗完成（包含维度）！")
    print(f"{'='*80}")

if __name__ == "__main__":
    clean_all_batches()