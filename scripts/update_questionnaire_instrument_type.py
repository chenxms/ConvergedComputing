#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
更新questionnaire_question_scores表的instrument_type和scale_level字段
使其能够与questionnaire_scale_options表关联
"""

import sys
import os
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db_context
from sqlalchemy import text

def update_instrument_type(batch_code: str):
    """更新指定批次的instrument_type和scale_level"""

    with get_db_context() as db:
        try:
            # 1. 检查当前数据状态
            print(f"\n[1] 检查批次 {batch_code} 的问卷数据...")

            check_sql = text("""
                SELECT
                    subject_name,
                    instrument_type,
                    scale_level,
                    COUNT(DISTINCT question_id) as question_count,
                    COUNT(DISTINCT option_level) as option_levels,
                    MIN(option_level) as min_level,
                    MAX(option_level) as max_level
                FROM questionnaire_question_scores
                WHERE batch_code = :batch
                GROUP BY subject_name, instrument_type, scale_level
            """)

            results = db.execute(check_sql, {"batch": batch_code}).fetchall()

            print(f"找到 {len(results)} 个科目组合：")
            for row in results:
                print(f"  科目: {row[0]}")
                print(f"    instrument_type: {row[1] or 'NULL'}")
                print(f"    scale_level: {row[2] or 'NULL'}")
                print(f"    题目数: {row[3]}, 选项级别: {row[5]}-{row[6]} (共{row[4]}级)")

            # 2. 判断量表级别
            print(f"\n[2] 分析量表级别...")

            level_sql = text("""
                SELECT
                    subject_name,
                    COUNT(DISTINCT option_level) as levels,
                    MIN(option_level) as min_level,
                    MAX(option_level) as max_level
                FROM questionnaire_question_scores
                WHERE batch_code = :batch
                GROUP BY subject_name
            """)

            subjects = db.execute(level_sql, {"batch": batch_code}).fetchall()

            updates = []
            for subject_name, levels, min_level, max_level in subjects:
                print(f"\n科目: {subject_name}")
                print(f"  选项级别范围: {min_level}-{max_level}, 共{levels}级")

                # 根据级别数判断量表类型
                if levels == 4:
                    instrument_type = 'LIKERT'
                    scale_level = '4'
                    print(f"  → 判定为4级李克特量表")
                elif levels == 5:
                    instrument_type = 'LIKERT'
                    scale_level = '5'
                    print(f"  → 判定为5级李克特量表")
                elif levels == 10:
                    instrument_type = 'RATING'
                    scale_level = '10'
                    print(f"  → 判定为10级评分量表")
                else:
                    instrument_type = 'CUSTOM'
                    scale_level = str(levels)
                    print(f"  → 判定为自定义{levels}级量表")

                updates.append({
                    'subject_name': subject_name,
                    'instrument_type': instrument_type,
                    'scale_level': scale_level
                })

            # 3. 更新数据
            if updates:
                response = input(f"\n是否要更新以上{len(updates)}个科目的量表类型？(y/n): ")
                if response.lower() == 'y':
                    print("\n[3] 更新数据...")

                    update_sql = text("""
                        UPDATE questionnaire_question_scores
                        SET instrument_type = :instrument_type,
                            scale_level = :scale_level
                        WHERE batch_code = :batch
                        AND subject_name = :subject_name
                    """)

                    for update in updates:
                        params = {
                            'batch': batch_code,
                            'subject_name': update['subject_name'],
                            'instrument_type': update['instrument_type'],
                            'scale_level': update['scale_level']
                        }

                        result = db.execute(update_sql, params)
                        print(f"  更新 {update['subject_name']}: {result.rowcount} 条记录")

                    db.commit()
                    print("\n✅ 更新完成！")

                    # 4. 验证更新结果
                    print("\n[4] 验证更新结果...")

                    verify_sql = text("""
                        SELECT
                            subject_name,
                            instrument_type,
                            scale_level,
                            COUNT(*) as record_count
                        FROM questionnaire_question_scores
                        WHERE batch_code = :batch
                        GROUP BY subject_name, instrument_type, scale_level
                    """)

                    results = db.execute(verify_sql, {"batch": batch_code}).fetchall()
                    for row in results:
                        print(f"  {row[0]}: {row[1]} {row[2]}级 ({row[3]}条)")

                else:
                    print("已取消更新")

        except Exception as e:
            print(f"❌ 错误: {e}")
            db.rollback()
            import traceback
            traceback.print_exc()

def main():
    import argparse

    parser = argparse.ArgumentParser(description='更新问卷量表类型')
    parser.add_argument('--batch', '-b', default='G4-2025', help='批次代码')
    args = parser.parse_args()

    print("="*60)
    print(f"更新问卷量表类型 - 批次: {args.batch}")
    print("="*60)

    update_instrument_type(args.batch)

    print("\n" + "="*60)
    print("完成！")
    print("="*60)
    print("\n下一步：")
    print("运行区域数据重建：")
    print(f"  python scripts/rebuild_regional_v12.py --batch {args.batch} --include-detail")

if __name__ == "__main__":
    main()