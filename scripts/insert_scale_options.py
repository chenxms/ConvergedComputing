#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
插入问卷量表选项标签到questionnaire_scale_options表
根据您的数据，看起来是4级李克特量表
"""

import sys
import os
CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR_DIR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def insert_scale_options():
    """插入量表选项标签"""

    # 定义要插入的数据
    # 根据您的数据，问卷使用的是4级量表
    scale_options = [
        # 4级李克特量表 - 同意度
        ('LIKERT', '4', 1, '非常不同意'),
        ('LIKERT', '4', 2, '不同意'),
        ('LIKERT', '4', 3, '同意'),
        ('LIKERT', '4', 4, '非常同意'),

        # 备用：4级满意度量表
        ('SATISFACTION', '4', 1, '非常不满意'),
        ('SATISFACTION', '4', 2, '不满意'),
        ('SATISFACTION', '4', 3, '满意'),
        ('SATISFACTION', '4', 4, '非常满意'),

        # 5级李克特量表（以防其他批次使用）
        ('LIKERT', '5', 1, '非常不同意'),
        ('LIKERT', '5', 2, '不同意'),
        ('LIKERT', '5', 3, '一般'),
        ('LIKERT', '5', 4, '同意'),
        ('LIKERT', '5', 5, '非常同意'),
    ]

    with get_db_context() as db:
        try:
            # 先检查表中是否已有数据
            existing = db.execute(text("""
                SELECT COUNT(*) FROM questionnaire_scale_options
                WHERE instrument_type = 'LIKERT' AND scale_level = '4'
            """)).scalar()

            if existing > 0:
                print(f"表中已存在LIKERT 4级量表数据，共{existing}条")

                # 显示现有数据
                rows = db.execute(text("""
                    SELECT instrument_type, scale_level, option_level, option_label
                    FROM questionnaire_scale_options
                    WHERE instrument_type = 'LIKERT' AND scale_level = '4'
                    ORDER BY option_level
                """)).fetchall()

                print("\n现有数据：")
                for row in rows:
                    print(f"  {row[0]} {row[1]} - 选项{row[2]}: {row[3]}")

                response = input("\n是否要重新插入数据？(y/n): ")
                if response.lower() != 'y':
                    print("跳过插入")
                    return

                # 删除现有数据
                db.execute(text("""
                    DELETE FROM questionnaire_scale_options
                    WHERE instrument_type IN ('LIKERT', 'SATISFACTION')
                """))
                print("已删除现有数据")

            # 插入新数据
            insert_sql = text("""
                INSERT INTO questionnaire_scale_options
                (instrument_type, scale_level, option_level, option_label, display_order, is_active, created_at, updated_at)
                VALUES
                (:instrument_type, :scale_level, :option_level, :option_label, :display_order, 1, :created_at, :updated_at)
            """)

            now = datetime.now()
            inserted_count = 0

            for instrument_type, scale_level, option_level, option_label in scale_options:
                params = {
                    'instrument_type': instrument_type,
                    'scale_level': scale_level,
                    'option_level': option_level,
                    'option_label': option_label,
                    'display_order': option_level,
                    'created_at': now,
                    'updated_at': now
                }
                db.execute(insert_sql, params)
                inserted_count += 1
                print(f"插入: {instrument_type} {scale_level} - 选项{option_level}: {option_label}")

            db.commit()
            print(f"\n✅ 成功插入{inserted_count}条数据")

            # 验证插入结果
            print("\n验证插入结果：")
            verify_sql = text("""
                SELECT instrument_type, scale_level, COUNT(*) as count
                FROM questionnaire_scale_options
                GROUP BY instrument_type, scale_level
                ORDER BY instrument_type, scale_level
            """)

            results = db.execute(verify_sql).fetchall()
            for row in results:
                print(f"  {row[0]} {row[1]}级: {row[2]}条")

        except Exception as e:
            print(f"❌ 错误: {e}")
            db.rollback()
            import traceback
            traceback.print_exc()

def main():
    print("="*60)
    print("插入问卷量表选项标签")
    print("="*60)

    insert_scale_options()

    print("\n" + "="*60)
    print("完成！")
    print("="*60)
    print("\n下一步：")
    print("1. 更新questionnaire_question_scores表的instrument_type和scale_level字段")
    print("2. 运行区域数据重建：")
    print("   python scripts/rebuild_regional_v12.py --batch G4-2025 --include-detail")

if __name__ == "__main__":
    main()