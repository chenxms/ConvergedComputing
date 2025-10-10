#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""快速数据库状态检查脚本"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

# Set UTF-8 encoding for Windows
if sys.platform.startswith('win'):
    import locale
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from app.database.connection import get_db_context
from sqlalchemy import text
from datetime import datetime

def check_regional_updates():
    """检查区域记录更新状态"""
    print("=== 检查区域记录更新状态 ===")

    query = text("""
        SELECT id, updated_at, JSON_EXTRACT(statistics_data,'$.schema_version') AS ver
        FROM statistical_aggregations
        WHERE batch_code='G7-2025' AND aggregation_level='REGIONAL'
        ORDER BY updated_at DESC LIMIT 2
    """)

    try:
        with get_db_context() as db:
            result = db.execute(query)
            rows = result.fetchall()

            if not rows:
                print("[ERROR] 未找到G7-2025批次的区域级统计记录")
                return False

            print(f"[INFO] 找到 {len(rows)} 条区域记录:")
            for row in rows:
                print(f"  ID: {row[0]}, 更新时间: {row[1]}, 版本: {row[2]}")

            # 检查最新记录的时间
            latest_time = rows[0][1]
            now = datetime.now()
            time_diff = now - latest_time

            print(f"[TIME] 最新记录时间差: {time_diff}")
            if time_diff.total_seconds() < 3600:  # 1小时内
                print("[SUCCESS] 记录看起来是最近更新的")
                return True
            else:
                print("[WARNING] 记录可能不是最新的")
                return False

    except Exception as e:
        print(f"[ERROR] 查询失败: {e}")
        return False

def check_subject_core_metrics():
    """检查预聚合数据刷新状态"""
    print("\n=== 检查预聚合数据刷新状态 ===")

    # 检查记录数量
    count_query = text("SELECT COUNT(*) FROM subject_core_metrics WHERE batch_code='G7-2025'")

    # 检查最新更新时间
    time_query = text("SELECT MAX(updated_at) FROM subject_core_metrics WHERE batch_code='G7-2025'")

    try:
        with get_db_context() as db:
            # 检查数量
            count_result = db.execute(count_query)
            count = count_result.fetchone()[0]
            print(f"[INFO] G7-2025批次预聚合记录数量: {count}")

            if count == 0:
                print("[ERROR] 没有找到预聚合数据")
                return False

            # 检查最新时间
            time_result = db.execute(time_query)
            latest_time = time_result.fetchone()[0]
            print(f"[TIME] 最新更新时间: {latest_time}")

            if latest_time:
                now = datetime.now()
                time_diff = now - latest_time
                print(f"[TIME] 时间差: {time_diff}")

                if time_diff.total_seconds() < 3600:  # 1小时内
                    print("[SUCCESS] 预聚合数据看起来是最近刷新的")
                    return True
                else:
                    print("[WARNING] 预聚合数据可能不是最新的")
                    return False
            else:
                print("[ERROR] 无法获取更新时间")
                return False

    except Exception as e:
        print(f"[ERROR] 查询失败: {e}")
        return False

def check_subject_types():
    """检查题型识别"""
    print("\n=== 检查题型识别状态 ===")

    query = text("""
        SELECT subject_name, subject_type, COUNT(*) as count
        FROM subject_core_metrics
        WHERE batch_code='G7-2025'
        GROUP BY subject_name, subject_type
        ORDER BY subject_name, subject_type
    """)

    try:
        with get_db_context() as db:
            result = db.execute(query)
            rows = result.fetchall()

            if not rows:
                print("[ERROR] 没有找到科目统计数据")
                return False

            print("[INFO] 科目类型分布:")
            questionnaire_found = False

            for row in rows:
                subject_name, subject_type, count = row
                print(f"  {subject_name}: {subject_type} ({count}条)")

                if subject_type == 'questionnaire':
                    questionnaire_found = True

            if questionnaire_found:
                print("[SUCCESS] 发现问卷类型科目，题型识别正常")
                return True
            else:
                print("[WARNING] 未发现问卷类型科目，可能存在题型识别问题")
                return False

    except Exception as e:
        print(f"[ERROR] 查询失败: {e}")
        return False

def main():
    print("[START] 开始G7-2025数据库状态检查...")
    print(f"[TIME] 检查时间: {datetime.now()}")

    # 执行各项检查
    regional_ok = check_regional_updates()
    metrics_ok = check_subject_core_metrics()
    types_ok = check_subject_types()

    # 总结
    print("\n" + "="*50)
    print("[SUMMARY] 检查结果总结:")
    print(f"  区域记录更新: {'[OK] 正常' if regional_ok else '[ERROR] 异常'}")
    print(f"  预聚合数据刷新: {'[OK] 正常' if metrics_ok else '[ERROR] 异常'}")
    print(f"  题型识别: {'[OK] 正常' if types_ok else '[ERROR] 异常'}")

    overall_status = all([regional_ok, metrics_ok, types_ok])
    print(f"\n[RESULT] 整体状态: {'[OK] 系统正常' if overall_status else '[WARNING] 发现问题'}")

    return overall_status

if __name__ == "__main__":
    main()