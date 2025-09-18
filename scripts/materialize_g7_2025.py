#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
解除G7-2025批次锁并执行v1.2物化（区域+学校级）

步骤：
- 检查并删除数据库触发器 prevent_g7_2025_insert / prevent_g7_2025_update
- 清空当前进程环境中的 DISABLE_WRITES_FOR_BATCHES 以避免仓库层写入阻断
- 清理 G7-2025 的旧统计记录（statistical_aggregations）
- 调用 subjects_v12_api 的内部方法执行区域与学校级生成并入库
- 输出最终学校数与耗时
"""

import os
import sys
import time
from typing import List

from sqlalchemy import text

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURR_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.database.connection import get_db


def _drop_g7_triggers() -> List[str]:
    """查找并删除包含 g7_2025 的触发器，返回已删除列表"""
    dropped: List[str] = []
    with next(get_db()) as db:
        rows = db.execute(text(
            """
            SELECT TRIGGER_NAME 
            FROM information_schema.TRIGGERS 
            WHERE TRIGGER_SCHEMA = DATABASE() 
              AND TRIGGER_NAME LIKE '%g7_2025%'
            """
        )).fetchall()
        triggers = [r[0] for r in rows]

        for trg in triggers:
            try:
                db.execute(text(f"DROP TRIGGER IF EXISTS {trg}"))
                dropped.append(trg)
            except Exception:
                # 尝试继续删除其他触发器
                pass
        db.commit()
    return dropped


def _clear_env_block():
    """清空当前进程的批次写入阻断环境变量"""
    # Repository 使用 os.getenv('DISABLE_WRITES_FOR_BATCHES') 判断是否阻断
    os.environ['DISABLE_WRITES_FOR_BATCHES'] = ''


def _materialize_v12(batch_code: str) -> int:
    """执行 subjects v1.2 物化，返回学校处理数量"""
    # 直接复用 FastAPI 模块中的内部方法，避免重复实现
    from app.api.subjects_v12_api import _fetch_v12_regional, _fetch_v12_school

    count = 0
    with next(get_db()) as db:
        # 清理旧数据
        db.execute(text("DELETE FROM statistical_aggregations WHERE batch_code=:b"), {"b": batch_code})
        db.commit()

        # 区域级
        _fetch_v12_regional(db, batch_code)

        # 学校列表（以主数据为准，仅 ACTIVE）
        rows = db.execute(text(
            "SELECT school_id FROM school_master_data WHERE batch_code=:b AND status='ACTIVE' ORDER BY school_id"
        ), {"b": batch_code}).fetchall()

        for (school_id,) in rows:
            _fetch_v12_school(db, batch_code, school_id)
            count += 1
    return count


def main():
    batch_code = 'G7-2025'
    print(f"=== 开始解除锁并物化批次 {batch_code} (v1.2) ===")

    # 1) 删除触发器锁
    dropped = _drop_g7_triggers()
    if dropped:
        print(f"已删除触发器: {', '.join(dropped)}")
    else:
        print("未发现 g7_2025 相关触发器或已删除")

    # 2) 清空环境阻断
    _clear_env_block()
    print("已清空进程环境变量 DISABLE_WRITES_FOR_BATCHES")

    # 3) 执行物化
    start = time.time()
    try:
        schools = _materialize_v12(batch_code)
    except Exception as e:
        print(f"物化失败: {e}")
        raise
    duration = time.time() - start
    print(f"物化完成: 学校={schools} 耗时={duration:.2f}s")


if __name__ == '__main__':
    main()

