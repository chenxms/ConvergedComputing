#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
导出 v1.2 聚合快照到 docs/qa/snapshots

示例：
- 导出区域级：
  poetry run python scripts/export_v12_snapshot.py --batch G7-2025 --type regional
- 导出学校级：
  poetry run python scripts/export_v12_snapshot.py --batch G7-2025 --type school --school SCH_001

说明：
- 直接使用内部构建逻辑（_fetch_v12_regional/_fetch_v12_school），
  自动触发数据不完整时的重建（percentiles/discrimination 等）。
- 输出 JSON 采用 UTF-8 编码，保留中文（ensure_ascii=False）。
"""

import argparse
import os
import sys
import json
from datetime import datetime

CURR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(CURR, '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import SessionLocal
from app.api import subjects_v12_api as api


def export_regional(batch: str, out_dir: str) -> str:
    db = SessionLocal()
    try:
        data = api._fetch_v12_regional(db, batch, include_detail=False)  # noqa: SLF001 (internal by design)
    finally:
        db.close()
    stamp = datetime.now().strftime('%Y%m%d')
    out_name = f'REGIONAL_{batch}_{stamp}.json'
    out_path = os.path.join(out_dir, out_name)
    os.makedirs(out_dir, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return out_path


def export_school(batch: str, school_id: str, out_dir: str) -> str:
    db = SessionLocal()
    try:
        data = api._fetch_v12_school(db, batch, school_id)  # noqa: SLF001 (internal by design)
    finally:
        db.close()
    stamp = datetime.now().strftime('%Y%m%d')
    out_name = f'SCHOOL_{batch}_{school_id}_{stamp}.json'
    out_path = os.path.join(out_dir, out_name)
    os.makedirs(out_dir, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--batch', required=True, help='批次代码，如 G7-2025')
    ap.add_argument('--type', choices=['regional', 'school'], required=True, help='导出类型')
    ap.add_argument('--school', help='学校ID（type=school 时必填）')
    ap.add_argument('--out', default=os.path.join('docs', 'qa', 'snapshots'), help='输出目录')
    args = ap.parse_args()

    if args.type == 'regional':
        out = export_regional(args.batch, args.out)
        print(f'已导出区域级快照: {out}')
    else:
        if not args.school:
            ap.error('--type school 时必须提供 --school')
        out = export_school(args.batch, args.school, args.out)
        print(f'已导出学校级快照: {out}')


if __name__ == '__main__':
    main()

