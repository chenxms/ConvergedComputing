#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
用法:
  poetry run python scripts/backfill_option_labels.py --batch <批次编码> [--subject <科目名称>]

说明:
  - 一次性回填 questionnaire_option_distribution 表中 option_label 为空的历史记录。
  - 优先使用“题目级量表映射（instrument_type+scale_level [+is_reverse]）”，
    其次回退到科目级量表映射，最后兜底为“选项{level}”。
  - 执行前请确保数据库连接配置正确（.env / 环境变量）。
"""

import argparse
import json
from app.services.question_option_distribution_service import (
    QuestionOptionDistributionService,
)


def main():
    parser = argparse.ArgumentParser(description="回填问卷选项标签为空的历史记录")
    parser.add_argument("--batch", required=True, help="批次编码 batch_code")
    parser.add_argument("--subject", required=False, help="可选：限定科目名称")
    args = parser.parse_args()

    svc = QuestionOptionDistributionService()
    result = svc.backfill_null_option_labels(batch_code=args.batch, subject_name=args.subject)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

