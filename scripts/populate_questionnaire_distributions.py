#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
用法:
  docker compose exec app python scripts/populate_questionnaire_distributions.py --batch <批次> [--subject <科目>]

说明:
  - 基于 questionnaire_question_scores 明细计算并写入 questionnaire_option_distribution（学校维度）。
  - 若不指定 --subject，则自动遍历该批次所有问卷科目。
"""

import argparse
import json
from app.services.question_option_distribution_service import (
    QuestionOptionDistributionService,
    populate_questionnaire_distributions,
)


def main():
    parser = argparse.ArgumentParser(description="计算并写入问卷题目选项分布（学校维度）")
    parser.add_argument("--batch", required=True, help="批次编码 batch_code")
    parser.add_argument("--subject", required=False, help="可选：限定科目 subject_name")
    args = parser.parse_args()

    if args.subject:
        svc = QuestionOptionDistributionService()
        result = svc.populate_school_option_distributions(args.batch, args.subject)
    else:
        result = populate_questionnaire_distributions(args.batch)

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

