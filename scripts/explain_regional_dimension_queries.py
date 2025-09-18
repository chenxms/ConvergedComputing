#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Explain plans for set-based regional dimension queries

Usage:
  python scripts/explain_regional_dimension_queries.py <BATCH_CODE> <SUBJECT_NAME>
"""
from __future__ import annotations
import sys
import os
from typing import Any
from sqlalchemy import text

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.database.connection import get_db


def explain(title: str, sql: str, params: dict[str, Any]) -> None:
    print(f"\n=== EXPLAIN: {title} ===")
    with next(get_db()) as db:
        rows = db.execute(text("EXPLAIN " + sql), params).fetchall()
        for r in rows:
            try:
                print(" | ".join(str(c) for c in r))
            except Exception:
                print(str(r))


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print("Usage: python scripts/explain_regional_dimension_queries.py <BATCH_CODE> <SUBJECT_NAME>")
        return 1
    batch, subject = argv[1], argv[2]

    sql_avg = (
        """
        WITH dim_list AS (
          SELECT DISTINCT dimension_code FROM batch_dimension_definition WHERE batch_code=:batch AND subject_name=:subject
          UNION
          SELECT DISTINCT dimension_code FROM question_dimension_mapping WHERE batch_code=:batch AND subject_name=:subject
        ), per_row AS (
          SELECT dl.dimension_code,
                 CAST(JSON_UNQUOTE(JSON_EXTRACT(CAST(scs.dimension_scores AS JSON), CONCAT('$."', dl.dimension_code, '".score'))) AS DECIMAL(10,4)) AS dim_score
          FROM student_cleaned_scores scs
          JOIN school_master_data smd ON smd.batch_code=scs.batch_code AND smd.school_id=scs.school_code AND smd.status='ACTIVE'
          JOIN dim_list dl
          WHERE scs.batch_code=:batch AND scs.subject_name=:subject AND scs.subject_type IN ('exam','questionnaire')
        ), per_row_q AS (
          SELECT qdm.dimension_code, qqs.original_score AS dim_score
          FROM questionnaire_question_scores qqs
          JOIN question_dimension_mapping qdm ON qdm.batch_code=qqs.batch_code AND qdm.question_id=qqs.question_id AND qdm.subject_name=:subject
          JOIN school_master_data smd ON smd.batch_code=qqs.batch_code AND smd.school_id=qqs.school_id AND smd.status='ACTIVE'
          WHERE qqs.batch_code=:batch AND qqs.subject_name=:subject
        ), unioned AS (
          SELECT dimension_code, dim_score FROM per_row WHERE dim_score IS NOT NULL
          UNION ALL
          SELECT dimension_code, dim_score FROM per_row_q
        )
        SELECT dimension_code, ROUND(AVG(dim_score), 2) AS dim_avg FROM unioned GROUP BY dimension_code
        """
    )

    sql_max = (
        """
        WITH dim_list AS (
          SELECT DISTINCT dimension_code FROM batch_dimension_definition WHERE batch_code=:batch AND subject_name=:subject
          UNION
          SELECT DISTINCT dimension_code FROM question_dimension_mapping WHERE batch_code=:batch AND subject_name=:subject
        ), bdd_scores AS (
          SELECT dimension_code, dimension_max_score AS max_score, 1 AS priority
          FROM batch_dimension_definition WHERE batch_code=:batch AND subject_name=:subject AND dimension_max_score IS NOT NULL
        ), json_scores AS (
          SELECT dl.dimension_code,
                 ROUND(AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(CAST(scs.dimension_max_scores AS JSON), CONCAT('$."', dl.dimension_code, '".max_score'))) AS DECIMAL(10,4))), 2) AS max_score,
                 2 AS priority
          FROM student_cleaned_scores scs
          JOIN school_master_data smd ON smd.batch_code=scs.batch_code AND smd.school_id=scs.school_code AND smd.status='ACTIVE'
          JOIN dim_list dl
          WHERE scs.batch_code=:batch AND scs.subject_name=:subject AND scs.subject_type IN ('exam','questionnaire')
            AND JSON_EXTRACT(CAST(scs.dimension_max_scores AS JSON), CONCAT('$."', dl.dimension_code, '".max_score')) IS NOT NULL
          GROUP BY dl.dimension_code
        ), sqc_scores AS (
          SELECT qdm.dimension_code, SUM(sqc.max_score) AS max_score, 3 AS priority
          FROM subject_question_config sqc
          JOIN question_dimension_mapping qdm ON qdm.question_id=sqc.question_id AND qdm.batch_code=sqc.batch_code
          WHERE sqc.batch_code=:batch AND qdm.subject_name=:subject
          GROUP BY qdm.dimension_code
        ), all_scores AS (
          SELECT dimension_code, max_score, priority FROM bdd_scores
          UNION ALL SELECT dimension_code, max_score, priority FROM json_scores
          UNION ALL SELECT dimension_code, max_score, priority FROM sqc_scores
        ), ranked_scores AS (
          SELECT dimension_code, max_score, ROW_NUMBER() OVER (PARTITION BY dimension_code ORDER BY priority) AS rn
          FROM all_scores WHERE max_score IS NOT NULL
        )
        SELECT dimension_code, max_score FROM ranked_scores WHERE rn=1
        """
    )

    params = {"batch": batch, "subject": subject}
    explain("regional_dimension_avgs", sql_avg, params)
    explain("regional_dimension_max_scores", sql_max, params)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

