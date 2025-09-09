#!/usr/bin/env python3
import os
from sqlalchemy import create_engine, text

BATCH = os.getenv('BATCH_CODE', 'G4-2025')
URL = os.getenv('DATABASE_URL')

def main():
    if not URL:
        print('ERROR: DATABASE_URL not set')
        return 1
    engine = create_engine(URL)
    with engine.begin() as conn:
        try:
            conn.execute(text("SET SESSION innodb_lock_wait_timeout = 300"))
        except Exception:
            pass

        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_subject_max"))
        conn.execute(text(
            """
            CREATE TEMPORARY TABLE tmp_subject_max (
              batch_code VARCHAR(50) NOT NULL,
              subject_name VARCHAR(100) NOT NULL,
              total_max DECIMAL(10,2) NOT NULL,
              PRIMARY KEY (batch_code, subject_name)
            ) ENGINE=Memory
            """
        ))
        conn.execute(text(
            """
            INSERT INTO tmp_subject_max (batch_code, subject_name, total_max)
            SELECT batch_code, subject_name,
                   COALESCE(SUM(max_score),0) AS total_max
            FROM subject_question_config
            WHERE batch_code=:b AND question_type_enum IN ('exam','interaction')
            GROUP BY batch_code, subject_name
            """
        ), {'b': BATCH})

        conn.execute(text("DROP TEMPORARY TABLE IF EXISTS tmp_totals"))
        conn.execute(text(
            """
            CREATE TEMPORARY TABLE tmp_totals (
              batch_code VARCHAR(50) NOT NULL,
              subject_id VARCHAR(64) NOT NULL,
              student_id VARCHAR(100) NOT NULL,
              total_score DECIMAL(10,2) NOT NULL,
              PRIMARY KEY (batch_code, subject_id, student_id)
            ) ENGINE=Memory
            """
        ))
        conn.execute(text(
            """
            INSERT INTO tmp_totals (batch_code, subject_id, student_id, total_score)
            SELECT ssd.batch_code, ssd.subject_id, ssd.student_id,
                   ROUND(SUM(
                     COALESCE(
                       CAST(
                         JSON_UNQUOTE(
                           JSON_EXTRACT(ssd.subject_scores, CONCAT('$."', sqc.question_id, '"'))
                         ) AS DECIMAL(10,2)
                       ), 0
                     )
                   ), 2) AS total_score
            FROM student_score_detail ssd
            JOIN subject_question_config sqc
              ON BINARY sqc.batch_code=BINARY ssd.batch_code
             AND BINARY sqc.subject_name=BINARY ssd.subject_name
             AND sqc.question_type_enum IN ('exam','interaction')
            WHERE BINARY ssd.batch_code=BINARY :b
            GROUP BY ssd.batch_code, ssd.subject_id, ssd.student_id
            """
        ), {'b': BATCH})

        update_totals = text(
            """
            UPDATE student_cleaned_scores scs
            JOIN tmp_totals t ON BINARY t.batch_code=BINARY scs.batch_code
                             AND BINARY t.subject_id=BINARY scs.subject_id
                             AND BINARY t.student_id=BINARY scs.student_id
            SET scs.total_score = t.total_score
            WHERE BINARY scs.batch_code=BINARY :b
            """
        )
        conn.execute(update_totals, {'b': BATCH})

        update_max = text(
            """
            UPDATE student_cleaned_scores scs
            JOIN tmp_subject_max m ON BINARY m.batch_code=BINARY scs.batch_code
                                  AND BINARY m.subject_name=BINARY scs.subject_name
            SET scs.max_score = m.total_max
            WHERE BINARY scs.batch_code=BINARY :b
            """
        )
        conn.execute(update_max, {'b': BATCH})

        print("Backfill for exam+interaction totals completed.")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())

