#!/usr/bin/env python3
import os
from sqlalchemy import create_engine, text

BATCH = os.getenv('BATCH_CODE', 'G4-2025')
SUBJECT = os.getenv('SUBJECT_NAME', '问卷')
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
        conn.execute(text(
            """
            DELETE FROM questionnaire_question_scores
            WHERE BINARY batch_code=BINARY :b AND BINARY subject_name=BINARY :s
            """
        ), {'b': BATCH, 's': SUBJECT})
        ins = text(
            """
            INSERT INTO questionnaire_question_scores
              (batch_code, subject_name, student_id, question_id, original_score, max_score, scale_level)
            SELECT ssd.batch_code,
                   ssd.subject_name,
                   ssd.student_id,
                   sqc.question_id,
                   CAST(JSON_UNQUOTE(JSON_EXTRACT(ssd.subject_scores, CONCAT('$."', sqc.question_id, '"'))) AS DECIMAL(10,2)) AS original_score,
                   sqc.max_score,
                   CASE
                     WHEN sqc.instrument_id LIKE '%10%' THEN 10
                     WHEN sqc.instrument_id LIKE '%7%'  THEN 7
                     WHEN sqc.instrument_id LIKE '%5%'  THEN 5
                     ELSE 4
                   END AS scale_level
            FROM student_score_detail ssd
            JOIN subject_question_config sqc
              ON BINARY sqc.batch_code=BINARY ssd.batch_code
             AND BINARY sqc.subject_name=BINARY ssd.subject_name
             AND sqc.question_type_enum='questionnaire'
            WHERE BINARY ssd.batch_code=BINARY :b
              AND BINARY ssd.subject_name=BINARY :s
              AND JSON_EXTRACT(ssd.subject_scores, CONCAT('$."', sqc.question_id, '"')) IS NOT NULL
            """
        )
        res = conn.execute(ins, {'b': BATCH, 's': SUBJECT})
        inserted = res.rowcount or 0
        conn.execute(text(
            """
            REPLACE INTO questionnaire_option_distribution
              (batch_code, subject_name, question_id, option_level, count, updated_at)
            SELECT batch_code,
                   subject_name,
                   question_id,
                   GREATEST(1, LEAST(scale_level,
                     ROUND(COALESCE(original_score,0)/NULLIF(max_score,0) * scale_level,0))) AS option_level,
                   COUNT(*), NOW()
            FROM questionnaire_question_scores
            WHERE BINARY batch_code=BINARY :b AND BINARY subject_name=BINARY :s
            GROUP BY batch_code, subject_name, question_id, option_level
            """
        ), {'b': BATCH, 's': SUBJECT})
        print(f"Questionnaire cleaned. Inserted detail rows: {inserted}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
