#!/usr/bin/env python3
import os, re
from sqlalchemy import create_engine, text

BATCH = os.getenv('BATCH_CODE', 'G4-2025')
SUBJECT = os.getenv('SUBJECT_NAME', '问卷')
URL = os.getenv('DATABASE_URL')

def _guess_scale(instrument_id: str) -> int:
    if not instrument_id:
        return 4
    m = re.search(r'(10|7|5|4)', instrument_id)
    if m:
        return int(m.group(1))
    return 4

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

        # Load questionnaire questions config for this subject
        cfg_rows = conn.execute(text(
            """
            SELECT question_id, max_score, instrument_id
            FROM subject_question_config
            WHERE BINARY batch_code=BINARY :b AND BINARY subject_name=BINARY :s AND question_type_enum='questionnaire'
            ORDER BY question_id
            """
        ), {'b': BATCH, 's': SUBJECT}).fetchall()
        if not cfg_rows:
            print('No questionnaire config rows found for batch/subject.')
            return 0
        qcfg = {str(r[0]): {'max_score': float(r[1]) if r[1] is not None else 0.0,
                            'instrument_id': r[2],
                            'scale_level': _guess_scale(r[2])} for r in cfg_rows}

        # Prepare: delete existing rows for batch/subject
        conn.execute(text("DELETE FROM questionnaire_question_scores WHERE BINARY batch_code=BINARY :b AND BINARY subject_name=BINARY :s"), {'b': BATCH, 's': SUBJECT})

        # Iterate students and write per-question rows
        # We rely on subject_scoress JSON in student_score_detail
        rows = conn.execute(text(
            """
            SELECT student_id, subject_scores
            FROM student_score_detail
            WHERE BINARY batch_code=BINARY :b AND BINARY subject_name=BINARY :s
            """
        ), {'b': BATCH, 's': SUBJECT}).fetchall()

        total_inserted = 0
        for sid, subj_json in rows:
            # Insert via SQL using JSON_EXTRACT for each qid to avoid client-side JSON parsing
            # Build an INSERT SELECT over UNION ALL for this student
            vals = []
            params = {'b': BATCH, 's': SUBJECT, 'sid': sid}
            parts = []
            idx = 0
            for qid, meta in qcfg.items():
                idx += 1
                params[f'qid{idx}'] = qid
                params[f'max{idx}'] = meta['max_score']
                params[f'scale{idx}'] = meta['scale_level']
                parts.append(
                    f"SELECT :b AS batch_code, :s AS subject_name, :sid AS student_id, :qid{idx} AS question_id, "
                    f"CAST(JSON_UNQUOTE(JSON_EXTRACT(subject_scores, CONCAT('$." + '"' + f"', :qid{idx}, '" + '"' + "'))) AS DECIMAL(10,2)) AS original_score, "
                    f":max{idx} AS max_score, :scale{idx} AS scale_level"
                )
            sql = " UNION ALL ".join(parts)
            insert_sql = f"INSERT INTO questionnaire_question_scores (batch_code, subject_name, student_id, question_id, original_score, max_score, scale_level) {sql}"
            res = conn.execute(text(insert_sql), params)
            total_inserted += res.rowcount or 0

        # Build option distribution materialized table
        conn.execute(text(
            """
            REPLACE INTO questionnaire_option_distribution (batch_code, subject_name, question_id, option_level, count, updated_at)
            SELECT batch_code, subject_name, question_id,
                   -- derive option_level = round(original/max * scale), bounded [1, scale]
                   GREATEST(1, LEAST(scale_level, ROUND(COALESCE(NULLIF(max_score,0),1),0) * 0 + ROUND(COALESCE(original_score,0)/NULLIF(max_score,0) * scale_level,0))) AS option_level,
                   COUNT(*), NOW()
            FROM questionnaire_question_scores
            WHERE BINARY batch_code=BINARY :b AND BINARY subject_name=BINARY :s
            GROUP BY batch_code, subject_name, question_id, option_level
            """
        ), {'b': BATCH, 's': SUBJECT})

        print(f"Questionnaire cleaning done: inserted={total_inserted}")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())



