#!/usr/bin/env python3
import os, json
from sqlalchemy import create_engine, text

BATCH = os.getenv('BATCH_CODE', 'G4-2025')
URL = os.getenv('DATABASE_URL')

def main():
    if not URL:
        print(json.dumps({'error': 'DATABASE_URL not set'}, ensure_ascii=False))
        return
    engine = create_engine(URL)
    out = {}
    with engine.connect() as conn:
        # Out-of-range
        q1 = text(
            """
            SELECT SUM(CASE WHEN total_score<0 OR total_score>max_score THEN 1 ELSE 0 END) AS out_of_range
            FROM student_cleaned_scores WHERE batch_code=:b
            """
        )
        out['out_of_range'] = int(conn.execute(q1, {'b': BATCH}).scalar() or 0)

        # By subject
        q2 = text(
            """
            SELECT subject_name, COALESCE(subject_type,'') subject_type,
                   COUNT(*) AS n,
                   SUM(CASE WHEN total_score<0 OR total_score>max_score THEN 1 ELSE 0 END) AS out_of_range,
                   MIN(total_score) AS min_s, MAX(total_score) AS max_s, ROUND(AVG(total_score),2) AS avg_s,
                   MAX(max_score) AS max_full
            FROM student_cleaned_scores
            WHERE batch_code=:b
            GROUP BY subject_name, subject_type
            ORDER BY subject_name
            """
        )
        out['by_subject'] = [dict(r._mapping) for r in conn.execute(q2, {'b': BATCH}).fetchall()]

        # Science stats
        q3 = text(
            """
            SELECT MIN(total_score) AS min_s, MAX(total_score) AS max_s, ROUND(AVG(total_score),2) AS avg_s
            FROM student_cleaned_scores WHERE batch_code=:b AND subject_name='科学'
            """
        )
        r3 = conn.execute(q3, {'b': BATCH}).first()
        out['science_stats'] = dict(r3._mapping) if r3 else {}

        # Questionnaire consistency
        try:
            q4 = text(
                """
                SELECT s.subject_name,
                       MAX(s.max_score) AS cleaned_max,
                       SUM(cfg.max_score) AS expected_max
                FROM student_cleaned_scores s
                JOIN subject_question_config cfg
                  ON cfg.batch_code=s.batch_code AND cfg.subject_name=s.subject_name
                 AND cfg.question_type_enum='questionnaire'
                WHERE s.batch_code=:b AND s.subject_type='questionnaire'
                GROUP BY s.subject_name
                """
            )
            out['questionnaire_consistency'] = [dict(r._mapping) for r in conn.execute(q4, {'b': BATCH}).fetchall()]
        except Exception as e:
            # Fallback: compute expected_max per subject without JOIN to avoid collation issues
            q_subjects = text(
                """
                SELECT DISTINCT subject_name, MAX(max_score) as cleaned_max
                FROM student_cleaned_scores
                WHERE batch_code=:b AND subject_type='questionnaire'
                GROUP BY subject_name
                """
            )
            rows = conn.execute(q_subjects, {'b': BATCH}).fetchall()
            results = []
            q_expected = text(
                """
                SELECT SUM(max_score) FROM subject_question_config
                WHERE batch_code=:b AND subject_name=:s AND question_type_enum='questionnaire'
                """
            )
            for r in rows:
                sname = r._mapping['subject_name']
                cleaned_max = float(r._mapping['cleaned_max'] or 0)
                expected = conn.execute(q_expected, {'b': BATCH, 's': sname}).scalar()
                results.append({'subject_name': sname, 'cleaned_max': cleaned_max, 'expected_max': float(expected or 0)})
            out['questionnaire_consistency'] = results

        # Dimension JSON presence
        q5 = text(
            """
            SELECT subject_name,
                   SUM(CASE WHEN dimension_max_scores IS NOT NULL AND dimension_max_scores<>'' AND dimension_max_scores<>'{}' THEN 1 ELSE 0 END) AS has_dim,
                   COUNT(*) AS n
            FROM student_cleaned_scores
            WHERE batch_code=:b
            GROUP BY subject_name
            """
        )
        out['dim_json_presence'] = [dict(r._mapping) for r in conn.execute(q5, {'b': BATCH}).fetchall()]

        # DESCRIBE via INFORMATION_SCHEMA
        qcols = text(
            """
            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME=:t
            ORDER BY ORDINAL_POSITION
            """
        )
        out['describe_student_cleaned_scores'] = [dict(r._mapping) for r in conn.execute(qcols, {'t':'student_cleaned_scores'}).fetchall()]
        out['describe_questionnaire_question_scores'] = [dict(r._mapping) for r in conn.execute(qcols, {'t':'questionnaire_question_scores'}).fetchall()]

        # Sample per subject
        subs = [r[0] for r in conn.execute(text("SELECT DISTINCT subject_name FROM student_cleaned_scores WHERE batch_code=:b"), {'b': BATCH}).fetchall()]
        samples = {}
        for sname in subs:
            qsample = text(
                """
                SELECT student_id, subject_name, subject_type, total_score, max_score,
                       SUBSTRING(COALESCE(dimension_scores,''),1,100) AS dim_scores_prefix
                FROM student_cleaned_scores
                WHERE batch_code=:b AND subject_name=:s
                LIMIT 3
                """
            )
            samples[sname] = [dict(r._mapping) for r in conn.execute(qsample, {'b': BATCH, 's': sname}).fetchall()]
        out['samples'] = samples

    print(json.dumps(out, ensure_ascii=False, default=str))

if __name__ == '__main__':
    main()
